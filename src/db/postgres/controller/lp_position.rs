use std::sync::Arc;

use anyhow::Result;
use bigdecimal::BigDecimal;
use tracing::error;

use crate::db::postgres::PostgresDatabase;
pub use crate::types::token::LpPositionHistoryEvent;

/// SQL for `LpPositionController::batch_insert`.
///
/// Fires `trg_lp_position_on_history` (BEFORE INSERT) which fills cost basis
/// from `dex_mint`/`dex_burn` (for mint/burn) or from the sender's avg cost
/// basis (for transfer_in/out), then UPSERTs into `lp_position`, then DELETEs
/// the row if `lp_in == lp_out` (full burn / full transfer out).
///
/// `counterparty` is bound as `Vec<Option<&str>>`; sqlx maps it to a PostgreSQL
/// `varchar[]` with proper NULL semantics for the `NULL` slots.
///
/// `ON CONFLICT DO NOTHING` keeps backfill / restart idempotent — the same
/// (account, pool, tx, tx_idx, log_idx) row will not be double-applied.
///
/// Do not modify without updating the trigger in `migrations/0021_lp_position.sql`
/// (and the idempotent `migrations/v2_upgrade_lp_position.sql`).
pub const BATCH_INSERT_LP_POSITION_HISTORY_SQL: &str = r#"
    INSERT INTO lp_position_history (
        account_id, pool_id,
        lp_in, lp_out,
        event_type, counterparty,
        transaction_hash, block_number, tx_index, log_index, created_at
    )
    SELECT
        account_id, pool_id,
        lp_in, lp_out,
        event_type::lp_event_type, counterparty,
        transaction_hash, block_number, tx_index, log_index, created_at
    FROM UNNEST(
        $1::varchar(42)[], $2::varchar(42)[],
        $3::numeric[],     $4::numeric[],
        $5::text[],        $6::varchar(42)[],
        $7::text[],        $8::bigint[],   $9::int[], $10::int[], $11::bigint[]
    ) AS t(account_id, pool_id, lp_in, lp_out, event_type, counterparty, transaction_hash, block_number, tx_index, log_index, created_at)
    ON CONFLICT (account_id, pool_id, transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub struct LpPositionController {
    db: Arc<PostgresDatabase>,
}

impl LpPositionController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        Self { db }
    }

    /// Batch insert LP position history rows. Empty input is a no-op.
    ///
    /// Errors are logged and propagated; the caller decides whether to retry.
    pub async fn batch_insert(&self, items: &[LpPositionHistoryEvent]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let accounts: Vec<&str> = items.iter().map(|x| x.account_id.as_str()).collect();
        let pools: Vec<&str> = items.iter().map(|x| x.pool_id.as_str()).collect();
        let lp_ins: Vec<BigDecimal> = items.iter().map(|x| (*x.lp_in).clone()).collect();
        let lp_outs: Vec<BigDecimal> = items.iter().map(|x| (*x.lp_out).clone()).collect();
        let event_types: Vec<&str> = items.iter().map(|x| x.event_type).collect();
        let counters: Vec<Option<&str>> = items
            .iter()
            .map(|x| x.counterparty.as_deref().map(String::as_str))
            .collect();
        let tx_hashes: Vec<&str> = items
            .iter()
            .map(|x| x.transaction_hash.as_str())
            .collect();
        let blocks: Vec<i64> = items.iter().map(|x| x.block_number as i64).collect();
        let tx_idxs: Vec<i32> = items.iter().map(|x| x.transaction_index as i32).collect();
        let log_idxs: Vec<i32> = items.iter().map(|x| x.log_index as i32).collect();
        let timestamps: Vec<i64> = items.iter().map(|x| x.block_timestamp as i64).collect();

        sqlx::query(BATCH_INSERT_LP_POSITION_HISTORY_SQL)
            .bind(&accounts)
            .bind(&pools)
            .bind(&lp_ins)
            .bind(&lp_outs)
            .bind(&event_types)
            .bind(&counters)
            .bind(&tx_hashes)
            .bind(&blocks)
            .bind(&tx_idxs)
            .bind(&log_idxs)
            .bind(&timestamps)
            .execute(&self.db.pool)
            .await
            .map_err(|e| {
                error!("[LP_TOKEN] batch insert failed: {}", e);
                anyhow::anyhow!(e)
            })?;
        Ok(())
    }
}
