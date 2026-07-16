use std::{sync::Arc, time::Duration};

use crate::{db::postgres::PostgresDatabase, measure_postgres};

use anyhow::{Result, anyhow};
use bigdecimal::BigDecimal;
use tokio::time::sleep;
use tracing::{error, warn};

/// SQL for batch inserting pool rows via UNNEST.
pub const BATCH_INSERT_POOLS_SQL: &str = r#"
                    INSERT INTO pool (pool_id, token0, token1, reserve0, reserve1, price, created_at, block_number, tx_hash)
                    SELECT * FROM UNNEST(
                        $1::varchar(42)[], $2::varchar(42)[], $3::varchar(42)[],
                        $4::numeric[], $5::numeric[], $6::numeric[],
                        $7::bigint[], $8::bigint[], $9::text[]
                    )
                    ON CONFLICT (pool_id) DO NOTHING
                    "#;

/// SQL for batch updating pool reserves via UNNEST.
///
/// Updates reserve0/1, price, value (TVL), and latest_trade_at in one
/// statement. The value (= pool TVL in USD) is computed receive-side from
/// the same Sync event that produced the new reserves, so the application
/// is the single source of truth for the snapshot.
///
/// Three guards stack to keep pool state consistent under out-of-order or
/// duplicated Syncs:
///   1. `DISTINCT ON (pool_id) ... ORDER BY block_number DESC, tx_index DESC,
///      log_index DESC` collapses multiple same-batch Sync rows for the same
///      pool down to the on-chain-newest row, deterministic even when several
///      syncs share a block_timestamp (every sync in the same block does).
///   2. The outer `latest_trade_at <= d.block_timestamp` predicate rejects
///      any batch row older than what's already persisted (out-of-order
///      replay, reconnect, parallel batch). PR #209 N2 regression guard.
///   3. `GREATEST(pool.latest_trade_at, d.block_timestamp)` keeps
///      latest_trade_at monotonic across reorder/replay edge cases that
///      slipped through guard #2 by tying on block_timestamp.
pub const BATCH_UPDATE_POOL_RESERVES_SQL: &str = r#"
                    UPDATE pool SET
                        reserve0 = d.reserve0,
                        reserve1 = d.reserve1,
                        price = d.price,
                        -- value (TVL) carried as nullable: NULL means
                        -- "don't touch pool.value" (graduated Sync arm has no
                        -- inference run), any non-NULL — including 0 — is a
                        -- real measurement (drained pool, full orphan, etc.)
                        -- and overwrites. This avoids the 0-as-sentinel
                        -- ambiguity that would lose real zero updates.
                        value = COALESCE(d.value, pool.value),
                        -- token0/1_price_usd: same nullable-overwrite policy as
                        -- value. NULL = leave untouched (graduated Sync arm or
                        -- orphan side with no WMON-implied price); non-NULL
                        -- overwrites with the fresh per-token USD unit price.
                        token0_price_usd = COALESCE(d.token0_price_usd, pool.token0_price_usd),
                        token1_price_usd = COALESCE(d.token1_price_usd, pool.token1_price_usd),
                        latest_trade_at = GREATEST(pool.latest_trade_at, d.block_timestamp)
                    FROM (
                        SELECT DISTINCT ON (pool_id)
                               pool_id, reserve0, reserve1, price, value, token0_price_usd, token1_price_usd, block_timestamp
                        FROM UNNEST(
                            $1::varchar(42)[], $2::numeric[], $3::numeric[], $4::numeric[], $5::numeric[],
                            $6::numeric[], $7::numeric[],
                            $8::bigint[], $9::bigint[], $10::int[], $11::int[]
                        ) AS t(pool_id, reserve0, reserve1, price, value, token0_price_usd, token1_price_usd, block_timestamp, block_number, tx_index, log_index)
                        ORDER BY pool_id, block_number DESC, tx_index DESC, log_index DESC
                    ) d
                    WHERE pool.pool_id = d.pool_id
                      AND pool.latest_trade_at <= d.block_timestamp
                    "#;

pub struct PoolData {
    pub pool_id: String,
    pub token0: String,
    pub token1: String,
    pub reserve0: BigDecimal,
    pub reserve1: BigDecimal,
    pub price: BigDecimal,
    pub created_at: i64,
    pub block_number: i64,
    pub tx_hash: String,
}

pub struct PoolSyncData {
    pub pool_id: String,
    pub reserve0: BigDecimal,
    pub reserve1: BigDecimal,
    pub price: BigDecimal,
    /// TVL update intent. `None` = leave pool.value untouched (graduated Sync
    /// arm). `Some(v)` = overwrite with v, including v = 0 for drained pools
    /// or full orphan rows.
    pub value: Option<BigDecimal>,
    /// Per-token USD unit price. `None` = leave pool.token{0,1}_price_usd
    /// untouched (graduated Sync arm, or the orphan side with no WMON-implied
    /// price). `Some(p)` = overwrite. token0/token1 are resolved independently
    /// so a partial-orphan pool fills the known side and leaves the other NULL.
    pub token0_price_usd: Option<BigDecimal>,
    pub token1_price_usd: Option<BigDecimal>,
    pub block_timestamp: i64,
    /// On-chain freshness tuple: rows in a batch are ordered by
    /// (block_number, tx_index, log_index) DESC so the row that won on-chain
    /// also wins in the UPDATE, deterministic even when multiple syncs share
    /// a block_timestamp.
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
}

pub struct PoolController {
    pub db: Arc<PostgresDatabase>,
}

impl PoolController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        PoolController { db }
    }

    pub async fn batch_insert_pools(&self, data: &[PoolData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let pool_ids: Vec<&str> = data.iter().map(|d| d.pool_id.as_str()).collect();
        let token0s: Vec<&str> = data.iter().map(|d| d.token0.as_str()).collect();
        let token1s: Vec<&str> = data.iter().map(|d| d.token1.as_str()).collect();
        let reserve0s: Vec<&BigDecimal> = data.iter().map(|d| &d.reserve0).collect();
        let reserve1s: Vec<&BigDecimal> = data.iter().map(|d| &d.reserve1).collect();
        let prices: Vec<&BigDecimal> = data.iter().map(|d| &d.price).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.tx_hash.as_str()).collect();

        let max_attempts = 10;
        let mut attempt = 0;

        loop {
            attempt += 1;
            match measure_postgres!("pool_batch_insert", {
                sqlx::query(BATCH_INSERT_POOLS_SQL)
                .bind(&pool_ids).bind(&token0s).bind(&token1s)
                .bind(&reserve0s).bind(&reserve1s).bind(&prices)
                .bind(&created_ats).bind(&block_numbers).bind(&tx_hashes)
                .execute(&self.db.pool)
                .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt >= max_attempts {
                        error!("[POOL] Failed to batch insert {} pools after {} attempts: {}", data.len(), attempt, e);
                        return Err(anyhow!("Failed to batch insert pools: {}", e));
                    }
                    let delay = Duration::from_millis(100).mul_f32(1.5_f32.powi(attempt - 1));
                    warn!("[POOL] Insert retry {}/{}: {}", attempt, max_attempts, e);
                    sleep(delay).await;
                }
            }
        }
    }

    pub async fn batch_update_pool_reserves(&self, data: &[PoolSyncData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let pool_ids: Vec<&str> = data.iter().map(|d| d.pool_id.as_str()).collect();
        let reserve0s: Vec<&BigDecimal> = data.iter().map(|d| &d.reserve0).collect();
        let reserve1s: Vec<&BigDecimal> = data.iter().map(|d| &d.reserve1).collect();
        let prices: Vec<&BigDecimal> = data.iter().map(|d| &d.price).collect();
        let values: Vec<Option<BigDecimal>> = data.iter().map(|d| d.value.clone()).collect();
        let token0_price_usds: Vec<Option<BigDecimal>> =
            data.iter().map(|d| d.token0_price_usd.clone()).collect();
        let token1_price_usds: Vec<Option<BigDecimal>> =
            data.iter().map(|d| d.token1_price_usd.clone()).collect();
        let timestamps: Vec<i64> = data.iter().map(|d| d.block_timestamp).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let tx_indexes: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let log_indexes: Vec<i32> = data.iter().map(|d| d.log_index).collect();

        let max_attempts = 10;
        let mut attempt = 0;

        loop {
            attempt += 1;
            match measure_postgres!("pool_batch_update_reserves", {
                sqlx::query(BATCH_UPDATE_POOL_RESERVES_SQL)
                .bind(&pool_ids).bind(&reserve0s).bind(&reserve1s).bind(&prices).bind(&values)
                .bind(&token0_price_usds).bind(&token1_price_usds)
                .bind(&timestamps).bind(&block_numbers).bind(&tx_indexes).bind(&log_indexes)
                .execute(&self.db.pool)
                .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt >= max_attempts {
                        error!("[POOL] Failed to batch update {} pool reserves after {} attempts: {}", data.len(), attempt, e);
                        return Err(anyhow!("Failed to batch update pool reserves: {}", e));
                    }
                    let delay = Duration::from_millis(100).mul_f32(1.5_f32.powi(attempt - 1));
                    warn!("[POOL] Update retry {}/{}: {}", attempt, max_attempts, e);
                    sleep(delay).await;
                }
            }
        }
    }
}
