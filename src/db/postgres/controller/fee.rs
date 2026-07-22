use crate::measure_postgres;
use anyhow::Result;
use bigdecimal::BigDecimal;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::DEFAULT_DELAY;
use crate::db::postgres::PostgresDatabase;
use crate::types::fee::FeeHistoryEvent;

/// SQL for batch inserting `fee_history` rows via UNNEST.
pub const BATCH_INSERT_FEE_HISTORY_SQL: &str = r#"
    INSERT INTO fee_history (
        transaction_hash,
        tx_index,
        log_index,
        account_id,
        token_id,
        quote_amount,
        usd_amount,
        fee_type,
        block_number,
        created_at
    )
    SELECT * FROM UNNEST(
        $1::text[],
        $2::int[],
        $3::int[],
        $4::text[],
        $5::text[],
        $6::numeric[],
        $7::numeric[],
        $8::text[],
        $9::bigint[],
        $10::bigint[]
    )
    ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub struct FeeController {
    pub db: Arc<PostgresDatabase>,
}

impl FeeController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        FeeController { db }
    }

    /// Batch insert fee_history
    pub async fn batch_insert_fee_history(&self, events: &[FeeHistoryEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        for chunk in events.chunks(1000) {
            self.batch_insert_fee_history_chunk(chunk).await?;
        }

        info!("[FEE] Batch inserted {} fee_history records", events.len());
        Ok(())
    }

    async fn batch_insert_fee_history_chunk(&self, events: &[FeeHistoryEvent]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let query = BATCH_INSERT_FEE_HISTORY_SQL;

            let transaction_hashes: Vec<&str> =
                events.iter().map(|e| e.transaction_hash.as_str()).collect();
            let tx_indices: Vec<i32> = events.iter().map(|e| e.tx_index as i32).collect();
            let log_indices: Vec<i32> = events.iter().map(|e| e.log_index as i32).collect();
            let account_ids: Vec<&str> = events.iter().map(|e| e.account_id.as_str()).collect();
            let token_ids: Vec<&str> = events.iter().map(|e| e.token_id.as_str()).collect();
            let quote_amounts: Vec<&BigDecimal> =
                events.iter().map(|e| e.quote_amount.as_ref()).collect();
            let usd_amounts: Vec<&BigDecimal> =
                events.iter().map(|e| e.usd_amount.as_ref()).collect();
            let fee_types: Vec<&str> = events.iter().map(|e| e.fee_type.as_str()).collect();
            let block_numbers: Vec<i64> = events.iter().map(|e| e.block_number as i64).collect();
            let created_ats: Vec<i64> = events.iter().map(|e| e.block_timestamp as i64).collect();

            match measure_postgres!("fee_batch_insert_fee_history", {
                sqlx::query(query)
                    .bind(&transaction_hashes)
                    .bind(&tx_indices)
                    .bind(&log_indices)
                    .bind(&account_ids)
                    .bind(&token_ids)
                    .bind(&quote_amounts)
                    .bind(&usd_amounts)
                    .bind(&fee_types)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        "[FEE] Failed to batch insert {} fee_history on attempt {}: {}",
                        events.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[FEE] Deadlock detected, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert fee_history after {} attempts: {}",
                            attempt,
                            e
                        ));
                    }
                    sleep(current_delay).await;
                    continue;
                }
            }
        }
    }
}
