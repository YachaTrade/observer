use crate::measure_postgres;
use anyhow::Result;
use bigdecimal::BigDecimal;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, instrument, warn};

use crate::config::DEFAULT_DELAY;
use crate::db::postgres::PostgresDatabase;
use crate::types::dex::SetFeeProtocol;
use crate::types::fee::FeeHistoryEvent;

/// SQL for inserting a single `set_fee_history` row.
pub const HANDLE_SET_FEE_PROTOCOL_SQL: &str = r#"
    INSERT INTO set_fee_history (
        pool_id,
        block_number,
        transaction_hash,
        tx_index,
        log_index,
        fee_protocol0_old,
        fee_protocol1_old,
        fee_protocol0_new,
        fee_protocol1_new
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (pool_id, block_number, transaction_hash, tx_index, log_index) DO NOTHING
"#;

/// SQL for batch inserting `set_fee_history` rows via UNNEST.
pub const BATCH_INSERT_SET_FEE_PROTOCOLS_SQL: &str = r#"
    INSERT INTO set_fee_history (
        pool_id,
        block_number,
        transaction_hash,
        tx_index,
        log_index,
        fee_protocol0_old,
        fee_protocol1_old,
        fee_protocol0_new,
        fee_protocol1_new
    )
    SELECT * FROM UNNEST(
        $1::text[],
        $2::bigint[],
        $3::text[],
        $4::int[],
        $5::int[],
        $6::smallint[],
        $7::smallint[],
        $8::smallint[],
        $9::smallint[]
    )
    ON CONFLICT (pool_id, block_number, transaction_hash, tx_index, log_index) DO NOTHING
"#;

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

    #[instrument(skip(self, event))]
    pub async fn handle_set_fee_protocol(&self, event: &SetFeeProtocol) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            match measure_postgres!("fee_handle_set_fee_protocol", {
                sqlx::query(HANDLE_SET_FEE_PROTOCOL_SQL)
                .bind(event.pool_id.as_ref().as_str())
                .bind(event.block_number as i64)
                .bind(event.transaction_hash.as_ref().as_str())
                .bind(event.transaction_index as i32)
                .bind(event.log_index as i32)
                .bind(event.fee_protocol0_old as i16)
                .bind(event.fee_protocol1_old as i16)
                .bind(event.fee_protocol0_new as i16)
                .bind(event.fee_protocol1_new as i16)
                .execute(&self.db.pool)
                .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        "[FEE] Failed to handle_set_fee_protocol on attempt {}: {}",
                        attempt, e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[FEE] Deadlock detected in handle_set_fee_protocol, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to handle_set_fee_protocol after {} attempts: {}",
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

    pub async fn batch_insert_set_fee_protocols(&self, events: &[SetFeeProtocol]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        for chunk in events.chunks(1000) {
            self.batch_insert_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_insert_chunk(&self, events: &[SetFeeProtocol]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let query = BATCH_INSERT_SET_FEE_PROTOCOLS_SQL;

            let pool_ids: Vec<&str> = events.iter().map(|e| e.pool_id.as_ref().as_str()).collect();
            let block_numbers: Vec<i64> = events.iter().map(|e| e.block_number as i64).collect();
            let transaction_hashes: Vec<&str> = events
                .iter()
                .map(|e| e.transaction_hash.as_ref().as_str())
                .collect();
            let tx_indices: Vec<i32> = events.iter().map(|e| e.transaction_index as i32).collect();
            let log_indices: Vec<i32> = events.iter().map(|e| e.log_index as i32).collect();
            let fee_protocol0_olds: Vec<i16> =
                events.iter().map(|e| e.fee_protocol0_old as i16).collect();
            let fee_protocol1_olds: Vec<i16> =
                events.iter().map(|e| e.fee_protocol1_old as i16).collect();
            let fee_protocol0_news: Vec<i16> =
                events.iter().map(|e| e.fee_protocol0_new as i16).collect();
            let fee_protocol1_news: Vec<i16> =
                events.iter().map(|e| e.fee_protocol1_new as i16).collect();

            match measure_postgres!("fee_batch_insert_set_fee_protocols", {
                sqlx::query(query)
                    .bind(&pool_ids)
                    .bind(&block_numbers)
                    .bind(&transaction_hashes)
                    .bind(&tx_indices)
                    .bind(&log_indices)
                    .bind(&fee_protocol0_olds)
                    .bind(&fee_protocol1_olds)
                    .bind(&fee_protocol0_news)
                    .bind(&fee_protocol1_news)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        "[FEE] Failed to batch insert {} set_fee_protocols on attempt {}: {}",
                        events.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[FEE] Deadlock detected in batch_insert_set_fee_protocols, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert set_fee_protocols after {} attempts: {}",
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

    /// Batch insert fee_history
    pub async fn batch_insert_fee_history(&self, events: &[FeeHistoryEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        for chunk in events.chunks(1000) {
            self.batch_insert_fee_history_chunk(chunk).await?;
        }

        info!(
            "[FEE] Batch inserted {} fee_history records",
            events.len()
        );
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

            let transaction_hashes: Vec<&str> = events
                .iter()
                .map(|e| e.transaction_hash.as_str())
                .collect();
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
