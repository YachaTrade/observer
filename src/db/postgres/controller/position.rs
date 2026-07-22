use crate::measure_postgres;
use anyhow::Result;
use bigdecimal::BigDecimal;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::DEFAULT_DELAY;
use crate::db::postgres::PostgresDatabase;
use crate::types::token::{PositionHistoryEvent, TransferType};

/// SQL for `PositionController::batch_insert_position_history`, exposed as
/// a pub const so integration tests can exercise the exact statement the
/// production code runs. The INSERT fires `trg_position_on_history`
/// (BEFORE INSERT) which aggregates into the `position` table; the RETURNING
/// clause lets the caller see which rows actually landed (duplicates
/// swallowed by ON CONFLICT are absent from the result).
/// Do not modify without updating the tests in `tests/group_a_controllers.rs`.
pub const BATCH_INSERT_POSITION_HISTORY_SQL: &str = r#"
                INSERT INTO position_history (
                    account_id,
                    token_id,
                    quote_in,
                    quote_out,
                    usd_in,
                    usd_out,
                    token_in,
                    token_out,
                    transaction_hash,
                    block_number,
                    tx_index,
                    log_index,
                    created_at,
                    transfer_type,
                    sender_address
                )
                SELECT
                    account_id,
                    token_id,
                    quote_in,
                    quote_out,
                    usd_in,
                    usd_out,
                    token_in,
                    token_out,
                    transaction_hash,
                    block_number,
                    tx_index,
                    log_index,
                    created_at,
                    transfer_type,
                    sender_address
                FROM UNNEST(
                    $1::text[],     -- account_ids
                    $2::text[],     -- token_ids
                    $3::numeric[],  -- quote_ins
                    $4::numeric[],  -- quote_outs
                    $5::numeric[],  -- usd_ins
                    $6::numeric[],  -- usd_outs
                    $7::numeric[],  -- token_ins
                    $8::numeric[],  -- token_outs
                    $9::text[],     -- transaction_hashes
                    $10::bigint[],  -- block_numbers
                    $11::int[],     -- tx_indices
                    $12::int[],     -- log_indices
                    $13::bigint[],  -- created_ats
                    $14::text[],    -- transfer_types
                    $15::text[]     -- counterparties
                ) AS t(account_id, token_id, quote_in, quote_out, usd_in, usd_out, token_in, token_out, transaction_hash, block_number, tx_index, log_index, created_at, transfer_type, sender_address)
                ON CONFLICT (account_id, token_id, transaction_hash, tx_index, log_index) DO NOTHING
                RETURNING account_id, token_id, quote_in, quote_out, usd_in, usd_out, token_in, token_out, transaction_hash, block_number, tx_index, log_index, created_at, transfer_type, sender_address
            "#;

pub struct PositionController {
    pub db: Arc<PostgresDatabase>,
}

impl PositionController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        PositionController { db }
    }

    /// Batch insert position_history with RETURNING
    /// 실제로 insert된 row만 반환 (중복은 제외)
    pub async fn batch_insert_position_history(
        &self,
        histories: &[PositionHistoryEvent],
    ) -> Result<Vec<PositionHistoryEvent>> {
        if histories.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_inserted = Vec::new();

        // 1000개씩 chunk로 나눠서 처리
        for chunk in histories.chunks(1000) {
            let inserted = self.batch_insert_position_history_chunk(chunk).await?;
            all_inserted.extend(inserted);
        }

        Ok(all_inserted)
    }

    async fn batch_insert_position_history_chunk(
        &self,
        histories: &[PositionHistoryEvent],
    ) -> Result<Vec<PositionHistoryEvent>> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            // RETURNING으로 실제 insert된 row만 반환
            let query = BATCH_INSERT_POSITION_HISTORY_SQL;

            let account_ids: Vec<&str> = histories.iter().map(|h| h.account_id.as_str()).collect();
            let token_ids: Vec<&str> = histories.iter().map(|h| h.token_id.as_str()).collect();
            let quote_ins: Vec<&BigDecimal> =
                histories.iter().map(|h| h.quote_in.as_ref()).collect();
            let quote_outs: Vec<&BigDecimal> =
                histories.iter().map(|h| h.quote_out.as_ref()).collect();
            let usd_ins: Vec<&BigDecimal> = histories.iter().map(|h| h.usd_in.as_ref()).collect();
            let usd_outs: Vec<&BigDecimal> = histories.iter().map(|h| h.usd_out.as_ref()).collect();
            let token_ins: Vec<&BigDecimal> =
                histories.iter().map(|h| h.token_in.as_ref()).collect();
            let token_outs: Vec<&BigDecimal> =
                histories.iter().map(|h| h.token_out.as_ref()).collect();
            let transaction_hashes: Vec<&str> = histories
                .iter()
                .map(|h| h.transaction_hash.as_str())
                .collect();
            let block_numbers: Vec<i64> = histories.iter().map(|h| h.block_number as i64).collect();
            let tx_indices: Vec<i32> = histories.iter().map(|h| h.tx_index as i32).collect();
            let log_indices: Vec<i32> = histories.iter().map(|h| h.log_index as i32).collect();
            let created_ats: Vec<i64> =
                histories.iter().map(|h| h.block_timestamp as i64).collect();
            let transfer_types: Vec<&str> =
                histories.iter().map(|h| h.transfer_type.as_str()).collect();
            let counterparties: Vec<Option<&str>> = histories
                .iter()
                .map(|h| h.sender_address.as_ref().map(|c| c.as_str()))
                .collect();

            match measure_postgres!("position_history_batch_insert", {
                sqlx::query_as::<
                    _,
                    (
                        String,
                        String,
                        BigDecimal,
                        BigDecimal,
                        BigDecimal,
                        BigDecimal,
                        BigDecimal,
                        BigDecimal,
                        String,
                        i64,
                        i32,
                        i32,
                        i64,
                        Option<String>,
                        Option<String>,
                    ),
                >(query)
                .bind(&account_ids)
                .bind(&token_ids)
                .bind(&quote_ins)
                .bind(&quote_outs)
                .bind(&usd_ins)
                .bind(&usd_outs)
                .bind(&token_ins)
                .bind(&token_outs)
                .bind(&transaction_hashes)
                .bind(&block_numbers)
                .bind(&tx_indices)
                .bind(&log_indices)
                .bind(&created_ats)
                .bind(&transfer_types)
                .bind(&counterparties)
                .fetch_all(&self.db.pool)
                .await
            }) {
                Ok(rows) => {
                    let inserted: Vec<PositionHistoryEvent> = rows
                        .into_iter()
                        .map(
                            |(
                                account_id,
                                token_id,
                                quote_in,
                                quote_out,
                                usd_in,
                                usd_out,
                                token_in,
                                token_out,
                                transaction_hash,
                                block_number,
                                tx_index,
                                log_index,
                                created_at,
                                transfer_type,
                                sender_address,
                            )| {
                                PositionHistoryEvent {
                                    account_id: Arc::new(account_id),
                                    token_id: Arc::new(token_id),
                                    quote_in: Arc::new(quote_in),
                                    quote_out: Arc::new(quote_out),
                                    usd_in: Arc::new(usd_in),
                                    usd_out: Arc::new(usd_out),
                                    token_in: Arc::new(token_in),
                                    token_out: Arc::new(token_out),
                                    transaction_hash: Arc::new(transaction_hash),
                                    block_number: block_number as u64,
                                    block_timestamp: created_at as u64,
                                    tx_index: tx_index as u64,
                                    log_index: log_index as u64,
                                    transfer_type: TransferType::from_db_value(
                                        transfer_type.as_deref().unwrap_or("other"),
                                    ),
                                    sender_address: sender_address.map(Arc::new),
                                }
                            },
                        )
                        .collect();

                    info!(
                        "[POSITION] Batch inserted {}/{} position_history successfully",
                        inserted.len(),
                        histories.len()
                    );
                    return Ok(inserted);
                }
                Err(e) => {
                    warn!(
                        "[POSITION] Failed to batch insert {} position_history on attempt {}: {}",
                        histories.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[POSITION] Deadlock detected, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert position_history after {} attempts: {}",
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
