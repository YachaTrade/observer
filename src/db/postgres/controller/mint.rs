use crate::measure_postgres;
use anyhow::Result;
use bigdecimal::BigDecimal;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, instrument, warn};

use crate::config::DEFAULT_DELAY;
use crate::db::postgres::PostgresDatabase;

/// SQL for batch inserting mint rows via UNNEST.
pub const BATCH_INSERT_MINTS_SQL: &str = r#"
                INSERT INTO mint (
                    token_id, account_id, market_id, quote_amount, token_amount,
                    reserve_quote, reserve_token, created_at, transaction_hash,
                    block_number, tx_index, log_index
                )
                SELECT
                    token_id, account_id, market_id, quote_amount, token_amount,
                    reserve_quote, reserve_token, created_at, transaction_hash,
                    block_number, tx_index, log_index
                FROM UNNEST(
                    $1::text[], $2::text[], $3::text[],
                    $4::numeric[], $5::numeric[], $6::numeric[], $7::numeric[],
                    $8::bigint[], $9::text[], $10::bigint[], $11::int[], $12::int[]
                ) AS t(token_id, account_id, market_id, quote_amount, token_amount, reserve_quote, reserve_token, created_at, transaction_hash, block_number, tx_index, log_index)
                ON CONFLICT (token_id, transaction_hash, tx_index, log_index) DO NOTHING
                "#;

/// SQL for batch inserting burn rows (from MintController) via UNNEST.
pub const BATCH_INSERT_BURNS_SQL: &str = r#"
                INSERT INTO burn (
                    token_id, account_id, market_id, quote_amount, token_amount,
                    reserve_quote, reserve_token, created_at, transaction_hash,
                    block_number, tx_index, log_index
                )
                SELECT
                    token_id, account_id, market_id, quote_amount, token_amount,
                    reserve_quote, reserve_token, created_at, transaction_hash,
                    block_number, tx_index, log_index
                FROM UNNEST(
                    $1::text[], $2::text[], $3::text[],
                    $4::numeric[], $5::numeric[], $6::numeric[], $7::numeric[],
                    $8::bigint[], $9::text[], $10::bigint[], $11::int[], $12::int[]
                ) AS t(token_id, account_id, market_id, quote_amount, token_amount, reserve_quote, reserve_token, created_at, transaction_hash, block_number, tx_index, log_index)
                ON CONFLICT (token_id, transaction_hash, tx_index, log_index) DO NOTHING
                "#;

// Batch insert용 데이터 구조
pub struct MintBatchData {
    pub token_id: Arc<String>,
    pub account_id: Arc<String>,
    pub market_id: Arc<String>,
    pub quote_amount: Arc<BigDecimal>,
    pub token_amount: Arc<BigDecimal>,
    pub reserve_quote: Arc<BigDecimal>,
    pub reserve_token: Arc<BigDecimal>,
    pub created_at: i64,
    pub transaction_hash: Arc<String>,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
}

pub struct BurnBatchData {
    pub token_id: Arc<String>,
    pub account_id: Arc<String>,
    pub market_id: Arc<String>,
    pub quote_amount: Arc<BigDecimal>,
    pub token_amount: Arc<BigDecimal>,
    pub reserve_quote: Arc<BigDecimal>,
    pub reserve_token: Arc<BigDecimal>,
    pub created_at: i64,
    pub transaction_hash: Arc<String>,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
}

pub struct MintController {
    pub db: Arc<PostgresDatabase>,
}

impl MintController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        MintController { db }
    }

    // Batch insert mints
    #[instrument(skip(self, mints))]
    pub async fn batch_insert_mints(&self, mints: &[MintBatchData]) -> Result<()> {
        if mints.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in mints.chunks(1000) {
            self.batch_insert_mints_chunk(chunk).await?;
        }

        Ok(())
    }

    // Chunk 단위로 insert하는 내부 함수
    async fn batch_insert_mints_chunk(&self, mints: &[MintBatchData]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let query = BATCH_INSERT_MINTS_SQL;

            // Collect arrays
            let token_ids: Vec<&str> = mints.iter().map(|m| m.token_id.as_str()).collect();
            let account_ids: Vec<&str> = mints.iter().map(|m| m.account_id.as_str()).collect();
            let market_ids: Vec<&str> = mints.iter().map(|m| m.market_id.as_str()).collect();
            let quote_amounts: Vec<&BigDecimal> =
                mints.iter().map(|m| m.quote_amount.as_ref()).collect();
            let token_amounts: Vec<&BigDecimal> =
                mints.iter().map(|m| m.token_amount.as_ref()).collect();
            let reserve_quotes: Vec<&BigDecimal> =
                mints.iter().map(|m| m.reserve_quote.as_ref()).collect();
            let reserve_tokens: Vec<&BigDecimal> =
                mints.iter().map(|m| m.reserve_token.as_ref()).collect();
            let created_ats: Vec<i64> = mints.iter().map(|m| m.created_at).collect();
            let transaction_hashes: Vec<&str> =
                mints.iter().map(|m| m.transaction_hash.as_str()).collect();
            let block_numbers: Vec<i64> = mints.iter().map(|m| m.block_number).collect();
            let tx_indexes: Vec<i32> = mints.iter().map(|m| m.tx_index).collect();
            let log_indexes: Vec<i32> = mints.iter().map(|m| m.log_index).collect();

            match measure_postgres!("mint_batch_insert", {
                sqlx::query(query)
                    .bind(&token_ids)
                    .bind(&account_ids)
                    .bind(&market_ids)
                    .bind(&quote_amounts)
                    .bind(&token_amounts)
                    .bind(&reserve_quotes)
                    .bind(&reserve_tokens)
                    .bind(&created_ats)
                    .bind(&transaction_hashes)
                    .bind(&block_numbers)
                    .bind(&tx_indexes)
                    .bind(&log_indexes)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    info!("[MINT] Batch inserted {} mints successfully", mints.len());
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[MINT] Failed to batch insert {} mints on attempt {}: {}",
                        mints.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[MINT] Deadlock detected in batch_insert_mints, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert mints after {} attempts: {}",
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

    // Batch insert burns
    #[instrument(skip(self, burns))]
    pub async fn batch_insert_burns(&self, burns: &[BurnBatchData]) -> Result<()> {
        if burns.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in burns.chunks(1000) {
            self.batch_insert_burns_chunk(chunk).await?;
        }

        Ok(())
    }

    // Chunk 단위로 insert하는 내부 함수
    async fn batch_insert_burns_chunk(&self, burns: &[BurnBatchData]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let query = BATCH_INSERT_BURNS_SQL;

            // Collect arrays
            let token_ids: Vec<&str> = burns.iter().map(|b| b.token_id.as_str()).collect();
            let account_ids: Vec<&str> = burns.iter().map(|b| b.account_id.as_str()).collect();
            let market_ids: Vec<&str> = burns.iter().map(|b| b.market_id.as_str()).collect();
            let quote_amounts: Vec<&BigDecimal> =
                burns.iter().map(|b| b.quote_amount.as_ref()).collect();
            let token_amounts: Vec<&BigDecimal> =
                burns.iter().map(|b| b.token_amount.as_ref()).collect();
            let reserve_quotes: Vec<&BigDecimal> =
                burns.iter().map(|b| b.reserve_quote.as_ref()).collect();
            let reserve_tokens: Vec<&BigDecimal> =
                burns.iter().map(|b| b.reserve_token.as_ref()).collect();
            let created_ats: Vec<i64> = burns.iter().map(|b| b.created_at).collect();
            let transaction_hashes: Vec<&str> =
                burns.iter().map(|b| b.transaction_hash.as_str()).collect();
            let block_numbers: Vec<i64> = burns.iter().map(|b| b.block_number).collect();
            let tx_indexes: Vec<i32> = burns.iter().map(|b| b.tx_index).collect();
            let log_indexes: Vec<i32> = burns.iter().map(|b| b.log_index).collect();

            match measure_postgres!("burn_batch_insert", {
                sqlx::query(query)
                    .bind(&token_ids)
                    .bind(&account_ids)
                    .bind(&market_ids)
                    .bind(&quote_amounts)
                    .bind(&token_amounts)
                    .bind(&reserve_quotes)
                    .bind(&reserve_tokens)
                    .bind(&created_ats)
                    .bind(&transaction_hashes)
                    .bind(&block_numbers)
                    .bind(&tx_indexes)
                    .bind(&log_indexes)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    info!("[BURN] Batch inserted {} burns successfully", burns.len());
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[BURN] Failed to batch insert {} burns on attempt {}: {}",
                        burns.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[BURN] Deadlock detected in batch_insert_burns, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert burns after {} attempts: {}",
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
