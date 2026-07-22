use anyhow::{Result, anyhow};
use bigdecimal::BigDecimal;
use std::{sync::Arc, time::Duration};

use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::{config::DEFAULT_DELAY, measure_postgres};

use crate::db::postgres::PostgresDatabase;

/// SQL for single price_history INSERT.
pub const INSERT_PRICE_HISTORY_SQL: &str = r#"
    INSERT INTO price_history (
        token_id,
        price,
        volume,
        created_at,
        block_number,
        transaction_hash,
        log_index,
        tx_index
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (token_id, block_number, transaction_hash, tx_index, log_index) DO NOTHING
"#;

/// SQL for batch price_history INSERT via UNNEST.
pub const BATCH_INSERT_PRICE_HISTORY_SQL: &str = r#"
    INSERT INTO price_history (
        token_id,
        price,
        volume,
        created_at,
        block_number,
        transaction_hash,
        log_index,
        tx_index
    )
    SELECT
        token_id,
        price,
        volume,
        created_at,
        block_number,
        transaction_hash,
        log_index,
        tx_index
    FROM UNNEST(
        $1::text[],     -- token_ids
        $2::numeric[],  -- prices
        $3::numeric[],  -- volumes
        $4::bigint[],   -- created_ats
        $5::bigint[],   -- block_numbers
        $6::text[],     -- transaction_hashes
        $7::int[],      -- log_indexes
        $8::int[]       -- tx_indexes
    ) AS t(token_id, price, volume, created_at, block_number, transaction_hash, log_index, tx_index)
    ON CONFLICT (token_id, block_number, transaction_hash, tx_index, log_index) DO NOTHING
"#;

// Batch insert용 데이터 구조
pub struct ChartBatchData {
    pub token_id: Arc<String>,
    pub price: BigDecimal,
    pub volume: BigDecimal,
    pub block_timestamp: i64,
    pub block_number: i64,
    pub transaction_hash: Arc<String>,
    pub log_index: i32,
    pub tx_index: i32,
}

pub struct ChartController {
    pub db: Arc<PostgresDatabase>,
}

impl ChartController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        ChartController { db }
    }

    // Batch insert 메서드
    pub async fn batch_insert_price_history(&self, charts: &[ChartBatchData]) -> Result<()> {
        if charts.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in charts.chunks(1000) {
            self.batch_insert_price_history_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_insert_price_history_chunk(&self, charts: &[ChartBatchData]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;

            // Collect arrays
            use bigdecimal::BigDecimal;
            let token_ids: Vec<&str> = charts.iter().map(|c| c.token_id.as_str()).collect();
            let prices: Vec<&BigDecimal> = charts.iter().map(|c| &c.price).collect();
            let volumes: Vec<&BigDecimal> = charts.iter().map(|c| &c.volume).collect();
            let created_ats: Vec<i64> = charts.iter().map(|c| c.block_timestamp).collect();
            let block_numbers: Vec<i64> = charts.iter().map(|c| c.block_number).collect();
            let transaction_hashes: Vec<&str> =
                charts.iter().map(|c| c.transaction_hash.as_str()).collect();
            let log_indexes: Vec<i32> = charts.iter().map(|c| c.log_index).collect();
            let tx_indexes: Vec<i32> = charts.iter().map(|c| c.tx_index).collect();

            match measure_postgres!("chart_batch_insert", {
                sqlx::query(BATCH_INSERT_PRICE_HISTORY_SQL)
                    .bind(&token_ids)
                    .bind(&prices)
                    .bind(&volumes)
                    .bind(&created_ats)
                    .bind(&block_numbers)
                    .bind(&transaction_hashes)
                    .bind(&log_indexes)
                    .bind(&tx_indexes)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    info!(
                        "[CHART] Batch inserted {} price histories successfully",
                        charts.len()
                    );
                    return Ok(());
                }
                Err(err) => {
                    if attempt >= max_attempts {
                        let err_msg = format!(
                            "Failed to batch insert price history after {} attempts: {}",
                            attempt, err
                        );
                        error!("[CHART] {}", err_msg);
                        return Err(anyhow!(err_msg));
                    }

                    let current_delay = if err.to_string().contains("deadlock") {
                        base_delay.mul_f32(2.0_f32.powi(attempt - 1))
                    } else {
                        base_delay.mul_f32(1.5_f32.powi(attempt - 1))
                    };
                    warn!(
                        "[CHART] Batch insert price history failed for {} items. Backing off for {}ms: {}",
                        charts.len(),
                        current_delay.as_millis(),
                        err
                    );
                    sleep(current_delay).await;
                }
            }
        }
    }
}
