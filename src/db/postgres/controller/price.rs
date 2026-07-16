use std::{sync::Arc, time::Duration};

use crate::{config::DEFAULT_DELAY, db::postgres::PostgresDatabase, measure_postgres};

use anyhow::{Result, anyhow};
use bigdecimal::BigDecimal;
use tokio::time::sleep;
use tracing::{error, warn};

/// SQL for single price INSERT.
pub const INSERT_PRICE_SQL: &str = r#"
    INSERT INTO price (quote_id, block_number, price, created_at)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (quote_id, block_number)
    DO NOTHING
"#;

/// SQL for batch price INSERT via UNNEST.
pub const BATCH_INSERT_PRICES_SQL: &str = r#"
    INSERT INTO price (quote_id, block_number, price, created_at)
    SELECT
        $1 AS quote_id,
        block_number,
        price,
        created_at
    FROM UNNEST(
        $2::bigint[],   -- block_numbers
        $3::numeric[],  -- prices
        $4::bigint[]    -- created_ats
    ) AS t(block_number, price, created_at)
    ON CONFLICT (quote_id, block_number) DO NOTHING
"#;

pub struct PriceController {
    pub db: Arc<PostgresDatabase>,
}

impl PriceController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        PriceController { db }
    }

    pub async fn insert_price(
        &self,
        quote_id: &str,
        block_number: u64,
        price: BigDecimal,
        timestamp: u64,
    ) -> Result<()> {
        let max_attempts = 5;
        let mut attempt = 0;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            match measure_postgres!("price_insert_price", {
                sqlx::query(INSERT_PRICE_SQL)
                .bind(quote_id)
                .bind(block_number as i64)
                .bind(&price)
                .bind(timestamp as i64)
                .execute(&self.db.pool)
                .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let err_msg = format!(
                        "Failed to insert price on attempt {}: block={}, price={}, error: {}",
                        attempt, block_number, price, e
                    );

                    // Check for deadlock
                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[PRICE] Deadlock detected in insert_price, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        error!("[PRICE] {}", err_msg);
                        return Err(anyhow!(err_msg));
                    } else {
                        warn!("[PRICE] {}, Retrying...", err_msg);
                        sleep(current_delay).await;
                        continue;
                    }
                }
            }
        }
    }

    // Batch insert prices
    pub async fn batch_insert_prices(
        &self,
        quote_id: &str,
        prices: &[(u64, BigDecimal, u64)], // (block_number, price, timestamp)
    ) -> Result<()> {
        if prices.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in prices.chunks(1000) {
            self.batch_insert_prices_chunk(quote_id, chunk).await?;
        }

        Ok(())
    }

    async fn batch_insert_prices_chunk(
        &self,
        quote_id: &str,
        prices: &[(u64, BigDecimal, u64)], // (block_number, price, timestamp)
    ) -> Result<()> {
        let max_attempts = 5;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let block_numbers: Vec<i64> = prices.iter().map(|(bn, _, _)| *bn as i64).collect();
            let price_vals: Vec<BigDecimal> = prices.iter().map(|(_, p, _)| p.clone()).collect();
            let timestamps: Vec<i64> = prices.iter().map(|(_, _, ts)| *ts as i64).collect();

            match measure_postgres!("price_batch_insert_prices", {
                sqlx::query(BATCH_INSERT_PRICES_SQL)
                    .bind(quote_id)
                    .bind(&block_numbers)
                    .bind(&price_vals)
                    .bind(&timestamps)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[PRICE] Failed to batch insert {} prices on attempt {}: {}",
                        prices.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[PRICE] Deadlock detected in batch_insert_prices, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert prices after {} attempts: {}",
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
