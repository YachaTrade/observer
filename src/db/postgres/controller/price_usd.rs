use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use bigdecimal::BigDecimal;
use tokio::time::sleep;
use tracing::warn;

use crate::{
    config::DEFAULT_DELAY, db::postgres::PostgresDatabase, event::common::price_usd::PriceUsdRow,
    measure_postgres,
};

pub const BATCH_INSERT_PRICE_USD_SQL: &str = r#"
    INSERT INTO price_usd (token_id, block_number, price, confidence, created_at)
    SELECT
        token_id,
        block_number,
        price,
        confidence,
        created_at
    FROM UNNEST(
        $1::varchar[],  -- token_ids
        $2::bigint[],   -- block_numbers
        $3::numeric[],  -- prices
        $4::numeric[],  -- confidences
        $5::bigint[]    -- created_ats
    ) AS t(token_id, block_number, price, confidence, created_at)
    ON CONFLICT (token_id, block_number) DO NOTHING
"#;

pub struct PriceUsdController {
    pub db: Arc<PostgresDatabase>,
}

impl PriceUsdController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        Self { db }
    }

    pub async fn batch_insert_price_usd(&self, rows: &[PriceUsdRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        for chunk in rows.chunks(1000) {
            self.batch_insert_price_usd_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_insert_price_usd_chunk(&self, rows: &[PriceUsdRow]) -> Result<()> {
        let max_attempts = 5;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let token_ids: Vec<String> = rows.iter().map(|row| row.token_id.clone()).collect();
            let block_numbers: Vec<i64> = rows.iter().map(|row| row.block_number as i64).collect();
            let prices: Vec<BigDecimal> = rows.iter().map(|row| row.price.clone()).collect();
            let confidences: Vec<Option<BigDecimal>> =
                rows.iter().map(|row| row.confidence.clone()).collect();
            let created_ats: Vec<i64> = rows.iter().map(|row| row.created_at as i64).collect();

            match measure_postgres!("price_usd_batch_insert", {
                sqlx::query(BATCH_INSERT_PRICE_USD_SQL)
                    .bind(&token_ids)
                    .bind(&block_numbers)
                    .bind(&prices)
                    .bind(&confidences)
                    .bind(&created_ats)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        "[PRICE_USD] Failed to batch insert {} prices on attempt {}: {}",
                        rows.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[PRICE_USD] Deadlock detected in batch_insert_price_usd, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                    } else if attempt >= max_attempts {
                        return Err(anyhow!(
                            "Failed to batch insert price_usd rows after {} attempts: {}",
                            attempt,
                            e
                        ));
                    } else {
                        sleep(current_delay).await;
                    }
                }
            }
        }
    }
}
