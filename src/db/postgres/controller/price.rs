use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::{config::DEFAULT_DELAY, db::postgres::PostgresDatabase, measure_postgres};

use anyhow::{Result, anyhow};
use bigdecimal::BigDecimal;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, warn};

/// SQL for single price INSERT.
pub const INSERT_PRICE_SQL: &str = r#"
    INSERT INTO price (quote_id, block_number, price, created_at)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (quote_id, block_number)
    DO NOTHING
"#;

const ATOMIC_BATCH_INSERT_PRICES_SQL: &str = r#"
    INSERT INTO price (quote_id, block_number, price, created_at)
    SELECT quote_id, block_number, price, created_at
    FROM UNNEST(
        $1::text[],
        $2::bigint[],
        $3::numeric[],
        $4::bigint[]
    ) AS requested(quote_id, block_number, price, created_at)
    ON CONFLICT (quote_id, block_number) DO NOTHING
"#;

const SELECT_CANONICAL_PRICES_SQL: &str = r#"
    SELECT price.quote_id::text, price.block_number, price.price
    FROM UNNEST($1::text[], $2::bigint[]) WITH ORDINALITY
        AS requested(quote_id, block_number, ordinal)
    JOIN price
      ON price.quote_id::text = requested.quote_id
     AND price.block_number = requested.block_number
    ORDER BY requested.ordinal
"#;

fn to_postgres_bigint(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{field}={value} is out of PostgreSQL BIGINT range"))
}

pub async fn has_persisted_prices_at_blocks(
    pool: &PgPool,
    quote_id: &str,
    block_numbers: &[i64],
) -> bool {
    let unique_blocks = block_numbers
        .iter()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if unique_blocks.is_empty() {
        return true;
    }

    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM price WHERE quote_id = $1 AND block_number = ANY($2::bigint[])",
    )
    .bind(quote_id)
    .bind(&unique_blocks)
    .fetch_one(pool)
    .await;

    match count {
        Ok(count) => count == unique_blocks.len() as i64,
        Err(error) => {
            warn!(
                "[PRICE] persisted price probe failed for quote={} block_count={}: {}",
                quote_id,
                unique_blocks.len(),
                error
            );
            false
        }
    }
}

pub struct PriceController {
    pub db: Arc<PostgresDatabase>,
}

impl PriceController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        PriceController { db }
    }

    /// Atomically persists a complete multi-quote Price cycle and returns the
    /// canonical rows that callers may safely place in memory caches.
    ///
    /// Existing rows win via `ON CONFLICT DO NOTHING`; reading them back in the
    /// same transaction prevents an acknowledged replay from caching a newly
    /// fetched value that differs from PostgreSQL.
    pub async fn persist_price_batch(
        &self,
        prices: &[(String, u64, BigDecimal, u64)],
    ) -> Result<Vec<(String, i64, BigDecimal)>> {
        if prices.is_empty() {
            return Ok(Vec::new());
        }

        let mut unique_keys = HashSet::with_capacity(prices.len());
        for (quote_id, block_number, _, _) in prices {
            if !unique_keys.insert((quote_id.as_str(), *block_number)) {
                return Err(anyhow!(
                    "duplicate price row in batch: quote={} block={}",
                    quote_id,
                    block_number
                ));
            }
        }

        let quote_ids = prices
            .iter()
            .map(|(quote_id, _, _, _)| quote_id.clone())
            .collect::<Vec<_>>();
        let block_numbers = prices
            .iter()
            .map(|(_, block_number, _, _)| to_postgres_bigint(*block_number, "block_number"))
            .collect::<Result<Vec<_>>>()?;
        let price_values = prices
            .iter()
            .map(|(_, _, price, _)| price.clone())
            .collect::<Vec<_>>();
        let timestamps = prices
            .iter()
            .map(|(_, _, _, timestamp)| to_postgres_bigint(*timestamp, "timestamp"))
            .collect::<Result<Vec<_>>>()?;

        let mut transaction = self.db.pool.begin().await?;
        measure_postgres!("price_atomic_batch_insert", {
            sqlx::query(ATOMIC_BATCH_INSERT_PRICES_SQL)
                .bind(&quote_ids)
                .bind(&block_numbers)
                .bind(&price_values)
                .bind(&timestamps)
                .execute(&mut *transaction)
                .await
        })?;

        let canonical = measure_postgres!("price_select_canonical_batch", {
            sqlx::query_as::<_, (String, i64, BigDecimal)>(SELECT_CANONICAL_PRICES_SQL)
                .bind(&quote_ids)
                .bind(&block_numbers)
                .fetch_all(&mut *transaction)
                .await
        })?;
        if canonical.len() != prices.len() {
            return Err(anyhow!(
                "canonical price batch incomplete after insert: expected={} actual={}",
                prices.len(),
                canonical.len()
            ));
        }

        transaction.commit().await?;
        Ok(canonical)
    }

    pub async fn insert_price(
        &self,
        quote_id: &str,
        block_number: u64,
        price: BigDecimal,
        timestamp: u64,
    ) -> Result<()> {
        let block_number = to_postgres_bigint(block_number, "block_number")?;
        let timestamp = to_postgres_bigint(timestamp, "timestamp")?;
        let max_attempts = 5;
        let mut attempt = 0;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            match measure_postgres!("price_insert_price", {
                sqlx::query(INSERT_PRICE_SQL)
                    .bind(quote_id)
                    .bind(block_number)
                    .bind(&price)
                    .bind(timestamp)
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
}
