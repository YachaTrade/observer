use std::{sync::Arc, time::Duration};

use crate::{
    config::DEFAULT_DELAY,
    db::postgres::PostgresDatabase,
    measure_postgres,
    types::lp_manager::{Allocate, Collect},
};

use anyhow::Result;
use tokio::time::sleep;
use tracing::warn;

/// SQL for inserting a single LP allocate history row.
pub const HANDLE_LP_ALLOCATE_SQL: &str = r#"
                    INSERT INTO lp_allocate_history (
                        token_id, quote_amount, token_amount, transaction_hash, created_at
                    )
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT DO NOTHING
                    "#;

/// SQL for inserting a single LP collect history row.
pub const HANDLE_LP_COLLECT_SQL: &str = r#"
                    INSERT INTO lp_collect_history (
                        token_id, quote_amount, token_amount, c_amount, ft_amount, ct_amount,
                        transaction_hash, tx_index, log_index, created_at
                    )
                    VALUES ($1, $2, $3, 0, 0, 0, $4, $5, $6, $7)
                    ON CONFLICT (token_id, transaction_hash, tx_index, log_index) DO NOTHING
                    "#;

/// SQL for batch inserting LP allocate history rows via UNNEST + CTE.
pub const BATCH_HANDLE_LP_ALLOCATE_SQL: &str = r#"
                WITH allocate_data AS (
                    SELECT token_id, quote_amount, token_amount, transaction_hash, created_at
                    FROM UNNEST(
                        $1::text[], $2::numeric[], $3::numeric[], $4::text[], $5::bigint[]
                    ) AS t(token_id, quote_amount, token_amount, transaction_hash, created_at)
                )
                INSERT INTO lp_allocate_history (
                    token_id, quote_amount, token_amount, transaction_hash, created_at
                )
                SELECT token_id, quote_amount, token_amount, transaction_hash, created_at
                FROM allocate_data
                ON CONFLICT DO NOTHING
            "#;

/// SQL for batch inserting LP collect history rows via UNNEST + CTE.
pub const BATCH_HANDLE_LP_COLLECT_SQL: &str = r#"
                WITH collect_data AS (
                    SELECT token_id, quote_amount, token_amount, transaction_hash, tx_index, log_index, created_at
                    FROM UNNEST(
                        $1::text[], $2::numeric[], $3::numeric[],
                        $4::text[], $5::int[], $6::int[], $7::bigint[]
                    ) AS t(token_id, quote_amount, token_amount, transaction_hash, tx_index, log_index, created_at)
                )
                INSERT INTO lp_collect_history (
                    token_id, quote_amount, token_amount, c_amount, ft_amount, ct_amount,
                    transaction_hash, tx_index, log_index, created_at
                )
                SELECT token_id, quote_amount, token_amount, 0, 0, 0,
                    transaction_hash, tx_index, log_index, created_at
                FROM collect_data
                ON CONFLICT (token_id, transaction_hash, tx_index, log_index) DO NOTHING
            "#;

pub struct LpController {
    pub db: Arc<PostgresDatabase>,
}

impl LpController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        LpController { db }
    }

    pub async fn handle_lp_allocate(&self, allocate: &Allocate) -> Result<()> {
        let max_attempts = 10;
        let mut attempt = 0;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));
            match measure_postgres!("lp_handle_lp_allocate", {
                sqlx::query(HANDLE_LP_ALLOCATE_SQL)
                    .bind(allocate.token.as_ref().as_str()) //$1
                    .bind(allocate.quote_amount.as_ref()) //$2
                    .bind(allocate.token_amount.as_ref()) //$3
                    .bind(allocate.transaction_hash.as_ref().as_str()) //$4
                    .bind(allocate.block_timestamp as i64) //$5
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        "[LP] Failed to insert_lp_allocate_history on attempt {}: {}",
                        attempt, e
                    );

                    // 데드락 감지 및 특별 처리
                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        // 데드락은 더 높은 지수 백오프 적용
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[LP] Deadlock detected in insert_lp_allocate_history, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to insert_lp_allocate_history after {} attempts: {}",
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

    pub async fn handle_lp_collect(&self, collect: &Collect) -> Result<()> {
        let max_attempts = 10;
        let mut attempt = 0;
        let base_delay = std::time::Duration::from_millis(*DEFAULT_DELAY);
        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));
            match measure_postgres!("lp_handle_lp_collect", {
                sqlx::query(HANDLE_LP_COLLECT_SQL)
                    .bind(collect.token.as_ref().as_str()) //$1
                    .bind(collect.quote_amount.as_ref()) //$2
                    .bind(collect.token_amount.as_ref()) //$3
                    .bind(collect.transaction_hash.as_ref().as_str()) //$4
                    .bind(collect.transaction_index as i32) //$5
                    .bind(collect.log_index as i32) //$6
                    .bind(collect.block_timestamp as i64) //$7
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        "[LP] Failed to handle_lp_collect on attempt {}: {}",
                        attempt, e
                    );

                    // 데드락 감지 및 특별 처리
                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        // 데드락은 더 높은 지수 백오프 적용
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[LP] Deadlock detected in handle_lp_collect, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to handle_lp_collect after {} attempts: {}",
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

    // Batch handle lp allocate
    pub async fn batch_handle_lp_allocate(&self, allocates: &[Allocate]) -> Result<()> {
        if allocates.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in allocates.chunks(1000) {
            self.batch_handle_lp_allocate_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_handle_lp_allocate_chunk(&self, allocates: &[Allocate]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let token_ids: Vec<&str> = allocates
                .iter()
                .map(|a| a.token.as_ref().as_str())
                .collect();
            let quote_amounts: Vec<&bigdecimal::BigDecimal> =
                allocates.iter().map(|a| a.quote_amount.as_ref()).collect();
            let token_amounts: Vec<&bigdecimal::BigDecimal> =
                allocates.iter().map(|a| a.token_amount.as_ref()).collect();
            let transaction_hashes: Vec<&str> = allocates
                .iter()
                .map(|a| a.transaction_hash.as_ref().as_str())
                .collect();
            let created_ats: Vec<i64> =
                allocates.iter().map(|a| a.block_timestamp as i64).collect();

            match measure_postgres!("lp_batch_handle_lp_allocate", {
                sqlx::query(BATCH_HANDLE_LP_ALLOCATE_SQL)
                    .bind(&token_ids) // $1
                    .bind(&quote_amounts) // $2
                    .bind(&token_amounts) // $3
                    .bind(&transaction_hashes) // $4
                    .bind(&created_ats) // $5
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[LP] Failed to batch handle {} allocates on attempt {}: {}",
                        allocates.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[LP] Deadlock detected in batch_handle_lp_allocate, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch handle allocates after {} attempts: {}",
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

    // Batch handle lp collect
    pub async fn batch_handle_lp_collect(&self, collects: &[Collect]) -> Result<()> {
        if collects.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in collects.chunks(1000) {
            self.batch_handle_lp_collect_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_handle_lp_collect_chunk(&self, collects: &[Collect]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let token_ids: Vec<&str> = collects.iter().map(|c| c.token.as_ref().as_str()).collect();
            let quote_amounts: Vec<&bigdecimal::BigDecimal> =
                collects.iter().map(|c| c.quote_amount.as_ref()).collect();
            let token_amounts: Vec<&bigdecimal::BigDecimal> =
                collects.iter().map(|c| c.token_amount.as_ref()).collect();
            let transaction_hashes: Vec<&str> = collects
                .iter()
                .map(|c| c.transaction_hash.as_ref().as_str())
                .collect();
            let tx_indexes: Vec<i32> = collects
                .iter()
                .map(|c| c.transaction_index as i32)
                .collect();
            let log_indexes: Vec<i32> = collects.iter().map(|c| c.log_index as i32).collect();
            let created_ats: Vec<i64> = collects.iter().map(|c| c.block_timestamp as i64).collect();

            match measure_postgres!("lp_batch_handle_lp_collect", {
                sqlx::query(BATCH_HANDLE_LP_COLLECT_SQL)
                    .bind(&token_ids) // $1
                    .bind(&quote_amounts) // $2
                    .bind(&token_amounts) // $3
                    .bind(&transaction_hashes) // $4
                    .bind(&tx_indexes) // $5
                    .bind(&log_indexes) // $6
                    .bind(&created_ats) // $7
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[LP] Failed to batch handle {} collects on attempt {}: {}",
                        collects.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[LP] Deadlock detected in batch_handle_lp_collect, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch handle collects after {} attempts: {}",
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
