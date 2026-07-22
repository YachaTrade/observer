use crate::measure_postgres;
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{instrument, warn};

use crate::config::DEFAULT_DELAY;

use crate::{db::postgres::PostgresDatabase, types::token::TokenBurn};

/// SQL for inserting a single burn into burn_history and decrementing
/// token.total_supply via CTE.
/// Bindings: $1 account_id (from), $2 token_id, $3 amount,
///           $4 transaction_hash, $5 log_index, $6 block_timestamp.
pub const HANDLE_BURN_SQL: &str = r#"
                    WITH
                    insert_burn_history AS (
                        INSERT INTO burn_history (
                            token_id, account_id, token_amount, transaction_hash, log_index, created_at
                        )
                        VALUES ($2, $1, $3, $4, $5, $6)
                        ON CONFLICT (token_id, account_id, transaction_hash, log_index) DO NOTHING
                        RETURNING 1
                    ),
                    update_total_supply AS (
                        UPDATE token
                        SET total_supply = GREATEST(total_supply - $3, 0)
                        WHERE token_id = $2
                          AND EXISTS (SELECT 1 FROM insert_burn_history)
                        RETURNING 1
                    )
                    SELECT 1 FROM update_total_supply
                    "#;

/// SQL for batch inserting burns into burn_history and decrementing
/// token.total_supply via UNNEST + CTE.
/// Bindings: $1 token_ids[], $2 account_ids[], $3 amounts[],
///           $4 transaction_hashes[], $5 log_indices[], $6 created_ats[].
pub const BATCH_HANDLE_BURNS_SQL: &str = r#"
                WITH burn_data AS (
                    SELECT token_id, account_id, amount, transaction_hash, log_index, created_at
                    FROM UNNEST(
                        $1::text[], $2::text[], $3::numeric[],
                        $4::text[], $5::int[], $6::bigint[]
                    ) AS t(token_id, account_id, amount, transaction_hash, log_index, created_at)
                ),
                insert_burn_history AS (
                    INSERT INTO burn_history (
                        token_id, account_id, token_amount, transaction_hash, log_index, created_at
                    )
                    SELECT token_id, account_id, amount, transaction_hash, log_index, created_at
                    FROM burn_data
                    ON CONFLICT (token_id, account_id, transaction_hash, log_index) DO NOTHING
                    RETURNING token_id, token_amount
                ),
                token_burn_totals AS (
                    SELECT token_id, SUM(token_amount) as total_burned
                    FROM insert_burn_history
                    GROUP BY token_id
                )
                UPDATE token t
                SET total_supply = GREATEST(t.total_supply - tbt.total_burned, 0)
                FROM token_burn_totals tbt
                WHERE t.token_id = tbt.token_id
            "#;

pub struct BurnController {
    pub db: Arc<PostgresDatabase>,
}

impl BurnController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        BurnController { db }
    }

    #[instrument(skip(self, burn))]
    pub async fn handle_burn(&self, burn: &TokenBurn) -> Result<()> {
        // Retry settings
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            match measure_postgres!("burn_handle_burn", {
                sqlx::query(HANDLE_BURN_SQL)
                    .bind(burn.from.as_ref().as_str()) // $1
                    .bind(burn.token.as_ref().as_str()) // $2
                    .bind(burn.amount.as_ref()) // $3
                    .bind(burn.transaction_hash.as_ref().as_str()) // $4
                    .bind(burn.log_index as i32) // $5
                    .bind(burn.block_timestamp as i64) // $6
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!("[BURN] Failed to handle_burn on attempt {}: {}", attempt, e);

                    // 데드락 감지 및 특별 처리
                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        // 데드락은 더 높은 지수 백오프 적용
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[BURN] Deadlock detected in handle_burn, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to handle_burn after {} attempts: {}",
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

    // Batch handle burns
    pub async fn batch_handle_burns(&self, burns: &[TokenBurn]) -> Result<()> {
        if burns.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in burns.chunks(1000) {
            self.batch_handle_burns_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_handle_burns_chunk(&self, burns: &[TokenBurn]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let token_ids: Vec<&str> = burns.iter().map(|b| b.token.as_ref().as_str()).collect();
            let account_ids: Vec<&str> = burns.iter().map(|b| b.from.as_ref().as_str()).collect();
            let amounts: Vec<&bigdecimal::BigDecimal> =
                burns.iter().map(|b| b.amount.as_ref()).collect();
            let transaction_hashes: Vec<&str> = burns
                .iter()
                .map(|b| b.transaction_hash.as_ref().as_str())
                .collect();
            let log_indices: Vec<i32> = burns.iter().map(|b| b.log_index as i32).collect();
            let created_ats: Vec<i64> = burns.iter().map(|b| b.block_timestamp as i64).collect();

            match measure_postgres!("burn_batch_handle_burns", {
                sqlx::query(BATCH_HANDLE_BURNS_SQL)
                    .bind(&token_ids)
                    .bind(&account_ids)
                    .bind(&amounts)
                    .bind(&transaction_hashes)
                    .bind(&log_indices)
                    .bind(&created_ats)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[BURN] Failed to batch handle {} burns on attempt {}: {}",
                        burns.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[BURN] Deadlock detected in batch_handle_burns, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch handle burns after {} attempts: {}",
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
