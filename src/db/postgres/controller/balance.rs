use crate::measure_postgres;
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::warn;

use crate::config::DEFAULT_DELAY;

use crate::{db::postgres::PostgresDatabase, types::token::TokenBalance};

/// SQL for `BalanceController::batch_set_balances`, exposed as a pub const
/// so integration tests can exercise the exact statement the production
/// code runs. The INSERT fires `trigger_update_balance_from_history` which
/// writes to the `balance` table; that trigger chains into
/// `trigger_delete_zero_balance` and `trg_update_holder_count`.
/// Do not modify without updating the tests in `tests/group_a_controllers.rs`.
pub const BATCH_SET_BALANCES_SQL: &str = r#"
                INSERT INTO balance_history (
                    token_id,
                    account_id,
                    balance,
                    block_number,
                    transaction_hash,
                    log_index,
                    tx_index,
                    created_at
                )
                SELECT
                    token_id,
                    account_id,
                    balance,
                    block_number,
                    transaction_hash,
                    log_index,
                    tx_index,
                    created_at
                FROM UNNEST(
                    $1::text[],     -- token_ids
                    $2::text[],     -- account_ids
                    $3::numeric[],  -- balances
                    $4::bigint[],   -- block_numbers
                    $5::text[],     -- transaction_hashes
                    $6::int[],      -- log_indices
                    $7::int[],      -- tx_indices
                    $8::bigint[]    -- created_ats
                ) AS t(token_id, account_id, balance, block_number, transaction_hash, log_index, tx_index, created_at)
                ON CONFLICT (token_id, account_id, transaction_hash, tx_index, log_index) DO NOTHING
            "#;

pub struct BalanceController {
    pub db: Arc<PostgresDatabase>,
}

impl BalanceController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        BalanceController { db }
    }

    // Batch set balances
    pub async fn batch_set_balances(&self, balances: &[TokenBalance]) -> Result<()> {
        if balances.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in balances.chunks(1000) {
            self.batch_set_balances_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_set_balances_chunk(&self, balances: &[TokenBalance]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            // balance_history에만 INSERT하면 트리거가 자동으로 balance 테이블 업데이트
            let query = BATCH_SET_BALANCES_SQL;

            let token_ids: Vec<&str> = balances.iter().map(|b| b.token.as_ref().as_str()).collect();
            let account_ids: Vec<&str> = balances
                .iter()
                .map(|b| b.account_id.as_ref().as_str())
                .collect();
            let balance_vals: Vec<&bigdecimal::BigDecimal> =
                balances.iter().map(|b| b.balance.as_ref()).collect();
            let block_numbers: Vec<i64> = balances.iter().map(|b| b.block_number as i64).collect();
            let transaction_hashes: Vec<&str> = balances
                .iter()
                .map(|b| b.transaction_hash.as_ref().as_str())
                .collect();
            let log_indices: Vec<i32> = balances.iter().map(|b| b.log_index as i32).collect();
            let tx_indices: Vec<i32> = balances
                .iter()
                .map(|b| b.transaction_index as i32)
                .collect();
            let created_ats: Vec<i64> = balances.iter().map(|b| b.block_timestamp as i64).collect();

            match measure_postgres!("balance_batch_set_balances", {
                sqlx::query(query)
                    .bind(&token_ids)
                    .bind(&account_ids)
                    .bind(&balance_vals)
                    .bind(&block_numbers)
                    .bind(&transaction_hashes)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .bind(&created_ats)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[BALANCE] Failed to batch set {} balances on attempt {}: {}",
                        balances.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[BALANCE] Deadlock detected in batch_set_balances, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch set balances after {} attempts: {}",
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
