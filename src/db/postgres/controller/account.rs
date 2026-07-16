use std::{env, sync::Arc, time::Duration};

use crate::{config::DEFAULT_DELAY, db::postgres::PostgresDatabase, measure_postgres};

use anyhow::{Result, anyhow};
use rand::Rng;
use sqlx::PgConnection;
use tokio::time::sleep;
use tracing::warn;

/// SQL for single account upsert (same as used in `query!` macro).
pub const UPSERT_ACCOUNT_SQL: &str = r#"
    INSERT INTO account (account_id, nickname, bio, image_uri, follower_count, following_count)
    VALUES ($1, $2, '', $3, 0, 0)
    ON CONFLICT (account_id) DO NOTHING
"#;

/// SQL for batch account upsert via UNNEST.
pub const BATCH_UPSERT_ACCOUNTS_SQL: &str = r#"
    INSERT INTO account (account_id, nickname, bio, image_uri, follower_count, following_count)
    SELECT
        account_id,
        nickname,
        bio,
        image_uri,
        follower_count,
        following_count
    FROM UNNEST(
        $1::text[],  -- account_ids
        $2::text[],  -- nicknames
        $3::text[],  -- bios (empty strings)
        $4::text[],  -- image_uris
        $5::int[],   -- follower_counts (0s)
        $6::int[]    -- following_counts (0s)
    ) AS t(account_id, nickname, bio, image_uri, follower_count, following_count)
    ON CONFLICT (account_id) DO NOTHING
"#;

pub struct AccountController {
    pub db: Arc<PostgresDatabase>,
}

impl AccountController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        AccountController { db }
    }

    pub async fn upsert_account(&self, account_id: &str) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        // 기본 이미지 선택: 1부터 5까지의 랜덤 숫자를 사용하여 이미지 URI 환경변수 가져오기
        let random_number = rand::thread_rng().gen_range(1..=5);
        let image_key = format!("DEFAULT_IMAGE_{}", random_number);
        let image_uri =
            env::var(image_key).map_err(|e| anyhow!("DEFAULT_IMAGE get fail: {}", e))?;

        loop {
            attempt += 1;
            let result = measure_postgres!("account_upsert_account", {
                sqlx::query!(
                    r#"
                    INSERT INTO account (account_id, nickname, bio, image_uri, follower_count, following_count)
                    VALUES ($1, $2, '', $3, 0, 0)
                    ON CONFLICT (account_id) DO NOTHING
                    "#,
                    account_id,
                    account_id,
                    image_uri
                )
                .execute(&self.db.pool)
                .await
            });

            match result {
                Ok(_row) => {
                    break;
                }
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "Failed to upsert account {} after {} attempts: {}",
                            account_id,
                            attempt,
                            e
                        ));
                    } else {
                        // 지수 백오프 적용: 기본 지연 시간 * (1.5^(attempt-1))
                        let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));
                        warn!(
                            "[ACCOUNT] Error upserting account {}: {}. Retrying attempt {} with backoff {}ms...",
                            account_id,
                            e,
                            attempt,
                            current_delay.as_millis()
                        );
                        sleep(current_delay).await;
                        continue;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn upsert_account_tx(&self, account_id: &str, tx: &mut PgConnection) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        // 기본 이미지 선택: 1부터 5까지의 랜덤 숫자를 사용하여 이미지 URI 환경변수 가져오기
        let random_number = rand::thread_rng().gen_range(1..=5);
        let image_key = format!("DEFAULT_IMAGE_{}", random_number);
        let image_uri =
            env::var(image_key).map_err(|e| anyhow!("DEFAULT_IMAGE get fail: {}", e))?;

        loop {
            attempt += 1;
            let result = measure_postgres!("account_upsert_account_tx", {
                sqlx::query!(
                    r#"
                    INSERT INTO account (account_id, nickname, bio, image_uri, follower_count, following_count)
                    VALUES ($1, $2, '', $3, 0, 0)
                    ON CONFLICT (account_id) DO NOTHING
                    "#,
                    account_id,
                    account_id,
                    image_uri
                )
                .execute(tx.as_mut())
                .await
            });

            match result {
                Ok(_row) => break,
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "Failed to upsert account {} after {} attempts: {}",
                            account_id,
                            attempt,
                            e
                        ));
                    } else {
                        // 지수 백오프 적용: 기본 지연 시간 * (1.5^(attempt-1))
                        let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));
                        warn!(
                            "[ACCOUNT] Error upserting account {}: {}. Retrying attempt {} with backoff {}ms...",
                            account_id,
                            e,
                            attempt,
                            current_delay.as_millis()
                        );
                        sleep(current_delay).await;
                    }
                }
            }
        }
        Ok(())
    }

    // Batch upsert accounts
    pub async fn batch_upsert_accounts(&self, account_ids: &[String]) -> Result<()> {
        if account_ids.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in account_ids.chunks(1000) {
            self.batch_upsert_accounts_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_upsert_accounts_chunk(&self, account_ids: &[String]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        // 각 계정에 대해 랜덤 이미지 생성
        let mut accounts_data: Vec<(String, String, String)> = Vec::new();
        for account_id in account_ids {
            let random_number = rand::thread_rng().gen_range(1..=5);
            let image_key = format!("DEFAULT_IMAGE_{}", random_number);
            let image_uri =
                env::var(image_key).map_err(|e| anyhow!("DEFAULT_IMAGE get fail: {}", e))?;

            accounts_data.push((
                account_id.clone(),
                account_id.clone(), // nickname = account_id
                image_uri,
            ));
        }

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let account_ids: Vec<&str> =
                accounts_data.iter().map(|(id, _, _)| id.as_str()).collect();
            let nicknames: Vec<&str> = accounts_data
                .iter()
                .map(|(_, nick, _)| nick.as_str())
                .collect();
            let bios: Vec<&str> = vec![""; accounts_data.len()];
            let image_uris: Vec<&str> = accounts_data
                .iter()
                .map(|(_, _, img)| img.as_str())
                .collect();
            let follower_counts: Vec<i32> = vec![0; accounts_data.len()];
            let following_counts: Vec<i32> = vec![0; accounts_data.len()];

            match measure_postgres!("account_batch_upsert", {
                sqlx::query(BATCH_UPSERT_ACCOUNTS_SQL)
                    .bind(&account_ids)
                    .bind(&nicknames)
                    .bind(&bios)
                    .bind(&image_uris)
                    .bind(&follower_counts)
                    .bind(&following_counts)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[ACCOUNT] Failed to batch upsert {} accounts on attempt {}: {}",
                        accounts_data.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[ACCOUNT] Deadlock detected in batch_upsert_accounts, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch upsert accounts after {} attempts: {}",
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
