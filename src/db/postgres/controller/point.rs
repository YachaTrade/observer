use crate::{config::DEFAULT_DELAY, db::postgres::PostgresDatabase, measure_postgres};
use anyhow::Result;

use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};

/// SQL for batch inserting `point_history` rows via UNNEST.
pub const BATCH_INSERT_POINTS_SQL: &str = r#"
    INSERT INTO point_history (
        account_id,
        point_type,
        value,
        transaction_hash,
        tx_index,
        log_index,
        created_at
    )
    SELECT
        account_id,
        point_type,
        value,
        transaction_hash,
        tx_index,
        log_index,
        created_at
    FROM UNNEST(
        $1::text[],      -- account_ids
        $2::text[],      -- point_types
        $3::numeric[],   -- values
        $4::text[],      -- transaction_hashes
        $5::int[],       -- tx_indexes
        $6::int[],       -- log_indexes
        $7::bigint[]     -- created_ats
    ) AS t(account_id, point_type, value, transaction_hash, tx_index, log_index, created_at)
    ON CONFLICT (account_id, transaction_hash, tx_index, log_index) DO NOTHING
"#;

/// SQL for batch inserting graduate `point_history` rows via UNNEST with token creator lookup.
pub const BATCH_INSERT_GRADUATE_POINTS_SQL: &str = r#"
    WITH graduates_data AS (
        SELECT token_id, transaction_hash, tx_index, log_index, value, created_at
        FROM UNNEST(
            $1::text[],    -- token_ids
            $2::text[],    -- transaction_hashes
            $3::int[],     -- tx_indexes
            $4::int[],     -- log_indexes
            $5::numeric[], -- values
            $6::bigint[]   -- created_ats
        ) AS t(token_id, transaction_hash, tx_index, log_index, value, created_at)
    ),
    token_creators AS (
        SELECT
            t.creator AS account_id,
            ld.transaction_hash,
            ld.tx_index,
            ld.log_index,
            ld.value,
            ld.created_at
        FROM token t
        INNER JOIN graduates_data ld ON t.token_id = ld.token_id
    )
    INSERT INTO point_history (
        account_id,
        point_type,
        value,
        transaction_hash,
        tx_index,
        log_index,
        created_at
    )
    SELECT
        tc.account_id,
        'GRADUATE',
        tc.value,
        tc.transaction_hash,
        tc.tx_index,
        tc.log_index,
        tc.created_at
    FROM token_creators tc
    ON CONFLICT (account_id, transaction_hash, tx_index, log_index) DO NOTHING
"#;

// Batch insert용 데이터 구조
pub struct PointBatchData {
    pub account_id: Arc<String>,
    pub point_type: &'static str,
    pub value: bigdecimal::BigDecimal, // USD value calculated from quote_amount * price or fee * price
    pub transaction_hash: Arc<String>,
    pub tx_index: i32,
    pub log_index: i32,
    pub created_at: i64,
}

pub struct PointController {
    pub db: Arc<PostgresDatabase>,
}

impl PointController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        PointController { db }
    }

    // Batch insert 메서드
    pub async fn batch_insert_points(&self, points: &[PointBatchData]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in points.chunks(1000) {
            self.batch_insert_points_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_insert_points_chunk(&self, points: &[PointBatchData]) -> Result<()> {
        let max_attempts = 3;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            // Build query with UNNEST for batch insert
            let query = BATCH_INSERT_POINTS_SQL;

            // Collect arrays
            let account_ids: Vec<&str> = points.iter().map(|p| p.account_id.as_str()).collect();
            let point_types: Vec<&str> = points.iter().map(|p| p.point_type).collect();
            let values: Vec<bigdecimal::BigDecimal> =
                points.iter().map(|p| p.value.clone()).collect();
            let transaction_hashes: Vec<&str> =
                points.iter().map(|p| p.transaction_hash.as_str()).collect();
            let tx_indexes: Vec<i32> = points.iter().map(|p| p.tx_index).collect();
            let log_indexes: Vec<i32> = points.iter().map(|p| p.log_index).collect();
            let created_ats: Vec<i64> = points.iter().map(|p| p.created_at).collect();

            match measure_postgres!("point_batch_insert", {
                sqlx::query(query)
                    .bind(&account_ids)
                    .bind(&point_types)
                    .bind(&values)
                    .bind(&transaction_hashes)
                    .bind(&tx_indexes)
                    .bind(&log_indexes)
                    .bind(&created_ats)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    info!(
                        "[POINT] Batch inserted {} points successfully",
                        points.len()
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[POINT] Failed to batch insert {} points on attempt {}: {}",
                        points.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[POINT] Deadlock detected in batch_insert_points, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert points after {} attempts: {}",
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

    // Batch insert Graduate points
    // Parameters: (token_id, transaction_hash, tx_index, log_index, value, created_at)
    pub async fn batch_insert_graduate_points(
        &self,
        graduates: &[(String, String, i32, i32, bigdecimal::BigDecimal, i64)],
    ) -> Result<()> {
        if graduates.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in graduates.chunks(1000) {
            self.batch_insert_graduate_points_chunk(chunk).await?;
        }

        Ok(())
    }

    async fn batch_insert_graduate_points_chunk(
        &self,
        graduates: &[(String, String, i32, i32, bigdecimal::BigDecimal, i64)],
    ) -> Result<()> {
        let max_attempts = 3;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            // WITH 절로 token creator를 조회한 후 배치 insert
            let query = BATCH_INSERT_GRADUATE_POINTS_SQL;

            let token_ids: Vec<&str> = graduates
                .iter()
                .map(|(token, _, _, _, _, _)| token.as_str())
                .collect();
            let transaction_hashes: Vec<&str> = graduates
                .iter()
                .map(|(_, tx, _, _, _, _)| tx.as_str())
                .collect();
            let tx_indexes: Vec<i32> = graduates
                .iter()
                .map(|(_, _, tx_idx, _, _, _)| *tx_idx)
                .collect();
            let log_indexes: Vec<i32> = graduates
                .iter()
                .map(|(_, _, _, log_idx, _, _)| *log_idx)
                .collect();
            let values: Vec<bigdecimal::BigDecimal> = graduates
                .iter()
                .map(|(_, _, _, _, val, _)| val.clone())
                .collect();
            let created_ats: Vec<i64> = graduates.iter().map(|(_, _, _, _, _, ts)| *ts).collect();

            match measure_postgres!("point_batch_insert_graduate", {
                sqlx::query(query)
                    .bind(&token_ids)
                    .bind(&transaction_hashes)
                    .bind(&tx_indexes)
                    .bind(&log_indexes)
                    .bind(&values)
                    .bind(&created_ats)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    info!(
                        "[POINT] Batch inserted {} Graduate points successfully",
                        graduates.len()
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[POINT] Failed to batch insert {} Graduate points on attempt {}: {}",
                        graduates.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[POINT] Deadlock detected in batch_insert_Graduate_points, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert Graduate points after {} attempts: {}",
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
