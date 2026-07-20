use std::{sync::Arc, time::Duration};

use crate::{
    config::DEFAULT_DELAY,
    db::postgres::PostgresDatabase,
    measure_postgres,
};

use anyhow::{Result, anyhow};
use bigdecimal::BigDecimal;
use tokio::time::sleep;
use tracing::{error, warn};

/// SQL for inserting or updating a market row on curve sync.
/// Bindings: $1 token_id, $2 price, $3 reserve_token, $4 reserve_quote,
///           $5 ath_price_usd, $6 ath_price_quote, $7 block_timestamp,
///           $8 market_type.
pub const HANDLE_CURVE_SYNC_SQL: &str = r#"
                    INSERT INTO market (
                        token_id, market_type, price, ath_price, ath_price_quote, reserve_token, reserve_quote, latest_trade_at, created_at
                    )
                    VALUES ($1, $8, $2, $5, $6, $3, $4, $7, $7)
                    ON CONFLICT (token_id)
                    DO UPDATE SET
                        price = CASE
                            WHEN market.latest_trade_at <= $7 THEN $2
                            ELSE market.price
                        END,
                        ath_price = GREATEST(market.ath_price, $5),
                        ath_price_quote = GREATEST(market.ath_price_quote, $6),
                        reserve_token = CASE
                            WHEN market.latest_trade_at <= $7 THEN $3
                            ELSE market.reserve_token
                        END,
                        reserve_quote = CASE
                            WHEN market.latest_trade_at <= $7 THEN $4
                            ELSE market.reserve_quote
                        END,
                        latest_trade_at = GREATEST(market.latest_trade_at, $7)
                    "#;

/// SQL for updating a market row on dex sync.
/// Bindings: $1 token_id, $2 price, $3 reserve_quote, $4 reserve_token,
///           $5 ath_price_usd, $6 ath_price_quote, $7 block_timestamp.
pub const HANDLE_DEX_SYNC_SQL: &str = r#"
                        UPDATE market
                        SET
                            price = CASE
                                WHEN latest_trade_at <= $7 THEN $2
                                ELSE price
                            END,
                            ath_price = GREATEST(market.ath_price, $5),
                            ath_price_quote = GREATEST(market.ath_price_quote, $6),
                            reserve_quote = CASE
                                WHEN latest_trade_at <= $7 THEN $3
                                ELSE reserve_quote
                            END,
                            reserve_token = CASE
                                WHEN latest_trade_at <= $7 THEN $4
                                ELSE reserve_token
                            END,
                            latest_trade_at = GREATEST(latest_trade_at, $7)
                        WHERE token_id = $1
                        "#;

/// SQL for batch graduating tokens. Updates token.is_graduated and
/// market.market_type + pool_id. Returns COUNT of updated market rows.
/// Bindings: $1 token_ids[], $2 pool_ids[], $3 graduated_market_type.
pub const BATCH_HANDLE_GRADUATES_SQL: &str = r#"
                WITH graduates_data AS (
                    SELECT token_id, pool_id
                    FROM UNNEST(
                        $1::text[],  -- token_ids
                        $2::text[]   -- pool_ids
                    ) AS t(token_id, pool_id)
                ),
                token_updates AS (
                    UPDATE token
                    SET is_graduated = true
                    FROM graduates_data
                    WHERE token.token_id = graduates_data.token_id
                    RETURNING token.token_id
                ),
                market_updates AS (
                    UPDATE market
                    SET
                        pool_id = graduates_data.pool_id,
                        market_type = $3
                    FROM graduates_data
                    WHERE market.token_id = graduates_data.token_id
                    RETURNING market.token_id
                )
                SELECT COUNT(*) FROM market_updates
            "#;

/// v1/v2 공용 curve sync 데이터
pub struct CurveSyncData {
    pub token: String,
    pub price: BigDecimal,
    pub reserve_token: BigDecimal,
    pub reserve_quote: BigDecimal,
    pub block_timestamp: i64,
    pub market_type: String, // "CURVE" or "DEX"
}

/// v1/v2 공용 dex sync 데이터
pub struct DexSyncData {
    pub token: String,
    pub price: BigDecimal,
    pub reserve_quote: BigDecimal,
    pub reserve_token: BigDecimal,
    pub block_timestamp: i64,
}

pub struct MarketController {
    pub db: Arc<PostgresDatabase>,
}

impl MarketController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        MarketController { db }
    }

    pub async fn handle_curve_sync(
        &self,
        sync: &CurveSyncData,
        ath_price_usd: &BigDecimal,
        ath_price_quote: &BigDecimal,
    ) -> Result<()> {
        let max_attempts = 10;
        let mut attempt = 0;

        let base_delay = Duration::from_millis(*DEFAULT_DELAY);

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));
            match measure_postgres!("market_insert_or_update_market_by_curve", {
                sqlx::query(HANDLE_CURVE_SYNC_SQL)
                    .bind(&sync.token) //$1
                    .bind(&sync.price) //$2
                    .bind(&sync.reserve_token) //$3
                    .bind(&sync.reserve_quote) //$4
                    .bind(ath_price_usd) //$5 - ath_price (USD)
                    .bind(ath_price_quote) //$6 - ath_price_quote
                    .bind(sync.block_timestamp) //$7 - block_timestamp
                    .bind(&sync.market_type) //$8 - market_type (CURVE or DEX)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[MarketController] Failed to execute handle_curve_sync on attempt {}: {}",
                        attempt, e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[MarketController] Deadlock detected in handle_curve_sync, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to execute handle_curve_sync after {} attempts: {}",
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

    pub async fn handle_dex_sync(
        &self,
        sync: &DexSyncData,
        ath_price_usd: &BigDecimal,
        ath_price_quote: &BigDecimal,
    ) -> Result<()> {
        let max_attempts = 10;
        let mut attempt = 0;

        loop {
            attempt += 1;
            match measure_postgres!("market_handle_dex_sync", {
                sqlx::query(HANDLE_DEX_SYNC_SQL)
                .bind(&sync.token) // $1
                .bind(&sync.price) // $2
                .bind(&sync.reserve_quote) // $3
                .bind(&sync.reserve_token) // $4
                .bind(ath_price_usd) // $5 - ath_price (USD)
                .bind(ath_price_quote) // $6 - ath_price_quote
                .bind(sync.block_timestamp) // $7 - block_timestamp
                .execute(&self.db.pool)
                .await
            }) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    if attempt >= max_attempts {
                        let err_msg = format!(
                            "Failed to handle dex sync after {} attempts for token={}, error: {}",
                            attempt, sync.token, e
                        );
                        error!("[MarketController] {}", err_msg);
                        return Err(anyhow!(err_msg));
                    } else {
                        warn!(
                            "[MarketController] handle_dex_sync Error updating market liquidity (pool): {}. Retrying attempt {} for token_id={}",
                            e, attempt, sync.token
                        );
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            }
        }
    }

    // Batch handle Graduates
    // graduated_market_type: "DEX"
    pub async fn batch_handle_graduates(
        &self,
        graduates: &[(String, String)],
        graduated_market_type: &str,
    ) -> Result<()> {
        if graduates.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in graduates.chunks(1000) {
            self.batch_handle_graduates_chunk(chunk, graduated_market_type).await?;
        }

        Ok(())
    }

    async fn batch_handle_graduates_chunk(
        &self,
        graduates: &[(String, String)],
        graduated_market_type: &str,
    ) -> Result<()> {
        let max_attempts = 10;
        let mut attempt = 0;
        let base_delay = Duration::from_millis(100);

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            let token_ids: Vec<&str> = graduates.iter().map(|(token, _)| token.as_str()).collect();
            let pool_ids: Vec<&str> = graduates.iter().map(|(_, pool)| pool.as_str()).collect();

            match measure_postgres!("market_batch_handle_graduates", {
                sqlx::query_scalar::<_, i64>(BATCH_HANDLE_GRADUATES_SQL)
                    .bind(&token_ids)
                    .bind(&pool_ids)
                    .bind(graduated_market_type)
                    .fetch_one(&self.db.pool)
                    .await
            }) {
                Ok(count) => {
                    if count as usize != graduates.len() {
                        error!(
                            "[CURVE_DBG] Batch Graduate update mismatch: expected {} updates, got {}, entries={:?}, market_type={}",
                            graduates.len(),
                            count,
                            graduates,
                            graduated_market_type
                        );
                    } else {
                        error!(
                            "[CURVE_DBG] Batch Graduate SQL ok: count={}, entries={:?}, market_type={}",
                            count, graduates, graduated_market_type
                        );
                    }

                    // Graduate 성공 시 metrics 증가
                    use crate::metrics::METRICS;
                    for _ in 0..count {
                        METRICS.graduate.increment_graduate_count();
                    }

                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[MarketController] Failed to batch handle {} Graduates on attempt {}: {}",
                        graduates.len(),
                        attempt,
                        e
                    );

                    if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch handle Graduates after {} attempts: {}",
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
