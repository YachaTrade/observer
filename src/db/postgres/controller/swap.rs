use crate::measure_postgres;
use anyhow::Result;
use bigdecimal::BigDecimal;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, instrument, warn};

use crate::config::{DEFAULT_DELAY, WNATIVE_ADDRESS};
use crate::db::cache::CacheManager;

use crate::db::postgres::PostgresDatabase;

/// SQL for `SwapController::batch_insert_swaps`, exposed as a pub const so
/// integration tests can exercise the exact statement the production code
/// runs. The INSERT fires `trg_update_market_volume` (adds quote_amount to
/// `market.volume`), `swap_count_trigger` (maintains per-token swap_count),
/// and `trg_update_account_swap_count` (per-account total_count).
/// Do not modify without updating the tests in `tests/group_a_controllers.rs`.
pub const BATCH_INSERT_SWAPS_SQL: &str = r#"
                INSERT INTO swap (
                    account_id,
                    token_id,
                    is_buy,
                    quote_amount,
                    token_amount,
                    reserve_quote,
                    reserve_token,
                    value,
                    market_type,
                    created_at,
                    transaction_hash,
                    block_number,
                    log_index,
                    tx_index
                )
                SELECT
                    account_id,
                    token_id,
                    is_buy,
                    quote_amount,
                    token_amount,
                    reserve_quote,
                    reserve_token,
                    value,
                    market_type,
                    created_at,
                    transaction_hash,
                    block_number,
                    log_index,
                    tx_index
                FROM UNNEST(
                    $1::text[],     -- account_ids
                    $2::text[],     -- token_ids
                    $3::boolean[],  -- is_buys
                    $4::numeric[],  -- quote_amounts
                    $5::numeric[],  -- token_amounts
                    $6::numeric[],  -- reserve_quotes
                    $7::numeric[],  -- reserve_tokens
                    $8::numeric[],  -- values
                    $9::text[],     -- market_types
                    $10::bigint[],  -- created_ats
                    $11::text[],    -- transaction_hashes
                    $12::bigint[],  -- block_numbers
                    $13::int[],     -- log_indexes
                    $14::int[]      -- tx_indexes
                ) AS t(account_id, token_id, is_buy, quote_amount, token_amount, reserve_quote, reserve_token, value, market_type, created_at, transaction_hash, block_number, log_index, tx_index)
                ON CONFLICT (account_id, token_id, transaction_hash, tx_index, log_index) DO NOTHING
                "#;

/// In-range price lookup SQL used by
/// `SwapController::get_prices_for_block_range`. Binds: $1 quote_id (text),
/// $2 min_block (bigint), $3 max_block (bigint). Returns `(block_number,
/// price)` rows ordered by `block_number ASC`. Exposed for integration tests.
pub const GET_PRICES_FOR_RANGE_SQL: &str = r#"
                            SELECT block_number, price
                            FROM price
                            WHERE quote_id = $1 AND block_number BETWEEN $2 AND $3
                            ORDER BY block_number ASC
                            "#;

/// Fallback-price lookup SQL used by
/// `SwapController::get_prices_for_block_range` when the range contains no
/// rows. Binds: $1 quote_id (text). Returns the latest price
/// (`block_number DESC LIMIT 1`). Exposed for integration tests.
pub const GET_FALLBACK_PRICE_SQL: &str = r#"
                                SELECT price
                                FROM price
                                WHERE quote_id = $1
                                ORDER BY block_number DESC
                                LIMIT 1
                                "#;

// Batch insert용 데이터 구조
pub struct SwapBatchData {
    pub account_id: Arc<String>,
    pub token_id: Arc<String>,
    pub is_buy: bool,
    pub quote_amount: Arc<BigDecimal>,
    pub token_amount: Arc<BigDecimal>,
    pub reserve_quote: Arc<BigDecimal>,
    pub reserve_token: Arc<BigDecimal>,
    pub value: BigDecimal,
    pub market_type: &'static str,
    pub created_at: i64,
    pub transaction_hash: Arc<String>,
    pub block_number: i64,
    pub log_index: i32,
    pub tx_index: i32,
}

pub struct SwapController {
    pub db: Arc<PostgresDatabase>,
}

impl SwapController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        SwapController { db }
    }

    // Price 테이블에서 block range의 모든 price 조회하고 각 block에 매핑
    // 빈 HashMap이 반환될 경우 재시도 (최소한 fallback price라도 가져올 때까지)
    #[instrument(skip(self))]
    pub async fn get_prices_for_block_range(
        &self,
        min_block: i64,
        max_block: i64,
    ) -> Result<std::collections::HashMap<i64, bigdecimal::BigDecimal>> {
        use bigdecimal::BigDecimal;
        use std::collections::HashMap;

        const MAX_RETRIES: u32 = 50;
        const RETRY_DELAY_MS: u64 = 100;

        for retry in 0..MAX_RETRIES {
            // 1. 먼저 메모리 캐시에서 조회
            let mut prices_in_range: Vec<(i64, BigDecimal)> = Vec::new();

            if let Ok(cache_manager) = CacheManager::instance() {
                let cached_prices = cache_manager
                    .get_prices_in_range_for_quote(&WNATIVE_ADDRESS, min_block, max_block)
                    .await;
                if !cached_prices.is_empty() {
                    info!(
                        "[SWAP] Found {} prices in memory cache for range [{}, {}]",
                        cached_prices.len(),
                        min_block,
                        max_block
                    );
                    prices_in_range = cached_prices
                        .into_iter()
                        .map(|(block, arc_price)| (block, (*arc_price).clone()))
                        .collect();
                    prices_in_range.sort_by_key(|(block, _)| *block);
                }
            }

            // 2. 캐시에 없으면 DB에서 조회
            if prices_in_range.is_empty() {
                prices_in_range = match measure_postgres!("swap_get_prices_in_range", {
                    sqlx::query_as::<_, (i64, BigDecimal)>(GET_PRICES_FOR_RANGE_SQL)
                        .bind(&*crate::config::WNATIVE_ADDRESS)
                        .bind(min_block)
                        .bind(max_block)
                        .fetch_all(&self.db.pool)
                        .await
                }) {
                    Ok(prices) => {
                        info!(
                            "[SWAP] Found {} prices in DB for range [{}, {}]",
                            prices.len(),
                            min_block,
                            max_block
                        );
                        prices
                    }
                    Err(e) => {
                        warn!(
                            "[SWAP] Failed to get prices for range [{}, {}]: {}. Will try fallback.",
                            min_block, max_block, e
                        );
                        Vec::new()
                    }
                };
            }

            // HashMap 생성: 각 block_number에 대해 해당 block 이하의 가장 가까운 price 매핑
            let mut price_map: HashMap<i64, BigDecimal> = HashMap::new();

            if !prices_in_range.is_empty() {
                let mut current_price = &prices_in_range[0].1;

                let mut price_idx = 0;
                for block_num in min_block..=max_block {
                    // 현재 block_num에 해당하는 price가 있으면 업데이트
                    while price_idx < prices_in_range.len()
                        && prices_in_range[price_idx].0 <= block_num
                    {
                        current_price = &prices_in_range[price_idx].1;
                        price_idx += 1;
                    }
                    price_map.insert(block_num, current_price.clone());
                }
                return Ok(price_map);
            } else {
                // range 내에 price가 없으면 fallback 시도
                // 1. 먼저 메모리 캐시에서 가장 최근 price 조회
                let mut fallback_price: Option<BigDecimal> = None;

                if let Ok(cache_manager) = CacheManager::instance() {
                    fallback_price = cache_manager
                        .get_latest_price_before_for_quote(&WNATIVE_ADDRESS, max_block)
                        .await
                        .map(|arc_price| (*arc_price).clone());
                    if fallback_price.is_some() {
                        info!("[SWAP] Found fallback price in memory cache");
                    }
                }

                // 2. 캐시에도 없으면 DB에서 조회
                if fallback_price.is_none() {
                    fallback_price = match measure_postgres!("swap_get_fallback_price", {
                        sqlx::query_as::<_, (BigDecimal,)>(GET_FALLBACK_PRICE_SQL)
                            .bind(&*crate::config::WNATIVE_ADDRESS)
                            .fetch_optional(&self.db.pool)
                            .await
                    }) {
                        Ok(Some(row)) => {
                            info!("[SWAP] Found fallback price in DB");
                            Some(row.0)
                        }
                        Ok(None) => {
                            warn!("[SWAP] No price found in price table");
                            None
                        }
                        Err(e) => {
                            warn!("[SWAP] Failed to get fallback price: {}", e);
                            None
                        }
                    };
                }

                // fallback price가 있으면 모든 block에 적용하고 반환
                if let Some(price) = fallback_price {
                    for block_num in min_block..=max_block {
                        price_map.insert(block_num, price.clone());
                    }
                    return Ok(price_map);
                }
            }

            // price_map이 비어있으면 재시도
            if retry < MAX_RETRIES - 1 {
                warn!(
                    "[SWAP] Price map is empty for range [{}, {}], retrying {}/{}...",
                    min_block,
                    max_block,
                    retry + 1,
                    MAX_RETRIES
                );
                sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }

        // MAX_RETRIES 후에도 price가 없으면 빈 HashMap 반환
        warn!(
            "[SWAP] Failed to get any price after {} retries for range [{}, {}]",
            MAX_RETRIES, min_block, max_block
        );
        Ok(HashMap::new())
    }

    // Batch insert 메서드
    #[instrument(skip(self, swaps))]
    pub async fn batch_insert_swaps(&self, swaps: &[SwapBatchData]) -> Result<()> {
        if swaps.is_empty() {
            return Ok(());
        }

        // 1000개씩 chunk로 나눠서 처리
        for chunk in swaps.chunks(1000) {
            self.batch_insert_swaps_chunk(chunk).await?;
        }

        Ok(())
    }

    // Chunk 단위로 insert하는 내부 함수
    async fn batch_insert_swaps_chunk(&self, swaps: &[SwapBatchData]) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_delay = base_delay.mul_f32(1.5_f32.powi(attempt - 1));

            // Build query with UNNEST for batch insert
            let query: &str = BATCH_INSERT_SWAPS_SQL;

            // Collect arrays
            let account_ids: Vec<&str> = swaps.iter().map(|s| s.account_id.as_str()).collect();
            let token_ids: Vec<&str> = swaps.iter().map(|s| s.token_id.as_str()).collect();
            let is_buys: Vec<bool> = swaps.iter().map(|s| s.is_buy).collect();
            let quote_amounts: Vec<&BigDecimal> =
                swaps.iter().map(|s| s.quote_amount.as_ref()).collect();
            let token_amounts: Vec<&BigDecimal> =
                swaps.iter().map(|s| s.token_amount.as_ref()).collect();
            let reserve_quotes: Vec<&BigDecimal> =
                swaps.iter().map(|s| s.reserve_quote.as_ref()).collect();
            let reserve_tokens: Vec<&BigDecimal> =
                swaps.iter().map(|s| s.reserve_token.as_ref()).collect();
            let values: Vec<&BigDecimal> = swaps.iter().map(|s| &s.value).collect();
            let market_types: Vec<&str> = swaps.iter().map(|s| s.market_type).collect();
            let created_ats: Vec<i64> = swaps.iter().map(|s| s.created_at).collect();
            let transaction_hashes: Vec<&str> =
                swaps.iter().map(|s| s.transaction_hash.as_str()).collect();
            let block_numbers: Vec<i64> = swaps.iter().map(|s| s.block_number).collect();
            let log_indexes: Vec<i32> = swaps.iter().map(|s| s.log_index).collect();
            let tx_indexes: Vec<i32> = swaps.iter().map(|s| s.tx_index).collect();

            match measure_postgres!("swap_batch_insert", {
                sqlx::query(query)
                    .bind(&account_ids)
                    .bind(&token_ids)
                    .bind(&is_buys)
                    .bind(&quote_amounts)
                    .bind(&token_amounts)
                    .bind(&reserve_quotes)
                    .bind(&reserve_tokens)
                    .bind(&values)
                    .bind(&market_types)
                    .bind(&created_ats)
                    .bind(&transaction_hashes)
                    .bind(&block_numbers)
                    .bind(&log_indexes)
                    .bind(&tx_indexes)
                    .execute(&self.db.pool)
                    .await
            }) {
                Ok(_) => {
                    info!("[SWAP] Batch inserted {} swaps successfully", swaps.len());
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "[SWAP] Failed to batch insert {} swaps on attempt {}: {}",
                        swaps.len(),
                        attempt,
                        e
                    );

                    let is_deadlock = e.to_string().to_lowercase().contains("deadlock");
                    if is_deadlock {
                        let deadlock_delay = base_delay.mul_f32(2.0_f32.powi(attempt - 1));
                        warn!(
                            "[SWAP] Deadlock detected in batch_insert_swaps, retrying with backoff of {}ms",
                            deadlock_delay.as_millis()
                        );
                        sleep(deadlock_delay).await;
                        continue;
                    } else if attempt >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Failed to batch insert swaps after {} attempts: {}",
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
