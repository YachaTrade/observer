use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::Result;
use bigdecimal::{BigDecimal, RoundingMode};

use crate::{
    config::{BONDING_CURVE_FEE_RATE, CREATE_FEE_AMOUNT, GRADUATE_FEE_AMOUNT},
    db::cache::CacheManager,
    db::postgres::{
        PostgresDatabase,
        controller::{
            account::AccountController,
            chart::{ChartBatchData, ChartController},
            market::{CurveSyncData, MarketController},
            point::{PointBatchData, PointController},
            swap::{SwapBatchData, SwapController},
            token::{TokenBatchData, TokenController},
            fee::FeeController,
        },
    },
    sync::{EventType, receive::RECEIVE_MANAGER},
    types::fee::{FeeHistoryEvent, FeeType},
    types::v2::curve::{MarketType, V2CurveEvent},
};

use super::V2CurveEventBatch;
use crate::metrics::MonitoredReceiver;
use tracing::{error, instrument, warn};

#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<V2CurveEventBatch>,
    event_type: EventType,
) -> Result<()> {
    let mut total_events = 0;
    while let Some(batch) = receiver.recv().await {
        let V2CurveEventBatch {
            events,
            to_block,
            latest_block,
        } = batch;
        RECEIVE_MANAGER
            .check_last_processed_block(to_block, event_type)
            .await;
        let db = PostgresDatabase::instance()?;

        let time = Instant::now();
        let event_count = events.len();
        total_events += event_count;

        let events_by_token = group_events_by_token(events);
        let token_count = events_by_token.len();

        let handles: Vec<_> = events_by_token
            .into_iter()
            .map(|(token, events)| {
                let db = db.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_token_events(token.clone(), events, db).await {
                        error!(
                            "[CURVE] Failed to process events for token {}: {:#}",
                            token, e
                        );
                    }
                })
            })
            .collect();

        for handle in handles {
            if let Err(e) = handle.await {
                total_events -= 1;
                error!("[CURVE] Failed to join handle: {:?}", e);
            }
        }

        let elapsed_ms = time.elapsed().as_millis();
        warn!(
            "📊 {:?} Receiver: Events: {} | Tokens: {} | Total Events: {} | Process time: {}ms | To Block: {} | Latest Block: {}",
            event_type, event_count, token_count, total_events, elapsed_ms, to_block, latest_block,
        );
        RECEIVE_MANAGER
            .set_last_processed_block(event_type, to_block, latest_block)
            .await;

        // Curve also emits prices into the multi-quote cache; mirror the DEX
        // trim so the cache cannot grow unbounded when Curve is the primary
        // price source (pre-graduation).
        if let Ok(cache_manager) = CacheManager::instance() {
            let cleanup_block = (to_block as i64).saturating_sub(1000);
            cache_manager
                .remove_prices_before_or_equal_all_quotes(cleanup_block)
                .await;
        }
    }

    Ok(())
}

fn group_events_by_token(events: Vec<V2CurveEvent>) -> HashMap<String, Vec<V2CurveEvent>> {
    let mut events_by_token: HashMap<String, Vec<V2CurveEvent>> = HashMap::new();

    for event in events {
        if let Some(token) = event.token().map(|t| t.to_string()) {
            events_by_token.entry(token).or_default().push(event);
        }
    }

    events_by_token
}

#[derive(Debug, Clone)]
struct SyncReserve {
    log_index: i32,
    reserve_quote: Arc<BigDecimal>,
    reserve_token: Arc<BigDecimal>,
}

fn find_closest_reserve(
    sync_reserves: &HashMap<(String, i32), Vec<SyncReserve>>,
    transaction_hash: &str,
    transaction_index: i32,
    log_index: i32,
) -> (Arc<BigDecimal>, Arc<BigDecimal>) {
    sync_reserves
        .get(&(transaction_hash.to_string(), transaction_index))
        .and_then(|reserves| {
            reserves
                .iter()
                .filter(|r| r.log_index < log_index)
                .max_by_key(|r| r.log_index)
        })
        .map(|r| (Arc::clone(&r.reserve_quote), Arc::clone(&r.reserve_token)))
        .unwrap_or_else(|| {
            static ZERO: once_cell::sync::Lazy<Arc<BigDecimal>> =
                once_cell::sync::Lazy::new(|| Arc::new(BigDecimal::from(0)));
            (Arc::clone(&ZERO), Arc::clone(&ZERO))
        })
}

fn giwa_market_type(market_type: &MarketType) -> &'static str {
    match market_type {
        MarketType::Curve => "NADFUN",
        MarketType::Dex => "UNISWAPV3",
    }
}

fn giwa_curve_fee_type(is_buy: bool) -> FeeType {
    if is_buy {
        FeeType::CurveBuy
    } else {
        FeeType::CurveSell
    }
}

#[allow(clippy::question_mark)]
async fn process_token_events(
    token: String,
    events: Vec<V2CurveEvent>,
    db: Arc<PostgresDatabase>,
) -> Result<()> {
    use crate::db::cache::CacheManager;
    use std::collections::HashSet;

    if events.is_empty() {
        return Ok(());
    }

    let cache_manager = CacheManager::instance()?;

    // Get fee config for this token (creator_rate, curve_rate, dex_rate) in BPS
    let fee_config = cache_manager.get_fee_config(&token).await.unwrap_or(None);
    let curve_fee_bps = fee_config
        .map(|(creator, curve, _dex)| BigDecimal::from(creator as u64 + curve as u64))
        .unwrap_or_else(|| &*BONDING_CURVE_FEE_RATE * BigDecimal::from(100)); // fallback to hardcoded (% → BPS)

    // Resolve quote token for USD price conversion. `get_token_quote_id`
    // returns EIP-55 checksum (normalize at the cache boundary), so legacy
    // lowercase market.quote_id rows are case-corrected here before any
    // downstream `==` / case-sensitive lookup (price cache key, WNATIVE
    // equality, etc.).
    let quote_id_str = cache_manager
        .get_token_quote_id(&token)
        .await
        .unwrap_or(None)
        .unwrap_or_else(|| (*crate::config::WNATIVE_ADDRESS).clone());
    let quote_decimals = crate::config::get_quote_decimals(&quote_id_str);

    let mut swap_batch = Vec::new();
    let mut point_batch = Vec::new();
    let mut chart_map: HashMap<String, ChartBatchData> = HashMap::new();
    let mut sync_reserves: HashMap<(String, i32), Vec<SyncReserve>> = HashMap::new();
    let mut create_batch: Vec<TokenBatchData> = Vec::new();
    let mut graduate_events = Vec::new();
    let mut sync_events = Vec::new();
    let mut sniping_batch = Vec::new();
    let mut fee_batch: Vec<FeeHistoryEvent> = Vec::new();
    let mut account_ids: HashSet<String> = HashSet::new();

    for event in events {
        match &event {
            V2CurveEvent::Create(create) => {
                account_ids.insert((*create.creator).clone());

                // Cache quote_id for this token
                let _ = cache_manager.insert_token_quote_id(&create.token, &create.quote_id).await;

                let price = (&*create.virtual_quote_reserve / &*create.virtual_token_reserve)
                    .with_scale_round(10, RoundingMode::Up);

                chart_map.insert(
                    (*create.transaction_hash).clone(),
                    ChartBatchData {
                        token_id: Arc::clone(&create.token),
                        price,
                        volume: BigDecimal::from(0),
                        block_timestamp: create.block_timestamp as i64,
                        block_number: create.block_number as i64,
                        transaction_hash: Arc::clone(&create.transaction_hash),
                        log_index: create.log_index as i32,
                        tx_index: create.transaction_index as i32,
                    },
                );

                let block_num = create.block_number as i64;
                let native_price = cache_manager.get_quote_usd_price(&quote_id_str, block_num).await;

                let value = if let Some(price) = &native_price {
                    (&*CREATE_FEE_AMOUNT / quote_decimals) * &**price
                } else {
                    error!(
                        "[CURVE] No price found for block {} in create event",
                        block_num
                    );
                    BigDecimal::from(0)
                };

                // Create fee history
                let fee_usd = if let Some(price) = &native_price {
                    (&*CREATE_FEE_AMOUNT / quote_decimals) * &**price
                } else {
                    BigDecimal::from(0)
                };
                fee_batch.push(FeeHistoryEvent {
                    transaction_hash: Arc::clone(&create.transaction_hash),
                    log_index: create.log_index,
                    account_id: Arc::clone(&create.creator),
                    token_id: Arc::clone(&create.token),
                    quote_amount: Arc::new((*CREATE_FEE_AMOUNT).clone()),
                    usd_amount: Arc::new(fee_usd),
                    fee_type: FeeType::Create,
                    block_number: create.block_number,
                    tx_index: create.transaction_index,
                    block_timestamp: create.block_timestamp,
                });

                point_batch.push(PointBatchData {
                    account_id: Arc::clone(&create.creator),
                    point_type: "CREATE",
                    value,
                    transaction_hash: Arc::clone(&create.transaction_hash),
                    tx_index: create.transaction_index as i32,
                    log_index: create.log_index as i32,
                    created_at: create.block_timestamp as i64,
                });

                create_batch.push(TokenBatchData {
                    token_id: (*create.token).clone(),
                    name: (*create.name).clone(),
                    symbol: (*create.symbol).clone(),
                    creator: (*create.creator).clone(),
                    description: create.token_metadata.description.clone(),
                    twitter: create.token_metadata.twitter.clone(),
                    telegram: create.token_metadata.telegram.clone(),
                    website: create.token_metadata.website.clone(),
                    image_uri: create.token_metadata.image_uri.clone(),
                    is_nsfw: create.token_metadata.is_nsfw,
                    version: "V2".to_string(),
                    market_type: "NADFUN".to_string(),
                    quote_id: (*create.quote_id).clone(),
                    virtual_native: create.virtual_quote_reserve.to_string(),
                    virtual_token: create.virtual_token_reserve.to_string(),
                    block_number: create.block_number as i64,
                    block_timestamp: create.block_timestamp as i64,
                    transaction_hash: (*create.transaction_hash).clone(),
                    log_index: create.log_index as i32,
                    tx_index: create.transaction_index as i32,
                });
            }
            V2CurveEvent::Graduate(graduate) => {
                error!(
                    "[CURVE_DBG] Graduate event received in receive: token={}, pool={}, tx={}, block={}",
                    graduate.token, graduate.pool, graduate.transaction_hash, graduate.block_number
                );
                graduate_events.push(graduate.clone());

                let block_num = graduate.block_number as i64;
                let native_price = cache_manager.get_quote_usd_price(&quote_id_str, block_num).await;

                let value = if let Some(price) = native_price {
                    (&*GRADUATE_FEE_AMOUNT / quote_decimals) * &*price
                } else {
                    error!(
                        "[CURVE] No price found for block {} in graduate event",
                        block_num
                    );
                    BigDecimal::from(0)
                };

                match cache_manager.get_token_creator(&graduate.token).await {
                    Ok(Some(creator)) => {
                        account_ids.insert(creator.clone());
                        point_batch.push(PointBatchData {
                            account_id: Arc::new(creator),
                            point_type: "GRADUATE",
                            value,
                            transaction_hash: Arc::clone(&graduate.transaction_hash),
                            tx_index: graduate.transaction_index as i32,
                            log_index: graduate.log_index as i32,
                            created_at: graduate.block_timestamp as i64,
                        });
                    }
                    Ok(None) => {
                        warn!(
                            "[CURVE] Creator not found for graduated token: {}",
                            graduate.token
                        );
                    }
                    Err(e) => {
                        error!(
                            "[CURVE] Failed to get creator for token {}: {}",
                            graduate.token, e
                        );
                    }
                }
            }
            V2CurveEvent::Sync(sync) => {
                let price = (&*sync.virtual_quote_reserve / &*sync.virtual_token_reserve)
                    .with_scale_round(10, RoundingMode::Up);

                chart_map.insert(
                    (*sync.transaction_hash).clone(),
                    ChartBatchData {
                        token_id: Arc::clone(&sync.token),
                        price,
                        volume: BigDecimal::from(0),
                        block_timestamp: sync.block_timestamp as i64,
                        block_number: sync.block_number as i64,
                        transaction_hash: Arc::clone(&sync.transaction_hash),
                        log_index: sync.log_index as i32,
                        tx_index: sync.transaction_index as i32,
                    },
                );

                let key = (
                    (*sync.transaction_hash).clone(),
                    sync.transaction_index as i32,
                );
                sync_reserves.entry(key).or_default().push(SyncReserve {
                    log_index: sync.log_index as i32,
                    reserve_quote: Arc::clone(&sync.virtual_quote_reserve),
                    reserve_token: Arc::clone(&sync.virtual_token_reserve),
                });

                sync_events.push(sync.clone());
            }
            V2CurveEvent::Buy(buy) => {
                account_ids.insert((*buy.tx_sender).clone());

                let (reserve_quote, reserve_token) = find_closest_reserve(
                    &sync_reserves,
                    &buy.transaction_hash,
                    buy.transaction_index as i32,
                    buy.log_index as i32,
                );

                let block_num = buy.block_number as i64;
                let price_opt = cache_manager.get_quote_usd_price(&quote_id_str, block_num).await;

                let value = if let Some(price) = &price_opt {
                    (&*buy.amount_in / quote_decimals) * &**price
                } else {
                    error!(
                        "[CURVE] No price found for block {} in buy event",
                        block_num
                    );
                    BigDecimal::from(0)
                };

                let market_type = giwa_market_type(&buy.market_type);

                let point_value = (&value * &curve_fee_bps) / BigDecimal::from(10000);

                swap_batch.push(SwapBatchData {
                    account_id: Arc::clone(&buy.tx_sender),
                    token_id: Arc::clone(&buy.token),
                    is_buy: true,
                    quote_amount: Arc::clone(&buy.amount_in),
                    token_amount: Arc::clone(&buy.amount_out),
                    reserve_quote,
                    reserve_token,
                    value,
                    market_type,
                    created_at: buy.block_timestamp as i64,
                    transaction_hash: Arc::clone(&buy.transaction_hash),
                    block_number: buy.block_number as i64,
                    log_index: buy.log_index as i32,
                    tx_index: buy.transaction_index as i32,
                });

                if let Some(chart_data) = chart_map.get_mut(&**buy.transaction_hash) {
                    chart_data.volume = (*buy.amount_in).clone();
                }

                point_batch.push(PointBatchData {
                    account_id: Arc::clone(&buy.tx_sender),
                    point_type: "CURVE",
                    value: point_value,
                    transaction_hash: Arc::clone(&buy.transaction_hash),
                    tx_index: buy.transaction_index as i32,
                    log_index: buy.log_index as i32,
                    created_at: buy.block_timestamp as i64,
                });

                // Fee history
                let fee_native =
                    (&*buy.amount_in * &curve_fee_bps) / BigDecimal::from(10000);
                let fee_usd = if let Some(price) = &price_opt {
                    (&fee_native / quote_decimals) * &**price
                } else {
                    BigDecimal::from(0)
                };
                fee_batch.push(FeeHistoryEvent {
                    transaction_hash: Arc::clone(&buy.transaction_hash),
                    log_index: buy.log_index,
                    account_id: Arc::clone(&buy.tx_sender),
                    token_id: Arc::clone(&buy.token),
                    quote_amount: Arc::new(fee_native),
                    usd_amount: Arc::new(fee_usd),
                    fee_type: giwa_curve_fee_type(true),
                    block_number: buy.block_number,
                    tx_index: buy.transaction_index,
                    block_timestamp: buy.block_timestamp,
                });
            }
            V2CurveEvent::Sell(sell) => {
                account_ids.insert((*sell.tx_sender).clone());

                let (reserve_quote, reserve_token) = find_closest_reserve(
                    &sync_reserves,
                    &sell.transaction_hash,
                    sell.transaction_index as i32,
                    sell.log_index as i32,
                );

                let block_num = sell.block_number as i64;
                let price_opt = cache_manager.get_quote_usd_price(&quote_id_str, block_num).await;

                let value = if let Some(price) = &price_opt {
                    (&*sell.amount_out / quote_decimals) * &**price
                } else {
                    error!(
                        "[CURVE] No price found for block {} in sell event",
                        block_num
                    );
                    BigDecimal::from(0)
                };

                let market_type = giwa_market_type(&sell.market_type);

                let point_value = (&value * &curve_fee_bps) / BigDecimal::from(10000);

                swap_batch.push(SwapBatchData {
                    account_id: Arc::clone(&sell.tx_sender),
                    token_id: Arc::clone(&sell.token),
                    is_buy: false,
                    quote_amount: Arc::clone(&sell.amount_out),
                    token_amount: Arc::clone(&sell.amount_in),
                    reserve_quote,
                    reserve_token,
                    value,
                    market_type,
                    created_at: sell.block_timestamp as i64,
                    transaction_hash: Arc::clone(&sell.transaction_hash),
                    block_number: sell.block_number as i64,
                    log_index: sell.log_index as i32,
                    tx_index: sell.transaction_index as i32,
                });

                if let Some(chart_data) = chart_map.get_mut(&**sell.transaction_hash) {
                    chart_data.volume = (*sell.amount_out).clone();
                }

                point_batch.push(PointBatchData {
                    account_id: Arc::clone(&sell.tx_sender),
                    point_type: "CURVE",
                    value: point_value,
                    transaction_hash: Arc::clone(&sell.transaction_hash),
                    tx_index: sell.transaction_index as i32,
                    log_index: sell.log_index as i32,
                    created_at: sell.block_timestamp as i64,
                });

                // Fee history
                let fee_native =
                    (&*sell.amount_out * &curve_fee_bps) / BigDecimal::from(10000);
                let fee_usd = if let Some(price) = &price_opt {
                    (&fee_native / quote_decimals) * &**price
                } else {
                    BigDecimal::from(0)
                };
                fee_batch.push(FeeHistoryEvent {
                    transaction_hash: Arc::clone(&sell.transaction_hash),
                    log_index: sell.log_index,
                    account_id: Arc::clone(&sell.tx_sender),
                    token_id: Arc::clone(&sell.token),
                    quote_amount: Arc::new(fee_native),
                    usd_amount: Arc::new(fee_usd),
                    fee_type: giwa_curve_fee_type(false),
                    block_number: sell.block_number,
                    tx_index: sell.transaction_index,
                    block_timestamp: sell.block_timestamp,
                });
            }
            V2CurveEvent::SnipingPenalty(penalty) => {
                sniping_batch.push(crate::db::postgres::controller::v2::SnipingPenaltyData {
                    token_id: (*penalty.token).clone(),
                    buyer: (*penalty.buyer).clone(),
                    sniping_fee: (*penalty.sniping_fee).clone(),
                    penalty_bps: (*penalty.penalty_bps).clone(),
                    transaction_hash: (*penalty.transaction_hash).clone(),
                    block_number: penalty.block_number as i64,
                    created_at: penalty.block_timestamp as i64,
                    log_index: penalty.log_index as i32,
                    tx_index: penalty.transaction_index as i32,
                });
            }
        }
    }

    // DB operations
    let account_controller = AccountController::new(db.clone());
    let token_controller = TokenController::new(db.clone());
    let market_controller = MarketController::new(db.clone());
    let pool_controller = crate::db::postgres::controller::pool::PoolController::new(db.clone());
    let fee_controller = FeeController::new(db.clone());
    let swap_controller = SwapController::new(db.clone());
    let chart_controller = ChartController::new(db.clone());
    let point_controller = PointController::new(db.clone());
    let v2_controller = crate::db::postgres::controller::v2::V2SnipingController::new(db.clone());

    let account_list: Vec<String> = account_ids.into_iter().collect();
    let chart_batch: Vec<ChartBatchData> = chart_map.into_values().collect();

    // 1. Account upsert
    if !account_list.is_empty() {
        if let Err(e) = account_controller.batch_upsert_accounts(&account_list).await {
            warn!(
                "[CURVE] Account batch upsert failed for token {}: {}",
                token, e
            );
        }
    }

    // 2. Token/Market creation
    if !create_batch.is_empty() {
        if let Err(e) = token_controller.batch_insert_tokens_and_markets(&create_batch).await {
            error!(
                "[CURVE] Token operation failed for token {}: {:#}",
                token, e
            );
        }
    }

    // 3. Market sync + graduate
    if let Some(last_sync) = sync_events.last() {
        let ath_price = sync_events
            .iter()
            .map(|s| &s.price)
            .max()
            .unwrap_or(&last_sync.price);

        let block_num = last_sync.block_number as i64;
        let native_price = cache_manager.get_quote_usd_price(&quote_id_str, block_num).await;

        let ath_price_usd = if let Some(price) = native_price {
            &**ath_price * &*price
        } else {
            BigDecimal::from(0)
        };

        let sync_data = CurveSyncData {
            token: (*last_sync.token).clone(),
            price: (*last_sync.price).clone(),
            reserve_token: (*last_sync.virtual_token_reserve).clone(),
            reserve_quote: (*last_sync.virtual_quote_reserve).clone(),
            block_timestamp: last_sync.block_timestamp as i64,
            market_type: "NADFUN".to_string(),
        };

        if let Err(e) = market_controller.handle_curve_sync(&sync_data, &ath_price_usd, ath_price).await {
            error!("[CURVE] Sync market operation failed: {:#}", e);
        }
    }

    if !graduate_events.is_empty() {
        let graduates_data: Vec<(String, String)> = graduate_events
            .iter()
            .map(|g| ((*g.token).clone(), (*g.pool).clone()))
            .collect();

        error!(
            "[CURVE_DBG] Graduate batch dispatch: count={}, entries={:?}",
            graduates_data.len(),
            graduates_data
        );

        if let Err(e) = market_controller
            .batch_handle_graduates(&graduates_data, "UNISWAPV3")
            .await
        {
            error!(
                "[CURVE] Graduate operation failed for token {}: {:#}",
                token, e
            );
        }

        // Insert pools for graduated tokens
        // V2 멀티 quote 지원: 토큰별 quote_id를 조회해서 token0/token1 결정.
        // Create 이후라 정상 흐름이면 항상 존재하나, miss 시 WMON fallback.
        use crate::db::postgres::controller::pool::PoolData;
        let mut pool_batch: Vec<PoolData> = Vec::with_capacity(graduate_events.len());
        for g in graduate_events.iter() {
            let token_str = (*g.token).clone();
            let quote_id = match cache_manager.get_token_quote_id(&token_str).await? {
                Some(quote) => quote,
                None => {
                    warn!(
                        "[CURVE] Graduate pool: quote_id not found for token={}, falling back to WMON",
                        token_str
                    );
                    (*crate::config::WNATIVE_ADDRESS).clone()
                }
            };
            // Solidity uint160 ordering = lowercase hex 비교; 저장값은 원본 casing 유지
            let (token0, token1) = if quote_id.to_lowercase() < token_str.to_lowercase() {
                (quote_id, token_str)
            } else {
                (token_str, quote_id)
            };
            pool_batch.push(PoolData {
                pool_id: (*g.pool).clone(),
                token0,
                token1,
                reserve0: BigDecimal::from(0),
                reserve1: BigDecimal::from(0),
                price: BigDecimal::from(0),
                created_at: g.block_timestamp as i64,
                block_number: g.block_number as i64,
                tx_hash: (*g.transaction_hash).clone(),
            });
        }

        if let Err(e) = pool_controller.batch_insert_pools(&pool_batch).await {
            error!(
                "[CURVE] Pool insert for graduates failed for token {}: {:#}",
                token, e
            );
        }
    }

    // 4. Parallel: swap, chart, point, sniping, fee
    let (swap_result, chart_result, point_result, sniping_result, fee_result) = tokio::join!(
        async {
            if !swap_batch.is_empty() {
                swap_controller.batch_insert_swaps(&swap_batch).await
            } else {
                Ok(())
            }
        },
        async {
            if !chart_batch.is_empty() {
                chart_controller.batch_insert_price_history(&chart_batch).await
            } else {
                Ok(())
            }
        },
        async {
            if !point_batch.is_empty() {
                point_controller.batch_insert_points(&point_batch).await
            } else {
                Ok(())
            }
        },
        async {
            if !sniping_batch.is_empty() {
                v2_controller.batch_insert_sniping_penalties(&sniping_batch).await
            } else {
                Ok(())
            }
        },
        async {
            if !fee_batch.is_empty() {
                fee_controller.batch_insert_fee_history(&fee_batch).await
            } else {
                Ok(())
            }
        },
    );

    if let Err(e) = swap_result {
        error!("[CURVE] Swap batch failed for token {}: {:#}", token, e);
    }
    if let Err(e) = chart_result {
        error!("[CURVE] Chart batch failed for token {}: {:#}", token, e);
    }
    if let Err(e) = point_result {
        error!("[CURVE] Point batch failed for token {}: {:#}", token, e);
    }
    if let Err(e) = sniping_result {
        error!(
            "[CURVE] Sniping penalty batch failed for token {}: {:#}",
            token, e
        );
    }
    if let Err(e) = fee_result {
        error!(
            "[CURVE] Fee history batch failed for token {}: {:#}",
            token, e
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{giwa_curve_fee_type, giwa_market_type};
    use crate::types::{fee::FeeType, v2::curve::MarketType};

    #[test]
    fn giwa_curve_uses_generic_database_categories() {
        assert_eq!(giwa_market_type(&MarketType::Curve), "NADFUN");
        assert_eq!(giwa_market_type(&MarketType::Dex), "UNISWAPV3");
        assert_eq!(giwa_curve_fee_type(true), FeeType::CurveBuy);
        assert_eq!(giwa_curve_fee_type(false), FeeType::CurveSell);
        assert_eq!(giwa_curve_fee_type(true).as_str(), "curve_buy");
        assert_eq!(giwa_curve_fee_type(false).as_str(), "curve_sell");
    }
}
