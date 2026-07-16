use std::{collections::HashMap, sync::Arc, time::Instant};

use alloy::{primitives::Address, sol};
use bigdecimal::BigDecimal;

use crate::{
    client::RpcClient,
    db::{
        cache::CacheManager,
        postgres::{
            PostgresDatabase,
            controller::{
                account::AccountController,
                chart::{ChartBatchData, ChartController},
                fee::FeeController,
                market::{DexSyncData, MarketController},
                mint::{BurnBatchData, MintBatchData, MintController},
                point::{PointBatchData, PointController},
                swap::{SwapBatchData, SwapController},
            },
        },
    },
    sync::{EventType, receive::RECEIVE_MANAGER},
    types::v1::dex::{DexEvent, SetFeeProtocol},
    types::fee::{FeeHistoryEvent, FeeType},
    utils::to_big_decimal,
};

use anyhow::Result;

use crate::config::{DEX_ROUTER_FEE_RATE, WNATIVE_ADDRESS, get_quote_decimals};
use crate::metrics::MonitoredReceiver;
use tracing::{error, instrument, warn};

use super::DexEventBatch;

sol! {
    #[allow(missing_docs, clippy::too_many_arguments)]
    #[sol(rpc)]
    IToken,
    "abi/v1/IToken.json"
}
#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<DexEventBatch>,
    event_type: EventType,
) -> Result<()> {
    let mut total_events = 0;
    while let Some(events) = receiver.recv().await {
        let db = PostgresDatabase::instance()?;
        let DexEventBatch {
            events,
            to_block,
            latest_block,
        } = events;

        // Process events as they arrive through the channel
        RECEIVE_MANAGER
            .check_last_processed_block(to_block, EventType::Dex)
            .await;
        let time = Instant::now();
        let event_count = events.len();
        total_events += event_count;

        // Separate SetFeeProtocol events (they have no token)
        let (set_fee_events, token_events): (Vec<_>, Vec<_>) = events
            .into_iter()
            .partition(|e| matches!(e, DexEvent::SetFeeProtocol(_)));

        // Process SetFeeProtocol events separately
        if !set_fee_events.is_empty() {
            let set_fee_protocols: Vec<SetFeeProtocol> = set_fee_events
                .into_iter()
                .filter_map(|e| {
                    if let DexEvent::SetFeeProtocol(fee) = e {
                        Some(fee)
                    } else {
                        None
                    }
                })
                .collect();

            let fee_controller = FeeController::new(db.clone());
            if let Err(e) = fee_controller
                .batch_insert_set_fee_protocols(&set_fee_protocols)
                .await
            {
                error!(
                    "[DEX] Failed to batch insert {} SetFeeProtocol events: {:?}",
                    set_fee_protocols.len(),
                    e
                );
            }
        }

        // Group events by token and process in parallel
        let events_by_token = group_events_by_token(token_events);
        let token_count = events_by_token.len();

        // Process events for each token sequentially, but different tokens in parallel
        let handles: Vec<_> = events_by_token
            .into_iter()
            .map(|(token, events)| {
                let db = db.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_token_events(token.clone(), events, db).await {
                        error!(
                            "[DEX] Failed to process events for token {}: {:?}",
                            token, e
                        );
                    }
                })
            })
            .collect();

        // Wait for all token processing to complete
        for handle in handles {
            if let Err(e) = handle.await {
                total_events -= 1;
                error!("[DEX] Failed to join handle: {:?}", e);
            }
        }

        let elapsed_ms = time.elapsed().as_millis();
        warn!(
            "📊 {:?} Receiver: Events: {} | Tokens: {} | Total Events: {} | Current State: Processing | Process time: {}ms | Event Buffer: {} | To Block: {} | Latest Block: {}",
            event_type,
            event_count,
            token_count,
            total_events,
            elapsed_ms,
            0, // receiver queue length not available in MonitoredReceiver
            to_block,
            latest_block,
        );

        RECEIVE_MANAGER
            .set_last_processed_block(event_type, to_block, latest_block)
            .await;

        // Dex 이벤트 처리 완료 후 해당 블록 -1000 이하의 price 캐시 제거
        // 최근 1000개 블록의 price는 유지하여 다른 stream에서 조회 가능하도록 함
        if let Ok(cache_manager) = CacheManager::instance() {
            let cleanup_block = (to_block as i64).saturating_sub(1000);
            cache_manager
                .remove_prices_before_or_equal(cleanup_block)
                .await;
        }
    }
    error!("[DEX] Event receiver has been closed");

    Ok(())
}

// token별로 이벤트 그룹화
fn group_events_by_token(events: Vec<DexEvent>) -> HashMap<String, Vec<DexEvent>> {
    let mut events_by_token: HashMap<String, Vec<DexEvent>> = HashMap::new();

    for event in events {
        if let Some(token) = event.token().map(|t| t.to_string()) {
            events_by_token.entry(token).or_default().push(event);
        }
    }

    events_by_token
}

// Sync reserve 정보를 저장하는 구조체
#[derive(Debug, Clone)]
struct SyncReserve {
    log_index: i32,
    reserve_quote: Arc<BigDecimal>,
    reserve_token: Arc<BigDecimal>,
}

// Sync reserves에서 가장 가까운 reserve를 찾는 함수 (Arc로 메모리 최적화)
fn find_closest_reserve(
    sync_reserves: &std::collections::HashMap<(String, i32), Vec<SyncReserve>>,
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

// token별 이벤트 처리
async fn process_token_events(
    token: String,
    events: Vec<DexEvent>,
    db: Arc<PostgresDatabase>,
) -> Result<()> {
    use bigdecimal::BigDecimal;
    use std::collections::HashSet;

    if events.is_empty() {
        return Ok(());
    }

    // CacheManager에서 직접 price 조회
    use crate::db::cache::CacheManager;
    let cache_manager = CacheManager::instance()?;

    // 배치 데이터를 수집할 벡터
    let mut swap_batch = Vec::new();
    let mut point_batch = Vec::new();
    let mut mint_batch = Vec::new();
    let mut burn_batch = Vec::new();
    let mut fee_batch: Vec<FeeHistoryEvent> = Vec::new();

    // transaction_hash별로 Chart 데이터를 모을 HashMap
    let mut chart_map: HashMap<String, ChartBatchData> = HashMap::new();

    // Sync의 reserves를 저장할 HashMap - O(1) 조회를 위해 (tx_hash, tx_index)를 키로 사용
    let mut sync_reserves: HashMap<(String, i32), Vec<SyncReserve>> = HashMap::new();

    // Sync 이벤트들 (마지막만 처리)
    let mut sync_events = Vec::new();

    // Account ID 수집 (중복 제거)
    let mut account_ids: HashSet<String> = HashSet::new();

    // 1단계: 이벤트 타입별로 분류하고 배치 데이터 수집
    for event in events {
        match &event {
            DexEvent::Sync(sync) => {
                // Sync에서 price를 chart_map에 추가
                chart_map.insert(
                    (*sync.transaction_hash).clone(),
                    ChartBatchData {
                        token_id: Arc::clone(&sync.token),
                        price: (*sync.price).clone(),
                        volume: BigDecimal::from(0), // 초기 volume은 0
                        block_timestamp: sync.block_timestamp as i64,
                        block_number: sync.block_number as i64,
                        transaction_hash: Arc::clone(&sync.transaction_hash),
                        log_index: sync.log_index as i32,
                        tx_index: sync.transaction_index as i32,
                    },
                );

                // Sync의 reserves를 저장 (Buy/Sell에서 사용)
                let key = (
                    (*sync.transaction_hash).clone(),
                    sync.transaction_index as i32,
                );
                sync_reserves.entry(key).or_default().push(SyncReserve {
                    log_index: sync.log_index as i32,
                    reserve_quote: Arc::clone(&sync.reserve_quote),
                    reserve_token: Arc::clone(&sync.reserve_token),
                });

                // Sync는 Market 업데이트 필요
                sync_events.push(sync.clone());
            }
            DexEvent::SwapBuy(buy) => {
                // Account 수집 (tx_sender 사용)
                account_ids.insert((*buy.tx_sender).clone());

                // Sync의 reserves 가져오기 (같은 tx_hash, tx_index이고 log_index < buy.log_index인 것 중 가장 가까운 것)
                let (reserve_quote, reserve_token) = find_closest_reserve(
                    &sync_reserves,
                    &buy.transaction_hash,
                    buy.transaction_index as i32,
                    buy.log_index as i32,
                );

                // value 계산: (quote_amount / 10^18) * price
                let block_num = buy.block_number as i64;
                let price_opt = match cache_manager.get_price(block_num).await {
                    Some(price) => Some(Arc::clone(&price)),
                    None => match cache_manager.get_latest_price_before(block_num).await {
                        Some(price) => Some(Arc::clone(&price)),
                        None => match cache_manager.get_latest_price().await {
                            Some(price) => Some(Arc::clone(&price)),
                            None => cache_manager
                                .get_price_from_db(block_num)
                                .await
                                .map(Arc::new),
                        },
                    },
                };

                let value = if let Some(ref price) = price_opt {

                    (&*buy.amount_in / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
                } else {
                    error!("[DEX] No price found for block {} in buy event", block_num);
                    BigDecimal::from(0)
                };

                // Swap 데이터 추가
                swap_batch.push(SwapBatchData {
                    account_id: Arc::clone(&buy.tx_sender),
                    token_id: Arc::clone(&buy.token),
                    is_buy: true,
                    quote_amount: Arc::clone(&buy.amount_in),
                    token_amount: Arc::clone(&buy.amount_out),
                    reserve_quote,
                    reserve_token,
                    value,
                    market_type: "DEX",
                    created_at: buy.block_timestamp as i64,
                    transaction_hash: Arc::clone(&buy.transaction_hash),
                    block_number: buy.block_number as i64,
                    log_index: buy.log_index as i32,
                    tx_index: buy.transaction_index as i32,
                });

                // Chart 데이터: 같은 transaction_hash의 volume 업데이트
                if let Some(chart_data) = chart_map.get_mut(&**buy.transaction_hash) {
                    chart_data.volume = (*buy.amount_in).clone();
                }

                // Fee 데이터 추가 (Pool SwapBuy: fee = amount_in * 1%, 이벤트의 amount_in은 fee 포함)
                let fee_native = &*buy.amount_in / BigDecimal::from(100);
                let fee_usd = if let Some(price) = &price_opt {

                    (&fee_native / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
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
                    fee_type: FeeType::SwapBuy,
                    block_number: buy.block_number,
                    tx_index: buy.transaction_index,
                    block_timestamp: buy.block_timestamp,
                });
            }
            DexEvent::SwapSell(sell) => {
                // Account 수집 (tx_sender 사용)
                account_ids.insert((*sell.tx_sender).clone());

                // Sync의 reserves 가져오기 (같은 tx_hash, tx_index이고 log_index < sell.log_index인 것 중 가장 가까운 것)
                let (reserve_quote, reserve_token) = find_closest_reserve(
                    &sync_reserves,
                    &sell.transaction_hash,
                    sell.transaction_index as i32,
                    sell.log_index as i32,
                );

                // value 계산: (quote_amount / 10^18) * price
                let block_num = sell.block_number as i64;
                let price_opt = match cache_manager.get_price(block_num).await {
                    Some(price) => Some(Arc::clone(&price)),
                    None => match cache_manager.get_latest_price_before(block_num).await {
                        Some(price) => Some(Arc::clone(&price)),
                        None => match cache_manager.get_latest_price().await {
                            Some(price) => Some(Arc::clone(&price)),
                            None => cache_manager
                                .get_price_from_db(block_num)
                                .await
                                .map(Arc::new),
                        },
                    },
                };

                let value = if let Some(price) = &price_opt {

                    (&*sell.amount_out / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
                } else {
                    error!("[DEX] No price found for block {} in sell event", block_num);
                    BigDecimal::from(0)
                };

                // Swap 데이터 추가
                swap_batch.push(SwapBatchData {
                    account_id: Arc::clone(&sell.tx_sender),
                    token_id: Arc::clone(&sell.token),
                    is_buy: false,
                    quote_amount: Arc::clone(&sell.amount_out),
                    token_amount: Arc::clone(&sell.amount_in),
                    reserve_quote,
                    reserve_token,
                    value,
                    market_type: "DEX",
                    created_at: sell.block_timestamp as i64,
                    transaction_hash: Arc::clone(&sell.transaction_hash),
                    block_number: sell.block_number as i64,
                    log_index: sell.log_index as i32,
                    tx_index: sell.transaction_index as i32,
                });

                // Chart 데이터: 같은 transaction_hash의 volume 업데이트
                if let Some(chart_data) = chart_map.get_mut(&**sell.transaction_hash) {
                    chart_data.volume = (*sell.amount_out).clone();
                }

                // Fee 데이터 추가 (Pool SwapSell: fee = amount_out * 1%)
                let fee_native = &*sell.amount_out / BigDecimal::from(100);
                let fee_usd = if let Some(price) = &price_opt {

                    (&fee_native / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
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
                    fee_type: FeeType::SwapSell,
                    block_number: sell.block_number,
                    tx_index: sell.transaction_index,
                    block_timestamp: sell.block_timestamp,
                });
            }
            DexEvent::RouterBuy(buy) => {
                // Account 수집 (tx_sender 사용)
                account_ids.insert((*buy.tx_sender).clone());

                // Point 데이터 추가: value = (quote_amount * price * DEX_ROUTER_FEE_RATE) / 100
                let block_num = buy.block_number as i64;
                let price_opt = match cache_manager.get_price(block_num).await {
                    Some(price) => Some(Arc::clone(&price)),
                    None => match cache_manager.get_latest_price_before(block_num).await {
                        Some(price) => Some(Arc::clone(&price)),
                        None => match cache_manager.get_latest_price().await {
                            Some(price) => Some(Arc::clone(&price)),
                            None => cache_manager
                                .get_price_from_db(block_num)
                                .await
                                .map(Arc::new),
                        },
                    },
                };

                let point_value = if let Some(price) = &price_opt {
                    ((&*buy.amount_in / get_quote_decimals(&WNATIVE_ADDRESS))
                        * &**price
                        * &*DEX_ROUTER_FEE_RATE)
                        / BigDecimal::from(100)
                } else {
                    error!("[DEX] No price found for block {} in router buy", block_num);
                    BigDecimal::from(0)
                };

                point_batch.push(PointBatchData {
                    account_id: Arc::clone(&buy.tx_sender),
                    point_type: "DEX",
                    value: point_value,
                    transaction_hash: Arc::clone(&buy.transaction_hash),
                    tx_index: buy.transaction_index as i32,
                    log_index: buy.log_index as i32,
                    created_at: buy.block_timestamp as i64,
                });

                // Fee 데이터 추가 (DexRouterBuy: amount_in * 0.5%)
                let fee_native =
                    (&*buy.amount_in * &*DEX_ROUTER_FEE_RATE) / BigDecimal::from(100);
                let fee_usd = if let Some(price) = &price_opt {

                    (&fee_native / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
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
                    fee_type: FeeType::DexRouterBuy,
                    block_number: buy.block_number,
                    tx_index: buy.transaction_index,
                    block_timestamp: buy.block_timestamp,
                });
            }
            DexEvent::RouterSell(sell) => {
                // Account 수집 (tx_sender 사용)
                account_ids.insert((*sell.tx_sender).clone());

                // Point 데이터 추가: value = (quote_amount * price * DEX_ROUTER_FEE_RATE) / 100
                let block_num = sell.block_number as i64;
                let native_price = match cache_manager.get_price(block_num).await {
                    Some(price) => Some(Arc::clone(&price)),
                    None => match cache_manager.get_latest_price_before(block_num).await {
                        Some(price) => Some(Arc::clone(&price)),
                        None => match cache_manager.get_latest_price().await {
                            Some(price) => Some(Arc::clone(&price)),
                            None => cache_manager
                                .get_price_from_db(block_num)
                                .await
                                .map(Arc::new),
                        },
                    },
                };

                let point_value = if let Some(ref price) = native_price {
                    ((&*sell.amount_out / get_quote_decimals(&WNATIVE_ADDRESS))
                        * &**price
                        * &*DEX_ROUTER_FEE_RATE)
                        / BigDecimal::from(100)
                } else {
                    error!(
                        "[DEX] No price found for block {} in router sell",
                        block_num
                    );
                    BigDecimal::from(0)
                };

                point_batch.push(PointBatchData {
                    account_id: Arc::clone(&sell.tx_sender),
                    point_type: "DEX",
                    value: point_value,
                    transaction_hash: Arc::clone(&sell.transaction_hash),
                    tx_index: sell.transaction_index as i32,
                    log_index: sell.log_index as i32,
                    created_at: sell.block_timestamp as i64,
                });

                // Fee 데이터 추가 (DexRouterSell: amount_out * 0.5%)
                let fee_native =
                    (&*sell.amount_out * &*DEX_ROUTER_FEE_RATE) / BigDecimal::from(100);
                let fee_usd = if let Some(ref price) = native_price {

                    (&fee_native / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
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
                    fee_type: FeeType::DexRouterSell,
                    block_number: sell.block_number,
                    tx_index: sell.transaction_index,
                    block_timestamp: sell.block_timestamp,
                });
            }
            DexEvent::Mint(mint) => {
                // Account 수집
                account_ids.insert((*mint.account_id).clone());

                // Mint 데이터 추가
                mint_batch.push(MintBatchData {
                    token_id: Arc::clone(&mint.token_id),
                    account_id: Arc::clone(&mint.account_id),
                    market_id: Arc::clone(&mint.market_id),
                    quote_amount: Arc::clone(&mint.quote_amount),
                    token_amount: Arc::clone(&mint.token_amount),
                    reserve_quote: Arc::clone(&mint.reserve_quote),
                    reserve_token: Arc::clone(&mint.reserve_token),
                    created_at: mint.block_timestamp as i64,
                    transaction_hash: Arc::clone(&mint.transaction_hash),
                    block_number: mint.block_number as i64,
                    tx_index: mint.transaction_index as i32,
                    log_index: mint.log_index as i32,
                });
            }
            DexEvent::Burn(burn) => {
                // Account 수집
                account_ids.insert((*burn.account_id).clone());

                // Burn 데이터 추가
                burn_batch.push(BurnBatchData {
                    token_id: Arc::clone(&burn.token_id),
                    account_id: Arc::clone(&burn.account_id),
                    market_id: Arc::clone(&burn.market_id),
                    quote_amount: Arc::clone(&burn.quote_amount),
                    token_amount: Arc::clone(&burn.token_amount),
                    reserve_quote: Arc::clone(&burn.reserve_quote),
                    reserve_token: Arc::clone(&burn.reserve_token),
                    created_at: burn.block_timestamp as i64,
                    transaction_hash: Arc::clone(&burn.transaction_hash),
                    block_number: burn.block_number as i64,
                    tx_index: burn.transaction_index as i32,
                    log_index: burn.log_index as i32,
                });
            }
            DexEvent::SetFeeProtocol(_) => {
                // SetFeeProtocol events are handled separately before grouping by token
                // This branch should never be reached
            }
        }
    }

    // 2단계: 모든 DB 작업을 병렬로 처리 (FK 제약 없음)
    let account_controller = AccountController::new(db.clone());
    let market_controller = MarketController::new(db.clone());
    let swap_controller = SwapController::new(db.clone());
    let chart_controller = ChartController::new(db.clone());
    let point_controller = PointController::new(db.clone());
    let mint_controller = MintController::new(db.clone());
    let fee_controller = FeeController::new(db.clone());

    let account_list: Vec<String> = account_ids.into_iter().collect();
    let chart_batch: Vec<ChartBatchData> = chart_map.into_values().collect();

    // Account upsert
    let account_operation = async {
        if !account_list.is_empty() {
            account_controller
                .batch_upsert_accounts(&account_list)
                .await
        } else {
            Ok(())
        }
    };

    // Sync Market 업데이트
    let sync_operation = async {
        if let Some(last_sync) = sync_events.last() {
            // ATH price (native 기준)
            let ath_price = sync_events
                .iter()
                .map(|s| &s.price)
                .max()
                .unwrap_or(&last_sync.price);

            // ATH price를 USD로 변환
            let block_num = last_sync.block_number as i64;
            let native_price = match cache_manager.get_price(block_num).await {
                Some(price) => Some(Arc::clone(&price)),
                None => match cache_manager.get_latest_price_before(block_num).await {
                    Some(price) => Some(Arc::clone(&price)),
                    None => match cache_manager.get_latest_price().await {
                        Some(price) => Some(Arc::clone(&price)),
                        None => cache_manager
                            .get_price_from_db(block_num)
                            .await
                            .map(Arc::new),
                    },
                },
            };

            let ath_price_usd = if let Some(price) = native_price {
                &**ath_price * &*price
            } else {
                error!(
                    "[DEX] No price found for block {} in sync event for ath_price_usd",
                    block_num
                );
                BigDecimal::from(0)
            };

            // Pool의 실제 balance 조회해서 업데이트
            match fetch_pool_balances(&last_sync.pool, &last_sync.token).await {
                Ok((actual_reserve_quote, actual_reserve_token)) => {
                    let sync_data = DexSyncData {
                        token: (*last_sync.token).clone(),
                        price: (*last_sync.price).clone(),
                        reserve_quote: actual_reserve_quote,
                        reserve_token: actual_reserve_token,
                        block_timestamp: last_sync.block_timestamp as i64,
                    };

                    market_controller
                        .handle_dex_sync(&sync_data, &ath_price_usd, ath_price)
                        .await
                }
                Err(e) => {
                    warn!(
                        "[DEX] Failed to fetch pool balances for pool={}, token={} after all retries: {:#}. Using sync event reserves.",
                        last_sync.pool, last_sync.token, e
                    );
                    let sync_data = DexSyncData {
                        token: (*last_sync.token).clone(),
                        price: (*last_sync.price).clone(),
                        reserve_quote: (*last_sync.reserve_quote).clone(),
                        reserve_token: (*last_sync.reserve_token).clone(),
                        block_timestamp: last_sync.block_timestamp as i64,
                    };

                    market_controller
                        .handle_dex_sync(&sync_data, &ath_price_usd, ath_price)
                        .await
                }
            }
        } else {
            Ok(())
        }
    };

    // Swap insert
    let swap_operation = async {
        if !swap_batch.is_empty() {
            swap_controller.batch_insert_swaps(&swap_batch).await
        } else {
            Ok(())
        }
    };

    // Chart insert
    let chart_operation = async {
        if !chart_batch.is_empty() {
            chart_controller
                .batch_insert_price_history(&chart_batch)
                .await
        } else {
            Ok(())
        }
    };

    // Point insert
    let point_operation = async {
        if !point_batch.is_empty() {
            point_controller.batch_insert_points(&point_batch).await
        } else {
            Ok(())
        }
    };

    // Mint insert
    let mint_operation = async {
        if !mint_batch.is_empty() {
            mint_controller.batch_insert_mints(&mint_batch).await
        } else {
            Ok(())
        }
    };

    // Burn insert
    let burn_operation = async {
        if !burn_batch.is_empty() {
            mint_controller.batch_insert_burns(&burn_batch).await
        } else {
            Ok(())
        }
    };

    // Fee insert
    let fee_operation = async {
        if !fee_batch.is_empty() {
            fee_controller.batch_insert_fee_history(&fee_batch).await
        } else {
            Ok(())
        }
    };

    // 순차 처리 (트리거 및 데이터 의존성 고려)

    // 1. Account (모든 작업이 참조)
    if let Err(e) = account_operation.await {
        warn!(
            "[DEX] Account batch upsert failed for token {}: {}. Continuing...",
            token, e
        );
    }

    // 2. Market 업데이트 (Sync)
    if let Err(e) = sync_operation.await {
        error!("[DEX] Sync operation failed for token {}: {:#}", token, e);
    }

    // 3. 나머지 병렬 처리 (서로 독립적)
    let (swap_result, chart_result, point_result, mint_result, burn_result, fee_result) =
        tokio::join!(
            swap_operation,
            chart_operation,
            point_operation,
            mint_operation,
            burn_operation,
            fee_operation
        );

    if let Err(e) = swap_result {
        error!(
            "[DEX] Swap batch operation failed for token {}: {:#}",
            token, e
        );
    }
    if let Err(e) = chart_result {
        error!(
            "[DEX] Chart batch operation failed for token {}: {:#}",
            token, e
        );
    }
    if let Err(e) = point_result {
        error!(
            "[DEX] Point batch operation failed for token {}: {:#}",
            token, e
        );
    }
    if let Err(e) = mint_result {
        error!(
            "[DEX] Mint batch operation failed for token {}: {:#}",
            token, e
        );
    }
    if let Err(e) = burn_result {
        error!(
            "[DEX] Burn batch operation failed for token {}: {:#}",
            token, e
        );
    }
    if let Err(e) = fee_result {
        error!(
            "[DEX] Fee batch operation failed for token {}: {:#}",
            token, e
        );
    }

    Ok(())
}

/// Pool의 실제 token과 WNATIVE balance를 조회
async fn fetch_pool_balances(
    pool_address: &str,
    token_address: &str,
) -> Result<(bigdecimal::BigDecimal, bigdecimal::BigDecimal)> {
    use crate::config::WNATIVE_ADDRESS;

    let client = RpcClient::instance()?;
    let pool_addr = pool_address.parse::<Address>()?;
    let token_addr = token_address.parse::<Address>()?;
    let wnative_addr = WNATIVE_ADDRESS.parse::<Address>()?;

    // balanceOf 호출 데이터 생성
    let token_balance_call = IToken::balanceOfCall { account: pool_addr };
    let wnative_balance_call = IToken::balanceOfCall { account: pool_addr };

    // 병렬로 두 balance 조회
    let (token_balance, wnative_balance) = tokio::try_join!(
        client.call_contract(token_balance_call, token_addr),
        client.call_contract(wnative_balance_call, wnative_addr)
    )?;

    Ok((
        to_big_decimal(wnative_balance),
        to_big_decimal(token_balance),
    ))
}
