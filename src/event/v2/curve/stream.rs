use std::{sync::Arc, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;

use bigdecimal::RoundingMode;
use tokio::{task::JoinSet, time::Instant};

use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, BONDING_CURVE_ADDRESS, WNATIVE_ADDRESS},
    db::cache::CacheManager,
    event::get_block_timestamp,
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::v2::curve::{
        Buy, CurveSync, Graduate, MarketType, Sell, SnipingPenalty, V2CreateCurve, V2CurveEvent,
    },
    utils::{metadata::fetch_token_metadata, to_big_decimal},
};

use super::V2CurveEventChannel;

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    V2IBondingCurve,
    "abi/v2/BondingCurve.json"
}

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let mut total_events = 0;
    let (channel, receiver) = V2CurveEventChannel::new("curve_events");

    tokio::spawn(async move {
        if let Err(e) = super::receive::receive_events(receiver, event_type).await {
            error!("Failed to receive Curve events: {}", e);
        }
    });

    let client = RpcClient::instance()?;

    loop {
        let latest_block = client.get_cached_latest_block();
        let time = Instant::now();
        let BlockRange {
            from_block,
            to_block,
        } = STREAM_MANAGER
            .get_next_block_range(event_type, block_batch_size, latest_block)
            .await;

        if from_block > to_block {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let filter = Filter::new()
            .from_block(BlockNumberOrTag::Number(from_block))
            .to_block(BlockNumberOrTag::Number(to_block))
            .address(BONDING_CURVE_ADDRESS.parse::<Address>().unwrap())
            .events(vec![
                V2IBondingCurve::Create::SIGNATURE,
                V2IBondingCurve::Buy::SIGNATURE,
                V2IBondingCurve::Sell::SIGNATURE,
                V2IBondingCurve::Sync::SIGNATURE,
                V2IBondingCurve::Graduate::SIGNATURE,
                V2IBondingCurve::SnipingPenalty::SIGNATURE,
            ]);

        let logs = match client.get_logs(filter).await {
            Ok(logs) => logs,
            Err(e) => {
                block_batch_size /= 2;
                error!("[CURVE] Failed to get logs: {}", e);
                continue;
            }
        };

        let logs_count = logs.len();
        let mut events: Vec<V2CurveEvent> = Vec::new();

        let mut join_set = JoinSet::new();
        for log in logs {
            let cache_manager = match CacheManager::instance() {
                Ok(cm) => cm,
                Err(e) => {
                    error!("Failed to get CacheManager instance: {}", e);
                    continue;
                }
            };

            join_set.spawn(async move {
                let result = parse_log(log.clone(), client, cache_manager).await;
                (log, result)
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((log, parse_result)) => match parse_result {
                    Ok(event) => {
                        events.push(event);
                    }
                    Err(e) => {
                        use crate::event::error::SkippableError;
                        if !SkippableError::should_skip_curve(&e.to_string()) {
                            error!(
                                error = %e,
                                log = ?log,
                                "Failed to parse Curve log"
                            );
                        }
                    }
                },
                Err(join_err) => {
                    error!("Task join error: {}", join_err);
                }
            }
        }

        events.sort_by(|a, b| {
            (a.block_number(), a.transaction_index(), a.log_index()).cmp(&(
                b.block_number(),
                b.transaction_index(),
                b.log_index(),
            ))
        });

        let events_count = events.len();
        total_events += events_count;
        let elapsed_ms = time.elapsed().as_millis();

        if let Err(e) = channel.send(events, to_block, to_block).await {
            error!("Failed to send Curve events: {}", e);
            continue;
        }

        let logging_format = format!(
            "📊 {:?} STREAM: Blocks: from={} to={} | Logs: {} | Events: {} | Total Events: {} | Process time: {}ms",
            event_type, from_block, to_block, logs_count, events_count, total_events, elapsed_ms
        );
        warn!("{}", logging_format);

        block_batch_size = *BLOCK_BATCH_SIZE;

        STREAM_MANAGER
            .set_event_block_processed_block(event_type, to_block)
            .await;
    }
}

async fn parse_log(
    log: Log,
    client: &RpcClient,
    cache_manager: Arc<CacheManager>,
) -> Result<V2CurveEvent> {
    let transaction_hash = log
        .transaction_hash
        .ok_or(anyhow::anyhow!("No transaction hash"))?
        .to_string();

    let block_number = match log.block_number {
        Some(number) => number,
        None => client
            .get_latest_block_number()
            .await
            .map_err(|e| anyhow::anyhow!("Fail to get Curve block number {e:?}"))?,
    };

    let block_timestamp = match log.block_timestamp {
        Some(timestamp) => timestamp,
        None => get_block_timestamp(client, block_number)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Fail to get block timestamp for block {block_number}: {e:?}")
            })?,
    };

    let log_index = log.log_index.unwrap_or(u64::MAX);
    let transaction_index = log.transaction_index.unwrap_or(u64::MAX);

    match log.topic0() {
        Some(&V2IBondingCurve::Create::SIGNATURE_HASH) => {
            let V2IBondingCurve::Create {
                creator,
                token,
                pair,
                quoteToken,
                name,
                symbol,
                tokenURI,
                virtualQuoteReserve,
                virtualTokenReserve,
                minTokenReserve,
            } = log.log_decode()?.inner.data;

            let token_str = token.to_string();
            if !token_str.ends_with(&*crate::config::VANITY_ADDRESS_SUFFIX) {
                return Err(anyhow::anyhow!(
                    "Token address does not end with required suffix: {}",
                    &*crate::config::VANITY_ADDRESS_SUFFIX
                ));
            }

            let token_metadata = match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                fetch_token_metadata(&tokenURI),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => {
                    error!("Fail to fetch token metadata");
                    return Err(anyhow::anyhow!("Fail to fetch token metadata"));
                }
            };

            {
                let token_str = token.to_string();
                let pair_str = pair.to_string();
                let creator_str = creator.to_string();

                let (white_list_token_result, white_list_pool_result, token_creator_result) = tokio::join!(
                    cache_manager.insert_white_list_token(&token_str, true),
                    cache_manager.set_token_pool_flag(&pair_str, true),
                    cache_manager.insert_token_creator(&token_str, &creator_str)
                );

                if let Err(e) = white_list_token_result {
                    warn!("[CURVE] Failed to cache white list token: {}", e);
                }
                if let Err(e) = white_list_pool_result {
                    warn!("[CURVE] Failed to cache white list pool: {}", e);
                }
                if let Err(e) = token_creator_result {
                    warn!("[CURVE] Failed to cache token creator: {}", e);
                }
            }

            let tx_sender = match cache_manager.get_tx_sender(&transaction_hash).await {
                Ok(Some(sender)) => sender.to_string(),
                _ => creator.to_string(),
            };

            let create = V2CreateCurve {
                creator: Arc::new(creator.to_string()),
                token: Arc::new(token.to_string()),
                pair: Arc::new(pair.to_string()),
                quote_id: Arc::new(quoteToken.to_string()),
                name: Arc::new(name),
                symbol: Arc::new(symbol),
                token_uri: Arc::new(tokenURI),
                token_metadata,
                virtual_quote_reserve: Arc::new(to_big_decimal(virtualQuoteReserve)),
                virtual_token_reserve: Arc::new(to_big_decimal(virtualTokenReserve)),
                min_token_reserve: Arc::new(to_big_decimal(minTokenReserve)),
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
                tx_sender: Arc::new(tx_sender),
            };

            Ok(V2CurveEvent::Create(create))
        }

        Some(&V2IBondingCurve::Buy::SIGNATURE_HASH) => {
            let curve = log.address().to_string();
            let V2IBondingCurve::Buy {
                token,
                buyer,
                quoteIn,
                tokenOut,
            } = log.log_decode()?.inner.data;
            let token = token.to_string();

            let sender = cache_manager
                .resolve_actor(&transaction_hash, &buyer.to_string(), &token, true)
                .await
                .unwrap_or_else(|e| {
                    error!("[CURVE] Failed to resolve actor for Buy: {}", e);
                    buyer.to_string()
                });

            let buy = Buy {
                sender: Arc::new(sender.clone()),
                amount_in: Arc::new(to_big_decimal(quoteIn)),
                amount_out: Arc::new(to_big_decimal(tokenOut)),
                token: Arc::new(token),
                market: Arc::new(curve),
                market_type: MarketType::Curve,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
                tx_sender: Arc::new(sender),
            };

            Ok(V2CurveEvent::Buy(buy))
        }

        Some(&V2IBondingCurve::Sell::SIGNATURE_HASH) => {
            let curve = log.address().to_string();
            let V2IBondingCurve::Sell {
                token,
                seller,
                tokenIn,
                quoteOut,
            } = log.log_decode()?.inner.data;
            let token = token.to_string();

            let sender = cache_manager
                .resolve_actor(&transaction_hash, &seller.to_string(), &token, false)
                .await
                .unwrap_or_else(|e| {
                    error!("[CURVE] Failed to resolve actor for Sell: {}", e);
                    seller.to_string()
                });

            let sell = Sell {
                sender: Arc::new(sender.clone()),
                amount_in: Arc::new(to_big_decimal(tokenIn)),
                amount_out: Arc::new(to_big_decimal(quoteOut)),
                token: Arc::new(token),
                market: Arc::new(curve),
                market_type: MarketType::Curve,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
                tx_sender: Arc::new(sender),
            };

            Ok(V2CurveEvent::Sell(sell))
        }

        Some(&V2IBondingCurve::Sync::SIGNATURE_HASH) => {
            let V2IBondingCurve::Sync {
                token,
                realQuoteReserve,
                realTokenReserve,
                virtualQuoteReserve,
                virtualTokenReserve,
            } = log.log_decode()?.inner.data;

            let sync = CurveSync {
                token: Arc::new(token.to_string()),
                price: Arc::new(
                    (to_big_decimal(virtualQuoteReserve) / to_big_decimal(virtualTokenReserve))
                        .with_scale_round(10, RoundingMode::Up),
                ),
                real_quote_reserve: Arc::new(to_big_decimal(realQuoteReserve)),
                real_token_reserve: Arc::new(to_big_decimal(realTokenReserve)),
                virtual_quote_reserve: Arc::new(to_big_decimal(virtualQuoteReserve)),
                virtual_token_reserve: Arc::new(to_big_decimal(virtualTokenReserve)),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
            };

            Ok(V2CurveEvent::Sync(sync))
        }

        Some(&V2IBondingCurve::Graduate::SIGNATURE_HASH) => {
            let V2IBondingCurve::Graduate { token, pair } = log.log_decode()?.inner.data;

            let token = token.to_string();
            let pair = pair.to_string();

            error!(
                "[CURVE] Graduate decoded: token={}, pair={}, tx={}, block={}",
                token, pair, transaction_hash, block_number
            );

            // quoteToken이 WMON이 아닐 수 있으므로 market에서 quote_id 조회
            // fallback: WMON (Create 이후라 정상 흐름이면 항상 존재)
            let quote_id = match cache_manager.get_token_quote_id(&token).await? {
                Some(quote) => quote,
                None => {
                    warn!(
                        "[CURVE] Graduate: quote_id not found for token={}, falling back to WMON",
                        token
                    );
                    WNATIVE_ADDRESS.clone()
                }
            };

            // Solidity의 token0/token1 순서는 uint160 비교 = 소문자 hex 비교이므로
            // 비교용으로만 lowercase 사용하고, 저장값은 원본 casing 유지
            let (token0, token1) = if quote_id.to_lowercase() < token.to_lowercase() {
                (quote_id.as_str(), token.as_str())
            } else {
                (token.as_str(), quote_id.as_str())
            };
            {
                cache_manager
                    .insert_pool_pair(&pair, token0, token1)
                    .await?;

                cache_manager
                    .set_token_pool_flag(&pair, true)
                    .await?;
            }

            info!("[CURVE] graduate: token={}, pair={}", token, pair);

            let graduate = Graduate {
                token: Arc::new(token),
                pool: Arc::new(pair),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
            };

            Ok(V2CurveEvent::Graduate(graduate))
        }

        Some(&V2IBondingCurve::SnipingPenalty::SIGNATURE_HASH) => {
            let V2IBondingCurve::SnipingPenalty {
                token,
                buyer,
                snipingFee,
                penaltyBps,
            } = log.log_decode()?.inner.data;

            let penalty = SnipingPenalty {
                token: Arc::new(token.to_string()),
                buyer: Arc::new(buyer.to_string()),
                sniping_fee: Arc::new(to_big_decimal(snipingFee)),
                penalty_bps: Arc::new(to_big_decimal(penaltyBps)),
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            };

            Ok(V2CurveEvent::SnipingPenalty(penalty))
        }

        _ => Err(anyhow::anyhow!("Unknown Curve event type")),
    }
}
