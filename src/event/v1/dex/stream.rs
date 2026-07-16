use std::{sync::Arc, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    rpc::types::{BlockId, Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;

use bigdecimal::{BigDecimal, RoundingMode};

use tokio::time::sleep;
use tokio::{task::JoinSet, time::Instant};
use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, DEX_ROUTER_ADDRESS, WNATIVE_ADDRESS},
    db::cache::CacheManager,
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::v1::{
        curve::{MarketType, Sell},
        dex::{DexBurn, DexEvent, DexMint, DexRouterBuy, DexRouterSell, DexSync, SetFeeProtocol},
    },
    utils::to_big_decimal,
};
use crate::{event::get_block_timestamp, types::v1::curve::Buy};

use super::{DexEventChannel, receive::receive_events};

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    ICapricornCLPool,
    "abi/v1/ICapricornCLPool.json"
}
sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    IDexRouter,
    "abi/v1/IDexRouter.json"
}

#[instrument()]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    let mut block_batch_size = *BLOCK_BATCH_SIZE;

    let (channel, receiver) = DexEventChannel::new("dex_events");
    tokio::spawn(async move {
        if let Err(e) = receive_events(receiver, event_type).await {
            error!("Failed to receive dex events: {}", e);
        }
    });
    let mut total_events = 0;

    let client = RpcClient::instance()?;
    // let mut stream = client.get_stream().await?;

    // while let Some(block) = stream.next().await {
    loop {
        let latest_block = client.get_cached_latest_block();
        let BlockRange {
            from_block,
            to_block,
        } = STREAM_MANAGER
            .get_next_block_range(event_type, block_batch_size, latest_block)
            .await;

        if from_block > to_block {
            continue;
        }
        let time = Instant::now();
        let filter = Filter::new()
            .from_block(BlockNumberOrTag::Number(from_block))
            .to_block(BlockNumberOrTag::Number(to_block))
            .events(vec![
                ICapricornCLPool::Swap::SIGNATURE,
                ICapricornCLPool::Mint::SIGNATURE,
                ICapricornCLPool::Burn::SIGNATURE,
                ICapricornCLPool::SetFeeProtocol::SIGNATURE,
                IDexRouter::DexRouterBuy::SIGNATURE,
                IDexRouter::DexRouterSell::SIGNATURE,
            ]);

        // 6) Fetch logs.
        let logs = match client.get_logs(filter).await {
            Ok(logs) => logs,
            Err(e) => {
                error!(
                    "[DEX] Failed to get logs: {} batch_size: {}",
                    e, block_batch_size
                );
                block_batch_size /= 2;
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let logs_count = logs.len();
        // 7) Parse logs into DexEvents with parallel processing using tokio
        let mut events: Vec<DexEvent> = Vec::new();

        // Process Other events with tokio parallel tasks
        let mut log_join_set = JoinSet::new();
        for log in logs {
            let cache_manager = CacheManager::instance()?;
            log_join_set.spawn(async move {
                let result = parse_log(&log, client, cache_manager).await;
                (log, result)
            });
        }

        // Collect Other event results
        while let Some(result) = log_join_set.join_next().await {
            match result {
                Ok((log, parse_result)) => match parse_result {
                    Ok(event_vec) => {
                        // event_vec의 모든 이벤트를 events에 추가
                        for event in event_vec {
                            events.push(event);
                        }
                    }
                    Err(e) => {
                        use crate::event::error::SkippableError;
                        if !SkippableError::should_skip_dex(&e.to_string()) {
                            error!(
                                error = %e,
                                log = ?log,
                                "[DEX] Failed to parse dex log"
                            );
                        }
                    }
                },
                Err(join_err) => {
                    error!("Task join error: {}", join_err);
                }
            }
        }

        // 이벤트 정렬: block_number -> transaction_index -> log_index 순서로 정렬
        events.sort_by(|a, b| {
            (a.block_number(), a.transaction_index(), a.log_index()).cmp(&(
                b.block_number(),
                b.transaction_index(),
                b.log_index(),
            ))
        });

        // Get stats before sending events
        let events_count = events.len();
        total_events += events_count;
        let elapsed_ms = time.elapsed().as_millis();

        if let Err(e) = channel.send(events, to_block, to_block).await {
            error!("[DEX] Failed to send events: {}", e);
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
    log: &Log,
    client: &RpcClient,
    cache_manager: Arc<CacheManager>,
) -> Result<Vec<DexEvent>> {
    let transaction_hash = log
        .transaction_hash
        .ok_or_else(|| {
            error!("No transaction hash found in log");
            anyhow::anyhow!("No transaction hash")
        })?
        .to_string();

    let block_number = match log.block_number {
        Some(number) => number,
        None => client.get_latest_block_number().await.map_err(|e| {
            error!("[DEX] Failed to get block number: {:?}, Log: {:?}", e, log);
            anyhow::anyhow!("Failed to get block number {e:?}  \n Log{log:?}")
        })?,
    };

    let block_timestamp = match log.block_timestamp {
        Some(timestamp) => timestamp,
        None => get_block_timestamp(client, block_number)
            .await
            .map_err(|e| {
                error!(
                    "[DEX] Failed to get block timestamp for block {block_number}: {e:?}\nLog: {log:?}"
                );
                anyhow::anyhow!(
                    "Failed to get block timestamp for block {block_number}: {e:?}\nLog: {log:?}"
                )
            })?,
    };
    let log_index = log.log_index.unwrap_or(u64::MAX); // 또는 unwrap_or(0)
    let transaction_index = log.transaction_index.unwrap_or(u64::MAX);
    match log.topic0() {
        Some(&ICapricornCLPool::Swap::SIGNATURE_HASH) => {
            let pool = log.address().to_string();
            let is_whitelist_dex = cache_manager.check_token_pool(&pool).await?;
            if !is_whitelist_dex {
                return Err(anyhow::anyhow!("Not a white list dex address"));
            }

            let pool_pair = cache_manager.get_pool_pair(&pool).await?;
            let (token0, token1) =
                pool_pair.ok_or_else(|| anyhow::anyhow!("DEX pair not found"))?;

            let ICapricornCLPool::Swap {
                sender: event_sender,
                recipient,
                amount0,
                amount1,
                sqrtPriceX96,
                liquidity,
                tick,
                ..
            } = log.log_decode()?.inner.data;

            let token0_is_mon = token0 == *WNATIVE_ADDRESS;

            // Determine swap direction first (needed for resolve_actor)
            let is_buy = match (token0_is_mon, amount0.is_positive()) {
                (true, true) => true,
                (true, false) => false,
                (false, true) => false,
                (false, false) => true,
            };

            let swap_token = if token0_is_mon { &token1 } else { &token0 };

            let sender = cache_manager
                .resolve_actor(&transaction_hash, &event_sender.to_string(), swap_token, is_buy)
                .await
                .unwrap_or_else(|e| {
                    error!("[DEX] Failed to resolve actor for Swap: {}", e);
                    event_sender.to_string()
                });

            info!(
                "DEX Swap: pool={}, sender={}, amount0={}, amount1={}, sqrtPriceX96={}, liquidity={}, tick={}",
                pool, sender, amount0, amount1, sqrtPriceX96, liquidity, tick
            );

            // Determine token address and swap direction
            let (token, amount_in, amount_out, is_buy) =
                match (token0_is_mon, amount0.is_positive()) {
                    (true, true) => {
                        // token0 is native, native in (+), ERC20 out (-) => Buy
                        (
                            token1.clone(),
                            to_big_decimal(amount0.abs()),
                            to_big_decimal(amount1.abs()),
                            true,
                        )
                    }
                    (true, false) => {
                        // token0 is native, native out (-), ERC20 in (+) => Sell
                        (
                            token1.clone(),
                            to_big_decimal(amount1.abs()),
                            to_big_decimal(amount0.abs()),
                            false,
                        )
                    }
                    (false, true) => {
                        // token1 is native, ERC20 in (+), native out (-) => Sell
                        (
                            token0.clone(),
                            to_big_decimal(amount0.abs()),
                            to_big_decimal(amount1.abs()),
                            false,
                        )
                    }
                    (false, false) => {
                        // token1 is native, ERC20 out (-), native in (+) => Buy
                        (
                            token0.clone(),
                            to_big_decimal(amount1.abs()),
                            to_big_decimal(amount0.abs()),
                            true,
                        )
                    }
                };

            let price = calculate_mon_token_price(to_big_decimal(sqrtPriceX96), token0_is_mon)
                .with_scale_round(10, RoundingMode::Up);

            // Calculate virtual reserves from liquidity and sqrtPriceX96
            // sqrtPrice = sqrtPriceX96 / 2^96
            let sqrt_price_x96_decimal = to_big_decimal(sqrtPriceX96);
            let two_pow_96 = BigDecimal::from(2u128.pow(96));
            let sqrt_price = &sqrt_price_x96_decimal / &two_pow_96;
            let liquidity_decimal = BigDecimal::from(liquidity);

            // Virtual reserves calculation:
            // reserve0 = L / sqrtPrice
            // reserve1 = L * sqrtPrice
            let (reserve_quote, reserve_token) = if token0_is_mon {
                // token0 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down); // native reserve (정수)
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down); // token reserve (정수)
                (reserve0, reserve1)
            } else {
                // token1 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down); // token reserve (정수)
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down); // native reserve (정수)
                (reserve1, reserve0)
            };

            // Sync의 log_index를 Swap보다 1 작게 설정 (receive에서 reserve 매칭 가능하도록)
            // 실제 Swap 이벤트에서 계산된 reserve이므로, Swap 직전의 상태로 간주
            let sync_log_index = if log_index > 0 { log_index - 1 } else { 0 };

            let sync_event = DexEvent::from(DexSync {
                token: Arc::new(token.clone()),
                pool: Arc::new(pool.clone()),
                price: Arc::new(price),
                reserve_quote: Arc::new(reserve_quote),
                reserve_token: Arc::new(reserve_token),
                transaction_hash: Arc::new(transaction_hash.clone()),
                block_number,
                block_timestamp,
                log_index: sync_log_index,
                transaction_index,
            });

            let swap_event = if is_buy {
                DexEvent::from(Buy {
                    sender: Arc::new(sender.clone()),
                    to: Some(Arc::new(recipient.to_string())),
                    amount_in: Arc::new(amount_in),
                    amount_out: Arc::new(amount_out),
                    token: Arc::new(token),
                    market: Arc::new(pool),
                    market_type: MarketType::DEX,
                    transaction_hash: Arc::new(transaction_hash),
                    block_number,
                    block_timestamp,
                    log_index,
                    transaction_index,
                    tx_sender: Arc::new(sender),
                })
            } else {
                DexEvent::from(Sell {
                    sender: Arc::new(sender.clone()),
                    to: Some(Arc::new(recipient.to_string())),
                    amount_in: Arc::new(amount_in),
                    amount_out: Arc::new(amount_out),
                    token: Arc::new(token),
                    market: Arc::new(pool),
                    market_type: MarketType::DEX,
                    transaction_hash: Arc::new(transaction_hash),
                    block_number,
                    block_timestamp,
                    log_index,
                    transaction_index,
                    tx_sender: Arc::new(sender),
                })
            };

            Ok(vec![sync_event, swap_event])
        }
        Some(&ICapricornCLPool::Mint::SIGNATURE_HASH) => {
            let pool = log.address().to_string();
            let is_whitelist_dex = cache_manager.check_token_pool(&pool).await?;
            if !is_whitelist_dex {
                return Err(anyhow::anyhow!("Not a white list dex address"));
            }

            let pool_pair = cache_manager.get_pool_pair(&pool).await?;
            let (token0, token1) =
                pool_pair.ok_or_else(|| anyhow::anyhow!("DEX pair not found"))?;

            let ICapricornCLPool::Mint {
                owner,
                amount,
                amount0,
                amount1,
                ..
            } = log.log_decode()?.inner.data;

            let token0_is_mon = token0 == *WNATIVE_ADDRESS;

            // Determine which token is the actual token (not native)
            let (token_id, quote_amount, token_amount) = if token0_is_mon {
                // token0 is native, token1 is the actual token
                (
                    token1.clone(),
                    to_big_decimal(amount1),
                    to_big_decimal(amount0),
                )
            } else {
                // token1 is native, token0 is the actual token
                (
                    token0.clone(),
                    to_big_decimal(amount1),
                    to_big_decimal(amount0),
                )
            };

            // Get pool state at the specific block to calculate reserves
            let pool_contract =
                ICapricornCLPool::new(log.address(), client.get_current_provider().await?);

            // Parallel RPC calls for better performance
            let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));
            let slot0_call = pool_contract.slot0().block(block_id);
            let liquidity_call = pool_contract.liquidity().block(block_id);

            let (slot0_result, liquidity_result) =
                tokio::join!(slot0_call.call(), liquidity_call.call());

            let ICapricornCLPool::slot0Return { sqrtPriceX96, .. } = slot0_result?;

            let liquidity = liquidity_result?;

            // Calculate virtual reserves from liquidity and sqrtPriceX96
            let sqrt_price_x96_decimal = to_big_decimal(sqrtPriceX96);
            let two_pow_96 = BigDecimal::from(2u128.pow(96));
            let sqrt_price = &sqrt_price_x96_decimal / &two_pow_96;
            let liquidity_decimal = BigDecimal::from(liquidity);

            // Virtual reserves calculation:
            // reserve0 = L / sqrtPrice
            // reserve1 = L * sqrtPrice
            let (reserve_quote, reserve_token) = if token0_is_mon {
                // token0 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down);
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down);
                (reserve0, reserve1)
            } else {
                // token1 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down);
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down);
                (reserve1, reserve0)
            };

            let mint = DexMint {
                token_id: Arc::new(token_id),
                account_id: Arc::new(owner.to_string()),
                market_id: Arc::new(pool.clone()),
                quote_amount: Arc::new(quote_amount),
                token_amount: Arc::new(token_amount),
                liquidity: Arc::new(to_big_decimal(amount)),
                reserve_quote: Arc::new(reserve_quote),
                reserve_token: Arc::new(reserve_token),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
            };

            Ok(vec![DexEvent::from(mint)])
        }
        Some(&ICapricornCLPool::Burn::SIGNATURE_HASH) => {
            let pool = log.address().to_string();
            let is_whitelist_dex = cache_manager.check_token_pool(&pool).await?;
            if !is_whitelist_dex {
                return Err(anyhow::anyhow!("Not a white list dex address"));
            }

            let pool_pair = cache_manager.get_pool_pair(&pool).await?;
            let (token0, token1) =
                pool_pair.ok_or_else(|| anyhow::anyhow!("DEX pair not found"))?;

            let ICapricornCLPool::Burn {
                owner,
                amount,
                amount0,
                amount1,
                ..
            } = log.log_decode()?.inner.data;

            let token0_is_mon = token0 == *WNATIVE_ADDRESS;

            // Determine which token is the actual token (not native)
            let (token_id, quote_amount, token_amount) = if token0_is_mon {
                // token0 is native, token1 is the actual token
                (
                    token1.clone(),
                    to_big_decimal(amount1),
                    to_big_decimal(amount0),
                )
            } else {
                // token1 is native, token0 is the actual token
                (
                    token0.clone(),
                    to_big_decimal(amount1),
                    to_big_decimal(amount0),
                )
            };

            // Get pool state at the specific block to calculate reserves
            let pool_contract =
                ICapricornCLPool::new(log.address(), client.get_current_provider().await?);

            // Parallel RPC calls for better performance
            let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));
            let slot0_call = pool_contract.slot0().block(block_id);
            let liquidity_call = pool_contract.liquidity().block(block_id);

            let (slot0_result, liquidity_result) =
                tokio::join!(slot0_call.call(), liquidity_call.call());

            let ICapricornCLPool::slot0Return { sqrtPriceX96, .. } = slot0_result?;

            let liquidity = liquidity_result?;

            // Calculate virtual reserves from liquidity and sqrtPriceX96
            let sqrt_price_x96_decimal = to_big_decimal(sqrtPriceX96);
            let two_pow_96 = BigDecimal::from(2u128.pow(96));
            let sqrt_price = &sqrt_price_x96_decimal / &two_pow_96;
            let liquidity_decimal = BigDecimal::from(liquidity);

            // Virtual reserves calculation:
            // reserve0 = L / sqrtPrice
            // reserve1 = L * sqrtPrice
            let (reserve_quote, reserve_token) = if token0_is_mon {
                // token0 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down);
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down);
                (reserve0, reserve1)
            } else {
                // token1 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down);
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down);
                (reserve1, reserve0)
            };

            let burn = DexBurn {
                token_id: Arc::new(token_id),
                account_id: Arc::new(owner.to_string()),
                market_id: Arc::new(pool.clone()),
                quote_amount: Arc::new(quote_amount),
                token_amount: Arc::new(token_amount),
                liquidity: Arc::new(to_big_decimal(amount)),
                reserve_quote: Arc::new(reserve_quote),
                reserve_token: Arc::new(reserve_token),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
            };

            Ok(vec![DexEvent::from(burn)])
        }
        Some(&IDexRouter::DexRouterBuy::SIGNATURE_HASH) => {
            let address = log.address().to_string();
            if !check_dex_router(address) {
                return Err(anyhow::anyhow!("Not a DexRouter address"));
            }
            let IDexRouter::DexRouterBuy {
                sender: event_sender,
                token,
                amountIn,
                amountOut,
            } = log.log_decode()?.inner.data;

            let sender = event_sender.to_string();
            let tx_sender = match cache_manager.get_tx_sender(&transaction_hash).await {
                Ok(Some(s)) => s.to_string(),
                _ => sender.clone(),
            };

            let buy = DexRouterBuy {
                token: Arc::new(token.to_string()),
                sender: Arc::new(sender),
                amount_in: Arc::new(to_big_decimal(amountIn)),
                amount_out: Arc::new(to_big_decimal(amountOut)),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
                tx_sender: Arc::new(tx_sender),
            };

            Ok(vec![DexEvent::from(buy)])
        }
        Some(&IDexRouter::DexRouterSell::SIGNATURE_HASH) => {
            let address = log.address().to_string();
            if !check_dex_router(address) {
                return Err(anyhow::anyhow!("Not a DexRouter address"));
            }
            let IDexRouter::DexRouterSell {
                sender: event_sender,
                token,
                amountIn,
                amountOut,
            } = log.log_decode()?.inner.data;

            let sender = event_sender.to_string();
            let tx_sender = match cache_manager.get_tx_sender(&transaction_hash).await {
                Ok(Some(s)) => s.to_string(),
                _ => sender.clone(),
            };

            let sell = DexRouterSell {
                token: Arc::new(token.to_string()),
                sender: Arc::new(sender),
                amount_in: Arc::new(to_big_decimal(amountIn)),
                amount_out: Arc::new(to_big_decimal(amountOut)),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
                tx_sender: Arc::new(tx_sender),
            };

            Ok(vec![DexEvent::from(sell)])
        }
        Some(&ICapricornCLPool::SetFeeProtocol::SIGNATURE_HASH) => {
            let pool = log.address().to_string();

            info!(
                "[DEX] SetFeeProtocol event detected! pool={}, block={}, tx_hash={}",
                pool, block_number, transaction_hash
            );

            let is_whitelist_dex = cache_manager.check_token_pool(&pool).await?;
            if !is_whitelist_dex {
                warn!(
                    "[DEX] SetFeeProtocol skipped - pool {} is not in whitelist",
                    pool
                );
                return Err(anyhow::anyhow!("Not a white list dex address"));
            }

            let ICapricornCLPool::SetFeeProtocol {
                feeProtocol0Old,
                feeProtocol1Old,
                feeProtocol0New,
                feeProtocol1New,
            } = log.log_decode()?.inner.data;

            info!(
                "[DEX] SetFeeProtocol parsed: pool={}, old=({},{}), new=({},{})",
                pool, feeProtocol0Old, feeProtocol1Old, feeProtocol0New, feeProtocol1New
            );

            let set_fee = SetFeeProtocol {
                pool_id: Arc::new(pool),
                fee_protocol0_old: feeProtocol0Old,
                fee_protocol1_old: feeProtocol1Old,
                fee_protocol0_new: feeProtocol0New,
                fee_protocol1_new: feeProtocol1New,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                transaction_index,
                log_index,
            };

            Ok(vec![DexEvent::from(set_fee)])
        }

        _ => Err(anyhow::anyhow!("Unknown event type")),
    }
}

/**
 * token0_is_mon 매개변수 설명:
 *
 * 이 함수는 항상 mon/token 형태의 가격 비율을 반환합니다.
 * token0_is_mon 매개변수는 token0이 mon(기준 토큰)인지 여부를 나타냅니다.
 *
 * ETH = MON, USDC = TOKEN이라고 정의할 때:
 *
 * 예시 1: MON/TOKEN 풀
 * - token0가 MON이고 token0_is_mon = true일 때:
 *   => 최종 반환값: 0.0000005 (1 MON당 TOKEN 가격)
 *
 * 예시 2: TOKEN/MON 풀
 * - token0가 TOKEN이고 token0_is_mon = false일 때:
 *   => 최종 반환값: 0.0000005 (1 MON당 TOKEN 가격)
 *
 * 중요: 이 함수는 풀의 구성이나 토큰 순서와 관계없이 항상 mon/token 형태의 가격을 반환합니다.
 * token0_is_mon 매개변수를 통해 어떤 토큰이 mon인지 지정하면, 그에 맞게 가격 비율이 계산됩니다.
 *
 * 계산 예시:
 *
 * 입력:
 * - sqrt_price_x96 = 1771845812128583464494622 (Uniswap V3 풀의 실제 값)
 * - token0_is_mon = true (MON이 token0이고 기준 토큰인 경우)
 *
 * 계산 과정:
 * 1. sqrt_price_x96 / 2^96 = 0.0000000223638...
 * 2. (sqrt_price_x96 / 2^96)^2 = 0.0000000000005002...
 * 3. 최종 가격 = 약 0.0000005 (1 MON당 TOKEN 가격)
 */
fn calculate_mon_token_price(sqrt_price_x96: BigDecimal, token0_is_mon: bool) -> BigDecimal {
    // 2^96을 상수로 미리 계산하여 성능 향상
    let two_96 = BigDecimal::from(79_228_162_514_264_337_593_543_950_336u128);

    if token0_is_mon {
        // price = (2^96 / sqrtP)^2
        let ratio = &two_96 / &sqrt_price_x96;
        let price_ratio = &ratio * &ratio;
        price_ratio.with_scale(60) // 극단적으로 큰 sqrtP 대비 높은 정밀도 유지
    } else {
        // price = (sqrtP / 2^96)^2
        let ratio = &sqrt_price_x96 / &two_96;
        let price_ratio = &ratio * &ratio;
        price_ratio.with_scale(60) // 극단적으로 작은 sqrtP 대비 높은 정밀도 유지
    }
}

fn check_dex_router(address: String) -> bool {
    if address == *DEX_ROUTER_ADDRESS {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_calculate_mon_token_price_normal_case() {
        // 정상적인 케이스: sqrtPriceX96=101738343501146834733007459140383
        let sqrt_price = BigDecimal::from_str("101738343501146834733007459140383").unwrap();
        let price = calculate_mon_token_price(sqrt_price, true);
        println!("Normal case price: {}", price);
        assert!(price > BigDecimal::from(0));
    }

    #[test]
    fn test_calculate_mon_token_price_depleted_liquidity() {
        // 유동성 고갈 케이스: sqrtPriceX96=4295128740 (극도로 작음)
        let sqrt_price = BigDecimal::from_str("4295128740").unwrap();
        let two_96: u128 = 79_228_162_514_264_337_593_543_950_336;

        println!("\n=== DEBUG: 극단적으로 작은 값 분석 ===");
        println!("sqrt_price_x96 = {}", sqrt_price);

        let normalized = &sqrt_price / BigDecimal::from(two_96);
        println!("sqrt_price_x96 / 2^96 = {}", normalized);

        let price_ratio_raw = &normalized * &normalized;
        println!("price_ratio (before with_scale) = {}", price_ratio_raw);

        let price = calculate_mon_token_price(sqrt_price.clone(), true);
        println!("Depleted liquidity (token0_is_mon=true) price: {}", price);
        assert!(price > BigDecimal::from(0));

        // token0_is_mon=false인 경우
        let price2 = calculate_mon_token_price(sqrt_price, false);
        println!("Depleted liquidity (token0_is_mon=false) price: {}", price2);
        assert!(price2 > BigDecimal::from(0));
    }

    #[test]
    fn test_calculate_mon_token_price_very_large() {
        // 매우 큰 sqrtPriceX96: 1461446703485210103287273052203988822378723970341
        let sqrt_price =
            BigDecimal::from_str("1461446703485210103287273052203988822378723970341").unwrap();
        let price = calculate_mon_token_price(sqrt_price.clone(), true);
        println!(
            "Very large sqrtPriceX96 (token0_is_mon=true) price: {}",
            price
        );
        assert!(price > BigDecimal::from(0));

        let price2 = calculate_mon_token_price(sqrt_price, false);
        println!(
            "Very large sqrtPriceX96 (token0_is_mon=false) price: {}",
            price2
        );
        assert!(price2 > BigDecimal::from(0));
    }

    #[test]
    fn test_calculate_mon_token_price_real_cases() {
        // 실제 프로덕션에서 발생한 케이스들
        let test_cases = vec![
            // 극단적으로 큰 sqrtPriceX96 (유동성 고갈)
            (
                "1461446703485210103287273052203988822378723970341",
                true,
                "극단적으로 큰 값",
            ),
            // 정상 범위
            ("69945130107116790297646487", false, "정상 케이스 1"),
            ("78615130332777875708302803", true, "정상 케이스 2"),
            ("21399104899234023875328045", false, "정상 케이스 3"),
            // 극단적으로 작은 sqrtPriceX96 (유동성 고갈)
            ("4295128740", true, "극단적으로 작은 값"),
            ("5519150528870277727550574670787", false, "정상 케이스 4"),
        ];

        println!("\n=== Real Production Cases ===");
        for (sqrt_price_str, token0_is_mon, description) in test_cases {
            let sqrt_price = BigDecimal::from_str(sqrt_price_str).unwrap();
            let price = calculate_mon_token_price(sqrt_price, token0_is_mon);
            println!(
                "{}\n  sqrtPriceX96={}\n  token0_is_mon={}\n  price={}\n",
                description, sqrt_price_str, token0_is_mon, price
            );

            // 가격이 양수여야 함
            assert!(price > BigDecimal::from(0), "Price must be positive");
        }
    }
}
