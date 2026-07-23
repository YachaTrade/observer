use std::{sync::Arc, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;

use tokio::time::sleep;
use tokio::{task::JoinSet, time::Instant};

use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, LP_MANAGER_ADDRESS},
    event::{
        get_block_timestamp,
        lp_manager::{LpManagerEventChannel, receive::receive_events},
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::lp_manager::{Allocate, Collect, LpManagerEvent},
    utils::to_big_decimal,
};

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    LPManager,
    "abi/LPManager.json"
}

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    info!("Starting lp manager event streaming");

    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let (channel, receiver) = LpManagerEventChannel::new("lp_manager_events");
    tokio::spawn(async move {
        if let Err(e) = receive_events(receiver, event_type).await {
            error!("[LP_MANAGER] Failed to receive events: {}", e);
        }
    });
    let mut total_events = 0;
    loop {
        let client = RpcClient::instance()?;
        let latest_block = client.get_cached_latest_block();
        let time = Instant::now();
        // Curve 동기화 대기
        let BlockRange {
            from_block,
            to_block,
        } = STREAM_MANAGER
            .get_next_block_range(event_type, block_batch_size, latest_block)
            .await;
        if from_block > to_block {
            continue;
        }
        let filter = Filter::new()
            .from_block(BlockNumberOrTag::Number(from_block))
            .to_block(BlockNumberOrTag::Number(to_block))
            .address(LP_MANAGER_ADDRESS.parse::<Address>().unwrap())
            .events(vec![
                LPManager::Allocate::SIGNATURE,
                LPManager::Collect::SIGNATURE,
            ]);

        let logs = match client.get_logs(filter).await {
            Ok(logs) => logs,
            Err(e) => {
                error!(
                    "Failed lp manager to get logs: {} batch_size: {}",
                    e, block_batch_size
                );
                block_batch_size /= 2;
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let logs_count = logs.len();

        // 화이트리스트 토큰 필터링은 async 코드로 작동해야 하므로 먼저 로그 목록을 만들고
        // 각 로그를 parse_log에서 검사하는 방식으로 변경
        let logs_clone = logs.clone();

        let mut join_set = JoinSet::new();
        for log in logs_clone {
            join_set.spawn(async move {
                let result = parse_log(log.clone(), client).await;
                (log, result)
            });
        }

        // 결과 수집
        let mut events: Vec<LpManagerEvent> = Vec::new();
        while let Some(result) = join_set.join_next().await {
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
                        if !SkippableError::should_skip_lp_manager(&e.to_string()) {
                            error!(
                                error = %e,
                                log = ?log,
                                "Is_failed to parse lp manager event log"
                            );
                        }
                    }
                },
                Err(join_err) => {
                    error!("[LP_MANAGER] Event task join error: {}", join_err);
                }
            }
        }

        // 이벤트 정렬 로직 개선: block_number -> transaction_index -> log_index 순서로 정렬
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
            error!("[LP_MANAGER] Failed to send events: {}", e);
            continue;
        }

        let logging_format = format!(
            "📊 {:?} Stream: Blocks: from={} to={} | Logs: {} | Events: {} | Total Events: {} | Process time: {}ms",
            event_type, from_block, to_block, logs_count, events_count, total_events, elapsed_ms
        );
        warn!("{}", logging_format);

        block_batch_size = *BLOCK_BATCH_SIZE;

        STREAM_MANAGER
            .set_event_block_processed_block(event_type, to_block)
            .await;
    }
}

async fn parse_log(log: Log, client: &RpcClient) -> Result<Vec<LpManagerEvent>> {
    // 트랜잭션 해시 추출
    let transaction_hash = log
        .transaction_hash
        .ok_or_else(|| anyhow::anyhow!("No transaction hash found in log"))?
        .to_string();

    // 블록 정보 추출
    let block_number = log
        .block_number
        .ok_or_else(|| anyhow::anyhow!("Missing block number in log"))?;

    let block_timestamp = match log.block_timestamp {
        Some(timestamp) => timestamp,
        None => get_block_timestamp(client, block_number).await?,
    };

    let log_index = log.log_index.unwrap_or(u64::MAX);
    let transaction_index = log.transaction_index.unwrap_or(u64::MAX);
    match log.topic0() {
        Some(&LPManager::Allocate::SIGNATURE_HASH) => {
            let LPManager::Allocate {
                token,
                pool,
                quoteAmount,
                tokenAmount,
                timestamp,
            } = log.log_decode()?.inner.data;

            let token = token.to_string();
            let pool = pool.to_string();
            let quote_amount = Arc::new(to_big_decimal(quoteAmount));
            let token_amount = Arc::new(to_big_decimal(tokenAmount));
            let event_timestamp: u64 = timestamp.to::<u64>();
            let allocate = Allocate {
                token: Arc::new(token),
                pool: Arc::new(pool),
                quote_amount,
                token_amount,
                event_timestamp,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            };

            Ok(vec![LpManagerEvent::Allocate(allocate)])
        }
        Some(&LPManager::Collect::SIGNATURE_HASH) => {
            let LPManager::Collect {
                token,
                pool,
                quoteAmount,
                timestamp,
            } = log.log_decode()?.inner.data;

            let token = token.to_string();
            let pool = pool.to_string();
            let quote_amount = Arc::new(to_big_decimal(quoteAmount));
            let event_timestamp: u64 = timestamp.to::<u64>();
            let collect = Collect {
                token: Arc::new(token),
                pool: Arc::new(pool),
                quote_amount,
                event_timestamp,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            };
            Ok(vec![LpManagerEvent::Collect(collect)])
        }
        _ => Err(anyhow::anyhow!("Unknown event type")),
    }
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, U256},
        sol_types::SolEvent,
    };

    use super::LPManager;

    #[test]
    fn lp_manager_collect_event_uses_quote_only_payload() {
        assert_eq!(
            LPManager::Collect::SIGNATURE,
            "Collect(address,address,uint256,uint256)"
        );
    }

    #[test]
    fn lp_manager_event_signatures_and_fields_round_trip() {
        let token = Address::repeat_byte(0x11);
        let pool = Address::repeat_byte(0x22);

        let allocate = LPManager::Allocate {
            token,
            pool,
            quoteAmount: U256::from(100u64),
            tokenAmount: U256::from(200u64),
            timestamp: U256::from(300u64),
        };
        let decoded = LPManager::Allocate::decode_log_data(&allocate.encode_log_data())
            .expect("Allocate decodes");
        assert_eq!(decoded.quoteAmount, U256::from(100u64));
        assert_eq!(decoded.tokenAmount, U256::from(200u64));
        assert_eq!(decoded.timestamp, U256::from(300u64));

        let collect = LPManager::Collect {
            token,
            pool,
            quoteAmount: U256::from(400u64),
            timestamp: U256::from(600u64),
        };
        let decoded = LPManager::Collect::decode_log_data(&collect.encode_log_data())
            .expect("Collect decodes");
        assert_eq!(decoded.quoteAmount, U256::from(400u64));
        assert_eq!(decoded.timestamp, U256::from(600u64));
    }
}
