use std::{collections::HashMap, sync::Arc, time::Instant};

use crate::{
    db::postgres::{PostgresDatabase, controller::lp::LpController},
    sync::{EventType, receive::RECEIVE_MANAGER},
    types::v1::lp_manager::LpManagerEvent,
};

use anyhow::Result;

use super::LpManagerEventBatch;
use crate::metrics::MonitoredReceiver;
use tracing::{error, instrument, warn};
#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<LpManagerEventBatch>,
    event_type: EventType,
) -> Result<()> {
    let mut total_events = 0;
    while let Some(events) = receiver.recv().await {
        let db = PostgresDatabase::instance()?;
        let LpManagerEventBatch {
            events,
            to_block,
            latest_block,
        } = events;

        // Process events as they arrive through the channel
        RECEIVE_MANAGER
            .check_last_processed_block(to_block, event_type)
            .await;
        let time = Instant::now();
        let event_count = events.len();
        total_events += event_count;

        // Group events by token and process in parallel
        let events_by_token = group_events_by_token(events);
        let token_count = events_by_token.len();

        // Process events for each token sequentially, but different tokens in parallel
        let handles: Vec<_> = events_by_token
            .into_iter()
            .map(|(token, events)| {
                let db = db.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_token_events(token.clone(), events, db).await {
                        error!(
                            "[LP_MANAGER] Failed to process events for token {}: {:?}",
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
                error!("[LP_MANAGER] Failed to join handle: {:?}", e);
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
    }
    error!("[LP_MANAGER] Event receiver has been closed");

    Ok(())
}

// token별로 이벤트 그룹화
fn group_events_by_token(events: Vec<LpManagerEvent>) -> HashMap<String, Vec<LpManagerEvent>> {
    let mut events_by_token: HashMap<String, Vec<LpManagerEvent>> = HashMap::new();

    for event in events {
        events_by_token
            .entry(event.token().to_string())
            .or_default()
            .push(event);
    }

    events_by_token
}

async fn process_token_events(
    token: String,
    events: Vec<LpManagerEvent>,
    db: Arc<PostgresDatabase>,
) -> Result<()> {
    // 배치 데이터를 수집할 벡터
    let mut allocate_batch = Vec::new();
    let mut collect_batch = Vec::new();

    // 1단계: 이벤트 타입별로 분류하고 배치 데이터 수집
    for event in events {
        match event {
            LpManagerEvent::Allocate(allocate) => {
                allocate_batch.push(allocate);
            }
            LpManagerEvent::Collect(collect) => {
                collect_batch.push(collect);
            }
        }
    }

    // 2단계: 배치 데이터 한번에 DB에 쓰기 (Allocate, Collect를 병렬로)
    let lp_controller = LpController::new(db.clone());

    let allocate_operation = async {
        if !allocate_batch.is_empty() {
            lp_controller
                .batch_handle_lp_allocate(&allocate_batch)
                .await
        } else {
            Ok(())
        }
    };

    let collect_operation = async {
        if !collect_batch.is_empty() {
            lp_controller.batch_handle_lp_collect(&collect_batch).await
        } else {
            Ok(())
        }
    };

    if let Err(e) = tokio::try_join!(allocate_operation, collect_operation) {
        error!(
            "[LP_MANAGER] Batch operations failed for token {}: {:#}",
            token, e
        );
    }

    Ok(())
}
