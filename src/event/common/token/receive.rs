use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::Result;

use crate::{
    db::postgres::{
        PostgresDatabase,
        controller::{
            account::AccountController, balance::BalanceController, burn::BurnController,
            lp_position::LpPositionController, position::PositionController,
        },
    },
    sync::{EventType, receive::RECEIVE_MANAGER},
    types::token::{LpPositionHistoryEvent, PositionHistoryEvent, TokenEvent},
};

use super::TokenEventBatch;
use crate::metrics::MonitoredReceiver;
use tracing::{error, info, instrument, warn};

#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<TokenEventBatch>,
    event_type: EventType,
) -> Result<()> {
    info!("Token event receiver started");
    let mut total_events = 0;
    // Process events as they arrive through the channel
    while let Some(events) = receiver.recv().await {
        let db = PostgresDatabase::instance()?;
        let TokenEventBatch {
            events,
            to_block,
            latest_block,
        } = events;
        RECEIVE_MANAGER
            .check_last_processed_block(to_block, event_type)
            .await;
        let time = Instant::now();
        let event_count = events.len();
        total_events += event_count;

        // 3-way partition: PositionHistory, LpPosition, everything else.
        // PositionHistory + LpPosition each go directly to their own batch
        // insert path; the rest flow through the address-grouped pipeline.
        let mut position_histories: Vec<TokenEvent> = Vec::new();
        let mut lp_positions: Vec<LpPositionHistoryEvent> = Vec::new();
        let mut other_events: Vec<TokenEvent> = Vec::new();
        for event in events {
            match event {
                TokenEvent::PositionHistory(_) => position_histories.push(event),
                TokenEvent::LpPosition(p) => lp_positions.push(p),
                _ => other_events.push(event),
            }
        }

        // 1. Balance/Burn/Transfer 처리 (address 기반)
        let event_by_address = group_events_by_address(other_events);
        let address_count = event_by_address.len();
        let handles: Vec<_> = event_by_address
            .into_iter()
            .map(|(address, events)| {
                let db = db.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_address_events(address.clone(), events, db).await {
                        error!(
                            "[TOKEN] Failed to process events for address {}: {:?}",
                            address, e
                        );
                    }
                })
            })
            .collect();
        for handle in handles {
            if let Err(e) = handle.await {
                total_events -= 1;
                error!("[TOKEN] Failed to join handle: {:?}", e);
            }
        }

        // 2. PositionHistory 단순 insert (stream에서 이미 분석 완료)
        let position_count = position_histories.len();
        if !position_histories.is_empty() {
            let position_data: Vec<PositionHistoryEvent> = position_histories
                .into_iter()
                .filter_map(|e| match e {
                    TokenEvent::PositionHistory(ph) => Some(ph),
                    _ => None,
                })
                .collect();

            let position_controller = PositionController::new(db.clone());
            match position_controller
                .batch_insert_position_history(&position_data)
                .await
            {
                Ok(inserted) => {
                    info!(
                        "[TOKEN] Inserted {}/{} position_history records",
                        inserted.len(),
                        position_data.len()
                    );
                }
                Err(e) => {
                    error!("[TOKEN] Failed to insert position_history: {:?}", e);
                }
            }
        }

        // 3. LP position history batch insert. Trigger
        // `update_lp_position_on_history` fills cost basis from dex_mint/dex_burn
        // (mint/burn) or sender's avg cost basis (transfer_in/out). Generic
        // Token stream ordering is enforced in
        // `src/sync/receive.rs::check_last_processed_block`.
        let lp_count = lp_positions.len();
        if !lp_positions.is_empty() {
            // Log each lp_position event's identifying coordinates so we can
            // cross-check against on-chain Transfer logs when an event seems to
            // have been missed.
            for lp in &lp_positions {
                info!(
                    "[LP] queued event_type={} pool={} tx={} log_index={} account={} lp_in={} lp_out={}",
                    lp.event_type,
                    lp.pool_id,
                    lp.transaction_hash,
                    lp.log_index,
                    lp.account_id,
                    lp.lp_in,
                    lp.lp_out,
                );
            }
            let lp_ctrl = LpPositionController::new(db.clone());
            match lp_ctrl.batch_insert(&lp_positions).await {
                Ok(()) => {
                    info!("[LP] batch_insert_ok count={}", lp_positions.len());
                }
                Err(e) => {
                    error!(
                        "[LP] batch_insert_failed count={} err={:?}",
                        lp_positions.len(),
                        e
                    );
                }
            }
        }

        let elapsed_ms = time.elapsed().as_millis();

        warn!(
            "📊 {:?} Receiver: Events: {} | Addresses: {} | Positions: {} | LpPositions: {} | Total: {} | Process time: {}ms | To Block: {} | Latest Block: {}",
            event_type,
            event_count,
            address_count,
            position_count,
            lp_count,
            total_events,
            elapsed_ms,
            to_block,
            latest_block,
        );
        RECEIVE_MANAGER
            .set_last_processed_block(event_type, to_block, latest_block)
            .await;
    }
    error!("[TOKEN] Event receiver has been closed");

    Ok(())
}

//Address별로 이벤트 그룹화
fn group_events_by_address(events: Vec<TokenEvent>) -> HashMap<String, Vec<TokenEvent>> {
    let mut event_map: HashMap<String, Vec<TokenEvent>> = HashMap::new();

    for event in events {
        event_map
            .entry(event.account_address().to_string())
            .or_default()
            .push(event);
    }

    event_map
}

// address 이벤트 처리
async fn process_address_events(
    address: String,
    events: Vec<TokenEvent>,
    db: Arc<PostgresDatabase>,
) -> Result<()> {
    use std::collections::HashSet;

    // 배치 데이터를 수집할 벡터
    let mut balance_batch = Vec::new();
    let mut burn_batch = Vec::new();
    let mut account_ids: HashSet<String> = HashSet::new();

    // 1단계: 이벤트 타입별로 분류하고 배치 데이터 수집
    for event in events {
        match event {
            TokenEvent::Balance(balance) => {
                account_ids.insert(balance.account_id.as_ref().clone());
                balance_batch.push(balance);
            }
            TokenEvent::Burn(burn) => {
                account_ids.insert(burn.from.as_ref().clone());
                burn_batch.push(burn);
            }
            // Transfer, PositionHistory는 이 함수로 오지 않음
            _ => {}
        }
    }

    // 2단계: Account 배치 upsert (맨 먼저)
    let account_controller = AccountController::new(db.clone());
    let account_list: Vec<String> = account_ids.into_iter().collect();

    if !account_list.is_empty()
        && let Err(e) = account_controller
            .batch_upsert_accounts(&account_list)
            .await
    {
        warn!(
            "[TOKEN] Failed to batch upsert {} accounts for address {}: {}. Continuing with other operations...",
            account_list.len(),
            address,
            e
        );
    }

    // 3단계: 배치 데이터 한번에 DB에 쓰기 (Balance, Burn를 병렬로)
    let balance_controller = BalanceController::new(db.clone());
    let burn_controller = BurnController::new(db.clone());

    let balance_operation = async {
        if !balance_batch.is_empty() {
            balance_controller.batch_set_balances(&balance_batch).await
        } else {
            Ok(())
        }
    };

    let burn_operation = async {
        if !burn_batch.is_empty() {
            burn_controller.batch_handle_burns(&burn_batch).await
        } else {
            Ok(())
        }
    };

    let (balance_result, burn_result) = tokio::join!(balance_operation, burn_operation);

    if let Err(e) = balance_result {
        error!(
            "[TOKEN] Balance batch operation failed for address {}: {:#}",
            address, e
        );
    }
    if let Err(e) = burn_result {
        error!(
            "[TOKEN] Burn batch operation failed for address {}: {:#}",
            address, e
        );
    }

    Ok(())
}
