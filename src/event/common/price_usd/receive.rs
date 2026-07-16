use std::time::Instant;

use anyhow::Result;
use tracing::{error, instrument, warn};

use crate::{
    db::postgres::{PostgresDatabase, controller::price_usd::PriceUsdController},
    metrics::MonitoredReceiver,
    sync::{EventType, receive::RECEIVE_MANAGER},
};

use super::PriceUsdEventBatch;

#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<PriceUsdEventBatch>,
    event_type: EventType,
) -> Result<()> {
    let mut total_events = 0;

    while let Some(batch) = receiver.recv().await {
        let db = PostgresDatabase::instance()?;
        let PriceUsdEventBatch {
            events,
            to_block,
            latest_block,
        } = batch;

        let time = Instant::now();
        let event_count = events.len();
        total_events += event_count;

        let price_usd_controller = PriceUsdController::new(db.clone());
        if let Err(e) = price_usd_controller.batch_insert_price_usd(&events).await {
            error!("[PRICE_USD] Batch insert failed: {:#}", e);
        }

        let elapsed_ms = time.elapsed().as_millis();
        warn!(
            "📊 {:?} Receiver: Events: {} | Total Events: {} | Process time: {}ms | To Block: {} | Latest Block: {}",
            event_type, event_count, total_events, elapsed_ms, to_block, latest_block,
        );

        RECEIVE_MANAGER
            .set_last_processed_block(event_type, to_block, latest_block)
            .await;
    }

    Ok(())
}
