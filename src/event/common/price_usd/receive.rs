use std::time::Instant;

use anyhow::Result;
use tracing::{instrument, warn};

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
        let PriceUsdEventBatch {
            events,
            to_block,
            latest_block,
            ack,
        } = batch;
        let db = match PostgresDatabase::instance() {
            Ok(db) => db,
            Err(error) => {
                let _ = ack.send(Err(format!("{error:#}")));
                return Err(error);
            }
        };

        let time = Instant::now();
        let event_count = events.len();

        let controller = PriceUsdController::new(db);
        if let Err(error) = controller.batch_insert_price_usd(&events).await {
            let _ = ack.send(Err(format!("{error:#}")));
            return Err(error);
        }

        total_events += event_count;
        warn!(
            "📊 {:?} Receiver: Events: {} | Total Events: {} | Process time: {}ms | To Block: {} | Latest Block: {}",
            event_type,
            event_count,
            total_events,
            time.elapsed().as_millis(),
            to_block,
            latest_block,
        );

        RECEIVE_MANAGER
            .set_last_processed_block(event_type, to_block, latest_block)
            .await;
        let _ = ack.send(Ok(()));
    }

    Ok(())
}
