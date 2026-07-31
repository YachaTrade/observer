use std::collections::BTreeMap;
use std::time::Instant;

use crate::{
    db::cache::CacheManager,
    db::postgres::{PostgresDatabase, controller::price::PriceController},
    sync::{EventType, receive::RECEIVE_MANAGER},
};

use super::PriceEventBatch;
use crate::metrics::MonitoredReceiver;
use anyhow::Result;

use tracing::{debug, error, instrument, warn};

#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<PriceEventBatch>,
    event_type: EventType,
) -> Result<()> {
    let mut total_events = 0;

    while let Some(batch) = receiver.recv().await {
        let PriceEventBatch {
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
        total_events += event_count;

        // Group events by quote_id for batched processing
        let mut by_quote: BTreeMap<String, Vec<(u64, bigdecimal::BigDecimal, u64)>> =
            BTreeMap::new();
        for e in events {
            by_quote.entry(e.quote_id).or_default().push((
                e.block_number,
                e.price,
                e.block_timestamp,
            ));
        }

        let price_controller = PriceController::new(db.clone());

        let writes = by_quote
            .iter()
            .flat_map(|(quote_id, price_batch)| {
                price_batch.iter().map(|(block, price, timestamp)| {
                    (quote_id.clone(), *block, price.clone(), *timestamp)
                })
            })
            .collect::<Vec<_>>();
        let canonical = match price_controller.persist_price_batch(&writes).await {
            Ok(canonical) => canonical,
            Err(error) => {
                error!("[PRICE] Atomic batch insert failed: {:#}", error);
                let _ = ack.send(Err(format!("{error:#}")));
                return Err(error);
            }
        };

        if let Ok(cache_manager) = CacheManager::instance() {
            let mut canonical_by_quote: BTreeMap<String, Vec<(i64, bigdecimal::BigDecimal)>> =
                BTreeMap::new();
            for (quote_id, block_number, price) in canonical {
                canonical_by_quote
                    .entry(quote_id)
                    .or_default()
                    .push((block_number, price));
            }
            for (quote_id, cache_batch) in canonical_by_quote {
                cache_manager
                    .insert_price_batch_for_quote(&quote_id, &cache_batch)
                    .await;
                debug!(
                    "[PRICE] Cached {} canonical prices for quote {} in memory",
                    cache_batch.len(),
                    quote_id
                );
            }
        }

        let elapsed_ms = time.elapsed().as_millis();
        warn!(
            "📊 {:?} Receiver: Events: {} ({} quotes) | Total Events: {} | Process time: {}ms | To Block: {} | Latest Block: {}",
            event_type,
            event_count,
            by_quote.len(),
            total_events,
            elapsed_ms,
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
