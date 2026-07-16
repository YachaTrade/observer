use std::collections::HashMap;
use std::time::Instant;

use crate::{
    config::WNATIVE_ADDRESS,
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
        let db = PostgresDatabase::instance()?;
        let PriceEventBatch {
            events,
            to_block,
            latest_block,
        } = batch;

        let time = Instant::now();
        let event_count = events.len();
        total_events += event_count;

        // Group events by quote_id for batched processing
        let mut by_quote: HashMap<String, Vec<(u64, bigdecimal::BigDecimal, u64)>> =
            HashMap::new();
        for e in events {
            by_quote
                .entry(e.quote_id)
                .or_default()
                .push((e.block_number, e.price, e.block_timestamp));
        }

        let price_controller = PriceController::new(db.clone());

        for (quote_id, price_batch) in &by_quote {
            // Cache in memory (quote-keyed for all quotes)
            if let Ok(cache_manager) = CacheManager::instance() {
                let cache_batch: Vec<(i64, bigdecimal::BigDecimal)> = price_batch
                    .iter()
                    .map(|(block, price, _)| (*block as i64, price.clone()))
                    .collect();

                cache_manager
                    .insert_price_batch_for_quote(quote_id, &cache_batch)
                    .await;

                // For WMON, also write to the legacy WMON-specific cache
                // so existing consumers (e.g. swap.rs get_prices_for_block_range)
                // continue to work without changes.
                if *quote_id == *WNATIVE_ADDRESS {
                    cache_manager.insert_price_batch(&cache_batch).await;
                }

                debug!(
                    "[PRICE] Cached {} prices for quote {} in memory",
                    price_batch.len(),
                    quote_id
                );
            }

            // Persist to DB
            if let Err(e) = price_controller
                .batch_insert_prices(quote_id, price_batch)
                .await
            {
                error!(
                    "[PRICE] Batch insert failed for quote {}: {:#}",
                    quote_id, e
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
    }

    Ok(())
}
