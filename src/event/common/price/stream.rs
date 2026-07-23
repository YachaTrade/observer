use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{sync::watch, time::Instant};
use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, QuoteConfig, quote_configs},
    db::cache::CacheManager,
    event::{
        common::price::{
            PriceEventChannel, provider,
            sampler::{PRICE_HEAD_OFFSET, PriceSnapshot, run_sampler},
        },
        get_block_timestamp,
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::price::UpdatePrice,
};

use super::receive::receive_events;

/// Cadence for Price range polling and checkpoint processing. Pyth sampling
/// runs independently in the 30-second sampler.
/// Catch-up iterations whose body already exceeds POLL_INTERVAL skip the
/// sleep and run back-to-back, so this caps idle frequency without
/// throttling backfill.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

fn build_events(
    snapshot: &PriceSnapshot,
    quotes: &[QuoteConfig],
    blocks: &[(u64, u64)],
    exact_cache_hits: &HashSet<(String, u64)>,
) -> Vec<UpdatePrice> {
    let mut events = Vec::with_capacity(blocks.len() * quotes.len());

    for &(block_number, block_timestamp) in blocks {
        for quote in quotes {
            if exact_cache_hits.contains(&(quote.address.clone(), block_number)) {
                continue;
            }

            let price = snapshot
                .prices_by_quote
                .get(&quote.address)
                .expect("published price snapshot contains every configured quote");
            events.push(UpdatePrice {
                quote_id: quote.address.clone(),
                block_number,
                price: price.clone(),
                block_timestamp,
            });
        }
    }

    events
}

fn current_snapshot(
    receiver: &watch::Receiver<Option<Arc<PriceSnapshot>>>,
) -> Option<Arc<PriceSnapshot>> {
    receiver.borrow().clone()
}

async fn collect_block_timestamps<F, Fut>(
    from_block: u64,
    to_block: u64,
    mut fetch_timestamp: F,
) -> Result<Vec<(u64, u64)>>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<u64>>,
{
    let mut blocks = Vec::new();
    for block_number in from_block..=to_block {
        let block_timestamp = fetch_timestamp(block_number)
            .await
            .with_context(|| format!("timestamp unavailable for block {block_number}"))?;
        blocks.push((block_number, block_timestamp));
    }
    Ok(blocks)
}

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let mut total_events = 0;
    let (channel, receiver) = PriceEventChannel::new("price_events");

    tokio::spawn(async move {
        if let Err(e) = receive_events(receiver, event_type).await {
            error!("[PRICE] Failed to receive events: {}", e);
        }
    });

    let client = RpcClient::instance()?;
    let cache_manager = CacheManager::instance()?;
    let price_provider = provider::build_provider()?;
    let (snapshot_tx, snapshot_rx) = watch::channel::<Option<Arc<PriceSnapshot>>>(None);
    let sampler_quotes = quote_configs().clone();
    let sampler_provider = Arc::clone(&price_provider);

    tokio::spawn(run_sampler(
        sampler_provider,
        sampler_quotes,
        snapshot_tx,
        move || async move {
            let latest_block = client.get_cached_latest_block();
            let source_block = latest_block.saturating_sub(PRICE_HEAD_OFFSET);
            let source_timestamp = get_block_timestamp(client, source_block).await?;
            Ok((source_block, source_timestamp))
        },
    ));

    loop {
        let iter_start = Instant::now();
        let latest_block = client.get_cached_latest_block();
        let time = Instant::now();
        let BlockRange {
            from_block,
            to_block,
        } = STREAM_MANAGER
            .get_next_block_range(event_type, block_batch_size, latest_block)
            .await;

        if from_block > to_block {
            // Caught up — wait one POLL_INTERVAL before re-checking instead
            // of spinning. New blocks within the interval will be picked up
            // on the next tick; price freshness lag is bounded by the
            // interval (10s) and the receive-side fallback chain
            // (`get_quote_usd_price` falls back to latest-before / latest /
            // DB so swaps never see value=0 unless the cache is cold).
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        }

        let Some(snapshot) = current_snapshot(&snapshot_rx) else {
            warn!("[PRICE] waiting for initial snapshot");
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        };

        let blocks = match collect_block_timestamps(from_block, to_block, |block_number| {
            get_block_timestamp(client, block_number)
        })
        .await
        {
            Ok(blocks) => blocks,
            Err(_) => {
                error!(
                    "[PRICE] block timestamp lookup failed for range {}..={}; retrying without checkpoint advance",
                    from_block, to_block
                );
                if let Some(remaining) = POLL_INTERVAL.checked_sub(iter_start.elapsed()) {
                    tokio::time::sleep(remaining).await;
                }
                continue;
            }
        };

        let mut exact_cache_hits = HashSet::new();
        for &(block_number, _) in &blocks {
            for quote in quote_configs() {
                if cache_manager
                    .get_price_for_quote(&quote.address, block_number as i64)
                    .await
                    .is_some()
                {
                    exact_cache_hits.insert((quote.address.clone(), block_number));
                }
            }
        }

        let events = build_events(
            snapshot.as_ref(),
            quote_configs(),
            &blocks,
            &exact_cache_hits,
        );

        info!(
            "[PRICE] cycle blocks={} rows={} exact_cache_hits={} snapshot_block={} snapshot_age_secs={}",
            blocks.len(),
            events.len(),
            exact_cache_hits.len(),
            snapshot.source_block,
            snapshot.sampled_at.elapsed().as_secs()
        );

        // Get stats before sending events
        let events_count = events.len();
        total_events += events_count;
        let elapsed_ms = time.elapsed().as_millis();

        if let Err(e) = channel.send(events, to_block, latest_block).await {
            error!("[PRICE] Failed to send events: {}", e);
            continue;
        }

        let logging_format = format!(
            "📊 {:?} STREAM: Blocks: from={} to={} | Events: {} | Total Events: {} | Process time: {}ms",
            event_type, from_block, to_block, events_count, total_events, elapsed_ms
        );
        warn!("{}", logging_format);

        block_batch_size = *BLOCK_BATCH_SIZE;

        STREAM_MANAGER
            .set_event_block_processed_block(event_type, to_block)
            .await;

        // Cap idle iteration frequency at POLL_INTERVAL. If the iteration
        // already took longer than that (catch-up / large batch), this is a
        // no-op and the next iteration runs immediately.
        if let Some(remaining) = POLL_INTERVAL.checked_sub(iter_start.elapsed()) {
            tokio::time::sleep(remaining).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use bigdecimal::BigDecimal;
    use tokio::{sync::watch, time::Instant};

    use super::{build_events, collect_block_timestamps, current_snapshot};
    use crate::{config::QuoteConfig, event::common::price::sampler::PriceSnapshot};

    fn quote(address: &str, pyth_feed_id: &str) -> QuoteConfig {
        QuoteConfig {
            address: address.to_string(),
            pyth_feed_id: pyth_feed_id.to_string(),
            decimals: BigDecimal::from(1),
        }
    }

    fn snapshot<const N: usize>(prices: [(&str, i64); N]) -> PriceSnapshot {
        PriceSnapshot {
            prices_by_quote: prices
                .into_iter()
                .map(|(quote_id, price)| (quote_id.to_string(), BigDecimal::from(price)))
                .collect(),
            source_block: 95,
            source_timestamp: 950,
            sampled_at: Instant::now(),
        }
    }

    #[test]
    fn one_snapshot_is_copied_to_every_block_with_original_timestamps() {
        let snapshot = snapshot([("0xaaa", 2_500)]);
        let blocks = vec![(100, 1_000), (101, 1_001), (102, 1_002)];

        let events = build_events(
            &snapshot,
            &[quote("0xaaa", "feed-a")],
            &blocks,
            &HashSet::new(),
        );

        assert_eq!(events.len(), 3);
        assert!(
            events
                .iter()
                .all(|event| event.price == BigDecimal::from(2_500))
        );
        assert_eq!(
            events
                .iter()
                .map(|event| (event.block_number, event.block_timestamp))
                .collect::<Vec<_>>(),
            blocks
        );
    }

    #[test]
    fn exact_cached_quote_block_rows_are_not_emitted_again() {
        let snapshot = snapshot([("0xaaa", 2_500)]);
        let hits = HashSet::from([("0xaaa".to_string(), 101)]);

        let events = build_events(
            &snapshot,
            &[quote("0xaaa", "feed-a")],
            &[(100, 1_000), (101, 1_001)],
            &hits,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 100);
    }

    #[test]
    fn expansion_order_is_block_then_configured_quote() {
        let snapshot = snapshot([("0xbbb", 20), ("0xaaa", 10)]);
        let blocks = [(100, 1_000), (101, 1_001)];

        let events = build_events(
            &snapshot,
            &[quote("0xaaa", "feed-a"), quote("0xbbb", "feed-b")],
            &blocks,
            &HashSet::new(),
        );

        assert_eq!(
            events
                .iter()
                .map(|event| (event.block_number, event.quote_id.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (100, "0xaaa"),
                (100, "0xbbb"),
                (101, "0xaaa"),
                (101, "0xbbb"),
            ]
        );
    }

    #[test]
    fn current_snapshot_is_none_until_sampler_publishes() {
        let (tx, rx) = watch::channel(None);

        assert!(current_snapshot(&rx).is_none());

        let published = Arc::new(snapshot([("0xaaa", 2_500)]));
        tx.send_replace(Some(Arc::clone(&published)));

        let current = current_snapshot(&rx).expect("snapshot is published");
        assert!(Arc::ptr_eq(&current, &published));
    }

    #[tokio::test]
    async fn timestamp_collection_fails_closed_on_any_missing_block() {
        let result = collect_block_timestamps(100, 102, |block_number| async move {
            if block_number == 101 {
                anyhow::bail!("timestamp unavailable");
            }
            Ok(block_number + 900)
        })
        .await;

        assert!(result.is_err());
    }
}
