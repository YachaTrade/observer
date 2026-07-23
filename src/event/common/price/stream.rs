use std::{collections::HashMap, future::Future, time::Duration};

use anyhow::Result;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, quote_configs},
    db::cache::CacheManager,
    event::{
        common::price::{PriceEventChannel, provider},
        get_block_timestamp,
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::price::UpdatePrice,
};

use super::receive::receive_events;

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::time::sleep;

    use super::collect_block_timestamps;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_collection_overlaps_and_keeps_peak_in_flight_under_limit() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let result = collect_block_timestamps(10, 14, 2, {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            move |block_number| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    let mut observed = peak.load(Ordering::SeqCst);
                    while current > observed
                        && peak
                            .compare_exchange(observed, current, Ordering::SeqCst, Ordering::SeqCst)
                            .is_err()
                    {
                        observed = peak.load(Ordering::SeqCst);
                    }
                    sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(block_number + 100)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            result,
            vec![(10, 110), (11, 111), (12, 112), (13, 113), (14, 114)]
        );
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak > 1, "timestamp lookups did not overlap");
        assert!(peak <= 2, "timestamp concurrency exceeded the limit");
    }

    #[tokio::test]
    async fn bounded_collection_returns_sorted_blocks_after_out_of_order_completion() {
        let result = collect_block_timestamps(1, 4, 4, |block_number| async move {
            let delay = match block_number {
                1 => 40,
                2 => 10,
                3 => 30,
                _ => 0,
            };
            sleep(Duration::from_millis(delay)).await;
            Ok::<_, anyhow::Error>(block_number + 1_000)
        })
        .await
        .unwrap();

        assert_eq!(result, vec![(1, 1_001), (2, 1_002), (3, 1_003), (4, 1_004)]);
    }

    #[tokio::test]
    async fn bounded_collection_rejects_the_entire_range_on_one_error() {
        let error = collect_block_timestamps(7, 9, 2, |block_number| async move {
            if block_number == 8 {
                Err::<u64, _>(anyhow::anyhow!("timestamp lookup failed"))
            } else {
                Ok(block_number + 10)
            }
        })
        .await
        .unwrap_err();

        assert!(error.to_string().contains("block 8"));
        assert!(error.to_string().contains("timestamp lookup failed"));
    }
}

pub(crate) async fn collect_block_timestamps<F, Fut>(
    from_block: u64,
    to_block: u64,
    max_concurrency: usize,
    load_timestamp: F,
) -> Result<Vec<(u64, u64)>>
where
    F: Fn(u64) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<u64>> + Send + 'static,
{
    let limit = max_concurrency.max(1);
    let mut join_set = JoinSet::new();
    let mut next_block = from_block;
    let mut collected = Vec::with_capacity((to_block - from_block + 1) as usize);

    while next_block <= to_block || !join_set.is_empty() {
        while next_block <= to_block && join_set.len() < limit {
            let block_number = next_block;
            next_block += 1;
            let future = load_timestamp(block_number);
            join_set.spawn(async move {
                let result = future.await;
                (block_number, result)
            });
        }

        let Some(join_result) = join_set.join_next().await else {
            break;
        };

        let (block_number, result) =
            join_result.map_err(|error| anyhow::anyhow!("timestamp task join failed: {error}"))?;
        let timestamp = result.map_err(|error| {
            anyhow::anyhow!(
                "failed to load timestamp for block {}: {}",
                block_number,
                error
            )
        })?;
        collected.push((block_number, timestamp));
    }

    collected.sort_by_key(|(block_number, _)| *block_number);
    Ok(collected)
}

/// Cadence for the price stream loop. With Pyth's 30 req / 10s budget and
/// 2s timestamp normalization, a 10s cycle keeps each iteration's fetch
/// count well below the limit (≈ quote_count × 5 fetches per cycle).
/// Catch-up iterations whose body already exceeds POLL_INTERVAL skip the
/// sleep and run back-to-back, so this caps idle frequency without
/// throttling backfill.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

async fn wait_for_next_cycle(iteration_started: Instant) {
    if let Some(remaining) = POLL_INTERVAL.checked_sub(iteration_started.elapsed()) {
        tokio::time::sleep(remaining).await;
    }
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

    'stream: loop {
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

        let mut events: Vec<UpdatePrice> = Vec::new();

        // Group blocks by normalized timestamp while keeping original timestamp info
        let mut timestamp_to_blocks: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        let block_timestamps = match collect_block_timestamps(
            from_block,
            to_block,
            32,
            move |block_number| async move { get_block_timestamp(client, block_number).await },
        )
        .await
        {
            Ok(block_timestamps) => block_timestamps,
            Err(e) => {
                error!("[PRICE] Failed to collect block timestamps: {}", e);
                wait_for_next_cycle(iter_start).await;
                continue 'stream;
            }
        };

        for (block_number, block_timestamp) in block_timestamps {
            const BUCKET_BLOCK_INTERVAL: u64 = 25;
            let bucket_block = block_number - (block_number % BUCKET_BLOCK_INTERVAL);
            timestamp_to_blocks
                .entry(bucket_block)
                .or_default()
                .push((block_number, block_timestamp));
        }

        // Batch-fetch prices for ALL quote tokens at each timestamp.
        // One Pyth call per (timestamp) regardless of quote count, so the
        // request budget scales with chain block production, not the size
        // of the quote set.
        let mut fetch_attempted = 0usize;
        let mut fetch_skipped_cached = 0usize;
        let mut fetch_succeeded = 0usize;
        let mut fetch_failed = 0usize;
        let mut bucket_blocks: Vec<u64> = timestamp_to_blocks.keys().copied().collect();
        bucket_blocks.sort_unstable();
        for bucket_block in bucket_blocks {
            let block_data = timestamp_to_blocks
                .get(&bucket_block)
                .expect("bucket key collected from timestamp_to_blocks");
            // Skip the bucket only if every quote already has a cached
            // price for the first block in the bucket. Any miss → batch fetch.
            let first_block = block_data.first().map(|(block, _)| *block as i64);
            let mut needs_fetch = first_block.is_none();
            if let Some(block_num) = first_block {
                for q in quote_configs().iter() {
                    if cache_manager
                        .get_price_for_quote(&q.address, block_num)
                        .await
                        .is_none()
                    {
                        needs_fetch = true;
                        break;
                    }
                }
            }
            if !needs_fetch {
                fetch_skipped_cached += 1;
                continue;
            }

            // Query Pyth with the bucket block's own timestamp so both
            // services hit the same /v2/updates/price/{ts} regardless of
            // which catch-up cycle is currently processing this bucket.
            let bucket_timestamp = match block_data
                .iter()
                .find_map(|(block, timestamp)| (*block == bucket_block).then_some(*timestamp))
            {
                Some(timestamp) => timestamp,
                None => match get_block_timestamp(client, bucket_block).await {
                    Ok(timestamp) => timestamp,
                    Err(error) => {
                        error!(
                            "[PRICE] Failed to load aligned bucket timestamp for block {}: {}",
                            bucket_block, error
                        );
                        fetch_failed += 1;
                        continue;
                    }
                },
            };

            let feed_ids: Vec<&str> = quote_configs()
                .iter()
                .map(|q| q.pyth_feed_id.as_str())
                .collect();

            fetch_attempted += 1;
            match price_provider
                .fetch_batch(&feed_ids, bucket_timestamp)
                .await
            {
                Ok(prices) => {
                    fetch_succeeded += 1;
                    for q in quote_configs().iter() {
                        let key = provider::normalize_feed_id(&q.pyth_feed_id);
                        let Some(price) = prices.get(&key) else {
                            warn!(
                                "[PRICE] Batch response missing feed for quote {} (feed_id={}) at timestamp {}",
                                q.address, q.pyth_feed_id, bucket_timestamp
                            );
                            continue;
                        };
                        for (block_number, original_timestamp) in block_data {
                            events.push(UpdatePrice {
                                quote_id: q.address.clone(),
                                block_number: *block_number,
                                price: price.clone(),
                                block_timestamp: *original_timestamp,
                            });
                        }
                    }
                }
                Err(e) => {
                    fetch_failed += 1;
                    error!(
                        "[PRICE] Batch fetch failed at timestamp {}: {}",
                        bucket_timestamp, e
                    );
                }
            }
        }

        info!(
            "[PRICE] cycle ts_buckets={} fetched={} skipped_cached={} ok={} fail={}",
            timestamp_to_blocks.len(),
            fetch_attempted,
            fetch_skipped_cached,
            fetch_succeeded,
            fetch_failed
        );

        // Get stats before sending events
        let events_count = events.len();
        total_events += events_count;
        let elapsed_ms = time.elapsed().as_millis();

        channel.send(events, to_block, latest_block).await?;

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
        wait_for_next_cycle(iter_start).await;
    }
}
