use std::{collections::HashMap, time::Duration};

use anyhow::Result;
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

/// Cadence for the price stream loop. With Pyth's 30 req / 10s budget and
/// 2s timestamp normalization, a 10s cycle keeps each iteration's fetch
/// count well below the limit (≈ quote_count × 5 fetches per cycle).
/// Catch-up iterations whose body already exceeds POLL_INTERVAL skip the
/// sleep and run back-to-back, so this caps idle frequency without
/// throttling backfill.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

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

        let mut events: Vec<UpdatePrice> = Vec::new();

        // Group blocks by normalized timestamp while keeping original timestamp info
        let mut timestamp_to_blocks: HashMap<u64, Vec<(u64, u64)>> = HashMap::new(); // normalized_ts -> Vec<(block_number, original_ts)>

        for block_number in from_block..=to_block {
            let block_timestamp = match get_block_timestamp(client, block_number).await {
                Ok(ts) => ts,
                Err(e) => {
                    error!("Failed to get timestamp for block {}: {}", block_number, e);
                    continue;
                }
            };

            // Block-modulo-25 bucketing — every 25 blocks shares a single
            // Pyth fetch. Block-modulo (vs the previous 10s
            // timestamp-modulo) is fully deterministic regardless of the
            // chain's block-time variance and matches websocket-server's
            // BUCKET_BLOCK_INTERVAL exactly, so both services always query
            // Pyth with the timestamp of the SAME bucket block. The price
            // stored in observer's `price` table and the price cached in
            // websocket-server's in-memory map never diverge for blocks
            // that share a bucket.
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
        for (bucket_block, block_data) in &timestamp_to_blocks {
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
            let bucket_timestamp = match get_block_timestamp(client, *bucket_block).await {
                Ok(ts) => ts,
                Err(e) => {
                    error!(
                        "Failed to get timestamp for bucket block {}: {}",
                        bucket_block, e
                    );
                    fetch_failed += 1;
                    continue;
                }
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
