use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    time::Duration,
};

use anyhow::Result;
use bigdecimal::BigDecimal;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, QuoteConfig, quote_configs},
    db::cache::CacheManager,
    event::{
        common::price::{PriceEventChannel, provider},
        get_block_timestamp,
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::price::UpdatePrice,
};

use super::receive::receive_events;

const PRICE_BUCKET_BLOCK_INTERVAL: u64 = 100;

fn canonical_bucket_block(block_number: u64) -> u64 {
    block_number - (block_number % PRICE_BUCKET_BLOCK_INTERVAL)
}

fn group_block_timestamps_by_bucket(blocks: Vec<(u64, u64)>) -> BTreeMap<u64, Vec<(u64, u64)>> {
    let mut buckets = BTreeMap::new();
    for (block_number, block_timestamp) in blocks {
        buckets
            .entry(canonical_bucket_block(block_number))
            .or_insert_with(Vec::new)
            .push((block_number, block_timestamp));
    }
    buckets
}

fn canonical_timestamp_from_range(bucket_block: u64, blocks: &[(u64, u64)]) -> Option<u64> {
    blocks
        .iter()
        .find_map(|(block, timestamp)| (*block == bucket_block).then_some(*timestamp))
}

fn expand_bucket_events(
    blocks: &[(u64, u64)],
    quote_prices: &BTreeMap<String, BigDecimal>,
) -> Vec<UpdatePrice> {
    let mut events = Vec::with_capacity(blocks.len() * quote_prices.len());
    for (quote_id, price) in quote_prices {
        for (block_number, block_timestamp) in blocks {
            events.push(UpdatePrice {
                quote_id: quote_id.clone(),
                block_number: *block_number,
                price: price.clone(),
                block_timestamp: *block_timestamp,
            });
        }
    }
    events
}

fn all_quotes_resolved(quotes: &[QuoteConfig], prices: &BTreeMap<String, BigDecimal>) -> bool {
    quotes
        .iter()
        .all(|quote| prices.contains_key(&quote.address))
}

fn merge_missing_quote_prices(
    quotes: &[QuoteConfig],
    prices: &mut BTreeMap<String, BigDecimal>,
    fetched: &HashMap<String, BigDecimal>,
) -> Vec<(String, BigDecimal)> {
    let mut newly_resolved = Vec::new();
    for quote in quotes {
        if prices.contains_key(&quote.address) {
            continue;
        }
        let feed_id = provider::normalize_feed_id(&quote.pyth_feed_id);
        if let Some(price) = fetched.get(&feed_id) {
            prices.insert(quote.address.clone(), price.clone());
            newly_resolved.push((quote.address.clone(), price.clone()));
        }
    }
    newly_resolved
}

enum BucketFetchOutcome {
    SkippedCached,
    Fetched {
        canonical_block: u64,
        newly_resolved: Vec<(String, BigDecimal)>,
    },
    Failed {
        canonical_block: u64,
        canonical_timestamp: Option<u64>,
        error: anyhow::Error,
    },
}

struct BucketResolution {
    prices: BTreeMap<String, BigDecimal>,
    fetch: BucketFetchOutcome,
}

async fn resolve_bucket_prices<F, Fut>(
    bucket_block: u64,
    blocks: &[(u64, u64)],
    quotes: &[QuoteConfig],
    mut prices: BTreeMap<String, BigDecimal>,
    price_provider: &dyn provider::PriceProvider,
    load_timestamp: F,
) -> BucketResolution
where
    F: FnOnce(u64) -> Fut,
    Fut: Future<Output = Result<u64>>,
{
    if all_quotes_resolved(quotes, &prices) {
        return BucketResolution {
            prices,
            fetch: BucketFetchOutcome::SkippedCached,
        };
    }

    let canonical_timestamp = match canonical_timestamp_from_range(bucket_block, blocks) {
        Some(timestamp) => timestamp,
        None => match load_timestamp(bucket_block).await {
            Ok(timestamp) => timestamp,
            Err(error) => {
                return BucketResolution {
                    prices,
                    fetch: BucketFetchOutcome::Failed {
                        canonical_block: bucket_block,
                        canonical_timestamp: None,
                        error,
                    },
                };
            }
        },
    };

    let feed_ids: Vec<&str> = quotes
        .iter()
        .map(|quote| quote.pyth_feed_id.as_str())
        .collect();
    match price_provider
        .fetch_batch(&feed_ids, canonical_timestamp)
        .await
    {
        Ok(fetched) => {
            let newly_resolved = merge_missing_quote_prices(quotes, &mut prices, &fetched);
            BucketResolution {
                prices,
                fetch: BucketFetchOutcome::Fetched {
                    canonical_block: bucket_block,
                    newly_resolved,
                },
            }
        }
        Err(error) => BucketResolution {
            prices,
            fetch: BucketFetchOutcome::Failed {
                canonical_block: bucket_block,
                canonical_timestamp: Some(canonical_timestamp),
                error,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use bigdecimal::BigDecimal;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    use crate::{config::QuoteConfig, event::common::price::provider::PriceProvider};

    use super::{
        BucketFetchOutcome, all_quotes_resolved, canonical_bucket_block,
        canonical_timestamp_from_range, collect_block_timestamps, expand_bucket_events,
        group_block_timestamps_by_bucket, merge_missing_quote_prices, resolve_bucket_prices,
    };

    fn quote(address: &str, feed: &str) -> QuoteConfig {
        QuoteConfig {
            address: address.to_string(),
            pyth_feed_id: feed.to_string(),
            decimals: BigDecimal::from(18),
        }
    }

    struct RecordingProvider {
        calls: AtomicUsize,
        timestamps: Mutex<Vec<u64>>,
        prices: HashMap<String, BigDecimal>,
        fail: bool,
    }

    #[async_trait]
    impl PriceProvider for RecordingProvider {
        async fn fetch(&self, _feed_id: &str, _timestamp: u64) -> Result<Option<BigDecimal>> {
            Ok(None)
        }

        async fn fetch_batch(
            &self,
            _feed_ids: &[&str],
            timestamp: u64,
        ) -> Result<HashMap<String, BigDecimal>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.timestamps.lock().await.push(timestamp);
            if self.fail {
                return Err(anyhow::anyhow!("provider failure"));
            }
            Ok(self.prices.clone())
        }
    }

    #[tokio::test]
    async fn cached_bucket_skips_timestamp_and_provider_orchestration() {
        let quotes = vec![quote("quote-a", "feed-a")];
        let cached = BTreeMap::from([("quote-a".to_string(), BigDecimal::from(10))]);
        let provider = RecordingProvider {
            calls: AtomicUsize::new(0),
            timestamps: Mutex::new(Vec::new()),
            prices: HashMap::new(),
            fail: false,
        };

        let resolution = resolve_bucket_prices(
            800,
            &[(855, 1_855)],
            &quotes,
            cached,
            &provider,
            |_| async { Err(anyhow::anyhow!("timestamp lookup must be skipped")) },
        )
        .await;

        assert!(matches!(
            resolution.fetch,
            BucketFetchOutcome::SkippedCached
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert!(provider.timestamps.lock().await.is_empty());
    }

    #[tokio::test]
    async fn mid_bucket_miss_fetches_boundary_timestamp_and_returns_canonical_cache_update() {
        let quotes = vec![quote("quote-a", "0xfeed-a")];
        let provider = RecordingProvider {
            calls: AtomicUsize::new(0),
            timestamps: Mutex::new(Vec::new()),
            prices: HashMap::from([("feed-a".to_string(), BigDecimal::from(42))]),
            fail: false,
        };
        let requested_blocks = Arc::new(Mutex::new(Vec::new()));

        let resolution =
            resolve_bucket_prices(800, &[(855, 1_855)], &quotes, BTreeMap::new(), &provider, {
                let requested_blocks = Arc::clone(&requested_blocks);
                move |block| async move {
                    requested_blocks.lock().await.push(block);
                    Ok(1_800)
                }
            })
            .await;

        let BucketFetchOutcome::Fetched {
            canonical_block,
            newly_resolved,
        } = resolution.fetch
        else {
            panic!("expected a provider fetch");
        };
        assert_eq!(canonical_block, 800);
        assert_eq!(
            newly_resolved,
            vec![("quote-a".to_string(), BigDecimal::from(42))]
        );
        assert_eq!(*requested_blocks.lock().await, vec![800]);
        assert_eq!(*provider.timestamps.lock().await, vec![1_800]);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(resolution.prices["quote-a"], BigDecimal::from(42));
    }

    #[tokio::test]
    async fn later_failed_bucket_does_not_discard_earlier_bucket_events() {
        let quotes = vec![quote("quote-a", "feed-a")];
        let successful_provider = RecordingProvider {
            calls: AtomicUsize::new(0),
            timestamps: Mutex::new(Vec::new()),
            prices: HashMap::from([("feed-a".to_string(), BigDecimal::from(42))]),
            fail: false,
        };
        let failed_provider = RecordingProvider {
            calls: AtomicUsize::new(0),
            timestamps: Mutex::new(Vec::new()),
            prices: HashMap::new(),
            fail: true,
        };
        let mut events = Vec::new();

        let first = resolve_bucket_prices(
            800,
            &[(899, 1_899)],
            &quotes,
            BTreeMap::new(),
            &successful_provider,
            |_| async { Ok(1_800) },
        )
        .await;
        events.extend(expand_bucket_events(&[(899, 1_899)], &first.prices));

        let second = resolve_bucket_prices(
            900,
            &[(900, 1_900)],
            &quotes,
            BTreeMap::new(),
            &failed_provider,
            |_| async { Ok(1_900) },
        )
        .await;
        assert!(matches!(
            second.fetch,
            BucketFetchOutcome::Failed {
                canonical_block: 900,
                canonical_timestamp: Some(1_900),
                ..
            }
        ));
        events.extend(expand_bucket_events(&[(900, 1_900)], &second.prices));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 899);
        assert_eq!(events[0].price, BigDecimal::from(42));
    }

    #[test]
    fn fully_cached_bucket_does_not_need_provider_fetch() {
        let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
        let prices = BTreeMap::from([
            ("quote-a".to_string(), BigDecimal::from(10)),
            ("quote-b".to_string(), BigDecimal::from(20)),
        ]);

        assert!(all_quotes_resolved(&quotes, &prices));
    }

    #[test]
    fn missing_canonical_quote_requires_provider_fetch() {
        let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
        let prices = BTreeMap::from([("quote-a".to_string(), BigDecimal::from(10))]);

        assert!(!all_quotes_resolved(&quotes, &prices));
    }

    #[test]
    fn provider_results_fill_only_missing_quotes() {
        let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
        let mut prices = BTreeMap::from([("quote-a".to_string(), BigDecimal::from(10))]);
        let fetched = HashMap::from([
            ("feed-a".to_string(), BigDecimal::from(999)),
            ("feed-b".to_string(), BigDecimal::from(20)),
        ]);

        let newly_resolved = merge_missing_quote_prices(&quotes, &mut prices, &fetched);

        assert_eq!(prices["quote-a"], BigDecimal::from(10));
        assert_eq!(prices["quote-b"], BigDecimal::from(20));
        assert_eq!(
            newly_resolved,
            vec![("quote-b".to_string(), BigDecimal::from(20))]
        );
    }

    #[test]
    fn unresolved_provider_quote_does_not_discard_cached_quote_events() {
        let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
        let mut prices = BTreeMap::from([("quote-a".to_string(), BigDecimal::from(10))]);

        let newly_resolved = merge_missing_quote_prices(&quotes, &mut prices, &HashMap::new());
        let events = expand_bucket_events(&[(855, 1_855)], &prices);

        assert!(newly_resolved.is_empty());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].quote_id, "quote-a");
        assert_eq!(events[0].block_number, 855);
        assert_eq!(events[0].price, BigDecimal::from(10));
    }

    #[test]
    fn canonical_price_bucket_floors_to_the_absolute_hundred_block() {
        assert_eq!(canonical_bucket_block(800), 800);
        assert_eq!(canonical_bucket_block(801), 800);
        assert_eq!(canonical_bucket_block(899), 800);
        assert_eq!(canonical_bucket_block(900), 900);
    }

    #[test]
    fn groups_blocks_across_hundred_block_boundaries() {
        let buckets =
            group_block_timestamps_by_bucket(vec![(899, 1_899), (900, 1_900), (901, 1_901)]);

        assert_eq!(buckets.get(&800), Some(&vec![(899, 1_899)]));
        assert_eq!(buckets.get(&900), Some(&vec![(900, 1_900), (901, 1_901)]));
    }

    #[test]
    fn mid_bucket_range_requires_canonical_boundary_timestamp_fallback() {
        assert_eq!(
            canonical_timestamp_from_range(800, &[(855, 1_855), (856, 1_856)]),
            None
        );
        assert_eq!(
            canonical_timestamp_from_range(800, &[(800, 1_800), (801, 1_801)]),
            Some(1_800)
        );
    }

    #[test]
    fn expands_one_canonical_price_to_every_block_in_the_bucket() {
        let blocks = vec![(801, 1_801), (802, 1_802), (899, 1_899)];
        let quote_prices = BTreeMap::from([("quote-a".to_string(), BigDecimal::from(3_500))]);

        let events = expand_bucket_events(&blocks, &quote_prices);

        assert_eq!(events.len(), 3);
        assert_eq!(
            events
                .iter()
                .map(|event| event.block_number)
                .collect::<Vec<_>>(),
            vec![801, 802, 899]
        );
        assert!(
            events
                .iter()
                .all(|event| event.price == BigDecimal::from(3_500))
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event.block_timestamp)
                .collect::<Vec<_>>(),
            vec![1_801, 1_802, 1_899]
        );
    }

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

/// Cadence for checking stream progress. Pyth fetch frequency is governed by
/// canonical 100-block cache misses and the provider's request limiter.
/// Catch-up iterations whose body already exceeds `POLL_INTERVAL` skip the
/// sleep and run back-to-back, so this caps idle frequency without throttling
/// backfill.
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
        let bucket_to_blocks = group_block_timestamps_by_bucket(block_timestamps);

        // Resolve every quote at the absolute 100-block boundary. A fully
        // cached bucket makes no provider call; otherwise all feeds are
        // fetched in one Pyth request at the canonical block timestamp.
        let mut fetch_attempted = 0usize;
        let mut fetch_skipped_cached = 0usize;
        let mut fetch_succeeded = 0usize;
        let mut fetch_failed = 0usize;
        for (bucket_block, block_data) in &bucket_to_blocks {
            let mut cached_prices = BTreeMap::new();
            for quote in quote_configs() {
                if let Some(price) = cache_manager
                    .get_price_for_quote(&quote.address, *bucket_block as i64)
                    .await
                {
                    cached_prices.insert(quote.address.clone(), price.as_ref().clone());
                }
            }

            let BucketResolution {
                prices: resolved_prices,
                fetch,
            } = resolve_bucket_prices(
                *bucket_block,
                block_data,
                quote_configs(),
                cached_prices,
                price_provider.as_ref(),
                move |block| async move { get_block_timestamp(client, block).await },
            )
            .await;

            match fetch {
                BucketFetchOutcome::SkippedCached => {
                    fetch_skipped_cached += 1;
                }
                BucketFetchOutcome::Fetched {
                    canonical_block,
                    newly_resolved,
                } => {
                    fetch_attempted += 1;
                    fetch_succeeded += 1;
                    for (quote_id, price) in newly_resolved {
                        cache_manager
                            .insert_price_for_quote(&quote_id, canonical_block as i64, price)
                            .await;
                    }
                }
                BucketFetchOutcome::Failed {
                    canonical_block,
                    canonical_timestamp,
                    error: fetch_error,
                } => {
                    fetch_failed += 1;
                    if let Some(canonical_timestamp) = canonical_timestamp {
                        fetch_attempted += 1;
                        error!(
                            "[PRICE] Batch fetch failed at canonical block {} timestamp {}: {}",
                            canonical_block, canonical_timestamp, fetch_error
                        );
                    } else {
                        error!(
                            "[PRICE] Failed to load canonical bucket timestamp for block {}: {}",
                            canonical_block, fetch_error
                        );
                    }
                }
            }

            for quote in quote_configs() {
                if !resolved_prices.contains_key(&quote.address) {
                    warn!(
                        "[PRICE] Canonical bucket {} has no price for quote {} (feed_id={})",
                        bucket_block, quote.address, quote.pyth_feed_id
                    );
                }
            }
            events.extend(expand_bucket_events(block_data, &resolved_prices));
        }

        info!(
            "[PRICE] cycle canonical_buckets={} fetched={} skipped_cached={} ok={} fail={}",
            bucket_to_blocks.len(),
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
