use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use observer::config::QuoteConfig;
use observer::event::common::price::{
    provider::{PriceProvider, normalize_feed_id},
    stream::{build_bucket_events, collect_block_timestamps},
};
use observer::event::common::price_usd::bucket::group_into_buckets;
use observer::event::common::{
    BUCKET_WINDOW_SECS, HISTORICAL_WINDOW_SECS, TIER_AGE_SECS, bucket_of_ts_tiered,
    bucket_width_for,
};
use tokio::time::sleep;

#[test]
fn groups_price_requests_by_sixty_second_timestamp_window() {
    let groups = group_into_buckets(&[(48, 6_042), (49, 6_059), (50, 6_060), (51, 6_075)]);

    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].bucket_ts, 6_000);
    assert_eq!(groups[0].blocks, vec![(48, 6_042), (49, 6_059)]);
    assert_eq!(groups[1].bucket_ts, 6_060);
    assert_eq!(groups[1].blocks, vec![(50, 6_060), (51, 6_075)]);
}

const NOW: u64 = 1_785_600_000;

#[test]
fn uses_sixty_seconds_near_tip_and_six_hundred_seconds_when_historical() {
    assert_eq!(
        bucket_width_for(NOW - TIER_AGE_SECS, NOW),
        BUCKET_WINDOW_SECS
    );
    assert_eq!(
        bucket_width_for(NOW - TIER_AGE_SECS - 1, NOW),
        HISTORICAL_WINDOW_SECS
    );

    let old_timestamp = 1_785_592_800 + 137;
    assert_eq!(bucket_of_ts_tiered(old_timestamp, NOW), 1_785_592_800);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_collection_is_bounded_concurrent_and_deterministic() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let collected = collect_block_timestamps(vec![4, 1, 3, 2], 2, {
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        move |block_number| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                sleep(Duration::from_millis(5 * (5 - block_number))).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(block_number + 1_000)
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(
        collected,
        vec![(1, 1_001), (2, 1_002), (3, 1_003), (4, 1_004)]
    );
    assert!(peak.load(Ordering::SeqCst) > 1);
    assert!(peak.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn one_timestamp_failure_rejects_the_complete_collection() {
    let error = collect_block_timestamps(vec![10, 11, 12], 3, |block_number| async move {
        if block_number == 11 {
            anyhow::bail!("simulated timestamp failure")
        }
        Ok(block_number + 1_000)
    })
    .await
    .unwrap_err();

    assert!(error.to_string().contains("block 11"));
    assert!(error.to_string().contains("simulated timestamp failure"));
}

struct RecordingProvider {
    fail_on: Option<usize>,
    calls: Mutex<Vec<u64>>,
}

#[async_trait]
impl PriceProvider for RecordingProvider {
    async fn fetch(&self, _feed_id: &str, _timestamp: u64) -> anyhow::Result<Option<BigDecimal>> {
        unreachable!("the bucket orchestrator only uses fetch_batch")
    }

    async fn fetch_batch(
        &self,
        feed_ids: &[&str],
        timestamp: u64,
    ) -> anyhow::Result<HashMap<String, BigDecimal>> {
        let index = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(timestamp);
            calls.len() - 1
        };
        if self.fail_on == Some(index) {
            anyhow::bail!("simulated Pyth failure")
        }
        Ok(feed_ids
            .iter()
            .map(|feed| {
                (
                    normalize_feed_id(feed),
                    BigDecimal::from_str(&timestamp.to_string()).unwrap(),
                )
            })
            .collect())
    }
}

fn quote() -> QuoteConfig {
    QuoteConfig {
        address: "0xquote".to_string(),
        pyth_feed_id: "0xfeed".to_string(),
        decimals: BigDecimal::from(18),
    }
}

struct SingleFeedProvider;

#[async_trait]
impl PriceProvider for SingleFeedProvider {
    async fn fetch(&self, _feed_id: &str, _timestamp: u64) -> anyhow::Result<Option<BigDecimal>> {
        unreachable!("the bucket orchestrator only uses fetch_batch")
    }

    async fn fetch_batch(
        &self,
        _feed_ids: &[&str],
        _timestamp: u64,
    ) -> anyhow::Result<HashMap<String, BigDecimal>> {
        Ok(HashMap::from([(
            "feed-a".to_string(),
            BigDecimal::from(42),
        )]))
    }
}

fn configured_quote(address: &str, feed: &str) -> QuoteConfig {
    QuoteConfig {
        address: address.to_string(),
        pyth_feed_id: feed.to_string(),
        decimals: BigDecimal::from(18),
    }
}

#[tokio::test]
async fn reuses_successful_bucket_across_cycles_and_keeps_failure_gap_retryable() {
    let provider = RecordingProvider {
        fail_on: Some(1),
        calls: Mutex::new(Vec::new()),
    };
    let quotes = vec![quote()];
    let mut carried = HashMap::new();
    let mut watermark = None;

    let first_cycle = BTreeMap::from([
        (60, vec![(100, 61)]),
        (120, vec![(200, 121)]),
        (180, vec![(300, 181)]),
    ]);
    let (first_events, all_ok) = build_bucket_events(
        &provider,
        &quotes,
        &first_cycle,
        &mut carried,
        &mut watermark,
    )
    .await;

    assert!(!all_ok);
    assert_eq!(watermark, Some(60));
    assert!(first_events.iter().all(|event| event.block_number != 200));
    assert!(first_events.iter().any(|event| event.block_number == 300));

    let replayed_early_bucket = BTreeMap::from([(60, vec![(101, 62)])]);
    let (replayed_events, replayed_ok) = build_bucket_events(
        &provider,
        &quotes,
        &replayed_early_bucket,
        &mut carried,
        &mut watermark,
    )
    .await;
    assert!(replayed_ok);
    assert_eq!(*provider.calls.lock().unwrap(), vec![60, 120, 180]);
    assert_eq!(replayed_events[0].price, BigDecimal::from(60));

    let retry = BTreeMap::from([(120, vec![(200, 121)])]);
    let (retry_events, retry_ok) =
        build_bucket_events(&provider, &quotes, &retry, &mut carried, &mut watermark).await;
    assert!(retry_ok);
    assert_eq!(*provider.calls.lock().unwrap(), vec![60, 120, 180, 120]);
    assert_eq!(retry_events.len(), 1);

    let same_bucket_later_cycle = BTreeMap::from([(120, vec![(201, 122)])]);
    let (reused_events, reused_ok) = build_bucket_events(
        &provider,
        &quotes,
        &same_bucket_later_cycle,
        &mut carried,
        &mut watermark,
    )
    .await;
    assert!(reused_ok);
    assert_eq!(*provider.calls.lock().unwrap(), vec![60, 120, 180, 120]);
    assert_eq!(reused_events.len(), 1);
}

#[tokio::test]
async fn a_lower_rebucketed_timestamp_is_fetched_instead_of_reusing_a_newer_bucket() {
    let provider = RecordingProvider {
        fail_on: None,
        calls: Mutex::new(Vec::new()),
    };
    let quotes = vec![quote()];
    let mut carried = HashMap::from([("0xquote".to_string(), BigDecimal::from(600))]);
    let mut watermark = Some(600);
    let rebucketed = BTreeMap::from([(0, vec![(100, 59)])]);

    let (events, all_ok) = build_bucket_events(
        &provider,
        &quotes,
        &rebucketed,
        &mut carried,
        &mut watermark,
    )
    .await;

    assert!(all_ok);
    assert_eq!(*provider.calls.lock().unwrap(), vec![0]);
    assert_eq!(events[0].price, BigDecimal::from(0));
    assert_eq!(watermark, Some(0));
}

#[tokio::test]
async fn incomplete_provider_response_does_not_mutate_stamp_or_advance() {
    let quotes = vec![
        configured_quote("quote-a", "feed-a"),
        configured_quote("quote-b", "feed-b"),
    ];
    let original = HashMap::from([("quote-a".to_string(), BigDecimal::from(7))]);
    let mut carried = original.clone();
    let mut watermark = None;
    let buckets = BTreeMap::from([(60, vec![(100, 61)])]);

    let (events, all_ok) = build_bucket_events(
        &SingleFeedProvider,
        &quotes,
        &buckets,
        &mut carried,
        &mut watermark,
    )
    .await;

    assert!(!all_ok);
    assert!(events.is_empty());
    assert_eq!(carried, original);
    assert_eq!(watermark, None);
}

#[tokio::test]
async fn duplicate_quotes_sharing_one_present_feed_both_stamp() {
    let quotes = vec![
        configured_quote("quote-a", "feed-a"),
        configured_quote("quote-b", "feed-a"),
    ];
    let mut carried = HashMap::new();
    let mut watermark = None;
    let buckets = BTreeMap::from([(60, vec![(100, 61)])]);

    let (events, all_ok) = build_bucket_events(
        &SingleFeedProvider,
        &quotes,
        &buckets,
        &mut carried,
        &mut watermark,
    )
    .await;

    assert!(all_ok);
    assert_eq!(events.len(), 2);
    assert_eq!(watermark, Some(60));
}
