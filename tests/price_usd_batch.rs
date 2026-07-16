//! TDD RED — per-batch bucket processing for the price_usd stream (REWORK).
//!
//! Codex review of the bucketing change flagged two backfill-correctness holes:
//!   * a FAILED DefiLlama fetch still advanced the watermark and dense-stamped a
//!     stale carry-forward price, so a transient 429 permanently mis-priced a
//!     backfilled bucket (never retried);
//!   * the main batch path (provider dispatch / failure) was untested.
//!
//! `build_bucket_events` is the extracted, testable batch step. Contract:
//!   - returns (events, all_ok). On any fetch error it STOPS: the failed bucket
//!     and every later bucket are NOT stamped, `last_fetched_bucket` is NOT
//!     advanced past the last SUCCESSFUL bucket, and all_ok=false so the caller
//!     skips the watermark advance and retries the batch next cycle.
//!   - tip buckets dispatch fetch_current; past buckets dispatch
//!     fetch_historical(bucket_ts). Each successful bucket dense-stamps its own
//!     blocks with the carry-forward price as of that bucket.
//!
//! Do NOT modify this file.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use bigdecimal::BigDecimal;

use observer::event::common::price_usd::provider::PriceUsdProvider;
use observer::event::common::price_usd::stream::build_bucket_events;
use observer::event::common::price_usd::{PriceUsdPoint, WhitelistToken};

const TIP_THRESHOLD: u64 = 120;
const SEARCH_WIDTH: u64 = 3600;

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}
fn wt(id: &str) -> WhitelistToken {
    WhitelistToken {
        token_id: id.to_string(),
        query_id: id.to_string(),
    }
}

/// Records each provider call as "current" or "historical:{ts}", and can be set
/// to fail starting at a given (0-based) call index.
struct RecordingProvider {
    price: BigDecimal,
    fail_from_call: Option<usize>,
    calls: Mutex<Vec<String>>,
}
impl RecordingProvider {
    fn new(price: &str, fail_from_call: Option<usize>) -> Self {
        Self {
            price: bd(price),
            fail_from_call,
            calls: Mutex::new(Vec::new()),
        }
    }
    fn record(&self, what: String) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        let mut calls = self.calls.lock().unwrap();
        let idx = calls.len();
        calls.push(what);
        if self.fail_from_call.is_some_and(|f| idx >= f) {
            anyhow::bail!("simulated DefiLlama failure at call {idx}");
        }
        Ok(HashMap::new()) // filled per-test via fixed price below
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl PriceUsdProvider for RecordingProvider {
    async fn fetch_current(
        &self,
        coin_refs: &[String],
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        self.record("current".to_string())?;
        Ok(coin_refs
            .iter()
            .map(|r| {
                (
                    r.clone(),
                    PriceUsdPoint {
                        price: self.price.clone(),
                        confidence: Some(bd("0.99")),
                    },
                )
            })
            .collect())
    }
    async fn fetch_historical(
        &self,
        coin_refs: &[String],
        timestamp: u64,
        _search_width_secs: u64,
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        self.record(format!("historical:{timestamp}"))?;
        Ok(coin_refs
            .iter()
            .map(|r| {
                (
                    r.clone(),
                    PriceUsdPoint {
                        price: self.price.clone(),
                        confidence: Some(bd("0.99")),
                    },
                )
            })
            .collect())
    }
}

// Build a batch of contiguous (block, ts) pairs with ~0.4s block spacing.
fn batch(from: u64, count: u64, base_ts: u64) -> Vec<(u64, u64)> {
    (0..count).map(|i| (from + i, base_ts + i)).collect()
}

#[tokio::test]
async fn tip_batch_dispatches_current_and_stamps_dense() {
    // Single bucket near `now` -> /current; dense one row per block.
    let now = 1_000_000;
    let blocks = batch(50, 5, now - 10); // bucket 50, ts within tip threshold
    let provider = RecordingProvider::new("0.0226", None);
    let tokens = vec![wt("0xTKN")];
    let mut last_good = HashMap::new();
    let mut last_fetched = None;

    let (events, all_ok) = build_bucket_events(
        &provider,
        &tokens,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(all_ok);
    assert_eq!(provider.calls(), vec!["current".to_string()]);
    assert_eq!(events.len(), 5, "one row per block");
    assert_eq!(last_fetched, Some(50));
}

#[tokio::test]
async fn past_batch_dispatches_historical_at_bucket_ts() {
    // Bucket far older than tip threshold -> /historical at the bucket timestamp.
    let now = 1_000_000;
    let base = now - 5000;
    let blocks = batch(50, 3, base);
    let provider = RecordingProvider::new("0.0226", None);
    let tokens = vec![wt("0xTKN")];
    let mut last_good = HashMap::new();
    let mut last_fetched = None;

    let (_events, all_ok) = build_bucket_events(
        &provider,
        &tokens,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(all_ok);
    // bucket 50's boundary-block ts is the first member ts (base).
    assert_eq!(provider.calls(), vec![format!("historical:{base}")]);
}

#[tokio::test]
async fn fetch_failure_does_not_advance_watermark_or_stamp_failed_bucket() {
    // Two buckets (50, 75); fail on the SECOND fetch. The first bucket stamps
    // and advances; the failed bucket must NOT stamp, watermark stays at 50,
    // and all_ok=false so the caller retries the batch.
    let now = 1_000_000;
    let base = now - 5000; // both buckets historical
    let mut blocks = batch(50, 25, base); // bucket 50: blocks 50..74
    blocks.extend(batch(75, 3, base + 25)); // bucket 75: blocks 75..77
    let provider = RecordingProvider::new("0.0226", Some(1)); // 2nd call (bucket 75) fails
    let tokens = vec![wt("0xTKN")];
    let mut last_good = HashMap::new();
    let mut last_fetched = None;

    let (events, all_ok) = build_bucket_events(
        &provider,
        &tokens,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(
        !all_ok,
        "a failed fetch makes the batch not-ok (retry, no advance)"
    );
    assert_eq!(
        last_fetched,
        Some(50),
        "watermark only past the SUCCESSFUL bucket"
    );
    // Only bucket 50's 25 blocks stamped; bucket 75 (failed) produced nothing.
    assert!(
        events.iter().all(|e| e.block_number < 75),
        "failed bucket's blocks must not be stamped with a stale carry-forward price"
    );
    assert_eq!(events.len(), 25);
}
