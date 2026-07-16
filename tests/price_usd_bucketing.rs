//! TDD contract tests (RED) for the price_usd 25-block bucketing helpers.
//!
//! These lock the pure decision logic that makes the DefiLlama (price_usd)
//! stream mirror the Pyth `price` stream's 25-block bucket structure:
//!   * `bucket_of` / `group_into_buckets` — block→bucket alignment (so price &
//!     price_usd rows share the SAME bucket boundaries).
//!   * `select_fetch` — tip buckets use `/current` (fresh); PAST buckets use
//!     `/historical/{ts}` so backfilling old block ranges records the price AS
//!     OF that time, not "now". This is the whole reason historical exists —
//!     a regression here silently stamps current price onto past blocks.
//!   * `buckets_to_fetch` — forward-scan dedupe so we issue ONE fetch per new
//!     bucket (rate-limit safety on DefiLlama free tier), never re-fetching a
//!     bucket already processed.
//!
//! Pure functions — no DB / no Docker. Design contract:
//!   branches/feat-price-usd-block-bucket.md
//!
//! GREEN target (Codex): src/event/common/price_usd/bucket.rs implementing the
//! exact signatures these tests reference, plus `pub mod bucket;` in the
//! price_usd module. Do NOT modify this test file.

use observer::event::common::price_usd::bucket::{
    BUCKET_BLOCK_INTERVAL, BucketGroup, FetchKind, bucket_of, buckets_to_fetch, group_into_buckets,
    select_fetch,
};

#[test]
fn bucket_interval_is_25_matching_pyth_price_stream() {
    // Must equal the Pyth `price` stream's BUCKET_BLOCK_INTERVAL so the two
    // price tables share identical bucket boundaries.
    assert_eq!(BUCKET_BLOCK_INTERVAL, 25);
}

#[test]
fn bucket_of_floors_to_25_block_boundary() {
    assert_eq!(bucket_of(0), 0);
    assert_eq!(bucket_of(1), 0);
    assert_eq!(bucket_of(24), 0);
    assert_eq!(bucket_of(25), 25);
    assert_eq!(bucket_of(26), 25);
    assert_eq!(bucket_of(49), 25);
    assert_eq!(bucket_of(50), 50);
    assert_eq!(bucket_of(124), 100);
    assert_eq!(bucket_of(125), 125);
}

#[test]
fn group_into_buckets_empty_input_yields_no_groups() {
    let groups = group_into_buckets(&[]);
    assert!(groups.is_empty());
}

#[test]
fn group_into_buckets_single_bucket_uses_first_member_timestamp() {
    // Blocks 48,49 both fall in bucket 25. bucket_ts must be the FIRST (lowest
    // block) member's timestamp — that is the anchor we query DefiLlama with.
    let groups = group_into_buckets(&[(48, 4800), (49, 4900)]);
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0],
        BucketGroup {
            bucket_block: 25,
            bucket_ts: 4800,
            blocks: vec![(48, 4800), (49, 4900)],
        }
    );
}

#[test]
fn group_into_buckets_splits_across_25_block_boundary() {
    // 48,49 -> bucket 25 ; 50,51 -> bucket 50. Ascending by bucket_block.
    let groups = group_into_buckets(&[(48, 4800), (49, 4900), (50, 5000), (51, 5100)]);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].bucket_block, 25);
    assert_eq!(groups[0].bucket_ts, 4800);
    assert_eq!(groups[0].blocks, vec![(48, 4800), (49, 4900)]);
    assert_eq!(groups[1].bucket_block, 50);
    assert_eq!(groups[1].bucket_ts, 5000);
    assert_eq!(groups[1].blocks, vec![(50, 5000), (51, 5100)]);
}

#[test]
fn group_into_buckets_block_on_boundary_starts_new_bucket() {
    let groups = group_into_buckets(&[(50, 5000)]);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].bucket_block, 50);
    assert_eq!(groups[0].bucket_ts, 5000);
    assert_eq!(groups[0].blocks, vec![(50, 5000)]);
}

const NOW: u64 = 1_000_000;
const TIP_THRESHOLD: u64 = 120;

#[test]
fn select_fetch_recent_bucket_uses_current() {
    // bucket 10s behind now → live tip → /current (freshest snapshot).
    assert_eq!(
        select_fetch(NOW - 10, NOW, TIP_THRESHOLD),
        FetchKind::Current
    );
}

#[test]
fn select_fetch_exactly_at_threshold_is_still_current() {
    // Boundary inclusive: now - bucket_ts == threshold → still tip.
    assert_eq!(
        select_fetch(NOW - TIP_THRESHOLD, NOW, TIP_THRESHOLD),
        FetchKind::Current
    );
}

#[test]
fn select_fetch_past_bucket_uses_historical_at_bucket_ts() {
    // One second past the threshold → must query historical AT the bucket ts.
    // If this ever returns Current, backfill would stamp "now" onto old blocks.
    assert_eq!(
        select_fetch(NOW - TIP_THRESHOLD - 1, NOW, TIP_THRESHOLD),
        FetchKind::Historical(NOW - TIP_THRESHOLD - 1)
    );
    // Clearly-historical (much older) carries the exact bucket ts through.
    assert_eq!(
        select_fetch(NOW - 5000, NOW, TIP_THRESHOLD),
        FetchKind::Historical(NOW - 5000)
    );
}

// Helper: build minimal BucketGroups with only bucket_block set (the field
// buckets_to_fetch filters on); ts/blocks are irrelevant to this filter.
fn groups_at(bucket_blocks: &[u64]) -> Vec<BucketGroup> {
    bucket_blocks
        .iter()
        .map(|&b| BucketGroup {
            bucket_block: b,
            bucket_ts: b * 100,
            blocks: vec![(b, b * 100)],
        })
        .collect()
}

#[test]
fn buckets_to_fetch_none_last_returns_all() {
    let grouped = groups_at(&[25, 50, 75]);
    let to_fetch = buckets_to_fetch(&grouped, None);
    assert_eq!(
        to_fetch.iter().map(|g| g.bucket_block).collect::<Vec<_>>(),
        vec![25, 50, 75]
    );
}

#[test]
fn buckets_to_fetch_skips_already_fetched_buckets() {
    // last_fetched = 50 → only buckets STRICTLY greater (75). 50 itself is
    // already done; re-fetching it would waste a DefiLlama call.
    let grouped = groups_at(&[25, 50, 75]);
    let to_fetch = buckets_to_fetch(&grouped, Some(50));
    assert_eq!(
        to_fetch.iter().map(|g| g.bucket_block).collect::<Vec<_>>(),
        vec![75]
    );
}

#[test]
fn buckets_to_fetch_all_already_fetched_returns_empty() {
    let grouped = groups_at(&[25, 50, 75]);
    let to_fetch = buckets_to_fetch(&grouped, Some(75));
    assert!(to_fetch.is_empty());
}
