//! Pure contracts for the `price_usd` 25-block bucketing helpers.

use observer::event::common::price_usd::bucket::{
    BUCKET_BLOCK_INTERVAL, BucketGroup, FetchKind, bucket_of, buckets_to_fetch, group_into_buckets,
    select_fetch,
};

#[test]
fn bucket_interval_is_25_matching_price_stream() {
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
    assert_eq!(
        select_fetch(NOW - 10, NOW, TIP_THRESHOLD),
        FetchKind::Current
    );
}

#[test]
fn select_fetch_exactly_at_threshold_is_still_current() {
    assert_eq!(
        select_fetch(NOW - TIP_THRESHOLD, NOW, TIP_THRESHOLD),
        FetchKind::Current
    );
}

#[test]
fn select_fetch_past_bucket_uses_historical_at_bucket_ts() {
    assert_eq!(
        select_fetch(NOW - TIP_THRESHOLD - 1, NOW, TIP_THRESHOLD),
        FetchKind::Historical(NOW - TIP_THRESHOLD - 1)
    );
    assert_eq!(
        select_fetch(NOW - 5000, NOW, TIP_THRESHOLD),
        FetchKind::Historical(NOW - 5000)
    );
}

fn groups_at(bucket_blocks: &[u64]) -> Vec<BucketGroup> {
    bucket_blocks
        .iter()
        .map(|&bucket| BucketGroup {
            bucket_block: bucket,
            bucket_ts: bucket * 100,
            blocks: vec![(bucket, bucket * 100)],
        })
        .collect()
}

#[test]
fn buckets_to_fetch_none_last_returns_all() {
    let grouped = groups_at(&[25, 50, 75]);
    let to_fetch = buckets_to_fetch(&grouped, None);
    assert_eq!(
        to_fetch
            .iter()
            .map(|group| group.bucket_block)
            .collect::<Vec<_>>(),
        vec![25, 50, 75]
    );
}

#[test]
fn buckets_to_fetch_skips_already_fetched_buckets() {
    let grouped = groups_at(&[25, 50, 75]);
    let to_fetch = buckets_to_fetch(&grouped, Some(50));
    assert_eq!(
        to_fetch
            .iter()
            .map(|group| group.bucket_block)
            .collect::<Vec<_>>(),
        vec![75]
    );
}

#[test]
fn buckets_to_fetch_all_already_fetched_returns_empty() {
    let grouped = groups_at(&[25, 50, 75]);
    let to_fetch = buckets_to_fetch(&grouped, Some(75));
    assert!(to_fetch.is_empty());
}
