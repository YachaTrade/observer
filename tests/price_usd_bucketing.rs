use observer::event::common::price_usd::bucket::{
    BucketGroup, FetchKind, buckets_to_fetch, group_into_buckets, select_fetch,
};
use observer::event::common::{BUCKET_WINDOW_SECS, bucket_of_ts};

#[test]
fn shared_bucket_window_is_sixty_seconds() {
    assert_eq!(BUCKET_WINDOW_SECS, 60);
    assert_eq!(bucket_of_ts(0), 0);
    assert_eq!(bucket_of_ts(59), 0);
    assert_eq!(bucket_of_ts(60), 60);
    assert_eq!(bucket_of_ts(119), 60);
}

#[test]
fn timestamp_on_boundary_is_unchanged_and_floor_never_moves_forward() {
    for timestamp in [0_u64, 60, 600, 1_785_138_300] {
        assert_eq!(bucket_of_ts(timestamp), timestamp);
    }
    for timestamp in [0_u64, 1, 59, 60, 61, 1_785_138_347] {
        assert!(bucket_of_ts(timestamp) <= timestamp);
    }
}

#[test]
fn empty_input_has_no_groups() {
    assert!(group_into_buckets(&[]).is_empty());
}

#[test]
fn a_single_window_carries_the_grid_floor_and_original_timestamps() {
    assert_eq!(
        group_into_buckets(&[(48, 6_042), (49, 6_058)]),
        vec![BucketGroup {
            bucket_ts: 6_000,
            blocks: vec![(48, 6_042), (49, 6_058)],
        }]
    );
}

#[test]
fn groups_by_timestamp_window_not_block_number() {
    let groups = group_into_buckets(&[(48, 6_042), (49, 6_059), (50, 6_060), (51, 6_075)]);
    assert_eq!(
        groups,
        vec![
            BucketGroup {
                bucket_ts: 6_000,
                blocks: vec![(48, 6_042), (49, 6_059)],
            },
            BucketGroup {
                bucket_ts: 6_060,
                blocks: vec![(50, 6_060), (51, 6_075)],
            },
        ]
    );
}

#[test]
fn block_count_does_not_change_the_number_of_time_windows() {
    let fast: Vec<(u64, u64)> = (0..200)
        .map(|offset| (1_000 + offset, 60_000 + offset * 60 / 200))
        .collect();
    let slow: Vec<(u64, u64)> = (0..120)
        .map(|offset| (9_000 + offset, 60_000 + offset * 60 / 120))
        .collect();

    let fast_groups = group_into_buckets(&fast);
    let slow_groups = group_into_buckets(&slow);
    assert_eq!(fast_groups.len(), 1);
    assert_eq!(slow_groups.len(), 1);
    assert_eq!(fast_groups[0].blocks.len(), 200);
    assert_eq!(slow_groups[0].blocks.len(), 120);
}

const NOW: u64 = 1_000_000;
const TIP_THRESHOLD: u64 = 120;

#[test]
fn selects_current_at_tip_and_historical_for_past_buckets() {
    assert_eq!(
        select_fetch(NOW - 10, NOW, TIP_THRESHOLD),
        FetchKind::Current
    );
    assert_eq!(
        select_fetch(NOW - TIP_THRESHOLD, NOW, TIP_THRESHOLD),
        FetchKind::Current
    );
    assert_eq!(
        select_fetch(NOW - TIP_THRESHOLD - 1, NOW, TIP_THRESHOLD),
        FetchKind::Historical(NOW - TIP_THRESHOLD - 1)
    );
}

fn groups_at(bucket_timestamps: &[u64]) -> Vec<BucketGroup> {
    bucket_timestamps
        .iter()
        .map(|&timestamp| BucketGroup {
            bucket_ts: timestamp,
            blocks: vec![(timestamp / 60, timestamp)],
        })
        .collect()
}

#[test]
fn only_strictly_newer_timestamp_buckets_are_fetched() {
    let grouped = groups_at(&[60, 120, 180]);
    assert_eq!(
        buckets_to_fetch(&grouped, None)
            .iter()
            .map(|group| group.bucket_ts)
            .collect::<Vec<_>>(),
        vec![60, 120, 180]
    );
    assert_eq!(
        buckets_to_fetch(&grouped, Some(120))
            .iter()
            .map(|group| group.bucket_ts)
            .collect::<Vec<_>>(),
        vec![180]
    );
    assert!(buckets_to_fetch(&grouped, Some(180)).is_empty());
}
