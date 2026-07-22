//! Pure 25-block bucketing for the PriceUsd stream.

pub const BUCKET_BLOCK_INTERVAL: u64 = 25;

pub fn bucket_of(block: u64) -> u64 {
    block - block % BUCKET_BLOCK_INTERVAL
}

#[derive(Debug, Clone, PartialEq)]
pub struct BucketGroup {
    pub bucket_block: u64,
    pub bucket_ts: u64,
    pub blocks: Vec<(u64, u64)>,
}

pub fn group_into_buckets(blocks: &[(u64, u64)]) -> Vec<BucketGroup> {
    let mut groups: Vec<BucketGroup> = Vec::new();
    for &(block, timestamp) in blocks {
        let bucket_block = bucket_of(block);
        match groups.last_mut() {
            Some(group) if group.bucket_block == bucket_block => {
                group.blocks.push((block, timestamp));
            }
            _ => groups.push(BucketGroup {
                bucket_block,
                bucket_ts: timestamp,
                blocks: vec![(block, timestamp)],
            }),
        }
    }
    groups
}

#[derive(Debug, Clone, PartialEq)]
pub enum FetchKind {
    Current,
    Historical(u64),
}

pub fn select_fetch(bucket_ts: u64, now: u64, tip_threshold_secs: u64) -> FetchKind {
    if now.saturating_sub(bucket_ts) <= tip_threshold_secs {
        FetchKind::Current
    } else {
        FetchKind::Historical(bucket_ts)
    }
}

pub fn buckets_to_fetch(grouped: &[BucketGroup], last_fetched: Option<u64>) -> Vec<BucketGroup> {
    grouped
        .iter()
        .filter(|group| last_fetched.is_none_or(|last| group.bucket_block > last))
        .cloned()
        .collect()
}
