//! 25-block bucketing for the price_usd (DefiLlama) stream, mirroring the Pyth
//! `price` stream's bucket structure so both price tables share identical
//! bucket boundaries.
//!
//! A bucket is `block - block % 25`. Each bucket is fetched once; tip buckets
//! use DefiLlama `/current` (freshest), past buckets use `/historical/{ts}` so
//! backfilling an old block range records the price AS OF that time rather than
//! "now". See branches/feat-price-usd-block-bucket.md.

/// Block span per bucket. MUST equal the Pyth `price` stream's
/// `BUCKET_BLOCK_INTERVAL` (src/event/common/price/stream.rs) so the `price`
/// and `price_usd` tables bucket on the same boundaries.
pub const BUCKET_BLOCK_INTERVAL: u64 = 25;

/// The bucket a block belongs to (floor to the 25-block boundary).
pub fn bucket_of(block: u64) -> u64 {
    block - block % BUCKET_BLOCK_INTERVAL
}

/// One 25-block bucket within a processed batch.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketGroup {
    /// `bucket_of(member block)` — the bucket's floor block.
    pub bucket_block: u64,
    /// Timestamp of the FIRST (lowest) member block — the anchor we query
    /// DefiLlama historical with.
    pub bucket_ts: u64,
    /// `(block_number, block_timestamp)` members, ascending by block.
    pub blocks: Vec<(u64, u64)>,
}

/// Group ascending `(block, ts)` pairs into 25-block buckets, ascending by
/// `bucket_block`. `bucket_ts` is the first (lowest-block) member's timestamp.
///
/// Assumes the input is ascending by block (members of one bucket are
/// contiguous), which is how the stream produces it from `from_block..=to_block`.
pub fn group_into_buckets(blocks: &[(u64, u64)]) -> Vec<BucketGroup> {
    let mut groups: Vec<BucketGroup> = Vec::new();
    for &(block, ts) in blocks {
        let bucket_block = bucket_of(block);
        match groups.last_mut() {
            Some(group) if group.bucket_block == bucket_block => group.blocks.push((block, ts)),
            _ => groups.push(BucketGroup {
                bucket_block,
                bucket_ts: ts,
                blocks: vec![(block, ts)],
            }),
        }
    }
    groups
}

/// Which DefiLlama endpoint to query for a bucket.
#[derive(Debug, Clone, PartialEq)]
pub enum FetchKind {
    /// Live tip — query `/current` for the freshest snapshot.
    Current,
    /// Past bucket — query `/historical/{ts}` so the recorded price matches the
    /// bucket's era, not "now". Carries the bucket timestamp.
    Historical(u64),
}

/// Tip buckets (within `tip_threshold_secs` of `now`) use `/current`; older
/// buckets use `/historical` at the bucket timestamp. Inclusive at the
/// threshold so a bucket exactly `tip_threshold_secs` old is still "tip".
pub fn select_fetch(bucket_ts: u64, now: u64, tip_threshold_secs: u64) -> FetchKind {
    if now.saturating_sub(bucket_ts) <= tip_threshold_secs {
        FetchKind::Current
    } else {
        FetchKind::Historical(bucket_ts)
    }
}

/// Buckets strictly newer than `last_fetched` (forward-scan dedupe: issue one
/// fetch per new bucket, never re-fetching an already-processed bucket).
/// `None` → every bucket is new.
pub fn buckets_to_fetch(grouped: &[BucketGroup], last_fetched: Option<u64>) -> Vec<BucketGroup> {
    grouped
        .iter()
        .filter(|group| last_fetched.is_none_or(|lf| group.bucket_block > lf))
        .cloned()
        .collect()
}
