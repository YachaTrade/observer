pub mod price;
pub mod price_usd;
pub mod token;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub const BUCKET_WINDOW_SECS: u64 = 60;
pub const HISTORICAL_WINDOW_SECS: u64 = 600;
pub const TIER_AGE_SECS: u64 = 300;

pub fn bucket_of_ts(block_timestamp: u64) -> u64 {
    block_timestamp - (block_timestamp % BUCKET_WINDOW_SECS)
}

pub fn bucket_width_for(block_timestamp: u64, now: u64) -> u64 {
    if now.saturating_sub(block_timestamp) <= TIER_AGE_SECS {
        BUCKET_WINDOW_SECS
    } else {
        HISTORICAL_WINDOW_SECS
    }
}

pub fn bucket_of_ts_tiered(block_timestamp: u64, now: u64) -> u64 {
    let width = bucket_width_for(block_timestamp, now);
    block_timestamp - (block_timestamp % width)
}
