use std::{
    collections::HashMap,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use sqlx::Row;
use tokio::time::Instant;
use tracing::{debug, error, instrument, warn};

use crate::{
    client::RpcClient,
    config::BLOCK_BATCH_SIZE,
    db::postgres::PostgresDatabase,
    event::{
        common::price_usd::{
            PriceUsdEventChannel, PriceUsdPoint, PriceUsdRow, WhitelistToken, apply_fresh_prices,
            bucket::{FetchKind, group_into_buckets, select_fetch},
            build_dense_rows, distinct_query_coin_refs, provider,
        },
        get_block_timestamp,
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
};

use super::receive::receive_events;

const POLL_INTERVAL: Duration = Duration::from_secs(10);
const MIN_CONFIDENCE: &str = "0.9";
/// Buckets within this many seconds of wall-clock now use DefiLlama `/current`
/// (freshest snapshot); older buckets use `/historical` at the bucket timestamp
/// so backfilling old block ranges records the era-correct price, not "now".
const TIP_THRESHOLD_SECS: u64 = 120;
/// Snapshot search window for `/historical`. DefiLlama snapshots are sparse, so
/// a narrow window returns empty (verified: searchWidth=600 → `{"coins":{}}`).
const HISTORICAL_SEARCH_WIDTH_SECS: u64 = 3600;

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let mut total_events = 0;
    let mut last_fetched_bucket: Option<u64> = None;
    let mut last_good_prices: HashMap<String, PriceUsdPoint> = HashMap::new();
    let min_confidence =
        BigDecimal::from_str(MIN_CONFIDENCE).context("Invalid PRICE_USD confidence threshold")?;

    let (channel, receiver) = PriceUsdEventChannel::new("price_usd_events");

    tokio::spawn(async move {
        if let Err(e) = receive_events(receiver, event_type).await {
            error!("[PRICE_USD] Failed to receive events: {}", e);
        }
    });

    let client = RpcClient::instance()?;
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
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        }

        let mut blocks = Vec::new();
        for block_number in from_block..=to_block {
            match get_block_timestamp(client, block_number).await {
                Ok(block_timestamp) => blocks.push((block_number, block_timestamp)),
                Err(e) => {
                    error!(
                        "[PRICE_USD] Failed to get timestamp for block {}: {}",
                        block_number, e
                    );
                }
            }
        }

        let tokens = match enabled_whitelist_tokens().await {
            Ok(tokens) => tokens,
            Err(e) => {
                error!(
                    "[PRICE_USD] Failed to load enabled whitelist tokens: {:#}",
                    e
                );
                Vec::new()
            }
        };

        let now = unix_now_secs();
        // Resolve the batch's 25-block buckets to DefiLlama prices. Returns
        // all_ok=false if ANY bucket fetch failed; in that case we do NOT advance
        // the processed watermark and retry the whole batch next cycle, so a
        // transient 429 never permanently stamps a stale carry-forward price.
        let (events, all_ok) = build_bucket_events(
            price_provider.as_ref(),
            &tokens,
            &blocks,
            now,
            TIP_THRESHOLD_SECS,
            HISTORICAL_SEARCH_WIDTH_SECS,
            &min_confidence,
            &mut last_good_prices,
            &mut last_fetched_bucket,
        )
        .await;

        if !all_ok {
            warn!(
                "[PRICE_USD] batch {}-{} had a failed DefiLlama fetch; not advancing watermark, retrying next cycle",
                from_block, to_block
            );
            if let Some(remaining) = POLL_INTERVAL.checked_sub(iter_start.elapsed()) {
                tokio::time::sleep(remaining).await;
            }
            continue;
        }

        let events_count = events.len();
        total_events += events_count;
        let elapsed_ms = time.elapsed().as_millis();

        if let Err(e) = channel.send(events, to_block, latest_block).await {
            error!("[PRICE_USD] Failed to send events: {}", e);
            continue;
        }

        warn!(
            "📊 {:?} STREAM: Blocks: from={} to={} | Events: {} | Total Events: {} | Process time: {}ms",
            event_type, from_block, to_block, events_count, total_events, elapsed_ms
        );

        block_batch_size = *BLOCK_BATCH_SIZE;

        STREAM_MANAGER
            .set_event_block_processed_block(event_type, to_block)
            .await;

        if let Some(remaining) = POLL_INTERVAL.checked_sub(iter_start.elapsed()) {
            tokio::time::sleep(remaining).await;
        }
    }
}

/// Resolve a batch's 25-block buckets to dense `price_usd` rows.
///
/// Each NEW bucket (beyond `last_fetched_bucket`) issues one DefiLlama fetch:
/// `/current` for the live tip, `/historical/{bucket_ts}` for past buckets. The
/// bucket timestamp prefers the boundary block's own ts (already fetched for the
/// batch, mirroring the Pyth `price` stream) and falls back to the bucket's first
/// member ts — equivalent under the historical `searchWidth` window. Every
/// successful bucket dense-stamps its own blocks with the carry-forward price as
/// of that bucket (so bucket N never inherits bucket N+1's price).
///
/// On a fetch failure it STOPS: the failed bucket and all later buckets are left
/// unstamped, `last_fetched_bucket` is not advanced past the last successful
/// bucket, and the bool result is `false` so the caller skips the watermark
/// advance and retries the batch (re-stamps are idempotent via ON CONFLICT).
#[allow(clippy::too_many_arguments)]
pub async fn build_bucket_events(
    provider: &dyn provider::PriceUsdProvider,
    tokens: &[WhitelistToken],
    blocks: &[(u64, u64)],
    now: u64,
    tip_threshold_secs: u64,
    search_width_secs: u64,
    min_confidence: &BigDecimal,
    last_good_prices: &mut HashMap<String, PriceUsdPoint>,
    last_fetched_bucket: &mut Option<u64>,
) -> (Vec<PriceUsdRow>, bool) {
    let coin_refs = distinct_query_coin_refs(tokens);
    let block_ts: HashMap<u64, u64> = blocks.iter().copied().collect();

    let mut events = Vec::new();
    for bucket in &group_into_buckets(blocks) {
        let is_new = last_fetched_bucket.is_none_or(|lf| bucket.bucket_block > lf);
        if is_new {
            if !tokens.is_empty() {
                // Prefer the boundary block's ts (matches Pyth); fall back to the
                // bucket's first member ts when the boundary block is outside this
                // batch (only the first bucket) — searchWidth absorbs the gap.
                let bucket_ts = block_ts
                    .get(&bucket.bucket_block)
                    .copied()
                    .unwrap_or(bucket.bucket_ts);
                let kind = select_fetch(bucket_ts, now, tip_threshold_secs);
                let fetched = match &kind {
                    FetchKind::Current => provider.fetch_current(&coin_refs).await,
                    FetchKind::Historical(ts) => {
                        provider
                            .fetch_historical(&coin_refs, *ts, search_width_secs)
                            .await
                    }
                };
                match fetched {
                    Ok(fresh_prices) => {
                        apply_fresh_prices(tokens, &fresh_prices, last_good_prices, min_confidence)
                    }
                    Err(e) => {
                        warn!(
                            "[PRICE_USD] DefiLlama fetch failed for bucket {} ({:?}); aborting batch for retry: {:#}",
                            bucket.bucket_block, kind, e
                        );
                        return (events, false);
                    }
                }
            }
            *last_fetched_bucket = Some(bucket.bucket_block);
        }

        for token in tokens {
            if let Some(point) = last_good_prices.get(&token.token_id) {
                events.extend(build_dense_rows(
                    &token.token_id,
                    &point.price,
                    point.confidence.clone(),
                    &bucket.blocks,
                ));
            } else if !bucket.blocks.is_empty() {
                debug!(
                    "[PRICE_USD] Cold start for token {}; no price rows for bucket {}",
                    token.token_id, bucket.bucket_block
                );
            }
        }
    }
    (events, true)
}

async fn enabled_whitelist_tokens() -> Result<Vec<WhitelistToken>> {
    let db = PostgresDatabase::instance()?;
    // query_id = price_source_id when set (testnet: mainnet address DefiLlama
    // knows), else token_id (mainnet: token_id already IS the mainnet address).
    let rows = sqlx::query(
        "SELECT token_id, COALESCE(NULLIF(price_source_id, ''), token_id) AS query_id \
         FROM whitelist_token WHERE enabled ORDER BY sort_order ASC, token_id ASC",
    )
    .fetch_all(&db.pool)
    .await
    .context("Failed to query enabled whitelist_token rows")?;

    Ok(rows
        .into_iter()
        .map(|row| WhitelistToken {
            token_id: row.get::<String, _>("token_id"),
            query_id: row.get::<String, _>("query_id"),
        })
        .collect())
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
