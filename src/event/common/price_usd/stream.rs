use std::{
    collections::HashMap,
    future::Future,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use sqlx::{PgPool, Row};
use tokio::time::Instant;
use tracing::{debug, error, instrument, warn};

use crate::{
    client::RpcClient,
    config::BLOCK_BATCH_SIZE,
    db::postgres::PostgresDatabase,
    event::{
        common::price_usd::{
            PriceUsdEventChannel, PriceUsdPoint, PriceUsdRow, PriceUsdTarget, apply_fresh_prices,
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
const TIP_THRESHOLD_SECS: u64 = 120;
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
        if let Err(error) = receive_events(receiver, event_type).await {
            error!("[PRICE_USD] Failed to receive events: {}", error);
        }
    });

    let client = RpcClient::instance()?;
    let price_provider = provider::build_provider()?;

    loop {
        let iteration_started = Instant::now();
        let latest_block = client.get_cached_latest_block();
        let processing_started = Instant::now();
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

        let blocks = collect_block_timestamps(from_block, to_block, |block_number| {
            get_block_timestamp(client, block_number)
        })
        .await?;

        let targets = load_price_usd_targets().await?;
        let missing_targets = targets
            .iter()
            .filter(|target| !last_good_prices.contains_key(&target.token_id))
            .cloned()
            .collect::<Vec<_>>();
        if !missing_targets.is_empty() {
            let db = PostgresDatabase::instance()?;
            let hydrated =
                load_last_good_prices_from_pool(&db.pool, &missing_targets, from_block).await?;
            last_good_prices.extend(hydrated);
        }

        let (events, all_ok) = build_bucket_events(
            price_provider.as_ref(),
            &targets,
            &blocks,
            unix_now_secs(),
            TIP_THRESHOLD_SECS,
            HISTORICAL_SEARCH_WIDTH_SECS,
            &min_confidence,
            &mut last_good_prices,
            &mut last_fetched_bucket,
        )
        .await;

        if !all_ok {
            warn!(
                "[PRICE_USD] batch {}-{} had a failed or incomplete DefiLlama fetch; not advancing watermark",
                from_block, to_block
            );
            if let Some(remaining) = POLL_INTERVAL.checked_sub(iteration_started.elapsed()) {
                tokio::time::sleep(remaining).await;
            }
            continue;
        }

        let event_count = events.len();
        total_events += event_count;
        channel.send(events, to_block, latest_block).await?;

        warn!(
            "📊 {:?} STREAM: Blocks: from={} to={} | Events: {} | Total Events: {} | Process time: {}ms",
            event_type,
            from_block,
            to_block,
            event_count,
            total_events,
            processing_started.elapsed().as_millis()
        );

        block_batch_size = *BLOCK_BATCH_SIZE;
        STREAM_MANAGER
            .set_event_block_processed_block(event_type, to_block)
            .await;

        if let Some(remaining) = POLL_INTERVAL.checked_sub(iteration_started.elapsed()) {
            tokio::time::sleep(remaining).await;
        }
    }
}

pub async fn collect_block_timestamps<F, Fut>(
    from_block: u64,
    to_block: u64,
    mut load_timestamp: F,
) -> Result<Vec<(u64, u64)>>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<u64>>,
{
    let mut blocks = Vec::new();
    for block_number in from_block..=to_block {
        let timestamp = load_timestamp(block_number).await.with_context(|| {
            format!("Failed to load PriceUsd timestamp for block {block_number}")
        })?;
        blocks.push((block_number, timestamp));
    }
    Ok(blocks)
}

#[allow(clippy::too_many_arguments)]
pub async fn build_bucket_events(
    provider: &dyn provider::PriceUsdProvider,
    targets: &[PriceUsdTarget],
    blocks: &[(u64, u64)],
    now: u64,
    tip_threshold_secs: u64,
    search_width_secs: u64,
    min_confidence: &BigDecimal,
    last_good_prices: &mut HashMap<String, PriceUsdPoint>,
    last_fetched_bucket: &mut Option<u64>,
) -> (Vec<PriceUsdRow>, bool) {
    let coin_refs = distinct_query_coin_refs(targets);
    let block_timestamps: HashMap<u64, u64> = blocks.iter().copied().collect();
    let mut candidate_prices = last_good_prices.clone();
    let mut candidate_last_fetched = *last_fetched_bucket;
    let mut events = Vec::new();

    for bucket in &group_into_buckets(blocks) {
        let has_unpriced_target = targets
            .iter()
            .any(|target| !candidate_prices.contains_key(&target.token_id));
        let should_fetch = candidate_last_fetched.is_none_or(|last| bucket.bucket_block > last)
            || has_unpriced_target;
        if should_fetch {
            if !targets.is_empty() {
                let bucket_timestamp = block_timestamps
                    .get(&bucket.bucket_block)
                    .copied()
                    .unwrap_or(bucket.bucket_ts);
                let fetch_kind = select_fetch(bucket_timestamp, now, tip_threshold_secs);
                let fetched = match fetch_kind {
                    FetchKind::Current => provider.fetch_current(&coin_refs).await,
                    FetchKind::Historical(timestamp) => {
                        provider
                            .fetch_historical(&coin_refs, timestamp, search_width_secs)
                            .await
                    }
                };

                match fetched {
                    Ok(fresh_prices) => apply_fresh_prices(
                        targets,
                        &fresh_prices,
                        &mut candidate_prices,
                        min_confidence,
                    ),
                    Err(error) => {
                        warn!(
                            "[PRICE_USD] DefiLlama fetch failed for bucket {}; aborting batch: {:#}",
                            bucket.bucket_block, error
                        );
                        return (Vec::new(), false);
                    }
                }
            }

            if let Some(target) = targets
                .iter()
                .find(|target| !candidate_prices.contains_key(&target.token_id))
            {
                warn!(
                    "[PRICE_USD] No accepted fresh or prior price for token {} in bucket {}; aborting batch",
                    target.token_id, bucket.bucket_block
                );
                return (Vec::new(), false);
            }
            candidate_last_fetched = Some(bucket.bucket_block);
        }

        for target in targets {
            if let Some(point) = candidate_prices.get(&target.token_id) {
                events.extend(build_dense_rows(
                    &target.token_id,
                    &point.price,
                    point.confidence.clone(),
                    &bucket.blocks,
                ));
            } else if !bucket.blocks.is_empty() {
                debug!(
                    "[PRICE_USD] Cold start for token {}; no rows for bucket {}",
                    target.token_id, bucket.bucket_block
                );
            }
        }
    }

    *last_good_prices = candidate_prices;
    *last_fetched_bucket = candidate_last_fetched;
    (events, true)
}

pub async fn load_last_good_prices_from_pool(
    pool: &PgPool,
    targets: &[PriceUsdTarget],
    before_block: u64,
) -> Result<HashMap<String, PriceUsdPoint>> {
    if targets.is_empty() {
        return Ok(HashMap::new());
    }

    let before_block = i64::try_from(before_block)
        .context("PriceUsd restart block does not fit PostgreSQL BIGINT")?;
    let token_ids = targets
        .iter()
        .map(|target| target.token_id.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "SELECT DISTINCT ON (token_id) token_id, price, confidence \
         FROM price_usd \
         WHERE token_id = ANY($1::varchar[]) AND block_number < $2 \
         ORDER BY token_id, block_number DESC",
    )
    .bind(&token_ids)
    .bind(before_block)
    .fetch_all(pool)
    .await
    .context("Failed to hydrate PriceUsd carry-forward state")?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let token_id: String = row.get("token_id");
            let point = PriceUsdPoint {
                price: row.get("price"),
                confidence: row.get("confidence"),
            };
            (token_id, point)
        })
        .collect())
}

pub async fn load_price_usd_targets() -> Result<Vec<PriceUsdTarget>> {
    let db = PostgresDatabase::instance()?;
    let rows = sqlx::query(
        "SELECT quote_id, price_usd_source_id \
         FROM quote_token \
         WHERE price_usd_source_id IS NOT NULL \
         ORDER BY quote_id",
    )
    .fetch_all(&db.pool)
    .await
    .context("Failed to query PriceUsd quote targets")?;

    Ok(rows
        .into_iter()
        .map(|row| PriceUsdTarget {
            token_id: row.get("quote_id"),
            query_id: row.get("price_usd_source_id"),
        })
        .collect())
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
