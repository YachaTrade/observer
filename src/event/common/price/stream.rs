use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    time::Duration,
};

use anyhow::Result;
use bigdecimal::BigDecimal;
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, QuoteConfig, quote_configs},
    db::postgres::{PostgresDatabase, controller::price::has_persisted_prices_at_blocks},
    event::{
        common::{
            bucket_of_ts_tiered,
            price::{PriceEventChannel, provider},
            unix_now_secs,
        },
        get_block_timestamp,
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::price::UpdatePrice,
};

use super::receive::receive_events;

const POLL_INTERVAL: Duration = Duration::from_millis(500);
const TIMESTAMP_LOOKUP_CONCURRENCY: usize = 32;

async fn wait_for_next_cycle(iteration_started: Instant) {
    if let Some(remaining) = POLL_INTERVAL.checked_sub(iteration_started.elapsed()) {
        tokio::time::sleep(remaining).await;
    }
}

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let mut total_events = 0;
    let (channel, receiver) = PriceEventChannel::new("price_events");

    tokio::spawn(async move {
        if let Err(error) = receive_events(receiver, event_type).await {
            error!("[PRICE] Failed to receive events: {}", error);
        }
    });

    let client = RpcClient::instance()?;
    let db = PostgresDatabase::instance()?;
    let price_provider = provider::build_provider()?;
    let mut last_fetched_bucket: Option<u64> = None;
    let mut carried_prices: HashMap<String, BigDecimal> = HashMap::new();

    info!(
        "[PRICE] bucket window = {}s",
        crate::event::common::BUCKET_WINDOW_SECS
    );

    'stream: loop {
        let iteration_started = Instant::now();
        let processing_started = Instant::now();
        let latest_block = client.get_cached_latest_block();
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

        let now = unix_now_secs();
        let mut bucket_to_blocks: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
        let block_timestamps = match collect_block_timestamps(
            (from_block..=to_block).collect(),
            TIMESTAMP_LOOKUP_CONCURRENCY,
            move |block_number| async move { get_block_timestamp(client, block_number).await },
        )
        .await
        {
            Ok(block_timestamps) => block_timestamps,
            Err(error) => {
                error!("[PRICE] Failed to collect block timestamps: {:#}", error);
                wait_for_next_cycle(iteration_started).await;
                continue 'stream;
            }
        };
        for (block_number, block_timestamp) in block_timestamps {
            bucket_to_blocks
                .entry(bucket_of_ts_tiered(block_timestamp, now))
                .or_default()
                .push((block_number, block_timestamp));
        }

        let total_buckets = bucket_to_blocks.len();
        let unpersisted =
            match retain_unpersisted_buckets_from_pool(&db.pool, quote_configs(), bucket_to_blocks)
                .await
            {
                Ok(unpersisted) => unpersisted,
                Err(error) => {
                    error!("[PRICE] Failed to validate persisted buckets: {:#}", error);
                    wait_for_next_cycle(iteration_started).await;
                    continue 'stream;
                }
            };
        let skipped_persisted = total_buckets - unpersisted.len();

        let (events, all_ok) = build_bucket_events(
            price_provider.as_ref(),
            quote_configs(),
            &unpersisted,
            &mut carried_prices,
            &mut last_fetched_bucket,
        )
        .await;

        info!(
            "[PRICE] cycle buckets={} skipped_persisted={} all_ok={}",
            total_buckets, skipped_persisted, all_ok
        );

        if !all_ok {
            warn!(
                "[PRICE] batch {}-{} had a failed Pyth fetch; discarding partial rows and not advancing checkpoints",
                from_block, to_block
            );
            wait_for_next_cycle(iteration_started).await;
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

        wait_for_next_cycle(iteration_started).await;
    }
}

pub async fn collect_block_timestamps<F, Fut>(
    block_numbers: Vec<u64>,
    max_concurrency: usize,
    load_timestamp: F,
) -> Result<Vec<(u64, u64)>>
where
    F: Fn(u64) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<u64>> + Send + 'static,
{
    let limit = max_concurrency.max(1);
    let mut pending = block_numbers.into_iter();
    let mut tasks = JoinSet::new();
    let mut collected = BTreeMap::new();

    loop {
        while tasks.len() < limit {
            let Some(block_number) = pending.next() else {
                break;
            };
            let future = load_timestamp(block_number);
            tasks.spawn(async move { (block_number, future.await) });
        }

        let Some(joined) = tasks.join_next().await else {
            break;
        };
        let (block_number, result) =
            joined.map_err(|error| anyhow::anyhow!("timestamp lookup task failed: {error}"))?;
        let timestamp = result.map_err(|error| {
            anyhow::anyhow!(
                "failed to load timestamp for block {}: {}",
                block_number,
                error
            )
        })?;
        collected.insert(block_number, timestamp);
    }

    Ok(collected.into_iter().collect())
}

pub async fn retain_unpersisted_buckets_from_pool(
    pool: &PgPool,
    quotes: &[QuoteConfig],
    bucket_to_blocks: BTreeMap<u64, Vec<(u64, u64)>>,
) -> Result<BTreeMap<u64, Vec<(u64, u64)>>> {
    let mut unpersisted = BTreeMap::new();
    for (bucket_timestamp, blocks) in bucket_to_blocks {
        let block_numbers = blocks
            .iter()
            .map(|(block_number, _)| {
                i64::try_from(*block_number).map_err(|_| {
                    anyhow::anyhow!(
                        "block_number={} is out of PostgreSQL BIGINT range",
                        block_number
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut complete = true;
        for quote in quotes {
            if !has_persisted_prices_at_blocks(pool, &quote.address, &block_numbers).await {
                complete = false;
                break;
            }
        }
        if !complete {
            unpersisted.insert(bucket_timestamp, blocks);
        }
    }
    Ok(unpersisted)
}

pub async fn build_bucket_events(
    price_provider: &dyn provider::PriceProvider,
    quotes: &[QuoteConfig],
    bucket_to_blocks: &BTreeMap<u64, Vec<(u64, u64)>>,
    carried_prices: &mut HashMap<String, BigDecimal>,
    last_fetched_bucket: &mut Option<u64>,
) -> (Vec<UpdatePrice>, bool) {
    let feed_ids: Vec<&str> = quotes
        .iter()
        .map(|quote| quote.pyth_feed_id.as_str())
        .collect();
    let mut events = Vec::new();
    let mut all_ok = true;
    let (mut fetched, mut reused, mut failed) = (0usize, 0usize, 0usize);

    for (bucket_timestamp, blocks) in bucket_to_blocks {
        let has_complete_cached_bucket = *last_fetched_bucket == Some(*bucket_timestamp)
            && quotes
                .iter()
                .all(|quote| carried_prices.contains_key(&quote.address));
        let bucket_prices = if has_complete_cached_bucket {
            reused += 1;
            carried_prices.clone()
        } else {
            match price_provider
                .fetch_batch(&feed_ids, *bucket_timestamp)
                .await
            {
                Ok(prices) => {
                    let mut resolved = HashMap::with_capacity(quotes.len());
                    let mut missing_quote = None;
                    for quote in quotes {
                        let feed_id = provider::normalize_feed_id(&quote.pyth_feed_id);
                        if let Some(price) = prices.get(&feed_id) {
                            resolved.insert(quote.address.clone(), price.clone());
                        } else {
                            missing_quote = Some(quote);
                            break;
                        }
                    }
                    if let Some(quote) = missing_quote {
                        failed += 1;
                        all_ok = false;
                        warn!(
                            "[PRICE] Batch response missing feed for quote {} (feed_id={}) at timestamp {}; leaving bucket unstamped",
                            quote.address, quote.pyth_feed_id, bucket_timestamp
                        );
                        continue;
                    }
                    fetched += 1;
                    if all_ok {
                        carried_prices.clone_from(&resolved);
                        *last_fetched_bucket = Some(*bucket_timestamp);
                    }
                    resolved
                }
                Err(error) => {
                    failed += 1;
                    all_ok = false;
                    error!(
                        "[PRICE] Batch fetch failed at timestamp {}: {}",
                        bucket_timestamp, error
                    );
                    continue;
                }
            }
        };

        for quote in quotes {
            if let Some(price) = bucket_prices.get(&quote.address) {
                for (block_number, block_timestamp) in blocks {
                    events.push(UpdatePrice {
                        quote_id: quote.address.clone(),
                        block_number: *block_number,
                        price: price.clone(),
                        block_timestamp: *block_timestamp,
                    });
                }
            }
        }
    }

    info!(
        "[PRICE] buckets fetched={} reused={} failed={} rows={}",
        fetched,
        reused,
        failed,
        events.len()
    );
    (events, all_ok)
}
