pub mod bucket;
pub mod provider;
pub mod receive;
pub mod stream;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    str::FromStr,
};

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use crate::{
    event::{
        core::{AcknowledgedEventBatch, AcknowledgedEventChannel},
        handler::{EventHandler, run_event_handler},
    },
    sync::EventType,
};

pub type PriceUsdEventBatch = AcknowledgedEventBatch<PriceUsdRow>;
pub type PriceUsdEventChannel = AcknowledgedEventChannel<PriceUsdRow>;

#[derive(Debug, Clone, PartialEq)]
pub struct PriceUsdPoint {
    pub price: BigDecimal,
    pub confidence: Option<BigDecimal>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceUsdRow {
    pub token_id: String,
    pub block_number: u64,
    pub price: BigDecimal,
    pub confidence: Option<BigDecimal>,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceUsdTarget {
    pub token_id: String,
    pub query_id: String,
}

#[derive(Debug, Deserialize)]
struct DefiLlamaCurrentResponse {
    coins: HashMap<String, DefiLlamaCurrentCoin>,
}

#[derive(Debug, Deserialize)]
struct DefiLlamaCurrentCoin {
    price: Value,
    #[serde(default)]
    confidence: Option<Value>,
}

pub fn parse_current(body: &str) -> Result<HashMap<String, PriceUsdPoint>> {
    let response: DefiLlamaCurrentResponse =
        serde_json::from_str(body).context("Failed to parse DefiLlama current price response")?;

    let mut prices = HashMap::with_capacity(response.coins.len());
    for (coin_ref, coin) in response.coins {
        let price = decimal_from_json(&coin.price)
            .with_context(|| format!("Failed to parse DefiLlama price for {coin_ref}"))?;
        let confidence = coin
            .confidence
            .as_ref()
            .map(decimal_from_json)
            .transpose()
            .with_context(|| format!("Failed to parse DefiLlama confidence for {coin_ref}"))?;

        prices.insert(coin_ref, PriceUsdPoint { price, confidence });
    }

    Ok(prices)
}

static DEFILLAMA_CHAIN_SLUG: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    std::env::var("DEFILLAMA_CHAIN_SLUG").unwrap_or_else(|_| "ethereum".to_string())
});

pub fn coin_ref(token_id: &str) -> String {
    format!("{}:{token_id}", *DEFILLAMA_CHAIN_SLUG)
}

pub fn should_refetch(last: Option<u64>, now: u64, interval_secs: u64) -> bool {
    match last {
        None => true,
        Some(last) => now.saturating_sub(last) >= interval_secs,
    }
}

pub fn build_dense_rows(
    token_id: &str,
    price: &BigDecimal,
    confidence: Option<BigDecimal>,
    blocks: &[(u64, u64)],
) -> Vec<PriceUsdRow> {
    blocks
        .iter()
        .map(|(block_number, block_timestamp)| PriceUsdRow {
            token_id: token_id.to_string(),
            block_number: *block_number,
            price: price.clone(),
            confidence: confidence.clone(),
            created_at: *block_timestamp,
        })
        .collect()
}

pub fn distinct_query_coin_refs(targets: &[PriceUsdTarget]) -> Vec<String> {
    let mut seen = HashSet::new();
    targets
        .iter()
        .filter(|target| seen.insert(target.query_id.clone()))
        .map(|target| coin_ref(&target.query_id))
        .collect()
}

pub fn apply_fresh_prices(
    targets: &[PriceUsdTarget],
    fresh_prices: &HashMap<String, PriceUsdPoint>,
    last_good_prices: &mut HashMap<String, PriceUsdPoint>,
    min_confidence: &BigDecimal,
) {
    for target in targets {
        let expected_ref = coin_ref(&target.query_id);
        let Some(point) = find_fresh_price(fresh_prices, &expected_ref) else {
            warn!(
                "[PRICE_USD] DefiLlama response missing {} (token {}); carrying forward last good price",
                expected_ref, target.token_id
            );
            continue;
        };

        match point.confidence.as_ref() {
            Some(confidence) if confidence >= min_confidence => {
                last_good_prices.insert(target.token_id.clone(), point.clone());
            }
            Some(confidence) => warn!(
                "[PRICE_USD] DefiLlama confidence below threshold for {} (token {}): {} < {}; carrying forward",
                expected_ref, target.token_id, confidence, min_confidence
            ),
            None => warn!(
                "[PRICE_USD] DefiLlama confidence missing for {} (token {}); carrying forward",
                expected_ref, target.token_id
            ),
        }
    }
}

fn find_fresh_price<'a>(
    fresh_prices: &'a HashMap<String, PriceUsdPoint>,
    expected_ref: &str,
) -> Option<&'a PriceUsdPoint> {
    fresh_prices.get(expected_ref).or_else(|| {
        fresh_prices
            .iter()
            .find(|(coin_ref, _)| coin_ref.eq_ignore_ascii_case(expected_ref))
            .map(|(_, point)| point)
    })
}

fn decimal_from_json(value: &Value) -> Result<BigDecimal> {
    match value {
        Value::Number(number) => BigDecimal::from_str(&number.to_string())
            .with_context(|| format!("Invalid decimal number: {number}")),
        Value::String(value) => {
            BigDecimal::from_str(value).with_context(|| format!("Invalid decimal string: {value}"))
        }
        _ => anyhow::bail!("Expected decimal number or string, got {value}"),
    }
}

pub struct PriceUsdEventHandler;

impl EventHandler for PriceUsdEventHandler {
    type Event = Vec<PriceUsdRow>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<PriceUsdEventHandler>(event_type).await
}
