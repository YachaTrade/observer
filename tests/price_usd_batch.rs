//! Per-batch bucket processing contracts for the `price_usd` stream.

use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::Mutex,
};

use async_trait::async_trait;
use bigdecimal::BigDecimal;

use observer::event::common::price_usd::{
    PriceUsdEventChannel, PriceUsdPoint, PriceUsdRow, PriceUsdTarget, coin_ref,
    provider::PriceUsdProvider,
    stream::{build_bucket_events, collect_block_timestamps},
};

const TIP_THRESHOLD: u64 = 120;
const SEARCH_WIDTH: u64 = 3600;

fn bd(value: &str) -> BigDecimal {
    BigDecimal::from_str(value).unwrap()
}

fn target(id: &str) -> PriceUsdTarget {
    PriceUsdTarget {
        token_id: id.to_string(),
        query_id: id.to_string(),
    }
}

struct RecordingProvider {
    price: BigDecimal,
    fail_from_call: Option<usize>,
    calls: Mutex<Vec<String>>,
}

enum ProviderStep {
    Price(BigDecimal),
    Failure,
}

struct SequencedProvider {
    steps: Mutex<VecDeque<ProviderStep>>,
}

struct StaticProvider {
    response: HashMap<String, PriceUsdPoint>,
}

impl SequencedProvider {
    fn new(steps: Vec<ProviderStep>) -> Self {
        Self {
            steps: Mutex::new(steps.into()),
        }
    }

    fn next(&self, coin_refs: &[String]) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        match self.steps.lock().unwrap().pop_front().unwrap() {
            ProviderStep::Price(price) => Ok(coin_refs
                .iter()
                .map(|coin_ref| {
                    (
                        coin_ref.clone(),
                        PriceUsdPoint {
                            price: price.clone(),
                            confidence: Some(bd("0.99")),
                        },
                    )
                })
                .collect()),
            ProviderStep::Failure => anyhow::bail!("simulated later-bucket failure"),
        }
    }
}

impl RecordingProvider {
    fn new(price: &str, fail_from_call: Option<usize>) -> Self {
        Self {
            price: bd(price),
            fail_from_call,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, what: String) -> anyhow::Result<()> {
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push(what);
        if self.fail_from_call.is_some_and(|failure| index >= failure) {
            anyhow::bail!("simulated DefiLlama failure at call {index}");
        }
        Ok(())
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn response(&self, coin_refs: &[String]) -> HashMap<String, PriceUsdPoint> {
        coin_refs
            .iter()
            .map(|coin_ref| {
                (
                    coin_ref.clone(),
                    PriceUsdPoint {
                        price: self.price.clone(),
                        confidence: Some(bd("0.99")),
                    },
                )
            })
            .collect()
    }
}

#[async_trait]
impl PriceUsdProvider for RecordingProvider {
    async fn fetch_current(
        &self,
        coin_refs: &[String],
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        self.record("current".to_string())?;
        Ok(self.response(coin_refs))
    }

    async fn fetch_historical(
        &self,
        coin_refs: &[String],
        timestamp: u64,
        _search_width_secs: u64,
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        self.record(format!("historical:{timestamp}"))?;
        Ok(self.response(coin_refs))
    }
}

#[async_trait]
impl PriceUsdProvider for SequencedProvider {
    async fn fetch_current(
        &self,
        coin_refs: &[String],
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        self.next(coin_refs)
    }

    async fn fetch_historical(
        &self,
        coin_refs: &[String],
        _timestamp: u64,
        _search_width_secs: u64,
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        self.next(coin_refs)
    }
}

#[async_trait]
impl PriceUsdProvider for StaticProvider {
    async fn fetch_current(
        &self,
        _coin_refs: &[String],
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        Ok(self.response.clone())
    }

    async fn fetch_historical(
        &self,
        _coin_refs: &[String],
        _timestamp: u64,
        _search_width_secs: u64,
    ) -> anyhow::Result<HashMap<String, PriceUsdPoint>> {
        Ok(self.response.clone())
    }
}

fn batch(from: u64, count: u64, base_timestamp: u64) -> Vec<(u64, u64)> {
    (0..count)
        .map(|offset| (from + offset, base_timestamp + offset))
        .collect()
}

#[tokio::test]
async fn timestamp_failure_rejects_the_complete_range() {
    let result = collect_block_timestamps(100, 102, |block_number| async move {
        if block_number == 101 {
            anyhow::bail!("timestamp unavailable");
        }
        Ok(block_number + 1_000)
    })
    .await;

    let error = result.unwrap_err();
    assert!(error.to_string().contains("block 101"));
}

#[tokio::test]
async fn price_usd_channel_propagates_receiver_failure() {
    let (channel, mut receiver) = PriceUsdEventChannel::new("price_usd_ack_failure");
    let receive = tokio::spawn(async move {
        let batch = receiver.recv().await.unwrap();
        batch
            .ack
            .send(Err("price_usd persistence failed".to_string()))
            .unwrap();
    });

    let error = channel
        .send(Vec::<PriceUsdRow>::new(), 10, 11)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("price_usd persistence failed"));
    receive.await.unwrap();
}

#[tokio::test]
async fn tip_batch_dispatches_current_and_stamps_dense() {
    let now = 1_000_000;
    let blocks = batch(50, 5, now - 10);
    let provider = RecordingProvider::new("0.0226", None);
    let targets = vec![target("0xTKN")];
    let mut last_good = HashMap::new();
    let mut last_fetched = None;

    let (events, all_ok) = build_bucket_events(
        &provider,
        &targets,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(all_ok);
    assert_eq!(provider.calls(), vec!["current".to_string()]);
    assert_eq!(events.len(), 5, "one row per block");
    assert_eq!(last_fetched, Some(50));
}

#[tokio::test]
async fn past_batch_dispatches_historical_at_bucket_timestamp() {
    let now = 1_000_000;
    let base = now - 5_000;
    let blocks = batch(50, 3, base);
    let provider = RecordingProvider::new("0.0226", None);
    let targets = vec![target("0xTKN")];
    let mut last_good = HashMap::new();
    let mut last_fetched = None;

    let (_events, all_ok) = build_bucket_events(
        &provider,
        &targets,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(all_ok);
    assert_eq!(provider.calls(), vec![format!("historical:{base}")]);
}

#[tokio::test]
async fn fetch_failure_leaves_all_caller_state_unchanged() {
    let now = 1_000_000;
    let base = now - 5_000;
    let mut blocks = batch(50, 25, base);
    blocks.extend(batch(75, 3, base + 25));
    let provider = RecordingProvider::new("0.0226", Some(1));
    let targets = vec![target("0xTKN")];
    let original_prices = HashMap::from([(
        "0xOTHER".to_string(),
        PriceUsdPoint {
            price: bd("7"),
            confidence: Some(bd("0.99")),
        },
    )]);
    let mut last_good = original_prices.clone();
    let mut last_fetched = Some(25);

    let (events, all_ok) = build_bucket_events(
        &provider,
        &targets,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(!all_ok);
    assert!(events.is_empty());
    assert_eq!(last_fetched, Some(25));
    assert_eq!(last_good, original_prices);
}

async fn assert_cold_start_rejected(response: HashMap<String, PriceUsdPoint>) {
    let now = 1_000_000;
    let blocks = batch(50, 3, now - 10);
    let provider = StaticProvider { response };
    let targets = vec![target("0xTKN")];
    let mut last_good = HashMap::new();
    let mut last_fetched = None;

    let (events, all_ok) = build_bucket_events(
        &provider,
        &targets,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(!all_ok);
    assert!(events.is_empty());
    assert!(last_good.is_empty());
    assert_eq!(last_fetched, None);
}

#[tokio::test]
async fn cold_start_missing_target_rejects_batch_without_advancing() {
    assert_cold_start_rejected(HashMap::new()).await;
}

#[tokio::test]
async fn cold_start_low_confidence_rejects_batch_without_advancing() {
    assert_cold_start_rejected(HashMap::from([(
        coin_ref("0xTKN"),
        PriceUsdPoint {
            price: bd("1"),
            confidence: Some(bd("0.89")),
        },
    )]))
    .await;
}

#[tokio::test]
async fn cold_start_missing_confidence_rejects_batch_without_advancing() {
    assert_cold_start_rejected(HashMap::from([(
        coin_ref("0xTKN"),
        PriceUsdPoint {
            price: bd("1"),
            confidence: None,
        },
    )]))
    .await;
}

#[tokio::test]
async fn retry_refetches_every_bucket_without_historical_restamping_corruption() {
    let now = 1_000_000;
    let base = now - 5_000;
    let mut blocks = batch(50, 25, base);
    blocks.extend(batch(75, 3, base + 25));
    let targets = vec![target("0xTKN")];
    let mut last_good = HashMap::new();
    let mut last_fetched = None;

    let failing = SequencedProvider::new(vec![ProviderStep::Price(bd("1")), ProviderStep::Failure]);
    let (failed_events, all_ok) = build_bucket_events(
        &failing,
        &targets,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;
    assert!(!all_ok);
    assert!(failed_events.is_empty());
    assert!(last_good.is_empty());
    assert_eq!(last_fetched, None);

    let retry = SequencedProvider::new(vec![
        ProviderStep::Price(bd("10")),
        ProviderStep::Price(bd("20")),
    ]);
    let (events, all_ok) = build_bucket_events(
        &retry,
        &targets,
        &blocks,
        now,
        TIP_THRESHOLD,
        SEARCH_WIDTH,
        &bd("0.9"),
        &mut last_good,
        &mut last_fetched,
    )
    .await;

    assert!(all_ok);
    assert_eq!(events.len(), 28);
    assert!(
        events
            .iter()
            .filter(|event| event.block_number < 75)
            .all(|event| event.price == bd("10"))
    );
    assert!(
        events
            .iter()
            .filter(|event| event.block_number >= 75)
            .all(|event| event.price == bd("20"))
    );
    assert_eq!(last_fetched, Some(75));
    assert_eq!(last_good["0xTKN"].price, bd("20"));
}
