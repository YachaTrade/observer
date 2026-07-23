use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};
use tracing::{info, warn};

use crate::{
    config::QuoteConfig,
    event::common::price::provider::{PriceProvider, normalize_feed_id},
};

pub const PRICE_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
pub const PRICE_HEAD_OFFSET: u64 = 5;

#[derive(Debug, Clone)]
pub struct PriceSnapshot {
    pub prices_by_quote: HashMap<String, BigDecimal>,
    pub source_block: u64,
    pub source_timestamp: u64,
    pub sampled_at: Instant,
}

pub async fn sample_once(
    provider: &dyn PriceProvider,
    quotes: &[QuoteConfig],
    source_block: u64,
    source_timestamp: u64,
) -> Result<PriceSnapshot> {
    let feed_ids: Vec<&str> = quotes
        .iter()
        .map(|quote| quote.pyth_feed_id.as_str())
        .collect();
    let fetched = provider.fetch_batch(&feed_ids, source_timestamp).await?;
    let mut prices_by_quote = HashMap::with_capacity(quotes.len());

    for quote in quotes {
        let key = normalize_feed_id(&quote.pyth_feed_id);
        let price = fetched.get(&key).with_context(|| {
            format!(
                "Pyth response missing quote {} feed {}",
                quote.address, quote.pyth_feed_id
            )
        })?;
        prices_by_quote.insert(quote.address.clone(), price.clone());
    }

    Ok(PriceSnapshot {
        prices_by_quote,
        source_block,
        source_timestamp,
        sampled_at: Instant::now(),
    })
}

pub async fn run_sampler<S, Fut>(
    provider: Arc<dyn PriceProvider>,
    quotes: Vec<QuoteConfig>,
    snapshot_tx: watch::Sender<Option<Arc<PriceSnapshot>>>,
    mut source: S,
) where
    S: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(u64, u64)>> + Send,
{
    let mut interval = tokio::time::interval(PRICE_SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        let started = Instant::now();
        let sample = match source().await {
            Ok((block, timestamp)) => sample_once(provider.as_ref(), &quotes, block, timestamp)
                .await
                .map_err(|_| "provider_or_incomplete_snapshot"),
            Err(_) => Err("source"),
        };

        match sample {
            Ok(snapshot) => {
                info!(
                    "[PRICE-SAMPLER] success block={} ts={} quotes={} elapsed={}ms",
                    snapshot.source_block,
                    snapshot.source_timestamp,
                    snapshot.prices_by_quote.len(),
                    started.elapsed().as_millis()
                );
                drop(snapshot_tx.send_replace(Some(Arc::new(snapshot))));
            }
            Err(failure_kind) => {
                let age = snapshot_tx
                    .borrow()
                    .as_ref()
                    .map(|snapshot| snapshot.sampled_at.elapsed().as_secs());
                warn!(
                    "[PRICE-SAMPLER] failed failure_kind={} active_snapshot_age_secs={:?}",
                    failure_kind, age
                );
            }
        }

        interval.reset();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        io::{self, Write},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use bigdecimal::BigDecimal;
    use tokio::sync::{Notify, watch};

    use super::{run_sampler, sample_once};
    use crate::{
        config::QuoteConfig,
        event::common::price::provider::{PriceProvider, normalize_feed_id},
    };

    struct RecordingProvider {
        prices: HashMap<String, BigDecimal>,
        calls: AtomicUsize,
    }

    impl RecordingProvider {
        fn with_prices<const N: usize>(prices: [(&str, BigDecimal); N]) -> Self {
            Self {
                prices: prices
                    .into_iter()
                    .map(|(feed_id, price)| (normalize_feed_id(feed_id), price))
                    .collect(),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn complete() -> Self {
            Self::with_prices([("feed-a", BigDecimal::from(10))])
        }
    }

    #[async_trait]
    impl PriceProvider for RecordingProvider {
        async fn fetch(&self, feed_id: &str, _timestamp: u64) -> Result<Option<BigDecimal>> {
            Ok(self.prices.get(&normalize_feed_id(feed_id)).cloned())
        }

        async fn fetch_batch(
            &self,
            _feed_ids: &[&str],
            _timestamp: u64,
        ) -> Result<HashMap<String, BigDecimal>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.prices.clone())
        }
    }

    enum ProviderResponse {
        Prices(HashMap<String, BigDecimal>),
        Error(&'static str),
    }

    struct SharedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedLogWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("log buffer lock is not poisoned")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn capture_logs() -> (Arc<Mutex<Vec<u8>>>, tracing::dispatcher::DefaultGuard) {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let writer_logs = Arc::clone(&logs);
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || SharedLogWriter(Arc::clone(&writer_logs)))
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (logs, guard)
    }

    fn captured_log_text(logs: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(
            logs.lock()
                .expect("log buffer lock is not poisoned")
                .clone(),
        )
        .expect("captured logs are UTF-8")
    }

    struct SequencedProvider {
        responses: Mutex<VecDeque<ProviderResponse>>,
        calls: AtomicUsize,
    }

    impl SequencedProvider {
        fn new<const N: usize>(responses: [ProviderResponse; N]) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl PriceProvider for SequencedProvider {
        async fn fetch(&self, _feed_id: &str, _timestamp: u64) -> Result<Option<BigDecimal>> {
            unreachable!("sampler uses batch fetches")
        }

        async fn fetch_batch(
            &self,
            _feed_ids: &[&str],
            _timestamp: u64,
        ) -> Result<HashMap<String, BigDecimal>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self
                .responses
                .lock()
                .expect("response queue lock is not poisoned")
                .pop_front()
                .expect("test configured enough responses")
            {
                ProviderResponse::Prices(prices) => Ok(prices),
                ProviderResponse::Error(message) => Err(anyhow!(message)),
            }
        }
    }

    fn quote(address: &str, pyth_feed_id: &str) -> QuoteConfig {
        QuoteConfig {
            address: address.to_string(),
            pyth_feed_id: pyth_feed_id.to_string(),
            decimals: BigDecimal::from(1),
        }
    }

    fn prices<const N: usize>(prices: [(&str, i64); N]) -> HashMap<String, BigDecimal> {
        prices
            .into_iter()
            .map(|(feed_id, price)| (normalize_feed_id(feed_id), BigDecimal::from(price)))
            .collect()
    }

    #[tokio::test]
    async fn sample_once_maps_every_feed_to_its_quote_address() {
        let provider = RecordingProvider::with_prices([
            ("feed-a", BigDecimal::from(10)),
            ("feed-b", BigDecimal::from(20)),
        ]);
        let quotes = vec![quote("0xaaa", "0xFeEd-A"), quote("0xbbb", "0xfEeD-B")];

        let snapshot = sample_once(&provider, &quotes, 100, 1_000).await.unwrap();

        assert_eq!(snapshot.source_block, 100);
        assert_eq!(snapshot.source_timestamp, 1_000);
        assert_eq!(snapshot.prices_by_quote["0xaaa"], BigDecimal::from(10));
        assert_eq!(snapshot.prices_by_quote["0xbbb"], BigDecimal::from(20));
        assert_eq!(provider.calls(), 1);
    }

    #[tokio::test]
    async fn partial_provider_response_is_rejected() {
        let provider = RecordingProvider::with_prices([("feed-a", BigDecimal::from(10))]);
        let quotes = vec![quote("0xaaa", "feed-a"), quote("0xbbb", "feed-b")];

        let error = sample_once(&provider, &quotes, 100, 1_000)
            .await
            .expect_err("a partial response must not publish a snapshot");

        assert!(error.to_string().contains("0xbbb"));
        assert_eq!(provider.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_calls_immediately_then_once_at_thirty_seconds() {
        let provider = Arc::new(RecordingProvider::complete());
        let source_calls = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = watch::channel(None);
        let handle = tokio::spawn(run_sampler(
            provider.clone(),
            vec![quote("0xaaa", "feed-a")],
            tx,
            {
                let source_calls = Arc::clone(&source_calls);
                move || {
                    source_calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok((100, 1_000)) }
                }
            },
        ));

        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls(), 1);
        assert!(rx.borrow().is_some());

        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls(), 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 2);
        assert_eq!(provider.calls(), 2);

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_skips_missed_ticks_without_a_catch_up_burst() {
        let provider = Arc::new(RecordingProvider::complete());
        let source_calls = Arc::new(AtomicUsize::new(0));
        let first_call_started = Arc::new(Notify::new());
        let release_first_call = Arc::new(Notify::new());
        let (tx, _rx) = watch::channel(None);
        let handle = tokio::spawn(run_sampler(
            provider.clone(),
            vec![quote("0xaaa", "feed-a")],
            tx,
            {
                let source_calls = Arc::clone(&source_calls);
                let first_call_started = Arc::clone(&first_call_started);
                let release_first_call = Arc::clone(&release_first_call);
                move || {
                    let attempt = source_calls.fetch_add(1, Ordering::SeqCst);
                    let first_call_started = Arc::clone(&first_call_started);
                    let release_first_call = Arc::clone(&release_first_call);
                    async move {
                        if attempt == 0 {
                            first_call_started.notify_one();
                            release_first_call.notified().await;
                        }
                        Ok((100, 1_000))
                    }
                }
            },
        ));

        first_call_started.notified().await;
        tokio::time::advance(Duration::from_secs(95)).await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls(), 0);

        release_first_call.notify_one();
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls(), 1);

        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls(), 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 2);
        assert_eq!(provider.calls(), 2);

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_retains_snapshot_and_skips_provider_after_source_failure() {
        const SENSITIVE_SOURCE_ERROR: &str =
            "https://user:password@rpc.invalid sensitive-source-body";

        let (logs, _log_guard) = capture_logs();
        let provider = Arc::new(RecordingProvider::complete());
        let source_calls = Arc::new(AtomicUsize::new(0));
        let mut source_responses = VecDeque::from([
            Ok((100, 1_000)),
            Err(anyhow!(SENSITIVE_SOURCE_ERROR)),
            Ok((300, 3_000)),
        ]);
        let (tx, rx) = watch::channel(None);
        let handle = tokio::spawn(run_sampler(
            provider.clone(),
            vec![quote("0xaaa", "feed-a")],
            tx,
            {
                let source_calls = Arc::clone(&source_calls);
                move || {
                    source_calls.fetch_add(1, Ordering::SeqCst);
                    let response = source_responses
                        .pop_front()
                        .expect("test configured enough source responses");
                    async move { response }
                }
            },
        ));

        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls(), 1);
        assert_eq!(rx.borrow().as_ref().unwrap().source_block, 100);

        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 2);
        assert_eq!(provider.calls(), 1);
        assert_eq!(rx.borrow().as_ref().unwrap().source_block, 100);
        let failure_logs = captured_log_text(&logs);
        assert!(failure_logs.contains("failure_kind=source"));
        assert!(failure_logs.contains("active_snapshot_age_secs=Some(30)"));
        assert!(!failure_logs.contains(SENSITIVE_SOURCE_ERROR));

        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 2);
        assert_eq!(provider.calls(), 1);
        assert_eq!(rx.borrow().as_ref().unwrap().source_block, 100);

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(source_calls.load(Ordering::SeqCst), 3);
        assert_eq!(provider.calls(), 2);
        assert_eq!(rx.borrow().as_ref().unwrap().source_block, 300);
        assert_eq!(rx.borrow().as_ref().unwrap().source_timestamp, 3_000);

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_retains_last_complete_snapshot_after_failure() {
        const SENSITIVE_PROVIDER_ERROR: &str =
            "https://user:password@provider.invalid sensitive-provider-body";

        let (logs, _log_guard) = capture_logs();
        let provider = Arc::new(SequencedProvider::new([
            ProviderResponse::Prices(prices([("feed-a", 10)])),
            ProviderResponse::Error(SENSITIVE_PROVIDER_ERROR),
            ProviderResponse::Prices(prices([("feed-a", 30)])),
        ]));
        let (tx, rx) = watch::channel(None);
        let handle = tokio::spawn(run_sampler(
            provider.clone(),
            vec![quote("0xaaa", "feed-a")],
            tx,
            || async { Ok((100, 1_000)) },
        ));

        tokio::task::yield_now().await;
        assert_eq!(
            rx.borrow().as_ref().unwrap().prices_by_quote["0xaaa"],
            BigDecimal::from(10)
        );

        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(provider.calls(), 2);
        assert_eq!(
            rx.borrow().as_ref().unwrap().prices_by_quote["0xaaa"],
            BigDecimal::from(10)
        );
        let failure_logs = captured_log_text(&logs);
        assert!(failure_logs.contains("failure_kind=provider_or_incomplete_snapshot"));
        assert!(failure_logs.contains("active_snapshot_age_secs=Some(30)"));
        assert!(!failure_logs.contains(SENSITIVE_PROVIDER_ERROR));

        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert_eq!(provider.calls(), 2);
        assert_eq!(
            rx.borrow().as_ref().unwrap().prices_by_quote["0xaaa"],
            BigDecimal::from(10)
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(provider.calls(), 3);
        assert_eq!(
            rx.borrow().as_ref().unwrap().prices_by_quote["0xaaa"],
            BigDecimal::from(30)
        );

        handle.abort();
    }
}
