use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, header::RETRY_AFTER};
use tracing::{error, info, warn};

use crate::event::common::price_usd::{PriceUsdPoint, parse_current};

use super::PriceUsdProvider;

const DEFILLAMA_CURRENT_URL: &str = "https://coins.llama.fi/prices/current";
const DEFILLAMA_HISTORICAL_URL: &str = "https://coins.llama.fi/prices/historical";
const REQUEST_TIMEOUT_SECS: u64 = 30;
const MAX_RETRIES: u32 = 3;
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const COIN_REF_CHUNK_SIZE: usize = 50;

pub struct DefiLlamaProvider {
    http: Client,
    current_url: String,
    historical_url: String,
}

impl DefiLlamaProvider {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for DefiLlamaProvider")?;

        Ok(Self {
            http,
            current_url: DEFILLAMA_CURRENT_URL.to_string(),
            historical_url: DEFILLAMA_HISTORICAL_URL.to_string(),
        })
    }

    async fn fetch_chunk(&self, coin_refs: &[String]) -> Result<HashMap<String, PriceUsdPoint>> {
        if coin_refs.is_empty() {
            return Ok(HashMap::new());
        }

        let joined = coin_refs.join(",");
        let url = format!("{}/{joined}", self.current_url);
        self.request_with_retry(&url, coin_refs.len()).await
    }

    async fn fetch_chunk_historical(
        &self,
        coin_refs: &[String],
        timestamp: u64,
        search_width_secs: u64,
    ) -> Result<HashMap<String, PriceUsdPoint>> {
        if coin_refs.is_empty() {
            return Ok(HashMap::new());
        }

        let joined = coin_refs.join(",");
        let url = format!(
            "{}/{timestamp}/{joined}?searchWidth={search_width_secs}",
            self.historical_url
        );
        self.request_with_retry(&url, coin_refs.len()).await
    }

    async fn request_with_retry(
        &self,
        url: &str,
        requested: usize,
    ) -> Result<HashMap<String, PriceUsdPoint>> {
        let mut retry_count = 0;
        let mut backoff = Duration::from_secs(1);

        loop {
            let response = self
                .http
                .get(url)
                .header("Accept", "application/json")
                .send()
                .await;

            match response {
                Ok(response) if response.status().is_success() => {
                    let body = response
                        .text()
                        .await
                        .context("Failed to read DefiLlama response body")?;
                    let prices =
                        parse_current(&body).context("Failed to decode DefiLlama prices")?;
                    info!(
                        "[PRICE_USD] DefiLlama fetch ok requested={} returned={}",
                        requested,
                        prices.len()
                    );
                    return Ok(prices);
                }
                Ok(response) if response.status() == StatusCode::TOO_MANY_REQUESTS => {
                    let wait = retry_after(&response).unwrap_or(backoff);
                    error!(
                        "[PRICE_USD] DefiLlama 429 attempt={}/{} backoff={}ms",
                        retry_count + 1,
                        MAX_RETRIES + 1,
                        wait.as_millis()
                    );
                    retry_count += 1;
                    if retry_count > MAX_RETRIES {
                        return Err(anyhow::anyhow!("DefiLlama rate limit exceeded"));
                    }
                    tokio::time::sleep(wait).await;
                    backoff = next_backoff(backoff);
                }
                Ok(response) => {
                    let status = response.status();
                    let retryable = status.is_server_error();
                    let body = response.text().await.unwrap_or_default();

                    if retryable && retry_count < MAX_RETRIES {
                        warn!(
                            "[PRICE_USD] DefiLlama status={} retrying attempt={}/{} backoff={}ms",
                            status,
                            retry_count + 1,
                            MAX_RETRIES + 1,
                            backoff.as_millis()
                        );
                        retry_count += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = next_backoff(backoff);
                    } else {
                        return Err(anyhow::anyhow!(
                            "DefiLlama API returned status {}: {}",
                            status,
                            body
                        ));
                    }
                }
                Err(error) => {
                    if retry_count < MAX_RETRIES {
                        warn!(
                            "[PRICE_USD] DefiLlama request failed, retrying attempt={}/{}: {}",
                            retry_count + 1,
                            MAX_RETRIES + 1,
                            error
                        );
                        retry_count += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = next_backoff(backoff);
                    } else {
                        return Err(anyhow::anyhow!(
                            "DefiLlama request failed after retries: {}",
                            error
                        ));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl PriceUsdProvider for DefiLlamaProvider {
    async fn fetch_current(&self, coin_refs: &[String]) -> Result<HashMap<String, PriceUsdPoint>> {
        if coin_refs.is_empty() {
            return Ok(HashMap::new());
        }

        let mut prices = HashMap::new();
        for chunk in coin_refs.chunks(COIN_REF_CHUNK_SIZE) {
            let chunk_prices = self.fetch_chunk(chunk).await?;
            prices.extend(chunk_prices);
        }
        Ok(prices)
    }

    async fn fetch_historical(
        &self,
        coin_refs: &[String],
        timestamp: u64,
        search_width_secs: u64,
    ) -> Result<HashMap<String, PriceUsdPoint>> {
        if coin_refs.is_empty() {
            return Ok(HashMap::new());
        }

        let mut prices = HashMap::new();
        for chunk in coin_refs.chunks(COIN_REF_CHUNK_SIZE) {
            let chunk_prices = self
                .fetch_chunk_historical(chunk, timestamp, search_width_secs)
                .await?;
            prices.extend(chunk_prices);
        }
        Ok(prices)
    }
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(|seconds| Duration::from_secs(seconds).min(MAX_BACKOFF))
}

fn next_backoff(backoff: Duration) -> Duration {
    (backoff * 2 + Duration::from_secs(1)).min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Router,
        body::Body,
        extract::Request,
        http::{HeaderValue, Response},
    };

    use super::*;

    async fn spawn_server(
        status: StatusCode,
        retry_after_value: Option<&'static str>,
        body: &'static str,
    ) -> (String, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_uris = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().fallback({
            let request_count = Arc::clone(&request_count);
            let request_uris = Arc::clone(&request_uris);
            move |request: Request| {
                let request_count = Arc::clone(&request_count);
                let request_uris = Arc::clone(&request_uris);
                async move {
                    request_count.fetch_add(1, Ordering::SeqCst);
                    request_uris
                        .lock()
                        .expect("request URI lock poisoned")
                        .push(request.uri().to_string());

                    let mut response = Response::new(Body::from(body));
                    *response.status_mut() = status;
                    if let Some(value) = retry_after_value {
                        response
                            .headers_mut()
                            .insert(RETRY_AFTER, HeaderValue::from_static(value));
                    }
                    response
                }
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener binds");
        let address = listener.local_addr().expect("listener has local address");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test HTTP server runs");
        });

        (format!("http://{address}"), request_count, request_uris)
    }

    fn provider_at(base_url: &str) -> DefiLlamaProvider {
        DefiLlamaProvider {
            http: Client::new(),
            current_url: format!("{base_url}/prices/current"),
            historical_url: format!("{base_url}/prices/historical"),
        }
    }

    #[tokio::test]
    async fn retry_after_is_capped_at_max_backoff() {
        let (base_url, _, _) =
            spawn_server(StatusCode::TOO_MANY_REQUESTS, Some("9999"), "rate limited").await;
        let response = Client::new()
            .get(base_url)
            .send()
            .await
            .expect("test response received");

        assert_eq!(retry_after(&response), Some(MAX_BACKOFF));
    }

    #[tokio::test]
    async fn request_with_retry_stops_after_max_retries_with_terminal_error() {
        let (base_url, request_count, _) =
            spawn_server(StatusCode::TOO_MANY_REQUESTS, Some("0"), "rate limited").await;
        let provider = provider_at(&base_url);

        let error = provider
            .request_with_retry(&base_url, 1)
            .await
            .expect_err("persistent rate limiting is terminal");

        assert_eq!(
            request_count.load(Ordering::SeqCst),
            MAX_RETRIES as usize + 1
        );
        assert_eq!(error.to_string(), "DefiLlama rate limit exceeded");
    }

    #[tokio::test]
    async fn fetch_historical_sends_timestamp_search_width_and_coin_refs() {
        let (base_url, request_count, request_uris) = spawn_server(
            StatusCode::OK,
            None,
            r#"{"coins":{"ethereum:0xabc":{"price":1.5,"confidence":0.99}}}"#,
        )
        .await;
        let provider = provider_at(&base_url);
        let coin_refs = vec!["ethereum:0xabc".to_string()];

        let prices = provider
            .fetch_historical(&coin_refs, 1_234_567, 900)
            .await
            .expect("historical response succeeds");

        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            request_uris
                .lock()
                .expect("request URI lock poisoned")
                .as_slice(),
            ["/prices/historical/1234567/ethereum:0xabc?searchWidth=900"]
        );
        assert_eq!(
            prices
                .get("ethereum:0xabc")
                .expect("requested price returned")
                .price
                .to_string(),
            "1.5"
        );
    }
}
