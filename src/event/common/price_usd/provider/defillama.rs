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
}

impl DefiLlamaProvider {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for DefiLlamaProvider")?;

        Ok(Self { http })
    }

    /// Current price for a chunk (DefiLlama `/current`).
    async fn fetch_chunk(&self, coin_refs: &[String]) -> Result<HashMap<String, PriceUsdPoint>> {
        if coin_refs.is_empty() {
            return Ok(HashMap::new());
        }
        let joined = coin_refs.join(",");
        let url = format!("{DEFILLAMA_CURRENT_URL}/{joined}");
        self.request_with_retry(&url, coin_refs.len()).await
    }

    /// Historical price for a chunk as of `timestamp` (DefiLlama `/historical`).
    /// `search_width_secs` widens the snapshot search window — DefiLlama
    /// snapshots are sparse, so a narrow window returns no data.
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
            "{DEFILLAMA_HISTORICAL_URL}/{timestamp}/{joined}?searchWidth={search_width_secs}"
        );
        self.request_with_retry(&url, coin_refs.len()).await
    }

    /// Shared GET + 429/server-error backoff + parse. Both `/current` and
    /// `/historical` responses share the same `{coins: {ref: {price,
    /// confidence, ...}}}` shape, so `parse_current` handles both.
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
                Ok(resp) if resp.status().is_success() => {
                    let body = resp
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
                Ok(resp) if resp.status() == StatusCode::TOO_MANY_REQUESTS => {
                    let wait = retry_after(&resp).unwrap_or(backoff);
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
                Ok(resp) => {
                    let status = resp.status();
                    let retryable = status.is_server_error();
                    let body = resp.text().await.unwrap_or_default();

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
                Err(e) => {
                    if retry_count < MAX_RETRIES {
                        warn!(
                            "[PRICE_USD] DefiLlama request failed, retrying attempt={}/{}: {}",
                            retry_count + 1,
                            MAX_RETRIES + 1,
                            e
                        );
                        retry_count += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = next_backoff(backoff);
                    } else {
                        return Err(anyhow::anyhow!(
                            "DefiLlama request failed after retries: {}",
                            e
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
        .map(Duration::from_secs)
}

fn next_backoff(backoff: Duration) -> Duration {
    (backoff * 2 + Duration::from_secs(1)).min(MAX_BACKOFF)
}
