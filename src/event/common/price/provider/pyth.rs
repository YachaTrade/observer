//! Pyth Hermes HTTP price provider.
//!
//! Owns the HTTP client, the Pyth rate limiter, and the retry/backoff
//! loop. All constants below are Pyth-specific and intentionally local
//! to this module.

use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use reqwest::Client;
use tokio::{sync::Mutex, time::Instant};
use tracing::{error, info, warn};

use super::{PriceProvider, normalize_feed_id};
use crate::{config::PYTH_API_URL, types::price::PriceFeedResponse};

const REQUEST_TIMEOUT_SECS: u64 = 30;
/// Pyth Hermes documents 30 req / 10s, but in practice that ceiling is
/// shared with other API users on the same egress IP and Pyth's burst
/// protection occasionally triggers below the documented limit. Run at
/// 20 req / 10s (~33% safety margin) to avoid 429s during heavy
/// catch-up cycles.
const MAX_REQUESTS_PER_10_SECONDS: usize = 20;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;

/// Sliding-window rate limiter for the Pyth Hermes API.
struct RateLimiter {
    request_times: Mutex<Vec<Instant>>,
    max_requests: usize,
    window: Duration,
}

impl RateLimiter {
    fn new(max_requests: usize, window: Duration) -> Self {
        Self {
            request_times: Mutex::new(Vec::new()),
            max_requests,
            window,
        }
    }

    async fn wait_if_needed(&self) {
        let wait = {
            let mut times = self.request_times.lock().await;
            let now = Instant::now();
            times.retain(|&t| now.duration_since(t) < self.window);

            let wait = if times.len() >= self.max_requests {
                times.first().map(|&oldest| {
                    self.window.saturating_sub(now.duration_since(oldest))
                        + Duration::from_millis(100)
                })
            } else {
                None
            };

            // Reserve our slot BEFORE sleeping so other callers see us in-flight.
            times.push(now);
            wait
        }; // MutexGuard dropped here

        if let Some(w) = wait
            && w > Duration::ZERO
        {
            // Visibility for diagnosing burst-vs-steady behavior. Only log
            // non-trivial waits to avoid spam at low load.
            if w >= Duration::from_millis(50) {
                info!(
                    "[PYTH-RL] sliding-window full, sleeping {}ms before next call",
                    w.as_millis()
                );
            }
            tokio::time::sleep(w).await;
        }
    }
}

pub struct PythProvider {
    http: Client,
    rate_limiter: RateLimiter,
}

impl PythProvider {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for PythProvider")?;
        Ok(Self {
            http,
            rate_limiter: RateLimiter::new(MAX_REQUESTS_PER_10_SECONDS, RATE_LIMIT_WINDOW),
        })
    }
}

#[async_trait]
impl PriceProvider for PythProvider {
    async fn fetch(&self, feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>> {
        self.rate_limiter.wait_if_needed().await;

        let mut retry_count: u32 = 0;
        let mut backoff = Duration::from_secs(1);

        loop {
            let url = format!(
                "{}/{}?ids%5B%5D={}&encoding=hex&parsed=true&ignore_invalid_price_ids=false",
                *PYTH_API_URL, timestamp, feed_id
            );

            let response = self
                .http
                .get(&url)
                .header("Accept", "application/json")
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let feed: PriceFeedResponse = resp
                            .json()
                            .await
                            .context("Failed to parse Pyth API response")?;

                        let Some(parsed) = feed.parsed.first() else {
                            return Ok(None);
                        };

                        let price_bigint = parsed
                            .price
                            .price
                            .parse::<i128>()
                            .context("Failed to parse price string")?;
                        let expo = parsed.price.expo;
                        // BigDecimal::new(int_val, scale) represents int_val * 10^(-scale).
                        // Pyth's `expo` is the power of 10, so our scale is -expo.
                        let price = BigDecimal::new(
                            bigdecimal::num_bigint::BigInt::from(price_bigint),
                            -(expo as i64),
                        );

                        info!(
                            "Fetched Pyth price: feed={} ts={} price={}",
                            feed_id, timestamp, price
                        );
                        return Ok(Some(price));
                    } else if resp.status() == 429 {
                        error!("Pyth rate limit hit (429), backoff={:?}", backoff);
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            return Err(anyhow::anyhow!("Max retries exceeded for rate limit"));
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = backoff * 2 + Duration::from_millis(1000);
                        if backoff > Duration::from_secs(60) {
                            backoff = Duration::from_secs(60);
                        }
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth API returned status: {}",
                            resp.status()
                        ));
                    }
                }
                Err(e) => {
                    if retry_count < MAX_RETRIES {
                        warn!("Pyth request failed, retrying: {}", e);
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        return Err(anyhow::anyhow!("Pyth request failed after retries: {}", e));
                    }
                }
            }
        }
    }

    async fn fetch_batch(
        &self,
        feed_ids: &[&str],
        timestamp: u64,
    ) -> Result<HashMap<String, BigDecimal>> {
        if feed_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let started = Instant::now();
        let pre_rl = Instant::now();
        // Single rate-limit slot covers the whole batch — this is the whole
        // point of fetch_batch.
        self.rate_limiter.wait_if_needed().await;
        let rl_wait_ms = pre_rl.elapsed().as_millis();

        info!(
            "[PYTH] → batch fetch ts={} feeds={} rl_wait={}ms",
            timestamp,
            feed_ids.len(),
            rl_wait_ms
        );

        let mut retry_count: u32 = 0;
        let mut backoff = Duration::from_secs(1);

        loop {
            let mut url = format!(
                "{}/{}?encoding=hex&parsed=true&ignore_invalid_price_ids=true",
                *PYTH_API_URL, timestamp
            );
            for feed_id in feed_ids {
                url.push_str("&ids%5B%5D=");
                url.push_str(feed_id);
            }

            let response = self
                .http
                .get(&url)
                .header("Accept", "application/json")
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let feed: PriceFeedResponse = resp
                            .json()
                            .await
                            .context("Failed to parse Pyth batch response")?;

                        let mut out: HashMap<String, BigDecimal> = HashMap::with_capacity(feed.parsed.len());
                        for parsed in feed.parsed {
                            let price_bigint = match parsed.price.price.parse::<i128>() {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(
                                        "Failed to parse price string for feed {} ts={}: {}",
                                        parsed.id, timestamp, e
                                    );
                                    continue;
                                }
                            };
                            let expo = parsed.price.expo;
                            // BigDecimal::new(int, scale) = int * 10^(-scale).
                            // Pyth `expo` is the power of 10, so scale = -expo.
                            let price = BigDecimal::new(
                                bigdecimal::num_bigint::BigInt::from(price_bigint),
                                -(expo as i64),
                            );
                            out.insert(normalize_feed_id(&parsed.id), price);
                        }

                        info!(
                            "[PYTH] ✓ batch ts={} feeds={} returned={} retries={} total={}ms",
                            timestamp,
                            feed_ids.len(),
                            out.len(),
                            retry_count,
                            started.elapsed().as_millis()
                        );
                        return Ok(out);
                    } else if resp.status() == 429 {
                        error!(
                            "[PYTH] ✗ 429 ts={} attempt={}/{} backoff={}ms",
                            timestamp,
                            retry_count + 1,
                            MAX_RETRIES + 1,
                            backoff.as_millis()
                        );
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            return Err(anyhow::anyhow!("Max retries exceeded for rate limit"));
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = backoff * 2 + Duration::from_millis(1000);
                        if backoff > Duration::from_secs(60) {
                            backoff = Duration::from_secs(60);
                        }
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth batch API returned status: {}",
                            resp.status()
                        ));
                    }
                }
                Err(e) => {
                    if retry_count < MAX_RETRIES {
                        warn!("Pyth batch request failed, retrying: {}", e);
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth batch request failed after retries: {}",
                            e
                        ));
                    }
                }
            }
        }
    }
}
