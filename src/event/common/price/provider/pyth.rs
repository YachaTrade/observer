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
const MAX_REQUESTS_PER_WINDOW: usize = 20;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;
const INITIAL_429_BACKOFF: Duration = Duration::from_secs(1);
const MAX_429_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PythRateLimitConfig {
    max_requests: usize,
    window: Duration,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl PythRateLimitConfig {
    const fn fixed() -> Self {
        Self {
            max_requests: MAX_REQUESTS_PER_WINDOW,
            window: RATE_LIMIT_WINDOW,
            max_retries: MAX_RETRIES,
            initial_backoff: INITIAL_429_BACKOFF,
            max_backoff: MAX_429_BACKOFF,
        }
    }
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current
        .saturating_mul(2)
        .saturating_add(Duration::from_secs(1))
        .min(maximum)
}

fn total_attempts(max_retries: u32) -> u64 {
    u64::from(max_retries) + 1
}

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
        loop {
            let wait = {
                let mut times = self.request_times.lock().await;
                let now = Instant::now();
                times.retain(|&time| now.duration_since(time) < self.window);

                if times.len() < self.max_requests {
                    times.push(now);
                    None
                } else {
                    times
                        .first()
                        .map(|&oldest| self.window.saturating_sub(now.duration_since(oldest)))
                }
            };

            match wait {
                None => return,
                Some(duration) if duration.is_zero() => tokio::task::yield_now().await,
                Some(duration) => {
                    info!(
                        "[PYTH-RL] sliding-window full, sleeping {}ms before next call",
                        duration.as_millis()
                    );
                    tokio::time::sleep(duration).await;
                }
            }
        }
    }
}

pub struct PythProvider {
    http: Client,
    base_url: String,
    rate_limiter: RateLimiter,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl PythProvider {
    pub fn new() -> Result<Self> {
        Self::with_config((*PYTH_API_URL).clone(), PythRateLimitConfig::fixed())
    }

    fn with_config(base_url: String, config: PythRateLimitConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for PythProvider")?;
        Ok(Self {
            http,
            base_url,
            rate_limiter: RateLimiter::new(config.max_requests, config.window),
            max_retries: config.max_retries,
            initial_backoff: config.initial_backoff,
            max_backoff: config.max_backoff,
        })
    }
}

#[async_trait]
impl PriceProvider for PythProvider {
    async fn fetch(&self, feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>> {
        let mut retry_count: u32 = 0;
        let mut backoff = self.initial_backoff;

        loop {
            let url = format!(
                "{}/{}?ids%5B%5D={}&encoding=hex&parsed=true&ignore_invalid_price_ids=false",
                self.base_url, timestamp, feed_id
            );

            self.rate_limiter.wait_if_needed().await;
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
                        let attempt = u64::from(retry_count) + 1;
                        if retry_count >= self.max_retries {
                            error!(
                                "[PYTH] 429 ts={} attempt={}/{}; retries exhausted",
                                timestamp,
                                attempt,
                                total_attempts(self.max_retries)
                            );
                            return Err(anyhow::anyhow!("Max retries exceeded for rate limit"));
                        }

                        let delay = backoff;
                        retry_count += 1;
                        warn!(
                            "[PYTH] 429 ts={} attempt={}/{}; retrying in {}ms",
                            timestamp,
                            attempt,
                            total_attempts(self.max_retries),
                            delay.as_millis()
                        );
                        tokio::time::sleep(delay).await;
                        backoff = next_backoff(backoff, self.max_backoff);
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth API returned status: {}",
                            resp.status()
                        ));
                    }
                }
                Err(_) => {
                    if retry_count < self.max_retries {
                        warn!("Pyth request failed, retrying");
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        return Err(anyhow::anyhow!("Pyth request failed after retries"));
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
        let mut retry_count: u32 = 0;
        let mut backoff = self.initial_backoff;

        loop {
            let mut url = format!(
                "{}/{}?encoding=hex&parsed=true&ignore_invalid_price_ids=true",
                self.base_url, timestamp
            );
            for feed_id in feed_ids {
                url.push_str("&ids%5B%5D=");
                url.push_str(feed_id);
            }

            let pre_rl = Instant::now();
            self.rate_limiter.wait_if_needed().await;
            let rl_wait_ms = pre_rl.elapsed().as_millis();
            info!(
                "[PYTH] → batch fetch ts={} feeds={} attempt={}/{} rl_wait={}ms",
                timestamp,
                feed_ids.len(),
                u64::from(retry_count) + 1,
                total_attempts(self.max_retries),
                rl_wait_ms
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
                            .context("Failed to parse Pyth batch response")?;

                        let mut out: HashMap<String, BigDecimal> =
                            HashMap::with_capacity(feed.parsed.len());
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
                        let attempt = u64::from(retry_count) + 1;
                        if retry_count >= self.max_retries {
                            error!(
                                "[PYTH] ✗ 429 ts={} attempt={}/{}; retries exhausted",
                                timestamp,
                                attempt,
                                total_attempts(self.max_retries)
                            );
                            return Err(anyhow::anyhow!("Max retries exceeded for rate limit"));
                        }

                        let delay = backoff;
                        retry_count += 1;
                        warn!(
                            "[PYTH] ✗ 429 ts={} attempt={}/{}; retrying in {}ms",
                            timestamp,
                            attempt,
                            total_attempts(self.max_retries),
                            delay.as_millis()
                        );
                        tokio::time::sleep(delay).await;
                        backoff = next_backoff(backoff, self.max_backoff);
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth batch API returned status: {}",
                            resp.status()
                        ));
                    }
                }
                Err(_) => {
                    if retry_count < self.max_retries {
                        warn!("Pyth batch request failed, retrying");
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        return Err(anyhow::anyhow!("Pyth batch request failed after retries"));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Router,
        body::Body,
        extract::Request,
        http::{Response, StatusCode},
    };
    use tokio::time::Instant;

    use super::{
        PriceProvider, PythProvider, PythRateLimitConfig, RateLimiter, next_backoff, total_attempts,
    };

    async fn spawn_pyth_server(statuses: Vec<StatusCode>) -> (String, Arc<AtomicUsize>) {
        let request_count = Arc::new(AtomicUsize::new(0));
        let statuses = Arc::new(statuses);
        let app = Router::new().fallback({
            let request_count = Arc::clone(&request_count);
            let statuses = Arc::clone(&statuses);
            move |_request: Request| {
                let request_count = Arc::clone(&request_count);
                let statuses = Arc::clone(&statuses);
                async move {
                    let attempt = request_count.fetch_add(1, Ordering::SeqCst);
                    let status = statuses
                        .get(attempt)
                        .copied()
                        .unwrap_or_else(|| *statuses.last().expect("at least one status"));
                    let body = if status.is_success() {
                        r#"{"binary":{"encoding":"hex","data":[]},"parsed":[]}"#
                    } else {
                        "rate limited"
                    };
                    let mut response = Response::new(Body::from(body));
                    *response.status_mut() = status;
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

        (format!("http://{address}"), request_count)
    }

    #[test]
    fn production_rate_limit_matches_nads_observer_twenty_requests_per_ten_seconds() {
        let config = PythRateLimitConfig::fixed();

        assert_eq!(config.max_requests, 20);
        assert_eq!(config.window, Duration::from_secs(10));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff, Duration::from_secs(1));
        assert_eq!(config.max_backoff, Duration::from_secs(60));
    }

    #[test]
    fn exponential_backoff_matches_reference_sequence_and_cap() {
        let maximum = Duration::from_secs(60);
        let mut delay = Duration::from_secs(1);
        let mut observed = Vec::new();

        for _ in 0..7 {
            observed.push(delay.as_secs());
            delay = next_backoff(delay, maximum);
        }

        assert_eq!(observed, vec![1, 3, 7, 15, 31, 60, 60]);
    }

    #[test]
    fn total_attempts_does_not_overflow_at_the_retry_limit() {
        assert_eq!(total_attempts(u32::MAX), u64::from(u32::MAX) + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_admits_twenty_requests_and_delays_the_twenty_first() {
        let limiter = Arc::new(RateLimiter::new(20, Duration::from_secs(10)));
        for _ in 0..20 {
            limiter.wait_if_needed().await;
        }

        let twenty_first = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move { limiter.wait_if_needed().await })
        };
        tokio::task::yield_now().await;
        assert!(!twenty_first.is_finished());

        tokio::time::advance(Duration::from_secs(9)).await;
        tokio::task::yield_now().await;
        assert!(!twenty_first.is_finished());

        tokio::time::advance(Duration::from_secs(1)).await;
        twenty_first.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_waiters_recheck_the_window_before_admission() {
        let limiter = Arc::new(RateLimiter::new(2, Duration::from_secs(10)));
        limiter.wait_if_needed().await;
        limiter.wait_if_needed().await;

        let third = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move { limiter.wait_if_needed().await })
        };
        let fourth = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move { limiter.wait_if_needed().await })
        };
        tokio::task::yield_now().await;
        assert!(!third.is_finished());
        assert!(!fourth.is_finished());

        tokio::time::advance(Duration::from_secs(10)).await;
        third.await.unwrap();
        fourth.await.unwrap();
        assert_eq!(limiter.request_times.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn batch_retries_reacquire_limiter_and_stop_at_configured_limit() {
        let (base_url, request_count) =
            spawn_pyth_server(vec![StatusCode::TOO_MANY_REQUESTS]).await;
        let provider = PythProvider::with_config(
            base_url,
            PythRateLimitConfig {
                max_requests: 10,
                window: Duration::from_secs(60),
                max_retries: 3,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            },
        )
        .unwrap();

        let error = provider
            .fetch_batch(&["feed"], 123)
            .await
            .expect_err("persistent 429 must stop after configured retries");

        assert_eq!(error.to_string(), "Max retries exceeded for rate limit");
        assert_eq!(request_count.load(Ordering::SeqCst), 4);
        assert_eq!(provider.rate_limiter.request_times.lock().await.len(), 4);
    }

    #[tokio::test]
    async fn batch_retry_waits_for_configured_backoff_before_success() {
        let (base_url, request_count) =
            spawn_pyth_server(vec![StatusCode::TOO_MANY_REQUESTS, StatusCode::OK]).await;
        let backoff = Duration::from_millis(50);
        let provider = PythProvider::with_config(
            base_url,
            PythRateLimitConfig {
                max_requests: 10,
                window: Duration::from_secs(60),
                max_retries: 1,
                initial_backoff: backoff,
                max_backoff: backoff,
            },
        )
        .unwrap();
        let started = Instant::now();

        let prices = provider.fetch_batch(&["feed"], 123).await.unwrap();

        assert!(prices.is_empty());
        assert_eq!(request_count.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= backoff);
    }
}
