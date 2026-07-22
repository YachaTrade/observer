//! Pyth Hermes HTTP price provider.
//!
//! Owns the HTTP client, the Pyth rate limiter, and the retry/backoff
//! loop. All constants below are Pyth-specific and intentionally local
//! to this module.

use std::{collections::HashMap, env::VarError, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use reqwest::{
    Client,
    header::{HeaderMap, RETRY_AFTER},
};
use tokio::{sync::Mutex, time::Instant};
use tracing::{error, info, warn};

use super::{PriceProvider, normalize_feed_id};
use crate::{config::PYTH_API_URL, types::price::PriceFeedResponse};

const REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_REQUESTS: usize = 8;
const DEFAULT_RATE_LIMIT_WINDOW_SECS: u64 = 10;
const DEFAULT_429_COOLDOWN_SECS: u64 = 60;
const DEFAULT_MAX_RETRIES: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PythRateLimitConfig {
    max_requests: usize,
    window: Duration,
    cooldown: Duration,
    max_retries: u32,
}

impl PythRateLimitConfig {
    fn from_env() -> Result<Self> {
        let values = [
            "PYTH_MAX_REQUESTS",
            "PYTH_RATE_LIMIT_WINDOW_SECS",
            "PYTH_429_COOLDOWN_SECS",
            "PYTH_MAX_RETRIES",
        ]
        .into_iter()
        .map(|key| Ok((key, env_value(key, std::env::var(key))?)))
        .collect::<Result<HashMap<_, _>>>()?;

        Self::from_lookup(|key| values.get(key).cloned().flatten())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let max_requests = parse_positive_u64(
            "PYTH_MAX_REQUESTS",
            lookup("PYTH_MAX_REQUESTS"),
            DEFAULT_MAX_REQUESTS as u64,
        )?;
        let window_secs = parse_positive_u64(
            "PYTH_RATE_LIMIT_WINDOW_SECS",
            lookup("PYTH_RATE_LIMIT_WINDOW_SECS"),
            DEFAULT_RATE_LIMIT_WINDOW_SECS,
        )?;
        let cooldown_secs = parse_positive_u64(
            "PYTH_429_COOLDOWN_SECS",
            lookup("PYTH_429_COOLDOWN_SECS"),
            DEFAULT_429_COOLDOWN_SECS,
        )?;
        let max_retries = parse_non_negative_u64(
            "PYTH_MAX_RETRIES",
            lookup("PYTH_MAX_RETRIES"),
            DEFAULT_MAX_RETRIES as u64,
        )?;

        Ok(Self {
            max_requests: usize::try_from(max_requests)
                .context("PYTH_MAX_REQUESTS is too large for this platform")?,
            window: Duration::from_secs(window_secs),
            cooldown: Duration::from_secs(cooldown_secs),
            max_retries: u32::try_from(max_retries).context("PYTH_MAX_RETRIES is too large")?,
        })
    }
}

fn env_value(key: &str, value: std::result::Result<String, VarError>) -> Result<Option<String>> {
    match value {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => anyhow::bail!("{key} must contain valid Unicode"),
    }
}

fn parse_positive_u64(key: &str, value: Option<String>, default: u64) -> Result<u64> {
    let parsed = parse_non_negative_u64(key, value, default)?;
    if parsed == 0 {
        anyhow::bail!("{key} must be at least 1");
    }
    Ok(parsed)
}

fn parse_non_negative_u64(key: &str, value: Option<String>, default: u64) -> Result<u64> {
    value.map_or(Ok(default), |raw| {
        raw.parse::<u64>()
            .with_context(|| format!("{key} must be a non-negative integer"))
    })
}

fn retry_after_delay(headers: &HeaderMap, configured_cooldown: Duration) -> Duration {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .map_or(configured_cooldown, |retry_after| {
            retry_after.max(configured_cooldown)
        })
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
    cooldown: Duration,
    max_retries: u32,
}

impl PythProvider {
    pub fn new() -> Result<Self> {
        let config = PythRateLimitConfig::from_env()?;
        Self::with_config((*PYTH_API_URL).clone(), config)
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
            cooldown: config.cooldown,
            max_retries: config.max_retries,
        })
    }
}

#[async_trait]
impl PriceProvider for PythProvider {
    async fn fetch(&self, feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>> {
        let mut retry_count: u32 = 0;

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

                        let delay = retry_after_delay(resp.headers(), self.cooldown);
                        retry_count += 1;
                        warn!(
                            "[PYTH] 429 ts={} attempt={}/{}; retrying in {}ms",
                            timestamp,
                            attempt,
                            total_attempts(self.max_retries),
                            delay.as_millis()
                        );
                        tokio::time::sleep(delay).await;
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

                        let delay = retry_after_delay(resp.headers(), self.cooldown);
                        retry_count += 1;
                        warn!(
                            "[PYTH] ✗ 429 ts={} attempt={}/{}; retrying in {}ms",
                            timestamp,
                            attempt,
                            total_attempts(self.max_retries),
                            delay.as_millis()
                        );
                        tokio::time::sleep(delay).await;
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
        collections::HashMap,
        env::VarError,
        ffi::OsString,
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
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
    use tokio::time::Instant;

    use super::{
        PriceProvider, PythProvider, PythRateLimitConfig, RateLimiter, env_value,
        retry_after_delay, total_attempts,
    };

    fn config_from(values: &[(&str, &str)]) -> anyhow::Result<PythRateLimitConfig> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        PythRateLimitConfig::from_lookup(|key| values.get(key).cloned())
    }

    async fn spawn_pyth_server(
        statuses: Vec<StatusCode>,
        retry_after_value: Option<&'static str>,
    ) -> (String, Arc<AtomicUsize>) {
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

        (format!("http://{address}"), request_count)
    }

    #[test]
    fn rate_limit_config_uses_safe_defaults() {
        let config = config_from(&[]).unwrap();

        assert_eq!(config.max_requests, 8);
        assert_eq!(config.window, Duration::from_secs(10));
        assert_eq!(config.cooldown, Duration::from_secs(60));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn rate_limit_config_accepts_development_overrides() {
        let config = config_from(&[
            ("PYTH_MAX_REQUESTS", "1"),
            ("PYTH_RATE_LIMIT_WINDOW_SECS", "60"),
            ("PYTH_429_COOLDOWN_SECS", "60"),
            ("PYTH_MAX_RETRIES", "3"),
        ])
        .unwrap();

        assert_eq!(config.max_requests, 1);
        assert_eq!(config.window, Duration::from_secs(60));
        assert_eq!(config.cooldown, Duration::from_secs(60));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn rate_limit_config_rejects_zero_positive_fields() {
        for key in [
            "PYTH_MAX_REQUESTS",
            "PYTH_RATE_LIMIT_WINDOW_SECS",
            "PYTH_429_COOLDOWN_SECS",
        ] {
            let error = config_from(&[(key, "0")]).unwrap_err();
            assert!(error.to_string().contains(key));
            assert!(error.to_string().contains("at least 1"));
        }
    }

    #[test]
    fn rate_limit_config_rejects_non_numeric_and_out_of_range_values() {
        let non_numeric = config_from(&[("PYTH_MAX_REQUESTS", "many")]).unwrap_err();
        assert!(non_numeric.to_string().contains("PYTH_MAX_REQUESTS"));

        let out_of_range = config_from(&[("PYTH_MAX_RETRIES", "4294967296")]).unwrap_err();
        assert!(out_of_range.to_string().contains("PYTH_MAX_RETRIES"));
    }

    #[test]
    fn rate_limit_config_allows_zero_retries() {
        let config = config_from(&[("PYTH_MAX_RETRIES", "0")]).unwrap();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn non_unicode_environment_values_fail_instead_of_using_defaults() {
        let error = env_value(
            "PYTH_MAX_REQUESTS",
            Err(VarError::NotUnicode(OsString::from("invalid"))),
        )
        .unwrap_err();

        assert!(error.to_string().contains("PYTH_MAX_REQUESTS"));
        assert!(error.to_string().contains("valid Unicode"));
    }

    #[test]
    fn total_attempts_does_not_overflow_at_the_retry_limit() {
        assert_eq!(total_attempts(u32::MAX), u64::from(u32::MAX) + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_waits_until_the_window_expires_before_admitting_next_attempt() {
        let limiter = std::sync::Arc::new(RateLimiter::new(1, Duration::from_secs(60)));
        limiter.wait_if_needed().await;

        let next = {
            let limiter = limiter.clone();
            tokio::spawn(async move { limiter.wait_if_needed().await })
        };
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert!(!next.is_finished());

        tokio::time::advance(Duration::from_secs(1)).await;
        next.await.unwrap();
    }

    #[test]
    fn retry_after_uses_configured_cooldown_as_a_minimum() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("30"));

        assert_eq!(
            retry_after_delay(&headers, Duration::from_secs(60)),
            Duration::from_secs(60)
        );

        headers.insert(RETRY_AFTER, HeaderValue::from_static("120"));
        assert_eq!(
            retry_after_delay(&headers, Duration::from_secs(60)),
            Duration::from_secs(120)
        );
    }

    #[test]
    fn retry_after_falls_back_when_header_is_missing_or_invalid() {
        let cooldown = Duration::from_secs(60);
        assert_eq!(retry_after_delay(&HeaderMap::new(), cooldown), cooldown);

        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("not-seconds"));
        assert_eq!(retry_after_delay(&headers, cooldown), cooldown);
    }

    #[tokio::test]
    async fn batch_retries_reacquire_limiter_and_stop_at_configured_limit() {
        let (base_url, request_count) =
            spawn_pyth_server(vec![StatusCode::TOO_MANY_REQUESTS], Some("0")).await;
        let provider = PythProvider::with_config(
            base_url,
            PythRateLimitConfig {
                max_requests: 10,
                window: Duration::from_secs(60),
                cooldown: Duration::ZERO,
                max_retries: 3,
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
    async fn batch_retry_waits_for_configured_cooldown_before_success() {
        let (base_url, request_count) = spawn_pyth_server(
            vec![StatusCode::TOO_MANY_REQUESTS, StatusCode::OK],
            Some("0"),
        )
        .await;
        let cooldown = Duration::from_millis(50);
        let provider = PythProvider::with_config(
            base_url,
            PythRateLimitConfig {
                max_requests: 10,
                window: Duration::from_secs(60),
                cooldown,
                max_retries: 1,
            },
        )
        .unwrap();
        let started = Instant::now();

        let prices = provider.fetch_batch(&["feed"], 123).await.unwrap();

        assert!(prices.is_empty());
        assert_eq!(request_count.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= cooldown);
    }
}
