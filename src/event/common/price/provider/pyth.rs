//! Pyth Hermes HTTP price provider.
//!
//! Owns the HTTP client and response parsing. Request cadence is managed
//! by the caller.

use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use reqwest::Client;
use tokio::time::Instant;
use tracing::{info, warn};

use super::{PriceProvider, normalize_feed_id};
use crate::{config::PYTH_API_URL, types::price::PriceFeedResponse};

const REQUEST_TIMEOUT_SECS: u64 = 30;

pub struct PythProvider {
    http: Client,
    base_url: String,
}

impl PythProvider {
    pub fn new() -> Result<Self> {
        Self::with_base_url((*PYTH_API_URL).clone())
    }

    fn with_base_url(base_url: String) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for PythProvider")?;
        Ok(Self { http, base_url })
    }
}

#[async_trait]
impl PriceProvider for PythProvider {
    async fn fetch(&self, feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>> {
        let url = format!(
            "{}/{}?ids%5B%5D={}&encoding=hex&parsed=true&ignore_invalid_price_ids=false",
            self.base_url, timestamp, feed_id
        );
        let response = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .context("Failed to request Pyth API")?;

        if !response.status().is_success() {
            anyhow::bail!("Pyth API returned status: {}", response.status());
        }

        let feed: PriceFeedResponse = response
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
        Ok(Some(price))
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
        let mut url = format!(
            "{}/{}?encoding=hex&parsed=true&ignore_invalid_price_ids=true",
            self.base_url, timestamp
        );
        for feed_id in feed_ids {
            url.push_str("&ids%5B%5D=");
            url.push_str(feed_id);
        }

        info!(
            "[PYTH] → batch fetch ts={} feeds={}",
            timestamp,
            feed_ids.len()
        );
        let response = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .context("Failed to request Pyth batch API")?;

        if !response.status().is_success() {
            anyhow::bail!("Pyth batch API returned status: {}", response.status());
        }

        let feed: PriceFeedResponse = response
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
            "[PYTH] ✓ batch ts={} feeds={} returned={} total={}ms",
            timestamp,
            feed_ids.len(),
            out.len(),
            started.elapsed().as_millis()
        );
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Router,
        body::Body,
        extract::Request,
        http::{Response, StatusCode},
    };

    use super::{PriceProvider, PythProvider};

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

    #[tokio::test]
    async fn batch_429_is_returned_after_one_http_attempt() {
        let (base_url, request_count) =
            spawn_pyth_server(vec![StatusCode::TOO_MANY_REQUESTS]).await;
        let provider = PythProvider::with_base_url(base_url).unwrap();

        let error = provider
            .fetch_batch(&["feed"], 123)
            .await
            .expect_err("429 must be returned without an internal retry");

        assert!(error.to_string().contains("429"));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }
}
