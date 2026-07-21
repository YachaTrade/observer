//! Price oracle provider abstraction.
//!
//! The streaming loop talks to providers exclusively through the
//! [`PriceProvider`] trait so oracles (Pyth, mocks, future providers)
//! can be swapped without touching orchestration code.

pub mod mock;
pub mod pyth;

use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use std::collections::HashMap;
use std::sync::Arc;

/// Normalize a Pyth feed ID for map-key comparison.
///
/// Pyth Hermes responses strip the `0x` prefix from `parsed[].id`, so we
/// normalize both sides (lowercase + strip prefix) before keying maps so
/// callers can pass feed IDs in either form.
pub fn normalize_feed_id(feed_id: &str) -> String {
    feed_id.trim_start_matches("0x").to_lowercase()
}

/// A price oracle capable of returning a spot price for a given feed id
/// at (or near) the supplied unix timestamp (seconds).
#[async_trait]
pub trait PriceProvider: Send + Sync {
    /// Fetch price for `feed_id` at `timestamp`.
    ///
    /// Returns `Ok(None)` when the oracle has no data for that point in time.
    /// Returns `Err(_)` for transport, parsing, or rate-limit failures that
    /// the provider could not recover from internally.
    async fn fetch(&self, feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>>;

    /// Batch-fetch multiple feeds at one timestamp.
    ///
    /// Returns a map keyed by [`normalize_feed_id`] (lowercase, no `0x` prefix).
    /// Missing entries indicate the oracle had no data for that feed at the
    /// given timestamp; callers should treat them like `Ok(None)` from
    /// [`fetch`].
    ///
    /// Implementations should consume only one oracle slot for the whole
    /// batch — that's the entire point of this method (rate-limit pressure
    /// becomes O(timestamps) instead of O(timestamps × feeds)).
    async fn fetch_batch(
        &self,
        feed_ids: &[&str],
        timestamp: u64,
    ) -> Result<HashMap<String, BigDecimal>>;
}

/// Build the provider selected by runtime env (`MODE`).
///
/// - `MODE=testnet` → [`mock::MockProvider`] with a fixed 0.03 price,
///   preserving the legacy testnet hardcoded value.
/// - otherwise      → [`pyth::PythProvider`] backed by the Pyth Hermes API.
pub fn build_provider() -> Result<Arc<dyn PriceProvider>> {
    // `PRICE_MODE` overrides `MODE` for the quote (Pyth) provider only, so
    // Pyth can run live while the token-USD (DefiLlama) provider stays mocked
    // via its own `PRICE_USD_MODE`. Falls back to `MODE`, then "mainnet".
    let mode = std::env::var("PRICE_MODE")
        .or_else(|_| std::env::var("MODE"))
        .unwrap_or_else(|_| "mainnet".to_string());
    if mode.to_lowercase() == "testnet" {
        tracing::info!("[PRICE] Using MockProvider (mode=testnet)");
        Ok(Arc::new(mock::MockProvider::fixed_str("0.03")))
    } else {
        tracing::info!("[PRICE] Using PythProvider (mode={})", mode);
        Ok(Arc::new(pyth::PythProvider::new()?))
    }
}
