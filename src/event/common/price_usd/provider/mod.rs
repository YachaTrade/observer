pub mod defillama;
pub mod mock;

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use super::PriceUsdPoint;

#[async_trait]
pub trait PriceUsdProvider: Send + Sync {
    async fn fetch_current(&self, coin_refs: &[String]) -> Result<HashMap<String, PriceUsdPoint>>;

    /// Price as of `timestamp` (DefiLlama `/historical`). `search_width_secs`
    /// is how far DefiLlama may look for the nearest snapshot around the
    /// timestamp — too narrow returns no data, so callers pass a generous
    /// window (see `HISTORICAL_SEARCH_WIDTH_SECS`).
    async fn fetch_historical(
        &self,
        coin_refs: &[String],
        timestamp: u64,
        search_width_secs: u64,
    ) -> Result<HashMap<String, PriceUsdPoint>>;
}

pub fn build_provider() -> Result<Arc<dyn PriceUsdProvider>> {
    // `PRICE_USD_MODE` overrides `MODE` for the token-USD (DefiLlama) provider
    // only, so DefiLlama can be mocked while the quote (Pyth) provider runs
    // live via its own `PRICE_MODE`. Falls back to `MODE`, then "mainnet".
    let mode = std::env::var("PRICE_USD_MODE")
        .or_else(|_| std::env::var("MODE"))
        .unwrap_or_else(|_| "mainnet".to_string());
    if mode.to_lowercase() == "testnet" {
        tracing::info!("[PRICE_USD] Using MockProvider (mode=testnet)");
        Ok(Arc::new(mock::MockProvider::fixed_str("0.03", "0.99")))
    } else {
        tracing::info!("[PRICE_USD] Using DefiLlamaProvider (mode={})", mode);
        Ok(Arc::new(defillama::DefiLlamaProvider::new()?))
    }
}
