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
    let mode = std::env::var("MODE").unwrap_or_else(|_| "mainnet".to_string());
    if mode.to_lowercase() == "testnet" {
        tracing::info!("[PRICE_USD] Using MockProvider (MODE=testnet)");
        Ok(Arc::new(mock::MockProvider::fixed_str("0.03", "0.99")))
    } else {
        tracing::info!("[PRICE_USD] Using DefiLlamaProvider (MODE={})", mode);
        Ok(Arc::new(defillama::DefiLlamaProvider::new()?))
    }
}
