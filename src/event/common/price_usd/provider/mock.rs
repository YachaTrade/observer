use std::{collections::HashMap, str::FromStr};

use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;

use super::{PriceUsdPoint, PriceUsdProvider};

#[derive(Debug, Clone)]
pub struct MockProvider {
    price: BigDecimal,
    confidence: Option<BigDecimal>,
}

impl MockProvider {
    pub fn fixed(price: BigDecimal, confidence: Option<BigDecimal>) -> Self {
        Self { price, confidence }
    }

    pub fn fixed_str(price: &str, confidence: &str) -> Self {
        Self {
            price: BigDecimal::from_str(price)
                .expect("MockProvider::fixed_str received invalid price decimal"),
            confidence: Some(
                BigDecimal::from_str(confidence)
                    .expect("MockProvider::fixed_str received invalid confidence decimal"),
            ),
        }
    }
}

#[async_trait]
impl PriceUsdProvider for MockProvider {
    async fn fetch_current(&self, coin_refs: &[String]) -> Result<HashMap<String, PriceUsdPoint>> {
        Ok(coin_refs
            .iter()
            .map(|coin_ref| {
                (
                    coin_ref.clone(),
                    PriceUsdPoint {
                        price: self.price.clone(),
                        confidence: self.confidence.clone(),
                    },
                )
            })
            .collect())
    }

    /// Mock ignores timestamp/search_width and returns the same fixed price.
    async fn fetch_historical(
        &self,
        coin_refs: &[String],
        _timestamp: u64,
        _search_width_secs: u64,
    ) -> Result<HashMap<String, PriceUsdPoint>> {
        self.fetch_current(coin_refs).await
    }
}
