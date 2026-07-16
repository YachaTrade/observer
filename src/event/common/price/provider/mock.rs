//! In-memory price provider used for testnet runtime and unit tests.

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;

use super::{PriceProvider, normalize_feed_id};

/// Always returns a single fixed price regardless of feed/timestamp.
#[derive(Debug, Clone)]
pub struct MockProvider {
    price: BigDecimal,
}

impl MockProvider {
    pub fn fixed(price: BigDecimal) -> Self {
        Self { price }
    }

    pub fn fixed_str(price: &str) -> Self {
        Self {
            price: BigDecimal::from_str(price)
                .expect("MockProvider::fixed_str received invalid decimal"),
        }
    }
}

#[async_trait]
impl PriceProvider for MockProvider {
    async fn fetch(&self, _feed_id: &str, _timestamp: u64) -> Result<Option<BigDecimal>> {
        Ok(Some(self.price.clone()))
    }

    async fn fetch_batch(
        &self,
        feed_ids: &[&str],
        _timestamp: u64,
    ) -> Result<HashMap<String, BigDecimal>> {
        Ok(feed_ids
            .iter()
            .map(|id| (normalize_feed_id(id), self.price.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_fixed_price_for_any_feed_and_timestamp() {
        let provider = MockProvider::fixed_str("0.03");
        let price = provider
            .fetch("0xdeadbeef", 1_700_000_000)
            .await
            .expect("fetch must not error");
        assert_eq!(price, Some(BigDecimal::from_str("0.03").unwrap()));

        // Different feed / timestamp — still the same fixed value.
        let price2 = provider
            .fetch("0xcafebabe", 1_800_000_000)
            .await
            .expect("fetch must not error");
        assert_eq!(price2, Some(BigDecimal::from_str("0.03").unwrap()));
    }

    #[tokio::test]
    async fn fixed_accepts_arbitrary_bigdecimal() {
        let provider = MockProvider::fixed(BigDecimal::from_str("12345.6789").unwrap());
        let price = provider.fetch("any", 0).await.unwrap();
        assert_eq!(price, Some(BigDecimal::from_str("12345.6789").unwrap()));
    }
}
