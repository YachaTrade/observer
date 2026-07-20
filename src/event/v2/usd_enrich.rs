use std::sync::Arc;

use bigdecimal::BigDecimal;

use crate::{
    config::{WNATIVE_ADDRESS, get_quote_decimals},
    db::cache::CacheManager,
};

/// Resolve (quote_id, usd_value) for a token-scoped amount at a given block.
///
/// Runs `get_token_quote_id(token)` and a speculative `get_quote_usd_price(WNATIVE, block)`
/// in parallel via `tokio::join!`. WNATIVE is the dominant quote, so the speculative
/// price is reused for the common case; other quote tokens pay one extra await for
/// the actual quote price.
///
/// Returns `usd_value = 0` when no price is available; logs an error.
pub(crate) async fn enrich_usd(
    cache: &CacheManager,
    token: &str,
    amount: &BigDecimal,
    block_num: i64,
) -> (Arc<String>, Arc<BigDecimal>) {
    let (quote_id_res, wnative_price) = tokio::join!(
        cache.get_token_quote_id(token),
        cache.get_quote_usd_price(&WNATIVE_ADDRESS, block_num),
    );

    let quote_id = quote_id_res
        .unwrap_or(None)
        .unwrap_or_else(|| (*WNATIVE_ADDRESS).clone());

    let price_opt = if quote_id == *WNATIVE_ADDRESS {
        wnative_price
    } else {
        cache.get_quote_usd_price(&quote_id, block_num).await
    };

    let decimals = get_quote_decimals(&quote_id);
    let usd_value = match &price_opt {
        Some(p) => (amount / decimals) * &**p,
        None => {
            tracing::error!(
                "[VAULT] No price block={} token={} quote={}",
                block_num,
                token,
                quote_id
            );
            BigDecimal::from(0)
        }
    };

    (Arc::new(quote_id), Arc::new(usd_value))
}
