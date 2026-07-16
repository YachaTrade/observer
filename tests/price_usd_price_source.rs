//! TDD RED — price_source_id mapping for the price_usd (DefiLlama) stream.
//!
//! Testnet pools/balances use mock token addresses that DefiLlama doesn't know,
//! so we QUERY DefiLlama with a mainnet address (`price_source_id`) but STORE
//! price_usd under the on-chain `token_id`. Rule: query = COALESCE(price_source_id,
//! token_id); store = token_id. Multiple tokens can share one query address
//! (MON/LVMON/WMON all price via mainnet WMON), so one fetched price fans out to
//! several storage tokens.
//!
//! These lock the pure remap logic. GREEN target (inline): move WhitelistToken +
//! distinct_query_coin_refs + apply_fresh_prices into the price_usd module as
//! pub, keyed query->storage. Do NOT modify this file.

use bigdecimal::BigDecimal;
use std::collections::HashMap;
use std::str::FromStr;

use observer::event::common::price_usd::{
    PriceUsdPoint, WhitelistToken, apply_fresh_prices, coin_ref, distinct_query_coin_refs,
};

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}
fn wt(storage: &str, query: &str) -> WhitelistToken {
    WhitelistToken {
        token_id: storage.to_string(),
        query_id: query.to_string(),
    }
}
fn pt(price: &str, conf: &str) -> PriceUsdPoint {
    PriceUsdPoint {
        price: bd(price),
        confidence: Some(bd(conf)),
    }
}

// ── distinct query coin refs (one request per unique query address) ──────────

#[test]
fn distinct_query_coin_refs_dedupes_shared_query_address() {
    // MON, LVMON, WMON all query the same mainnet address -> request its coin ref ONCE.
    let tokens = vec![
        wt("0xMON", "0xWMON"),
        wt("0xLVMON", "0xWMON"),
        wt("0xWMON", "0xWMON"),
        wt("0xUSDC_t", "0xUSDC_m"),
    ];
    let refs = distinct_query_coin_refs(&tokens);
    assert_eq!(refs, vec![coin_ref("0xWMON"), coin_ref("0xUSDC_m")]);
}

// ── apply_fresh_prices: query_id lookup, token_id storage, fan-out ───────────

#[test]
fn fan_out_one_query_price_to_multiple_storage_tokens() {
    // Response keyed by the WMON query ref; MON and LVMON both consume it and
    // each lands under its OWN storage token_id.
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xWMON"), pt("0.0226", "0.99"));

    let tokens = vec![wt("0xMON", "0xWMON"), wt("0xLVMON", "0xWMON")];
    let mut last_good = HashMap::new();
    apply_fresh_prices(&tokens, &fresh, &mut last_good, &bd("0.9"));

    assert_eq!(last_good.get("0xMON").unwrap().price, bd("0.0226"));
    assert_eq!(last_good.get("0xLVMON").unwrap().price, bd("0.0226"));
    assert_eq!(last_good.len(), 2);
}

#[test]
fn stores_under_storage_token_id_not_query_id() {
    // USDC: storage = testnet mock, query = mainnet. Price arrives keyed by the
    // mainnet ref; it MUST be stored under the testnet token_id (the join key).
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xMAIN"), pt("1.0", "0.99"));

    let tokens = vec![wt("0xTESTNET", "0xMAIN")];
    let mut last_good = HashMap::new();
    apply_fresh_prices(&tokens, &fresh, &mut last_good, &bd("0.9"));

    assert!(last_good.contains_key("0xTESTNET"), "stored under token_id");
    assert!(!last_good.contains_key("0xMAIN"), "never under query_id");
    assert!(!last_good.contains_key(&coin_ref("0xMAIN")));
}

#[test]
fn low_confidence_carries_forward_existing() {
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xMAIN"), pt("9.99", "0.5")); // below 0.9 threshold

    let tokens = vec![wt("0xTESTNET", "0xMAIN")];
    let mut last_good = HashMap::new();
    last_good.insert("0xTESTNET".to_string(), pt("1.0", "0.99")); // prior good

    apply_fresh_prices(&tokens, &fresh, &mut last_good, &bd("0.9"));
    assert_eq!(
        last_good.get("0xTESTNET").unwrap().price,
        bd("1.0"),
        "low-conf must not overwrite; carry forward"
    );
}

#[test]
fn missing_from_response_carries_forward() {
    let fresh: HashMap<String, PriceUsdPoint> = HashMap::new(); // empty response

    let tokens = vec![wt("0xTESTNET", "0xMAIN")];
    let mut last_good = HashMap::new();
    last_good.insert("0xTESTNET".to_string(), pt("2.0", "0.99"));

    apply_fresh_prices(&tokens, &fresh, &mut last_good, &bd("0.9"));
    assert_eq!(last_good.get("0xTESTNET").unwrap().price, bd("2.0"));
}

#[test]
fn case_insensitive_query_ref_match() {
    // DefiLlama may echo a different casing than we sent; lookup is case-insensitive.
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xABCdef"), pt("3.0", "0.99"));

    let tokens = vec![wt("0xstore", "0xabcDEF")]; // different casing of query
    let mut last_good = HashMap::new();
    apply_fresh_prices(&tokens, &fresh, &mut last_good, &bd("0.9"));

    assert_eq!(last_good.get("0xstore").unwrap().price, bd("3.0"));
}
