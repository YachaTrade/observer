//! Pure query-to-storage remapping contracts for the `price_usd` stream.

use bigdecimal::BigDecimal;
use std::collections::HashMap;
use std::str::FromStr;

use observer::event::common::price_usd::{
    PriceUsdPoint, PriceUsdTarget, apply_fresh_prices, coin_ref, distinct_query_coin_refs,
};

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

fn target(storage: &str, query: &str) -> PriceUsdTarget {
    PriceUsdTarget {
        token_id: storage.to_string(),
        query_id: query.to_string(),
    }
}

fn point(price: &str, confidence: &str) -> PriceUsdPoint {
    PriceUsdPoint {
        price: bd(price),
        confidence: Some(bd(confidence)),
    }
}

#[test]
fn distinct_query_coin_refs_dedupes_shared_query_address() {
    let targets = vec![
        target("0xTOKEN_A", "0xSHARED"),
        target("0xTOKEN_B", "0xSHARED"),
        target("0xSHARED", "0xSHARED"),
        target("0xTOKEN_C", "0xSOURCE_C"),
    ];
    let refs = distinct_query_coin_refs(&targets);
    assert_eq!(refs, vec![coin_ref("0xSHARED"), coin_ref("0xSOURCE_C")]);
}

#[test]
fn fan_out_one_query_price_to_multiple_storage_tokens() {
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xSHARED"), point("0.0226", "0.99"));

    let targets = vec![
        target("0xTOKEN_A", "0xSHARED"),
        target("0xTOKEN_B", "0xSHARED"),
    ];
    let mut last_good = HashMap::new();
    apply_fresh_prices(&targets, &fresh, &mut last_good, &bd("0.9"));

    assert_eq!(last_good.get("0xTOKEN_A").unwrap().price, bd("0.0226"));
    assert_eq!(last_good.get("0xTOKEN_B").unwrap().price, bd("0.0226"));
    assert_eq!(last_good.len(), 2);
}

#[test]
fn stores_under_storage_token_id_not_query_id() {
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xSOURCE"), point("1.0", "0.99"));

    let targets = vec![target("0xTOKEN", "0xSOURCE")];
    let mut last_good = HashMap::new();
    apply_fresh_prices(&targets, &fresh, &mut last_good, &bd("0.9"));

    assert!(last_good.contains_key("0xTOKEN"));
    assert!(!last_good.contains_key("0xSOURCE"));
    assert!(!last_good.contains_key(&coin_ref("0xSOURCE")));
}

#[test]
fn low_confidence_carries_forward_existing() {
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xSOURCE"), point("9.99", "0.5"));

    let targets = vec![target("0xTOKEN", "0xSOURCE")];
    let mut last_good = HashMap::new();
    last_good.insert("0xTOKEN".to_string(), point("1.0", "0.99"));

    apply_fresh_prices(&targets, &fresh, &mut last_good, &bd("0.9"));
    assert_eq!(last_good.get("0xTOKEN").unwrap().price, bd("1.0"));
}

#[test]
fn missing_confidence_carries_forward_existing() {
    let mut fresh = HashMap::new();
    fresh.insert(
        coin_ref("0xSOURCE"),
        PriceUsdPoint {
            price: bd("9.99"),
            confidence: None,
        },
    );

    let targets = vec![target("0xTOKEN", "0xSOURCE")];
    let mut last_good = HashMap::new();
    last_good.insert("0xTOKEN".to_string(), point("1.0", "0.99"));

    apply_fresh_prices(&targets, &fresh, &mut last_good, &bd("0.9"));
    assert_eq!(last_good.get("0xTOKEN").unwrap().price, bd("1.0"));
}

#[test]
fn confidence_equal_to_threshold_is_accepted() {
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xSOURCE"), point("9.99", "0.9"));

    let targets = vec![target("0xTOKEN", "0xSOURCE")];
    let mut last_good = HashMap::new();
    last_good.insert("0xTOKEN".to_string(), point("1.0", "0.99"));

    apply_fresh_prices(&targets, &fresh, &mut last_good, &bd("0.9"));
    assert_eq!(last_good.get("0xTOKEN").unwrap().price, bd("9.99"));
}

#[test]
fn missing_from_response_carries_forward() {
    let fresh: HashMap<String, PriceUsdPoint> = HashMap::new();

    let targets = vec![target("0xTOKEN", "0xSOURCE")];
    let mut last_good = HashMap::new();
    last_good.insert("0xTOKEN".to_string(), point("2.0", "0.99"));

    apply_fresh_prices(&targets, &fresh, &mut last_good, &bd("0.9"));
    assert_eq!(last_good.get("0xTOKEN").unwrap().price, bd("2.0"));
}

#[test]
fn case_insensitive_query_ref_match() {
    let mut fresh = HashMap::new();
    fresh.insert(coin_ref("0xABCdef"), point("3.0", "0.99"));

    let targets = vec![target("0xTOKEN", "0xabcDEF")];
    let mut last_good = HashMap::new();
    apply_fresh_prices(&targets, &fresh, &mut last_good, &bd("0.9"));

    assert_eq!(last_good.get("0xTOKEN").unwrap().price, bd("3.0"));
}
