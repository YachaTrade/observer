//! Pure logic contracts for the `price_usd` feature.

use bigdecimal::BigDecimal;
use std::str::FromStr;

use observer::event::common::price_usd::{
    PriceUsdPoint, PriceUsdRow, build_dense_rows, coin_ref, parse_current, should_refetch,
};

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

#[test]
fn parse_current_extracts_price_and_confidence_keyed_by_coin_ref() {
    let body = r#"{"coins":{"ethereum:0x1001fF13bf368Aa4fa85F21043648079F00E1001":{"decimals":18,"symbol":"LV","price":0.051648,"timestamp":1781510344,"confidence":0.99}}}"#;
    let parsed = parse_current(body).expect("valid body parses");

    let point: &PriceUsdPoint = parsed
        .get("ethereum:0x1001fF13bf368Aa4fa85F21043648079F00E1001")
        .expect("entry keyed by the coin id with original casing");
    assert_eq!(point.price, bd("0.051648"));
    assert_eq!(point.confidence, Some(bd("0.99")));
}

#[test]
fn parse_current_empty_coins_yields_empty_map() {
    let parsed = parse_current(r#"{"coins":{}}"#).expect("empty body parses");
    assert!(parsed.is_empty());
}

#[test]
fn parse_current_missing_confidence_is_none() {
    let body = r#"{"coins":{"ethereum:0xabc":{"price":1.0,"timestamp":1}}}"#;
    let parsed = parse_current(body).expect("parses");
    let point = parsed.get("ethereum:0xabc").expect("entry present");
    assert_eq!(point.price, bd("1.0"));
    assert_eq!(point.confidence, None);
}

#[test]
fn coin_ref_builds_slug_prefixed_preserving_case() {
    let id = "0x1001fF13bf368Aa4fa85F21043648079F00E1001";
    let expected_slug =
        std::env::var("DEFILLAMA_CHAIN_SLUG").unwrap_or_else(|_| "ethereum".to_string());
    assert_eq!(coin_ref(id), format!("{expected_slug}:{id}"));
}

#[test]
fn should_refetch_true_when_never_fetched() {
    assert!(should_refetch(None, 1_000, 60));
}

#[test]
fn should_refetch_respects_interval() {
    assert!(!should_refetch(Some(1_000), 1_059, 60));
    assert!(should_refetch(Some(1_000), 1_060, 60));
}

#[test]
fn build_dense_rows_emits_one_row_per_block() {
    let blocks = vec![(100u64, 1_700u64), (101, 1_701), (102, 1_702)];
    let price = bd("0.051648");
    let confidence = Some(bd("0.99"));

    let rows: Vec<PriceUsdRow> = build_dense_rows("0xLV", &price, confidence.clone(), &blocks);

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].token_id, "0xLV");
    assert_eq!(rows[0].block_number, 100);
    assert_eq!(rows[0].price, price);
    assert_eq!(rows[0].confidence, confidence);
    assert_eq!(rows[0].created_at, 1_700);
    assert_eq!(rows[2].block_number, 102);
    assert_eq!(rows[2].created_at, 1_702);
}

#[test]
fn build_dense_rows_empty_range_is_empty() {
    let rows = build_dense_rows("0xLV", &bd("0.05"), None, &[]);
    assert!(rows.is_empty());
}
