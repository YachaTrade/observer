//! TDD RED — pure logic for the `price_usd` (DefiLlama whitelist price) feature.
//!
//! These reference the wished-for public API under
//! `observer::event::common::price_usd`. They MUST fail to compile until that
//! module exists, then pass WITHOUT modification (Codex implements GREEN — do
//! NOT edit this file).
//!
//! Design: docs/plans/2026-06-15-defillama-anchor-price-coexistence-design.md
//!
//! API contract pinned here (Codex must match exactly):
//!   parse_current(body: &str) -> Result<HashMap<String, PriceUsdPoint>>
//!   PriceUsdPoint { price: BigDecimal, confidence: Option<BigDecimal> }
//!   coin_ref(token_id: &str) -> String
//!   should_refetch(last: Option<u64>, now: u64, interval_secs: u64) -> bool
//!   build_dense_rows(token_id, &price, confidence, &[(block, ts)]) -> Vec<PriceUsdRow>
//!   PriceUsdRow { token_id: String, block_number: u64, price: BigDecimal,
//!                 confidence: Option<BigDecimal>, created_at: u64 }

use bigdecimal::BigDecimal;
use std::str::FromStr;

use observer::event::common::price_usd::{
    build_dense_rows, coin_ref, parse_current, should_refetch, PriceUsdPoint, PriceUsdRow,
};

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

// ── parser ──────────────────────────────────────────────────────────────

#[test]
fn parse_current_extracts_price_and_confidence_keyed_by_coin_ref() {
    // Real DefiLlama /prices/current shape (ethereum-slug LV example).
    let body = r#"{"coins":{"ethereum:0x1001fF13bf368Aa4fa85F21043648079F00E1001":{"decimals":18,"symbol":"LV","price":0.051648,"timestamp":1781510344,"confidence":0.99}}}"#;
    let parsed = parse_current(body).expect("valid body parses");

    let pt: &PriceUsdPoint = parsed
        .get("ethereum:0x1001fF13bf368Aa4fa85F21043648079F00E1001")
        .expect("entry keyed by the DefiLlama coin id (preserves EIP-55 casing)");
    assert_eq!(pt.price, bd("0.051648"));
    assert_eq!(pt.confidence, Some(bd("0.99")));
}

#[test]
fn parse_current_empty_coins_yields_empty_map() {
    let parsed = parse_current(r#"{"coins":{}}"#).expect("empty body parses");
    assert!(
        parsed.is_empty(),
        "empty coins -> no entries (caller skips; never inserts price 0)"
    );
}

#[test]
fn parse_current_missing_confidence_is_none() {
    let body = r#"{"coins":{"ethereum:0xabc":{"price":1.0,"timestamp":1}}}"#;
    let parsed = parse_current(body).expect("parses");
    let pt = parsed.get("ethereum:0xabc").expect("entry present");
    assert_eq!(pt.price, bd("1.0"));
    assert_eq!(
        pt.confidence, None,
        "missing confidence -> None so the caller's <0.9 gate can skip it"
    );
}

// ── coin_ref (EIP-55 preserved, no lowercasing) ─────────────────────────

#[test]
fn coin_ref_builds_slug_prefixed_preserving_case() {
    // DEFILLAMA_CHAIN_SLUG 미설정 → 기본 "ethereum".
    let id = "0x1001fF13bf368Aa4fa85F21043648079F00E1001";
    assert_eq!(
        coin_ref(id),
        format!("ethereum:{id}"),
        "{{slug}}:{{token_id}} with original EIP-55 casing (no .to_lowercase())"
    );
}

// ── throttle (DefiLlama at most once / interval) ────────────────────────

#[test]
fn should_refetch_true_when_never_fetched() {
    assert!(should_refetch(None, 1_000, 60), "no prior fetch -> fetch");
}

#[test]
fn should_refetch_respects_interval() {
    assert!(
        !should_refetch(Some(1_000), 1_059, 60),
        "59s elapsed (<60) -> reuse last price, do not call DefiLlama"
    );
    assert!(
        should_refetch(Some(1_000), 1_060, 60),
        "60s elapsed (>=60) -> refetch"
    );
}

// ── dense fill (no gap blocks; each block stamped with its own timestamp) ─

#[test]
fn build_dense_rows_emits_one_row_per_block() {
    let blocks = vec![(100u64, 1_700u64), (101, 1_701), (102, 1_702)];
    let price = bd("0.051648");
    let conf = Some(bd("0.99"));

    let rows: Vec<PriceUsdRow> = build_dense_rows("0xLV", &price, conf.clone(), &blocks);

    assert_eq!(rows.len(), 3, "one row per block -> no gap blocks");

    assert_eq!(rows[0].token_id, "0xLV");
    assert_eq!(rows[0].block_number, 100);
    assert_eq!(rows[0].price, price);
    assert_eq!(rows[0].confidence, conf);
    assert_eq!(rows[0].created_at, 1_700, "created_at = that block's timestamp");

    assert_eq!(rows[2].block_number, 102);
    assert_eq!(rows[2].created_at, 1_702);
}

#[test]
fn build_dense_rows_empty_range_is_empty() {
    let rows = build_dense_rows("0xLV", &bd("0.05"), None, &[]);
    assert!(rows.is_empty(), "no blocks elapsed -> nothing to write");
}
