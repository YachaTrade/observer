//! Integration tests for the active v2 Curve sniping controller.

mod common;

use anyhow::Result;
use bigdecimal::BigDecimal;
use common::setup_test_db;
use std::str::FromStr;

// Shared test constants
const TOKEN: &str = "0x1111111111111111111111111111111111111111";
const BUYER: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TX1: &str = "0xtx_v2_test_1";

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

// ============================================================================
// sniping.rs — batch_insert_sniping_penalties (INSERT_SNIPING_PENALTIES_SQL)
// ============================================================================

#[tokio::test]
async fn sniping_penalties_happy() -> Result<()> {
    let db = setup_test_db().await?;
    sqlx::query(observer::db::postgres::controller::v2::sniping::INSERT_SNIPING_PENALTIES_SQL)
        .bind(&[TOKEN] as &[&str])
        .bind(&[BUYER] as &[&str])
        .bind(&[bd("100")] as &[BigDecimal])
        .bind(&[bd("500")] as &[BigDecimal])
        .bind(&[TX1] as &[&str])
        .bind(&[100_i64] as &[i64])
        .bind(&[1_700_000_000_i64] as &[i64])
        .bind(&[0_i32] as &[i32])
        .bind(&[0_i32] as &[i32])
        .execute(&db.pool)
        .await?;

    let (sniping_fee, penalty_bps): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT sniping_fee, penalty_bps FROM v2_sniping_history WHERE token_id = $1",
    )
    .bind(TOKEN)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(sniping_fee, bd("100"));
    assert_eq!(penalty_bps, bd("500"));
    Ok(())
}

#[tokio::test]
async fn sniping_penalties_duplicate_ignored() -> Result<()> {
    let db = setup_test_db().await?;
    for _ in 0..2 {
        sqlx::query(observer::db::postgres::controller::v2::sniping::INSERT_SNIPING_PENALTIES_SQL)
            .bind(&[TOKEN] as &[&str])
            .bind(&[BUYER] as &[&str])
            .bind(&[bd("100")] as &[BigDecimal])
            .bind(&[bd("500")] as &[BigDecimal])
            .bind(&[TX1] as &[&str])
            .bind(&[100_i64] as &[i64])
            .bind(&[1_700_000_000_i64] as &[i64])
            .bind(&[0_i32] as &[i32])
            .bind(&[0_i32] as &[i32])
            .execute(&db.pool)
            .await?;
    }

    let count: (i64,) = sqlx::query_as("SELECT count(*) FROM v2_sniping_history WHERE token_id = $1")
        .bind(TOKEN)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count.0, 1);
    Ok(())
}
