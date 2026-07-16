//! TDD contract tests for the `price_usd` table — schema + downstream read
//! contract. SQL-level (no src deps) so they lock the migration shape and the
//! carry-forward semantics that the Codex-implemented refresher/controller and
//! the downstream `balance_usd` reader must satisfy.
//!
//! Design: docs/plans/2026-06-15-defillama-anchor-price-coexistence-design.md
//! price_usd is block-keyed dense (mirrors `price`), SEPARATE from the Pyth
//! indexing path. Requires Docker (testcontainers).

mod common;

use anyhow::Result;
use bigdecimal::BigDecimal;
use common::setup_test_db;
use sqlx::{PgPool, Row};
use std::str::FromStr;

const LV: &str = "0x1001fF13bf368Aa4fa85F21043648079F00E1001";

async fn insert_price_usd(
    pool: &PgPool,
    token: &str,
    block: i64,
    price: &str,
    confidence: &str,
    created_at: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO price_usd (token_id, block_number, price, confidence, created_at) \
         VALUES ($1, $2, $3::numeric, $4::numeric, $5) \
         ON CONFLICT (token_id, block_number) DO NOTHING",
    )
    .bind(token)
    .bind(block)
    .bind(price)
    .bind(confidence)
    .bind(created_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Dense fill: one row per block over the elapsed range — no gap blocks.
#[tokio::test]
async fn price_usd_dense_fill_has_no_gap_blocks() -> Result<()> {
    let db = setup_test_db().await?;
    for b in 100..=105 {
        insert_price_usd(&db.pool, LV, b, "0.051648", "0.99", 1_700 + b).await?;
    }
    let cnt: i64 = sqlx::query("SELECT COUNT(*) FROM price_usd WHERE token_id = $1")
        .bind(LV)
        .fetch_one(&db.pool)
        .await?
        .get(0);
    assert_eq!(cnt, 6, "one row per block 100..=105 (dense, no gaps)");
    Ok(())
}

/// Read contract: as-of a block resolves to the latest price AT OR BEFORE that
/// block (carry-forward) — never a future price.
#[tokio::test]
async fn price_usd_carry_forward_picks_latest_at_or_before_block() -> Result<()> {
    let db = setup_test_db().await?;
    insert_price_usd(&db.pool, LV, 100, "0.05", "0.99", 1_700).await?;
    insert_price_usd(&db.pool, LV, 110, "0.06", "0.99", 1_710).await?;

    let as_of_105: BigDecimal = sqlx::query(
        "SELECT price FROM price_usd WHERE token_id = $1 AND block_number <= $2 \
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(LV)
    .bind(105_i64)
    .fetch_one(&db.pool)
    .await?
    .get(0);
    assert_eq!(
        as_of_105,
        BigDecimal::from_str("0.05").unwrap(),
        "block 105 carries forward block 100, NOT the future block 110 price"
    );

    let latest: BigDecimal =
        sqlx::query("SELECT price FROM price_usd WHERE token_id = $1 ORDER BY block_number DESC LIMIT 1")
            .bind(LV)
            .fetch_one(&db.pool)
            .await?
            .get(0);
    assert_eq!(latest, BigDecimal::from_str("0.06").unwrap());
    Ok(())
}

/// PK (token_id, block_number) makes re-processing the same block idempotent.
#[tokio::test]
async fn price_usd_insert_is_idempotent_on_token_block() -> Result<()> {
    let db = setup_test_db().await?;
    insert_price_usd(&db.pool, LV, 100, "0.05", "0.99", 1_700).await?;
    // Re-insert same (token, block) with a different price → DO NOTHING.
    insert_price_usd(&db.pool, LV, 100, "999", "0.50", 1_700).await?;

    let row = sqlx::query("SELECT price, confidence FROM price_usd WHERE token_id = $1 AND block_number = 100")
        .bind(LV)
        .fetch_all(&db.pool)
        .await?;
    assert_eq!(row.len(), 1, "PK (token_id, block_number) idempotent — exactly one row");
    let price: BigDecimal = row[0].get(0);
    assert_eq!(price, BigDecimal::from_str("0.05").unwrap(), "first write wins (DO NOTHING)");
    Ok(())
}

/// confidence is nullable (cold-start / low-quality rows are skipped upstream,
/// but the column itself permits NULL for forward-compatibility).
#[tokio::test]
async fn price_usd_allows_null_confidence() -> Result<()> {
    let db = setup_test_db().await?;
    sqlx::query(
        "INSERT INTO price_usd (token_id, block_number, price, confidence, created_at) \
         VALUES ($1, 100, 0.05, NULL, 1700)",
    )
    .bind(LV)
    .execute(&db.pool)
    .await?;
    let conf: Option<BigDecimal> =
        sqlx::query("SELECT confidence FROM price_usd WHERE token_id = $1 AND block_number = 100")
            .bind(LV)
            .fetch_one(&db.pool)
            .await?
            .get(0);
    assert!(conf.is_none());
    Ok(())
}
