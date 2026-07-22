//! Database contracts for the block-keyed `price_usd` table.

mod common;

use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use bigdecimal::BigDecimal;
use common::setup_test_db;
use observer::{
    db::postgres::{PostgresDatabase, controller::price_usd::PriceUsdController},
    event::common::price_usd::{
        PriceUsdRow, PriceUsdTarget, stream::load_last_good_prices_from_pool,
    },
};
use sqlx::Row;

const TOKEN: &str = "0x1001fF13bf368Aa4fa85F21043648079F00E1001";

fn row(block_number: u64, price: &str, confidence: Option<&str>, created_at: u64) -> PriceUsdRow {
    PriceUsdRow {
        token_id: TOKEN.to_string(),
        block_number,
        price: BigDecimal::from_str(price).unwrap(),
        confidence: confidence.map(|value| BigDecimal::from_str(value).unwrap()),
        created_at,
    }
}

fn controller(db: &common::TestDb) -> PriceUsdController {
    PriceUsdController::new(Arc::new(PostgresDatabase {
        pool: db.pool.clone(),
    }))
}

#[tokio::test]
async fn price_usd_dense_fill_has_no_gap_blocks() -> Result<()> {
    let db = setup_test_db().await?;
    let rows = (100..=105)
        .map(|block| row(block, "0.051648", Some("0.99"), 1_700 + block))
        .collect::<Vec<_>>();
    controller(&db).batch_insert_price_usd(&rows).await?;

    let count: i64 = sqlx::query("SELECT COUNT(*) FROM price_usd WHERE token_id = $1")
        .bind(TOKEN)
        .fetch_one(&db.pool)
        .await?
        .get(0);
    assert_eq!(count, 6);
    Ok(())
}

#[tokio::test]
async fn price_usd_carry_forward_picks_latest_at_or_before_block() -> Result<()> {
    let db = setup_test_db().await?;
    controller(&db)
        .batch_insert_price_usd(&[
            row(100, "0.05", Some("0.99"), 1_700),
            row(110, "0.06", Some("0.99"), 1_710),
        ])
        .await?;

    let price: BigDecimal = sqlx::query(
        "SELECT price FROM price_usd WHERE token_id = $1 AND block_number <= $2 \
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(TOKEN)
    .bind(105_i64)
    .fetch_one(&db.pool)
    .await?
    .get(0);
    assert_eq!(price, BigDecimal::from_str("0.05").unwrap());
    Ok(())
}

#[tokio::test]
async fn price_usd_insert_is_idempotent_on_token_block() -> Result<()> {
    let db = setup_test_db().await?;
    let controller = controller(&db);
    controller
        .batch_insert_price_usd(&[row(100, "0.05", Some("0.99"), 1_700)])
        .await?;
    controller
        .batch_insert_price_usd(&[row(100, "999", Some("0.50"), 1_700)])
        .await?;

    let rows =
        sqlx::query("SELECT price FROM price_usd WHERE token_id = $1 AND block_number = 100")
            .bind(TOKEN)
            .fetch_all(&db.pool)
            .await?;
    assert_eq!(rows.len(), 1);
    let price: BigDecimal = rows[0].get(0);
    assert_eq!(price, BigDecimal::from_str("0.05").unwrap());
    Ok(())
}

#[tokio::test]
async fn price_usd_allows_null_confidence() -> Result<()> {
    let db = setup_test_db().await?;
    controller(&db)
        .batch_insert_price_usd(&[row(100, "0.05", None, 1_700)])
        .await?;

    let confidence: Option<BigDecimal> =
        sqlx::query("SELECT confidence FROM price_usd WHERE token_id = $1 AND block_number = 100")
            .bind(TOKEN)
            .fetch_one(&db.pool)
            .await?
            .get(0);
    assert!(confidence.is_none());
    Ok(())
}

#[tokio::test]
async fn price_usd_restart_hydrates_latest_point_before_current_range() -> Result<()> {
    let db = setup_test_db().await?;
    controller(&db)
        .batch_insert_price_usd(&[
            row(100, "0.05", Some("0.95"), 1_700),
            row(110, "0.06", Some("0.99"), 1_710),
        ])
        .await?;

    let targets = vec![
        PriceUsdTarget {
            token_id: TOKEN.to_string(),
            query_id: TOKEN.to_string(),
        },
        PriceUsdTarget {
            token_id: "0x0000000000000000000000000000000000000002".to_string(),
            query_id: "0x0000000000000000000000000000000000000002".to_string(),
        },
    ];

    let before_110 = load_last_good_prices_from_pool(&db.pool, &targets, 110).await?;
    assert_eq!(before_110[TOKEN].price, BigDecimal::from_str("0.05")?);
    assert_eq!(
        before_110[TOKEN].confidence,
        Some(BigDecimal::from_str("0.95")?)
    );

    let before_111 = load_last_good_prices_from_pool(&db.pool, &targets, 111).await?;
    assert_eq!(before_111[TOKEN].price, BigDecimal::from_str("0.06")?);
    assert!(!before_111.contains_key(&targets[1].token_id));
    Ok(())
}
