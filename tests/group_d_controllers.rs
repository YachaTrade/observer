//! Group D (Auxiliary) controller integration tests.
//!
//! Covers: chart.rs, price.rs, account.rs
//! Each test spins up an ephemeral Postgres container, applies baseline
//! migrations, and exercises the production SQL at the statement level.

mod common;

use std::str::FromStr;

use anyhow::Result;
use bigdecimal::BigDecimal;

// ============================================================================
// chart.rs tests — price_history INSERT + batch INSERT
// ============================================================================

/// Insert a single price_history row and verify it lands.
#[tokio::test]
async fn chart_insert_price_history_happy_path() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    // price_history needs a token row (trigger queries `token.total_supply`)
    common::insert_token(pool, "0xToken01", "0xCreator01").await?;

    let price = BigDecimal::from_str("0.0012345678")?;
    let volume = BigDecimal::from_str("500")?;

    sqlx::query(observer::db::postgres::controller::chart::INSERT_PRICE_HISTORY_SQL)
        .bind("0xToken01")    // $1 token_id
        .bind(&price)         // $2 price
        .bind(&volume)        // $3 volume
        .bind(1000i64)        // $4 created_at
        .bind(100i64)         // $5 block_number
        .bind("0xTxHash01")   // $6 transaction_hash
        .bind(0i32)           // $7 log_index — i32, not i64
        .bind(0i32)           // $8 tx_index — i32, not i64
        .execute(pool)
        .await?;

    let count = common::count_rows_for_token(pool, "price_history", "0xToken01").await?;
    assert_eq!(count, 1);
    Ok(())
}

/// Duplicate price_history insert is silently ignored (ON CONFLICT DO NOTHING).
#[tokio::test]
async fn chart_insert_price_history_duplicate_ignored() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;
    common::insert_token(pool, "0xToken02", "0xCreator02").await?;

    let price = BigDecimal::from_str("0.0050000000")?;
    let volume = BigDecimal::from_str("100")?;

    for _ in 0..2 {
        sqlx::query(observer::db::postgres::controller::chart::INSERT_PRICE_HISTORY_SQL)
            .bind("0xToken02")
            .bind(&price)
            .bind(&volume)
            .bind(2000i64)
            .bind(200i64)
            .bind("0xTxHash02")
            .bind(0i32)
            .bind(0i32)
            .execute(pool)
            .await?;
    }

    let count = common::count_rows_for_token(pool, "price_history", "0xToken02").await?;
    assert_eq!(count, 1, "duplicate should be ignored");
    Ok(())
}

/// Batch insert multiple price_history rows via UNNEST.
#[tokio::test]
async fn chart_batch_insert_price_history_happy_path() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;
    common::insert_token(pool, "0xToken03", "0xCreator03").await?;

    let token_ids = vec!["0xToken03", "0xToken03", "0xToken03"];
    let prices = vec![
        BigDecimal::from_str("0.0010000000")?,
        BigDecimal::from_str("0.0020000000")?,
        BigDecimal::from_str("0.0030000000")?,
    ];
    let volumes = vec![
        BigDecimal::from_str("10")?,
        BigDecimal::from_str("20")?,
        BigDecimal::from_str("30")?,
    ];
    let created_ats: Vec<i64> = vec![3000, 3001, 3002];
    let block_numbers: Vec<i64> = vec![300, 301, 302];
    let tx_hashes = vec!["0xBatchTx1", "0xBatchTx2", "0xBatchTx3"];
    let log_indexes: Vec<i32> = vec![0, 0, 0];
    let tx_indexes: Vec<i32> = vec![0, 0, 0];

    sqlx::query(observer::db::postgres::controller::chart::BATCH_INSERT_PRICE_HISTORY_SQL)
        .bind(&token_ids)
        .bind(&prices)
        .bind(&volumes)
        .bind(&created_ats)
        .bind(&block_numbers)
        .bind(&tx_hashes)
        .bind(&log_indexes)
        .bind(&tx_indexes)
        .execute(pool)
        .await?;

    let count = common::count_rows_for_token(pool, "price_history", "0xToken03").await?;
    assert_eq!(count, 3);
    Ok(())
}

/// Batch insert with duplicates: only new rows are inserted.
#[tokio::test]
async fn chart_batch_insert_price_history_partial_duplicate() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;
    common::insert_token(pool, "0xToken04", "0xCreator04").await?;

    // First: insert one row
    sqlx::query(observer::db::postgres::controller::chart::INSERT_PRICE_HISTORY_SQL)
        .bind("0xToken04")
        .bind(&BigDecimal::from_str("0.0010000000")?)
        .bind(&BigDecimal::from_str("10")?)
        .bind(4000i64)
        .bind(400i64)
        .bind("0xDupTx1")
        .bind(0i32)
        .bind(0i32)
        .execute(pool)
        .await?;

    // Batch includes the duplicate + a new one
    let token_ids = vec!["0xToken04", "0xToken04"];
    let prices = vec![
        BigDecimal::from_str("0.0010000000")?,
        BigDecimal::from_str("0.0020000000")?,
    ];
    let volumes = vec![
        BigDecimal::from_str("10")?,
        BigDecimal::from_str("20")?,
    ];
    let created_ats: Vec<i64> = vec![4000, 4001];
    let block_numbers: Vec<i64> = vec![400, 401];
    let tx_hashes = vec!["0xDupTx1", "0xDupTx2"];
    let log_indexes: Vec<i32> = vec![0, 0];
    let tx_indexes: Vec<i32> = vec![0, 0];

    sqlx::query(observer::db::postgres::controller::chart::BATCH_INSERT_PRICE_HISTORY_SQL)
        .bind(&token_ids)
        .bind(&prices)
        .bind(&volumes)
        .bind(&created_ats)
        .bind(&block_numbers)
        .bind(&tx_hashes)
        .bind(&log_indexes)
        .bind(&tx_indexes)
        .execute(pool)
        .await?;

    let count = common::count_rows_for_token(pool, "price_history", "0xToken04").await?;
    assert_eq!(count, 2, "only the new row should be added");
    Ok(())
}

// ============================================================================
// price.rs tests — price INSERT + batch INSERT
// ============================================================================

/// Insert a single price row.
#[tokio::test]
async fn price_insert_happy_path() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    let price = BigDecimal::from_str("1234.56789")?;

    sqlx::query(observer::db::postgres::controller::price::INSERT_PRICE_SQL)
        .bind("0xQuoteAAA")   // $1 quote_id
        .bind(500i64)         // $2 block_number
        .bind(&price)         // $3 price
        .bind(5000i64)        // $4 created_at
        .execute(pool)
        .await?;

    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM price WHERE quote_id = $1 AND block_number = $2",
    )
    .bind("0xQuoteAAA")
    .bind(500i64)
    .fetch_one(pool)
    .await?;
    assert_eq!(row.0, 1);
    Ok(())
}

/// Duplicate (quote_id, block_number) is silently ignored.
#[tokio::test]
async fn price_insert_duplicate_ignored() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    let price = BigDecimal::from_str("100.0")?;
    for _ in 0..2 {
        sqlx::query(observer::db::postgres::controller::price::INSERT_PRICE_SQL)
            .bind("0xQuoteBBB")
            .bind(600i64)
            .bind(&price)
            .bind(6000i64)
            .execute(pool)
            .await?;
    }

    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM price WHERE quote_id = $1",
    )
    .bind("0xQuoteBBB")
    .fetch_one(pool)
    .await?;
    assert_eq!(row.0, 1);
    Ok(())
}

/// Batch insert multiple prices via UNNEST.
#[tokio::test]
async fn price_batch_insert_happy_path() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    let block_numbers: Vec<i64> = vec![700, 701, 702];
    let prices = vec![
        BigDecimal::from_str("10.0")?,
        BigDecimal::from_str("20.0")?,
        BigDecimal::from_str("30.0")?,
    ];
    let timestamps: Vec<i64> = vec![7000, 7001, 7002];

    sqlx::query(observer::db::postgres::controller::price::BATCH_INSERT_PRICES_SQL)
        .bind("0xQuoteCCC")   // $1 quote_id (scalar)
        .bind(&block_numbers) // $2
        .bind(&prices)        // $3
        .bind(&timestamps)    // $4
        .execute(pool)
        .await?;

    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM price WHERE quote_id = $1",
    )
    .bind("0xQuoteCCC")
    .fetch_one(pool)
    .await?;
    assert_eq!(row.0, 3);
    Ok(())
}

/// Batch insert with partial duplicate — only new rows land.
#[tokio::test]
async fn price_batch_insert_partial_duplicate() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    // Seed one row
    sqlx::query(observer::db::postgres::controller::price::INSERT_PRICE_SQL)
        .bind("0xQuoteDDD")
        .bind(800i64)
        .bind(&BigDecimal::from_str("50.0")?)
        .bind(8000i64)
        .execute(pool)
        .await?;

    // Batch includes block 800 (dup) + block 801 (new)
    let block_numbers: Vec<i64> = vec![800, 801];
    let prices = vec![
        BigDecimal::from_str("50.0")?,
        BigDecimal::from_str("60.0")?,
    ];
    let timestamps: Vec<i64> = vec![8000, 8001];

    sqlx::query(observer::db::postgres::controller::price::BATCH_INSERT_PRICES_SQL)
        .bind("0xQuoteDDD")
        .bind(&block_numbers)
        .bind(&prices)
        .bind(&timestamps)
        .execute(pool)
        .await?;

    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM price WHERE quote_id = $1",
    )
    .bind("0xQuoteDDD")
    .fetch_one(pool)
    .await?;
    assert_eq!(row.0, 2);
    Ok(())
}

// ============================================================================
// account.rs tests — upsert + batch upsert
// ============================================================================

/// Single account upsert via extracted SQL.
#[tokio::test]
async fn account_upsert_happy_path() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    sqlx::query(observer::db::postgres::controller::account::UPSERT_ACCOUNT_SQL)
        .bind("0xAccount01")           // $1 account_id
        .bind("0xAccount01")           // $2 nickname
        .bind("https://img/default.png") // $3 image_uri
        .execute(pool)
        .await?;

    let row: (String, String) = sqlx::query_as(
        "SELECT nickname, image_uri FROM account WHERE account_id = $1",
    )
    .bind("0xAccount01")
    .fetch_one(pool)
    .await?;
    assert_eq!(row.0, "0xAccount01");
    assert_eq!(row.1, "https://img/default.png");
    Ok(())
}

/// Duplicate account upsert is ignored (ON CONFLICT DO NOTHING).
#[tokio::test]
async fn account_upsert_duplicate_ignored() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    sqlx::query(observer::db::postgres::controller::account::UPSERT_ACCOUNT_SQL)
        .bind("0xAccount02")
        .bind("0xAccount02")
        .bind("https://img/1.png")
        .execute(pool)
        .await?;

    // Second insert with different image — should be ignored
    sqlx::query(observer::db::postgres::controller::account::UPSERT_ACCOUNT_SQL)
        .bind("0xAccount02")
        .bind("0xAccount02")
        .bind("https://img/2.png")
        .execute(pool)
        .await?;

    let row: (String,) = sqlx::query_as(
        "SELECT image_uri FROM account WHERE account_id = $1",
    )
    .bind("0xAccount02")
    .fetch_one(pool)
    .await?;
    assert_eq!(row.0, "https://img/1.png", "first insert should win");
    Ok(())
}

/// Batch upsert multiple accounts via UNNEST.
#[tokio::test]
async fn account_batch_upsert_happy_path() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    let account_ids = vec!["0xBatch01", "0xBatch02", "0xBatch03"];
    let nicknames = vec!["0xBatch01", "0xBatch02", "0xBatch03"];
    let bios: Vec<&str> = vec!["", "", ""];
    let image_uris = vec!["img1", "img2", "img3"];
    let follower_counts: Vec<i32> = vec![0, 0, 0];
    let following_counts: Vec<i32> = vec![0, 0, 0];

    sqlx::query(observer::db::postgres::controller::account::BATCH_UPSERT_ACCOUNTS_SQL)
        .bind(&account_ids)
        .bind(&nicknames)
        .bind(&bios)
        .bind(&image_uris)
        .bind(&follower_counts)
        .bind(&following_counts)
        .execute(pool)
        .await?;

    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM account")
        .fetch_one(pool)
        .await?;
    assert_eq!(row.0, 3);
    Ok(())
}

/// Batch upsert with existing accounts — new ones added, existing untouched.
#[tokio::test]
async fn account_batch_upsert_partial_existing() -> Result<()> {
    let db = common::setup_test_db().await?;
    let pool = &db.pool;

    // Pre-insert one account
    sqlx::query(observer::db::postgres::controller::account::UPSERT_ACCOUNT_SQL)
        .bind("0xExist01")
        .bind("0xExist01")
        .bind("original_img")
        .execute(pool)
        .await?;

    // Batch includes existing + new
    let account_ids = vec!["0xExist01", "0xNew01"];
    let nicknames = vec!["0xExist01", "0xNew01"];
    let bios: Vec<&str> = vec!["", ""];
    let image_uris = vec!["changed_img", "new_img"];
    let follower_counts: Vec<i32> = vec![0, 0];
    let following_counts: Vec<i32> = vec![0, 0];

    sqlx::query(observer::db::postgres::controller::account::BATCH_UPSERT_ACCOUNTS_SQL)
        .bind(&account_ids)
        .bind(&nicknames)
        .bind(&bios)
        .bind(&image_uris)
        .bind(&follower_counts)
        .bind(&following_counts)
        .execute(pool)
        .await?;

    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM account")
        .fetch_one(pool)
        .await?;
    assert_eq!(row.0, 2, "should have 2 total accounts");

    // Existing account should retain original image
    let img: (String,) = sqlx::query_as(
        "SELECT image_uri FROM account WHERE account_id = $1",
    )
    .bind("0xExist01")
    .fetch_one(pool)
    .await?;
    assert_eq!(img.0, "original_img", "existing account not overwritten");
    Ok(())
}
