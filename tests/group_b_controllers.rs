//! Integration tests for Group B controllers (token, market, mint, burn, pool, lp).
//! Each test section validates one controller method at the SQL level via
//! testcontainers-backed Postgres 17.

mod common;

use anyhow::Result;
use common::{
    call_batch_handle_burns, call_batch_insert_burns_mint, call_batch_insert_mints,
    call_batch_insert_pools, call_batch_insert_tokens_and_markets, call_batch_update_pool_reserves,
    call_handle_burn, call_handle_curve_sync, call_handle_dex_sync, call_handle_lp_allocate,
    call_handle_lp_collect, call_batch_handle_graduates, count_lp_allocate, count_lp_collect,
    count_rows_for_token, count_token_metadata, get_balances, get_is_graduated, get_market_row,
    get_pool_row, get_token_count, get_total_supply, insert_market, insert_token,
    insert_token_metadata, setup_test_db,
};

// Shared test constants
const TOKEN: &str = "0x1111111111111111111111111111111111111111";
const TOKEN2: &str = "0x2222222222222222222222222222222222222222";
const CREATOR: &str = "0x9999999999999999999999999999999999999999";
const ALICE: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const POOL_ID: &str = "0xpppppppppppppppppppppppppppppppppppppppp";
const WMON: &str = "0x760AfE15c6AB78f59cd24C2f5b9aeB8C82d95c5b";

// ============================================================================
// token.rs -- token_metadata CRUD + batch_insert_tokens_and_markets
// ============================================================================

/// Insert metadata, verify it exists, then delete and verify it is gone.
#[tokio::test]
async fn token_metadata_insert_and_delete() -> Result<()> {
    let db = setup_test_db().await?;
    let url = "https://example.com/meta.json";

    insert_token_metadata(&db.pool, url, "TestCoin", "TC").await?;
    assert_eq!(count_token_metadata(&db.pool, url).await?, 1);

    sqlx::query("DELETE FROM token_metadata WHERE metadata_url = $1")
        .bind(url)
        .execute(&db.pool)
        .await?;
    assert_eq!(count_token_metadata(&db.pool, url).await?, 0);
    Ok(())
}

/// Batch delete metadata: insert two rows, batch delete them.
#[tokio::test]
async fn token_metadata_batch_delete() -> Result<()> {
    let db = setup_test_db().await?;
    let url1 = "https://example.com/a.json";
    let url2 = "https://example.com/b.json";

    insert_token_metadata(&db.pool, url1, "A", "A").await?;
    insert_token_metadata(&db.pool, url2, "B", "B").await?;

    sqlx::query("DELETE FROM token_metadata WHERE metadata_url = ANY($1)")
        .bind(&vec![url1.to_string(), url2.to_string()])
        .execute(&db.pool)
        .await?;

    assert_eq!(count_token_metadata(&db.pool, url1).await?, 0);
    assert_eq!(count_token_metadata(&db.pool, url2).await?, 0);
    Ok(())
}

/// batch_insert_tokens_and_markets: inserts token + market + price_history
/// rows via CTE chain. Verifies token_count trigger fires.
#[tokio::test]
async fn token_batch_insert_tokens_and_markets_happy_path() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_tokens_and_markets(
        &db.pool, TOKEN, "MyCoin", "MC", CREATOR, "CURVE",
        "1000", // virtual_native
        "500",  // virtual_token
        100, 1_700_000_000, "0xtx1", 0, 0,
    )
    .await?;

    // Token row exists
    let supply = get_total_supply(&db.pool, TOKEN).await?;
    assert_eq!(supply, "1000000000000000000000000000");

    let (chain,): (String,) =
        sqlx::query_as("SELECT chain FROM token WHERE token_id = $1")
            .bind(TOKEN)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(chain, "MON");

    let (version,): (String,) = sqlx::query_as("SELECT version FROM token WHERE token_id = $1")
        .bind(TOKEN)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(version, "V1");

    // Market row exists with correct price (1000/500 = 2.0000000000)
    let m = get_market_row(&db.pool, TOKEN).await?;
    assert!(m.is_some(), "market row must exist");
    let (mtype, price, _, _, rq, rt, _) = m.unwrap();
    assert_eq!(mtype, "CURVE");
    assert_eq!(price, "2.0000000000");
    assert_eq!(rq, "1000");
    assert_eq!(rt, "500");

    // price_history row exists
    assert_eq!(count_rows_for_token(&db.pool, "price_history", TOKEN).await?, 1);

    // token_count trigger: total_count should be 1
    let (total, _) = get_token_count(&db.pool).await?;
    assert_eq!(total, 1);
    Ok(())
}

/// Duplicate token_id: ON CONFLICT DO NOTHING prevents double insert.
#[tokio::test]
async fn token_batch_insert_duplicate_no_op() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_tokens_and_markets(
        &db.pool, TOKEN, "MyCoin", "MC", CREATOR, "CURVE",
        "1000", "500", 100, 1_700_000_000, "0xtx1", 0, 0,
    )
    .await?;
    // Same token_id, different tx
    call_batch_insert_tokens_and_markets(
        &db.pool, TOKEN, "MyCoin2", "MC2", CREATOR, "CURVE",
        "2000", "1000", 101, 1_700_000_001, "0xtx2", 0, 0,
    )
    .await?;

    // token_count still 1 (second insert was no-op on token)
    let (total, _) = get_token_count(&db.pool).await?;
    assert_eq!(total, 1);
    Ok(())
}

// ============================================================================
// market.rs -- handle_curve_sync, handle_dex_sync, batch_handle_graduates
// ============================================================================

/// handle_curve_sync INSERT path: creates a fresh market row.
#[tokio::test]
async fn market_curve_sync_insert() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_handle_curve_sync(
        &db.pool, TOKEN, "0.5", "800", "400", "1.5", "0.5",
        1_700_000_000, "CURVE",
    )
    .await?;

    let m = get_market_row(&db.pool, TOKEN).await?;
    assert!(m.is_some());
    let (mtype, price, ath, ath_q, rq, rt, _) = m.unwrap();
    assert_eq!(mtype, "CURVE");
    assert_eq!(price, "0.5000000000");
    assert_eq!(ath, "1.5000000000");
    assert_eq!(ath_q, "0.5000000000");
    assert_eq!(rq, "400");
    assert_eq!(rt, "800");
    Ok(())
}

/// handle_curve_sync UPDATE path: existing market gets updated.
#[tokio::test]
async fn market_curve_sync_update() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    // Initial insert
    call_handle_curve_sync(
        &db.pool, TOKEN, "0.5", "800", "400", "1.5", "0.5",
        1_700_000_000, "CURVE",
    )
    .await?;

    // Update with newer timestamp and higher price
    call_handle_curve_sync(
        &db.pool, TOKEN, "0.8", "900", "500", "2.4", "0.8",
        1_700_000_001, "CURVE",
    )
    .await?;

    let m = get_market_row(&db.pool, TOKEN).await?.unwrap();
    assert_eq!(m.1, "0.8000000000", "price should update to newer value");
    assert_eq!(m.2, "2.4000000000", "ath_price should be GREATEST");
    assert_eq!(m.3, "0.8000000000", "ath_price_quote should be GREATEST");
    Ok(())
}

/// handle_curve_sync with older timestamp: price stays, ath still updates.
#[tokio::test]
async fn market_curve_sync_older_timestamp_keeps_price() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_handle_curve_sync(
        &db.pool, TOKEN, "0.8", "900", "500", "2.4", "0.8",
        1_700_000_001, "CURVE",
    )
    .await?;

    // Older timestamp, higher ath_price_usd
    call_handle_curve_sync(
        &db.pool, TOKEN, "0.3", "700", "300", "3.0", "0.3",
        1_700_000_000, "CURVE",
    )
    .await?;

    let m = get_market_row(&db.pool, TOKEN).await?.unwrap();
    assert_eq!(m.1, "0.8000000000", "price stays (newer timestamp wins)");
    assert_eq!(m.2, "3.0000000000", "ath_price takes GREATEST");
    Ok(())
}

/// handle_dex_sync: updates existing market row.
#[tokio::test]
async fn market_dex_sync_update() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;
    insert_market(&db.pool, TOKEN, "DEX").await?;

    call_handle_dex_sync(
        &db.pool, TOKEN, "1.5", "2000", "1000", "4.5", "1.5",
        1_700_000_000,
    )
    .await?;

    let m = get_market_row(&db.pool, TOKEN).await?.unwrap();
    assert_eq!(m.1, "1.5000000000");
    assert_eq!(m.2, "4.5000000000");
    assert_eq!(m.3, "1.5000000000");
    Ok(())
}

/// batch_handle_graduates: sets is_graduated=true on token, updates
/// market.market_type and pool_id.
#[tokio::test]
async fn market_batch_handle_graduates() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;
    insert_market(&db.pool, TOKEN, "CURVE").await?;

    let count = call_batch_handle_graduates(
        &db.pool,
        &[(TOKEN, POOL_ID)],
        "DEX",
    )
    .await?;

    assert_eq!(count, 1);
    assert!(get_is_graduated(&db.pool, TOKEN).await?);
    let m = get_market_row(&db.pool, TOKEN).await?.unwrap();
    assert_eq!(m.0, "DEX");
    assert_eq!(m.6, Some(POOL_ID.to_string()));
    Ok(())
}

// ============================================================================
// mint.rs -- batch_insert_mints, batch_insert_burns
// ============================================================================

/// batch_insert_mints: inserts a row into the `mint` table.
#[tokio::test]
async fn mint_batch_insert_happy_path() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_mints(
        &db.pool, TOKEN, ALICE, TOKEN, "100", "50", "1000", "500",
        1_700_000_000, "0xtx1", 100, 0, 0,
    )
    .await?;

    assert_eq!(count_rows_for_token(&db.pool, "mint", TOKEN).await?, 1);
    Ok(())
}

/// batch_insert_mints: duplicate PK is silently ignored.
#[tokio::test]
async fn mint_batch_insert_duplicate_no_op() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_mints(
        &db.pool, TOKEN, ALICE, TOKEN, "100", "50", "1000", "500",
        1_700_000_000, "0xtx1", 100, 0, 0,
    )
    .await?;
    // Same PK (token_id, transaction_hash, tx_index, log_index)
    call_batch_insert_mints(
        &db.pool, TOKEN, ALICE, TOKEN, "999", "999", "999", "999",
        1_700_000_000, "0xtx1", 100, 0, 0,
    )
    .await?;

    assert_eq!(count_rows_for_token(&db.pool, "mint", TOKEN).await?, 1);
    Ok(())
}

/// batch_insert_burns (mint.rs): inserts a row into the `burn` table.
#[tokio::test]
async fn mint_batch_insert_burns_happy_path() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_burns_mint(
        &db.pool, TOKEN, ALICE, TOKEN, "100", "50", "1000", "500",
        1_700_000_000, "0xtx1", 100, 0, 0,
    )
    .await?;

    assert_eq!(count_rows_for_token(&db.pool, "burn", TOKEN).await?, 1);
    Ok(())
}

// ============================================================================
// burn.rs -- handle_burn, batch_handle_burns (burn_history + total_supply)
// ============================================================================

/// handle_burn: inserts into burn_history and decrements token.total_supply.
#[tokio::test]
async fn burn_handle_burn_happy_path() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_handle_burn(
        &db.pool, ALICE, TOKEN, "100", "0xtx1", 0, 1_700_000_000,
    )
    .await?;

    assert_eq!(
        count_rows_for_token(&db.pool, "burn_history", TOKEN).await?,
        1
    );
    // total_supply was 1000000, burned 100 => 999900
    assert_eq!(get_total_supply(&db.pool, TOKEN).await?, "999900");
    Ok(())
}

/// handle_burn: duplicate ON CONFLICT DO NOTHING, total_supply unchanged.
#[tokio::test]
async fn burn_handle_burn_duplicate_no_op() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_handle_burn(
        &db.pool, ALICE, TOKEN, "100", "0xtx1", 0, 1_700_000_000,
    )
    .await?;
    // Same PK
    call_handle_burn(
        &db.pool, ALICE, TOKEN, "100", "0xtx1", 0, 1_700_000_000,
    )
    .await?;

    assert_eq!(
        count_rows_for_token(&db.pool, "burn_history", TOKEN).await?,
        1
    );
    assert_eq!(get_total_supply(&db.pool, TOKEN).await?, "999900");
    Ok(())
}

/// batch_handle_burns: batch insert + total_supply decrement.
#[tokio::test]
async fn burn_batch_handle_burns_happy_path() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_handle_burns(
        &db.pool, TOKEN, ALICE, "200", "0xtx1", 0, 1_700_000_000,
    )
    .await?;

    assert_eq!(
        count_rows_for_token(&db.pool, "burn_history", TOKEN).await?,
        1
    );
    assert_eq!(get_total_supply(&db.pool, TOKEN).await?, "999800");
    Ok(())
}

// ============================================================================
// pool.rs -- batch_insert_pools, batch_update_pool_reserves
// ============================================================================

/// batch_insert_pools: inserts a pool row.
#[tokio::test]
async fn pool_batch_insert_happy_path() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_pools(
        &db.pool, POOL_ID, TOKEN, TOKEN2, "1000", "500", "2.0",
        1_700_000_000, 100, "0xtx1",
    )
    .await?;

    let p = get_pool_row(&db.pool, POOL_ID).await?;
    assert!(p.is_some());
    let (r0, r1, price, _) = p.unwrap();
    assert_eq!(r0, "1000");
    assert_eq!(r1, "500");
    assert_eq!(price, "2.0");
    Ok(())
}

/// batch_insert_pools: duplicate pool_id is silently ignored.
#[tokio::test]
async fn pool_batch_insert_duplicate_no_op() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_pools(
        &db.pool, POOL_ID, TOKEN, TOKEN2, "1000", "500", "2.0",
        1_700_000_000, 100, "0xtx1",
    )
    .await?;
    call_batch_insert_pools(
        &db.pool, POOL_ID, TOKEN, TOKEN2, "9999", "9999", "9.0",
        1_700_000_001, 101, "0xtx2",
    )
    .await?;

    let p = get_pool_row(&db.pool, POOL_ID).await?.unwrap();
    assert_eq!(p.0, "1000", "reserve0 unchanged by duplicate");
    Ok(())
}

/// batch_update_pool_reserves: updates reserves and price.
#[tokio::test]
async fn pool_batch_update_reserves() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_pools(
        &db.pool, POOL_ID, TOKEN, TOKEN2, "1000", "500", "2.0",
        1_700_000_000, 100, "0xtx1",
    )
    .await?;

    call_batch_update_pool_reserves(
        &db.pool, POOL_ID, "2000", "1000", "3.0", 1_700_000_001,
    )
    .await?;

    let p = get_pool_row(&db.pool, POOL_ID).await?.unwrap();
    assert_eq!(p.0, "2000");
    assert_eq!(p.1, "1000");
    assert_eq!(p.2, "3.0");
    assert_eq!(p.3, 1_700_000_001_i64, "latest_trade_at updated");
    Ok(())
}

/// batch_update_pool_reserves: stale Sync (older timestamp) is rejected so
/// out-of-order replay cannot regress reserves to a previous snapshot.
/// (PR #209 N2 guard.)
#[tokio::test]
async fn pool_batch_update_reserves_stale_sync_rejected() -> Result<()> {
    let db = setup_test_db().await?;

    call_batch_insert_pools(
        &db.pool, POOL_ID, TOKEN, TOKEN2, "1000", "500", "2.0",
        1_700_000_000, 100, "0xtx1",
    )
    .await?;

    // Newer Sync at t+5: should take effect.
    call_batch_update_pool_reserves(
        &db.pool, POOL_ID, "2000", "1000", "3.0", 1_700_000_005,
    )
    .await?;

    // Out-of-order stale Sync at t+1: must NOT regress reserves/price.
    call_batch_update_pool_reserves(
        &db.pool, POOL_ID, "999", "999", "9.0", 1_700_000_001,
    )
    .await?;

    let p = get_pool_row(&db.pool, POOL_ID).await?.unwrap();
    assert_eq!(p.0, "2000", "reserve0 must stay at newer value");
    assert_eq!(p.1, "1000", "reserve1 must stay at newer value");
    assert_eq!(p.2, "3.0", "price must stay at newer value");
    assert_eq!(p.3, 1_700_000_005_i64, "latest_trade_at must stay at newest");
    Ok(())
}

/// Same-batch same-timestamp ordering: two RawSyncs for the same pool land in
/// one batch sharing block_timestamp but differing in log_index. The SQL must
/// pick the row with the higher (block_number, tx_index, log_index) tuple,
/// deterministically — not whichever row Postgres happens to scan first.
/// Complements the stale-Sync test above by covering the same-block case
/// that block_timestamp alone cannot disambiguate.
#[tokio::test]
async fn pool_batch_update_reserves_freshness_breaks_timestamp_tie() -> Result<()> {
    use bigdecimal::BigDecimal;
    use observer::db::postgres::controller::pool::BATCH_UPDATE_POOL_RESERVES_SQL;
    use std::str::FromStr;

    let db = setup_test_db().await?;
    call_batch_insert_pools(
        &db.pool, POOL_ID, TOKEN, TOKEN2, "1000", "500", "2.0",
        1_700_000_000, 100, "0xtx1",
    )
    .await?;

    // Both rows share block_timestamp 1_700_000_500 and block_number 200, but
    // log_index 1 is the chronologically newer sync. With the old DISTINCT ON
    // ORDER BY block_timestamp DESC, Postgres could return either row; with
    // the new ORDER BY (block_number, tx_index, log_index) DESC the row at
    // log_index 1 wins every time.
    let parse = |s: &str| BigDecimal::from_str(s).unwrap();
    let pool_ids = vec![POOL_ID, POOL_ID];
    let r0s = vec![parse("8888"), parse("9999")]; // 8888 at log 0, 9999 at log 1 (winner)
    let r1s = vec![parse("4444"), parse("5555")];
    let prices = vec![parse("1.0"), parse("2.0")];
    let values: Vec<Option<BigDecimal>> = vec![None, None];
    let block_timestamps = vec![1_700_000_500_i64, 1_700_000_500_i64]; // identical
    let block_numbers = vec![200_i64, 200_i64]; // identical
    let tx_indexes = vec![0_i32, 0_i32]; // identical
    let log_indexes = vec![0_i32, 1_i32]; // tie broken here
    sqlx::query(BATCH_UPDATE_POOL_RESERVES_SQL)
        .bind(&pool_ids)
        .bind(&r0s)
        .bind(&r1s)
        .bind(&prices)
        .bind(&values)
        .bind(&block_timestamps)
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&log_indexes)
        .execute(&db.pool)
        .await?;

    let p = get_pool_row(&db.pool, POOL_ID).await?.unwrap();
    assert_eq!(p.0, "9999", "reserve0 must come from the highest log_index row");
    assert_eq!(p.1, "5555", "reserve1 must come from the same row as reserve0");
    assert_eq!(p.2, "2.0", "price must come from the same row");
    Ok(())
}

// ============================================================================
// lp.rs -- handle_lp_allocate, handle_lp_collect, batch wrappers
// ============================================================================

/// handle_lp_allocate: inserts into lp_allocate_history.
#[tokio::test]
async fn lp_allocate_happy_path() -> Result<()> {
    let db = setup_test_db().await?;

    call_handle_lp_allocate(
        &db.pool, TOKEN, "500", "250", "0xtx1", 1_700_000_000,
    )
    .await?;

    assert_eq!(count_lp_allocate(&db.pool, TOKEN).await?, 1);
    Ok(())
}

/// handle_lp_allocate: duplicate PK (token_id, transaction_hash) is ignored.
#[tokio::test]
async fn lp_allocate_duplicate_no_op() -> Result<()> {
    let db = setup_test_db().await?;

    call_handle_lp_allocate(
        &db.pool, TOKEN, "500", "250", "0xtx1", 1_700_000_000,
    )
    .await?;
    call_handle_lp_allocate(
        &db.pool, TOKEN, "999", "999", "0xtx1", 1_700_000_001,
    )
    .await?;

    assert_eq!(count_lp_allocate(&db.pool, TOKEN).await?, 1);
    Ok(())
}

/// handle_lp_collect: inserts into lp_collect_history.
#[tokio::test]
async fn lp_collect_happy_path() -> Result<()> {
    let db = setup_test_db().await?;

    call_handle_lp_collect(
        &db.pool, TOKEN, "500", "250", "100", "50", "25",
        "0xtx1", 0, 0, 1_700_000_000,
    )
    .await?;

    assert_eq!(count_lp_collect(&db.pool, TOKEN).await?, 1);
    Ok(())
}

/// handle_lp_collect: duplicate PK is ignored.
#[tokio::test]
async fn lp_collect_duplicate_no_op() -> Result<()> {
    let db = setup_test_db().await?;

    call_handle_lp_collect(
        &db.pool, TOKEN, "500", "250", "100", "50", "25",
        "0xtx1", 0, 0, 1_700_000_000,
    )
    .await?;
    call_handle_lp_collect(
        &db.pool, TOKEN, "999", "999", "999", "999", "999",
        "0xtx1", 0, 0, 1_700_000_001,
    )
    .await?;

    assert_eq!(count_lp_collect(&db.pool, TOKEN).await?, 1);
    Ok(())
}

/// handle_lp_collect fires trigger `update_creator_treasury_balance_from_collect`:
/// credits c_amount to the token's creator.
#[tokio::test]
async fn lp_collect_triggers_creator_treasury_balance() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_handle_lp_collect(
        &db.pool, TOKEN, "0", "0", "1000", "0", "0",
        "0xtx1", 0, 0, 1_700_000_000,
    )
    .await?;

    let balances = get_balances(&db.pool, TOKEN).await?;
    assert_eq!(
        balances,
        vec![(CREATOR.to_string(), "1000".to_string())],
        "creator_treasury_balance should be credited with c_amount"
    );
    Ok(())
}

/// Two collects accumulate the creator treasury balance.
#[tokio::test]
async fn lp_collect_accumulates_creator_treasury() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_handle_lp_collect(
        &db.pool, TOKEN, "0", "0", "1000", "0", "0",
        "0xtx1", 0, 0, 1_700_000_000,
    )
    .await?;
    call_handle_lp_collect(
        &db.pool, TOKEN, "0", "0", "500", "0", "0",
        "0xtx2", 0, 0, 1_700_000_001,
    )
    .await?;

    let balances = get_balances(&db.pool, TOKEN).await?;
    assert_eq!(
        balances,
        vec![(CREATOR.to_string(), "1500".to_string())],
        "two collects should sum"
    );
    Ok(())
}
