//! Integration tests for Group A controllers (balance, position, swap).
//! Each test section validates one controller method plus the DB triggers
//! it cascades into.
//!
//! Note: `transfer.rs` is not part of the active controller surface: it is
//! not exported from
//! `src/db/postgres/controller/mod.rs`, the `TokenTransfer` field it
//! references (`tx_sender`) does not exist on the current struct, and the
//! baseline migrations define no `transfer` table. Wiring tests against
//! it would require fixing dead code that production never runs. It is
//! therefore excluded from this test suite.

mod common;

use anyhow::Result;
use common::{
    call_batch_insert_position_history, call_batch_insert_swaps, call_batch_set_balances,
    call_get_fallback_price, call_get_prices_for_range, count_balance_history,
    count_position_history, get_balance, get_market_volume, get_position_token_flow,
    get_swap_count, get_token_holder_count, insert_market, insert_price, insert_token,
    setup_test_db,
};

// Shared test constants
const TOKEN: &str = "0x1111111111111111111111111111111111111111";
const CREATOR: &str = "0x9999999999999999999999999999999999999999";
const ALICE: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CAROL: &str = "0xcccccccccccccccccccccccccccccccccccccccc";

// ============================================================================
// balance.rs — batch_set_balances (BATCH_SET_BALANCES_SQL)
// ============================================================================
//
// Trigger chain verified in migrations/0005_balance.sql:
//   balance_history INSERT
//     -> trigger_update_balance_from_history (AFTER INSERT)
//        -> INSERT .. ON CONFLICT DO UPDATE SET balance = EXCLUDED.balance
//           on `balance` (always overwrites with the latest-inserted value;
//           most-recent *insert* wins, not highest block number)
//     -> trigger_delete_zero_balance (AFTER UPDATE on `balance`)
//        -> DELETEs the row when the UPDATE lands balance = 0
//     -> trg_update_holder_count (AFTER INSERT/UPDATE/DELETE on `balance`)
//        -> token.token_holder_count +=/-=/stays based on zero-crossing

/// Happy path: insert a single balance row. Asserts the full trigger
/// chain: balance_history row lands, balance table gets the value, and
/// token_holder_count bumps to 1.
#[tokio::test]
async fn balance_happy_path_single() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "1000",
        100,
        "0xtx1",
        0,
        0,
        1_700_000_000,
    )
    .await?;

    assert_eq!(count_balance_history(&db.pool, TOKEN, ALICE).await?, 1);
    assert_eq!(
        get_balance(&db.pool, TOKEN, ALICE).await?,
        Some("1000".to_string())
    );
    assert_eq!(get_token_holder_count(&db.pool, TOKEN).await?, 1);
    Ok(())
}

/// Two distinct accounts each get a positive balance. Holder count is 2
/// and both balance rows are present.
#[tokio::test]
async fn balance_multi_account_holder_count() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "1000",
        100,
        "0xtxA",
        0,
        0,
        1_700_000_000,
    )
    .await?;
    call_batch_set_balances(
        &db.pool,
        TOKEN,
        BOB,
        "500",
        101,
        "0xtxB",
        0,
        0,
        1_700_000_001,
    )
    .await?;

    assert_eq!(
        get_balance(&db.pool, TOKEN, ALICE).await?,
        Some("1000".to_string())
    );
    assert_eq!(
        get_balance(&db.pool, TOKEN, BOB).await?,
        Some("500".to_string())
    );
    assert_eq!(get_token_holder_count(&db.pool, TOKEN).await?, 2);
    Ok(())
}

/// Set balance to 1000 then to 0. `trigger_delete_zero_balance` fires on
/// the UPDATE (the second INSERT .. ON CONFLICT DO UPDATE triggers it) and
/// deletes the balance row. `trg_update_holder_count` sees the DELETE and
/// decrements token_holder_count.
#[tokio::test]
async fn balance_zero_deletes_row() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "1000",
        100,
        "0xtx1",
        0,
        0,
        1_700_000_000,
    )
    .await?;
    assert_eq!(get_token_holder_count(&db.pool, TOKEN).await?, 1);

    // Second insert with balance=0 — trigger UPDATE on balance sets it to 0,
    // trigger_delete_zero_balance then deletes the row.
    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "0",
        101,
        "0xtx2",
        0,
        0,
        1_700_000_001,
    )
    .await?;

    assert_eq!(get_balance(&db.pool, TOKEN, ALICE).await?, None);
    assert_eq!(get_token_holder_count(&db.pool, TOKEN).await?, 0);
    // Both history rows persist regardless of balance row deletion.
    assert_eq!(count_balance_history(&db.pool, TOKEN, ALICE).await?, 2);
    Ok(())
}

/// Same composite PK inserted twice — `ON CONFLICT DO NOTHING` blocks the
/// second insert. Only one balance_history row, trigger fires only once.
#[tokio::test]
async fn balance_duplicate_event_no_op() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "1000",
        100,
        "0xtx1",
        0,
        0,
        1_700_000_000,
    )
    .await?;
    // Identical composite PK — must be a no-op at balance_history level.
    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "2000",
        100,
        "0xtx1",
        0,
        0,
        1_700_000_000,
    )
    .await?;

    assert_eq!(count_balance_history(&db.pool, TOKEN, ALICE).await?, 1);
    assert_eq!(
        get_balance(&db.pool, TOKEN, ALICE).await?,
        Some("1000".to_string()),
        "duplicate insert must NOT overwrite the first balance"
    );
    Ok(())
}

/// Insert balance=1000 at block 100 (tx1), then balance=500 at block 101
/// (tx2). Because the trigger overwrites balance unconditionally with
/// EXCLUDED.balance on every insert, the latest-inserted value wins — here
/// that is 500.
#[tokio::test]
async fn balance_latest_insert_overwrites() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "1000",
        100,
        "0xtx1",
        0,
        0,
        1_700_000_000,
    )
    .await?;
    call_batch_set_balances(
        &db.pool,
        TOKEN,
        ALICE,
        "500",
        101,
        "0xtx2",
        0,
        0,
        1_700_000_001,
    )
    .await?;

    assert_eq!(
        get_balance(&db.pool, TOKEN, ALICE).await?,
        Some("500".to_string())
    );
    // Both events are in history.
    assert_eq!(count_balance_history(&db.pool, TOKEN, ALICE).await?, 2);
    // Still one holder (Alice).
    assert_eq!(get_token_holder_count(&db.pool, TOKEN).await?, 1);
    Ok(())
}

// ============================================================================
// position.rs — batch_insert_position_history (BATCH_INSERT_POSITION_HISTORY_SQL)
// ============================================================================
//
// Trigger chain verified in migrations/0013_position.sql:
//   position_history INSERT (BEFORE INSERT)
//     -> trg_position_on_history
//        -> may mutate NEW.quote_in/out for transfer_in/out
//        -> INSERT .. ON CONFLICT DO UPDATE adds EXCLUDED.* to `position`
//   position_history INSERT (AFTER INSERT, unrelated)
//     -> (no chained fee trigger; fee_history is its own table)

/// Happy path: insert one buy event, assert RETURNING reports 1 and the
/// row is in position_history.
#[tokio::test]
async fn position_happy_path_insert_and_returning() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    let inserted = call_batch_insert_position_history(
        &db.pool,
        ALICE,
        TOKEN,
        "0",   // quote_in
        "100", // quote_out
        "0",   // usd_in
        "300", // usd_out
        "50",  // token_in
        "0",   // token_out
        "0xtx1",
        100,
        0,
        0,
        1_700_000_000,
        "buy",
        Some(ALICE),
    )
    .await?;

    assert_eq!(inserted, 1, "RETURNING must surface exactly one row");
    assert_eq!(
        count_position_history(&db.pool, ALICE, TOKEN, "0xtx1", 0, 0).await?,
        1
    );
    Ok(())
}

/// Same composite PK inserted twice. The first call RETURNs one row, the
/// second call RETURNs zero (ON CONFLICT DO NOTHING swallows it).
#[tokio::test]
async fn position_duplicate_returns_zero() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    let first = call_batch_insert_position_history(
        &db.pool,
        ALICE,
        TOKEN,
        "0",
        "100",
        "0",
        "300",
        "50",
        "0",
        "0xtx1",
        100,
        0,
        0,
        1_700_000_000,
        "buy",
        Some(ALICE),
    )
    .await?;
    assert_eq!(first, 1);

    let second = call_batch_insert_position_history(
        &db.pool,
        ALICE,
        TOKEN,
        "0",
        "100",
        "0",
        "300",
        "50",
        "0",
        "0xtx1",
        100,
        0,
        0,
        1_700_000_000,
        "buy",
        Some(ALICE),
    )
    .await?;
    assert_eq!(second, 0, "duplicate insert must RETURN zero rows");

    assert_eq!(
        count_position_history(&db.pool, ALICE, TOKEN, "0xtx1", 0, 0).await?,
        1
    );
    Ok(())
}

/// Two distinct accounts buying the same token in the same block. Both
/// rows land because the composite PK includes account_id.
#[tokio::test]
async fn position_multiple_accounts() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_insert_position_history(
        &db.pool,
        ALICE,
        TOKEN,
        "0",
        "100",
        "0",
        "300",
        "50",
        "0",
        "0xtxA",
        100,
        0,
        0,
        1_700_000_000,
        "buy",
        Some(ALICE),
    )
    .await?;
    call_batch_insert_position_history(
        &db.pool,
        BOB,
        TOKEN,
        "0",
        "200",
        "0",
        "600",
        "80",
        "0",
        "0xtxB",
        100,
        0,
        0,
        1_700_000_000,
        "buy",
        Some(BOB),
    )
    .await?;

    assert_eq!(
        count_position_history(&db.pool, ALICE, TOKEN, "0xtxA", 0, 0).await?,
        1
    );
    assert_eq!(
        count_position_history(&db.pool, BOB, TOKEN, "0xtxB", 0, 0).await?,
        1
    );
    Ok(())
}

/// Insert a buy event and verify `trg_position_on_history` aggregated the
/// flows into the `position` table (token_in accumulates the bought
/// amount, token_out stays zero).
#[tokio::test]
async fn position_trigger_aggregates_into_position_table() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;

    call_batch_insert_position_history(
        &db.pool,
        ALICE,
        TOKEN,
        "0",   // quote_in
        "100", // quote_out
        "0",   // usd_in
        "300", // usd_out
        "50",  // token_in
        "0",   // token_out
        "0xtx1",
        100,
        0,
        0,
        1_700_000_000,
        "buy",
        Some(ALICE),
    )
    .await?;

    // Second buy, same account — should accumulate.
    call_batch_insert_position_history(
        &db.pool,
        ALICE,
        TOKEN,
        "0",
        "50",
        "0",
        "150",
        "25",
        "0",
        "0xtx2",
        101,
        0,
        0,
        1_700_000_001,
        "buy",
        Some(ALICE),
    )
    .await?;

    let flow = get_position_token_flow(&db.pool, ALICE, TOKEN).await?;
    assert_eq!(
        flow,
        Some(("75".to_string(), "0".to_string())),
        "position.token_in must sum both events"
    );
    Ok(())
}

// ============================================================================
// swap.rs — batch_insert_swaps (BATCH_INSERT_SWAPS_SQL)
// ============================================================================
//
// Trigger chain verified in migrations/0004_swap.sql:
//   swap INSERT
//     -> trg_update_market_volume : market.volume += NEW.quote_amount
//     -> swap_count_trigger        : swap_count.count/buy_count/sell_count++
//     -> trg_update_account_swap_count : account_swap_count.total_count++

/// Happy path: insert one buy swap, assert swap_count = 1 and market
/// volume was incremented by the quote_amount.
#[tokio::test]
async fn swap_batch_insert_happy_path() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;
    insert_market(&db.pool, TOKEN, "CURVE").await?;

    call_batch_insert_swaps(
        &db.pool,
        ALICE,
        TOKEN,
        true,          // is_buy
        "100",         // quote_amount
        "50",          // token_amount
        "1000",        // reserve_quote
        "500",         // reserve_token
        "300",         // value
        "CURVE",       // market_type
        1_700_000_000, // created_at
        "0xtx1",
        100,
        0,
        0,
    )
    .await?;

    assert_eq!(get_swap_count(&db.pool, TOKEN).await?, Some(1));
    // market.volume starts at 0 (from insert_market), plus the trigger
    // adds NEW.quote_amount = 100.
    assert_eq!(
        get_market_volume(&db.pool, TOKEN).await?,
        Some("100".to_string())
    );
    Ok(())
}

/// Three distinct swaps (2 buys + 1 sell) all land and swap_count
/// reflects the total.
#[tokio::test]
async fn swap_batch_insert_multiple() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;
    insert_market(&db.pool, TOKEN, "CURVE").await?;

    call_batch_insert_swaps(
        &db.pool,
        ALICE,
        TOKEN,
        true,
        "100",
        "50",
        "1000",
        "500",
        "300",
        "CURVE",
        1_700_000_000,
        "0xtx1",
        100,
        0,
        0,
    )
    .await?;
    call_batch_insert_swaps(
        &db.pool,
        BOB,
        TOKEN,
        true,
        "200",
        "80",
        "1200",
        "420",
        "600",
        "CURVE",
        1_700_000_001,
        "0xtx2",
        101,
        0,
        0,
    )
    .await?;
    call_batch_insert_swaps(
        &db.pool,
        CAROL,
        TOKEN,
        false,
        "50",
        "20",
        "1150",
        "440",
        "150",
        "CURVE",
        1_700_000_002,
        "0xtx3",
        102,
        0,
        0,
    )
    .await?;

    assert_eq!(get_swap_count(&db.pool, TOKEN).await?, Some(3));
    // 100 + 200 + 50 = 350
    assert_eq!(
        get_market_volume(&db.pool, TOKEN).await?,
        Some("350".to_string())
    );
    Ok(())
}

/// Same composite PK inserted twice — `ON CONFLICT DO NOTHING` blocks the
/// second insert. swap_count stays at 1, volume only bumps once.
#[tokio::test]
async fn swap_batch_insert_duplicate_no_op() -> Result<()> {
    let db = setup_test_db().await?;
    insert_token(&db.pool, TOKEN, CREATOR).await?;
    insert_market(&db.pool, TOKEN, "CURVE").await?;

    call_batch_insert_swaps(
        &db.pool,
        ALICE,
        TOKEN,
        true,
        "100",
        "50",
        "1000",
        "500",
        "300",
        "CURVE",
        1_700_000_000,
        "0xtx1",
        100,
        0,
        0,
    )
    .await?;
    // Same PK — ignored.
    call_batch_insert_swaps(
        &db.pool,
        ALICE,
        TOKEN,
        true,
        "999",
        "999",
        "999",
        "999",
        "999",
        "CURVE",
        1_700_000_000,
        "0xtx1",
        100,
        0,
        0,
    )
    .await?;

    assert_eq!(get_swap_count(&db.pool, TOKEN).await?, Some(1));
    assert_eq!(
        get_market_volume(&db.pool, TOKEN).await?,
        Some("100".to_string())
    );
    Ok(())
}

// ----------------------------------------------------------------------
// swap.rs — GET_PRICES_FOR_RANGE_SQL / GET_FALLBACK_PRICE_SQL
// ----------------------------------------------------------------------
//
// We test the SQL consts directly rather than instantiating
// `SwapController` because the Rust wrapper around these queries has a
// 50-attempt retry loop with 100ms sleeps (5 seconds per empty-price
// test) and depends on a global `CacheManager::instance()`. The Rust
// HashMap-building logic is mechanical; the interesting correctness is
// in what the SQL returns.

/// Seed the `price` table with three rows at blocks 100, 105, 110, then
/// query the range [100, 112]. Expect all three rows returned in
/// ascending block order.
#[tokio::test]
async fn swap_get_prices_for_range_sql_returns_in_range() -> Result<()> {
    let db = setup_test_db().await?;

    insert_price(&db.pool, 100, "1.5").await?;
    insert_price(&db.pool, 105, "2.0").await?;
    insert_price(&db.pool, 110, "2.5").await?;
    // A price outside the range — must NOT appear.
    insert_price(&db.pool, 50, "0.5").await?;

    let rows = call_get_prices_for_range(&db.pool, 100, 112).await?;
    assert_eq!(
        rows,
        vec![
            (100_i64, "1.5".to_string()),
            (105_i64, "2.0".to_string()),
            (110_i64, "2.5".to_string()),
        ],
        "in-range SQL must return all rows in [min,max] ascending"
    );
    Ok(())
}

/// Seed the `price` table with rows at blocks 50 and 60 (both below the
/// range we intend to query with the fallback SQL). The fallback SQL has
/// no range filter — it returns the single latest price across the whole
/// table, which here is the block-60 row.
#[tokio::test]
async fn swap_get_fallback_price_sql_returns_latest() -> Result<()> {
    let db = setup_test_db().await?;

    insert_price(&db.pool, 50, "0.5").await?;
    insert_price(&db.pool, 60, "0.7").await?;
    insert_price(&db.pool, 55, "0.6").await?;

    let p = call_get_fallback_price(&db.pool).await?;
    assert_eq!(
        p,
        Some("0.7".to_string()),
        "fallback SQL must pick the highest-block-number row"
    );

    // With no rows at all, the fallback returns None.
    let empty = setup_test_db().await?;
    assert_eq!(call_get_fallback_price(&empty.pool).await?, None);
    Ok(())
}
