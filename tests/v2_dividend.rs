//! Integration tests for DividendVault indexing: migration schema, controller
//! SQL constants, stats triggers (insert-success -> update), idempotent
//! replay, merkle root resolution, and backfill consistency.

mod common;

use anyhow::Result;
use bigdecimal::BigDecimal;
use common::setup_test_db;
use std::str::FromStr;

use observer::db::postgres::controller::v2::dividend::{
    INSERT_DIVIDEND_CLAIMS_SQL, INSERT_DIVIDEND_CONVERSIONS_SQL, INSERT_DIVIDEND_DEPOSITS_SQL,
    INSERT_DIVIDEND_MERKLE_ROOTS_SQL, INSERT_DIVIDEND_SETUPS_SQL,
};

const SOURCE: &str = "0x1111111111111111111111111111111111111111";
const DIV_QUOTE: &str = "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A";
const DIV_OTHER: &str = "0x4444444444444444444444444444444444444444";
const HOLDER: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ROOT1: &str = "0x0101010101010101010101010101010101010101010101010101010101010101";
const ROOT2: &str = "0x0202020202020202020202020202020202020202020202020202020202020202";

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

#[tokio::test]
async fn dividend_tables_and_triggers_exist() -> Result<()> {
    let db = setup_test_db().await?;
    for t in [
        "v2_dividend_setups",
        "v2_dividend_deposits",
        "v2_dividend_conversions",
        "v2_dividend_merkle_roots",
        "v2_dividend_claims",
        "v2_dividend_vault_stats",
    ] {
        let (exists,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
        )
        .bind(t)
        .fetch_one(&db.pool)
        .await?;
        assert!(exists, "missing table {t}");
    }
    for trg in [
        "trg_dividend_stats_on_setup",
        "trg_dividend_stats_on_deposit",
        "trg_dividend_stats_on_conversion",
        "trg_dividend_stats_on_claim",
    ] {
        let (exists,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.triggers WHERE trigger_name = $1)",
        )
        .bind(trg)
        .fetch_one(&db.pool)
        .await?;
        assert!(exists, "missing trigger {trg}");
    }
    Ok(())
}

/// Insert one setup event exploded into two rows (SOURCE -> DIV_QUOTE 60% / DIV_OTHER 40%).
async fn insert_setup(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(INSERT_DIVIDEND_SETUPS_SQL)
        .bind(vec![SOURCE, SOURCE])
        .bind(vec![DIV_QUOTE, DIV_OTHER])
        .bind(vec![6000_i32, 4000_i32])
        .bind(vec![bd("1000"), bd("1000")])
        .bind(vec![0_i32, 1_i32])
        .bind(vec!["0xtx_setup", "0xtx_setup"])
        .bind(vec![100_i64, 100_i64])
        .bind(vec![1_700_000_000_i64, 1_700_000_000_i64])
        .bind(vec![1_i32, 1_i32])
        .bind(vec![0_i32, 0_i32])
        .execute(pool)
        .await?;
    Ok(())
}

/// Stats projection for assertions. Tuple order:
///   total_deposited, total_pending_deposited, total_consumed_quote,
///   total_converted_received, pending_swap_balance, dividend_balance,
///   total_claimed, claim_count.
async fn stats_row(
    pool: &sqlx::PgPool,
    source: &str,
    dividend: &str,
) -> Result<(
    BigDecimal,
    BigDecimal,
    BigDecimal,
    BigDecimal,
    BigDecimal,
    BigDecimal,
    BigDecimal,
    i32,
)> {
    let row = sqlx::query_as(
        "SELECT total_deposited, total_pending_deposited, total_consumed_quote, \
         total_converted_received, pending_swap_balance, dividend_balance, \
         total_claimed, claim_count \
         FROM v2_dividend_vault_stats WHERE source_token = $1 AND dividend_token = $2",
    )
    .bind(source)
    .bind(dividend)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

#[tokio::test]
async fn setup_seeds_stats_rows() -> Result<()> {
    let db = setup_test_db().await?;
    insert_setup(&db.pool).await?;

    let (deposited, pending_deposited, consumed, received, pending_swap, balance, claimed, count) =
        stats_row(&db.pool, SOURCE, DIV_QUOTE).await?;
    assert_eq!(deposited, bd("0"));
    assert_eq!(pending_deposited, bd("0"));
    assert_eq!(consumed, bd("0"));
    assert_eq!(received, bd("0"));
    assert_eq!(pending_swap, bd("0"));
    assert_eq!(balance, bd("0"));
    assert_eq!(claimed, bd("0"));
    assert_eq!(count, 0);
    let (n,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM v2_dividend_vault_stats WHERE source_token = $1")
            .bind(SOURCE)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(n, 2);
    Ok(())
}

/// Insert one IMMEDIATE deposit slice (pending=false, dividend == quote).
/// Bind order matches INSERT_DIVIDEND_DEPOSITS_SQL: source_token,
/// dividend_token, amount, pending, entry_index, transaction_hash,
/// block_number, created_at, log_index, tx_index, quote_id, usd_value.
async fn insert_one_deposit(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(INSERT_DIVIDEND_DEPOSITS_SQL)
        .bind(vec![SOURCE])
        .bind(vec![DIV_QUOTE])
        .bind(vec![bd("600")])
        .bind(vec![false])
        .bind(vec![0_i32])
        .bind(vec!["0xtx_dep1"])
        .bind(vec![110_i64])
        .bind(vec![1_700_000_010_i64])
        .bind(vec![3_i32])
        .bind(vec![0_i32])
        .bind(vec![Some(DIV_QUOTE)])
        .bind(vec![bd("1.5")])
        .execute(pool)
        .await?;
    Ok(())
}

/// Insert one PENDING deposit slice (pending=true, dividend != quote).
/// pending=true accrues to total_pending_deposited only; dividend_balance
/// is untouched until a later Converted consumes it.
async fn insert_one_pending_deposit(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(INSERT_DIVIDEND_DEPOSITS_SQL)
        .bind(vec![SOURCE])
        .bind(vec![DIV_OTHER])
        .bind(vec![bd("400")])
        .bind(vec![true])
        .bind(vec![1_i32])
        .bind(vec!["0xtx_dep_pending"])
        .bind(vec![112_i64])
        .bind(vec![1_700_000_012_i64])
        .bind(vec![4_i32])
        .bind(vec![0_i32])
        .bind(vec![Some(DIV_QUOTE)])
        .bind(vec![bd("0.8")])
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn deposit_insert_updates_stats_and_replay_is_idempotent() -> Result<()> {
    let db = setup_test_db().await?;

    insert_one_deposit(&db.pool).await?;
    let (deposited, _, _, _, _, balance, _, _) = stats_row(&db.pool, SOURCE, DIV_QUOTE).await?;
    assert_eq!(deposited, bd("600"));
    assert_eq!(balance, bd("600"));

    // Replay: same PK -> ON CONFLICT DO NOTHING -> trigger must NOT fire again.
    insert_one_deposit(&db.pool).await?;
    let (deposited2, _, _, _, _, balance2, _, _) = stats_row(&db.pool, SOURCE, DIV_QUOTE).await?;
    assert_eq!(
        deposited2,
        bd("600"),
        "replay double-counted total_deposited"
    );
    assert_eq!(
        balance2,
        bd("600"),
        "replay double-counted dividend_balance"
    );
    Ok(())
}

#[tokio::test]
async fn deposit_pending_slice_tracked_separately() -> Result<()> {
    let db = setup_test_db().await?;

    insert_one_pending_deposit(&db.pool).await?;
    let (deposited, pending_deposited, _, _, pending_swap, balance, _, _) =
        stats_row(&db.pool, SOURCE, DIV_OTHER).await?;

    assert_eq!(
        pending_deposited,
        bd("400"),
        "pending slice must accrue to total_pending_deposited"
    );
    assert_eq!(
        deposited,
        bd("0"),
        "pending slice must NOT touch total_deposited"
    );
    assert_eq!(
        balance,
        bd("0"),
        "pending slice must NOT touch dividend_balance (no conversion yet)"
    );
    assert_eq!(
        pending_swap,
        bd("400"),
        "pending_swap_balance = pending_deposited - consumed (no conversions yet)"
    );
    Ok(())
}

#[tokio::test]
async fn pending_swap_balance_nets_against_conversion() -> Result<()> {
    let db = setup_test_db().await?;

    // Pending deposit 400 for DIV_OTHER, then a conversion consuming 400.
    insert_one_pending_deposit(&db.pool).await?;
    insert_one_conversion(&db.pool).await?;

    let (_, pending_deposited, consumed, received, pending_swap, balance, _, _) =
        stats_row(&db.pool, SOURCE, DIV_OTHER).await?;
    assert_eq!(pending_deposited, bd("400"));
    assert_eq!(consumed, bd("400"));
    assert_eq!(
        pending_swap,
        bd("0"),
        "fully-consumed pending must net to zero pending_swap_balance"
    );
    assert_eq!(
        received,
        bd("123456"),
        "conversion received credited to total_converted_received"
    );
    assert_eq!(
        balance,
        bd("123456"),
        "converted received credited to dividend_balance"
    );
    Ok(())
}

async fn insert_one_conversion(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(INSERT_DIVIDEND_CONVERSIONS_SQL)
        .bind(vec![SOURCE])
        .bind(vec![DIV_OTHER])
        .bind(vec![bd("400")])
        .bind(vec![bd("123456")])
        .bind(vec![0_i32])
        .bind(vec!["0xtx_conv1"])
        .bind(vec![120_i64])
        .bind(vec![1_700_000_020_i64])
        .bind(vec![5_i32])
        .bind(vec![1_i32])
        .bind(vec![Some(DIV_QUOTE)])
        .bind(vec![bd("1.0")])
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn conversion_insert_updates_stats() -> Result<()> {
    let db = setup_test_db().await?;
    insert_one_conversion(&db.pool).await?;

    let (_, _, consumed, received, _, balance, _, _) =
        stats_row(&db.pool, SOURCE, DIV_OTHER).await?;
    assert_eq!(consumed, bd("400"));
    assert_eq!(received, bd("123456"));
    assert_eq!(
        balance,
        bd("123456"),
        "dividend_balance must accumulate received"
    );
    Ok(())
}

async fn insert_root1(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(INSERT_DIVIDEND_MERKLE_ROOTS_SQL)
        .bind(vec![ROOT1])
        .bind(vec!["0xtx_root1"])
        .bind(vec![100_i64])
        .bind(vec![1_700_000_000_i64])
        .bind(vec![0_i32])
        .bind(vec![0_i32])
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_claim1(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(INSERT_DIVIDEND_CLAIMS_SQL)
        .bind(vec![HOLDER])
        .bind(vec![SOURCE])
        .bind(vec![DIV_QUOTE])
        .bind(vec![bd("100")])
        .bind(vec![0_i32])
        .bind(vec!["0xtx_claim1"])
        .bind(vec![150_i64])
        .bind(vec![1_700_000_050_i64])
        .bind(vec![2_i32])
        .bind(vec![0_i32])
        .bind(vec![bd("0.5")])
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn claim_resolves_merkle_root_and_updates_stats() -> Result<()> {
    let db = setup_test_db().await?;

    // Two roots: ROOT1 @ block 100, ROOT2 @ block 200.
    sqlx::query(INSERT_DIVIDEND_MERKLE_ROOTS_SQL)
        .bind(vec![ROOT1, ROOT2])
        .bind(vec!["0xtx_root1", "0xtx_root2"])
        .bind(vec![100_i64, 200_i64])
        .bind(vec![1_700_000_000_i64, 1_700_000_100_i64])
        .bind(vec![0_i32, 0_i32])
        .bind(vec![0_i32, 0_i32])
        .execute(&db.pool)
        .await?;

    // Claim @ block 150 -> ROOT1; claim @ block 250 -> ROOT2.
    sqlx::query(INSERT_DIVIDEND_CLAIMS_SQL)
        .bind(vec![HOLDER, HOLDER])
        .bind(vec![SOURCE, SOURCE])
        .bind(vec![DIV_QUOTE, DIV_QUOTE])
        .bind(vec![bd("100"), bd("200")])
        .bind(vec![0_i32, 0_i32])
        .bind(vec!["0xtx_claim1", "0xtx_claim2"])
        .bind(vec![150_i64, 250_i64])
        .bind(vec![1_700_000_050_i64, 1_700_000_150_i64])
        .bind(vec![2_i32, 2_i32])
        .bind(vec![0_i32, 0_i32])
        .bind(vec![bd("0.5"), bd("1.0")])
        .execute(&db.pool)
        .await?;

    let roots: Vec<(Option<String>,)> = sqlx::query_as(
        "SELECT merkle_root FROM v2_dividend_claims WHERE holder = $1 ORDER BY block_number",
    )
    .bind(HOLDER)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(roots[0].0.as_deref(), Some(ROOT1));
    assert_eq!(roots[1].0.as_deref(), Some(ROOT2));

    let (_, _, _, _, _, _, claimed, count) = stats_row(&db.pool, SOURCE, DIV_QUOTE).await?;
    assert_eq!(claimed, bd("300"));
    assert_eq!(count, 2);
    Ok(())
}

#[tokio::test]
async fn claim_resolves_merkle_root_within_same_block() -> Result<()> {
    let db = setup_test_db().await?;

    // Two roots in the SAME block, same tx_index, different log_index:
    // ROOT1 @ (100, 0, 1), ROOT2 @ (100, 0, 5).
    sqlx::query(INSERT_DIVIDEND_MERKLE_ROOTS_SQL)
        .bind(vec![ROOT1, ROOT2])
        .bind(vec!["0xtx_root_a", "0xtx_root_b"])
        .bind(vec![100_i64, 100_i64])
        .bind(vec![1_700_000_000_i64, 1_700_000_001_i64])
        .bind(vec![1_i32, 5_i32])
        .bind(vec![0_i32, 0_i32])
        .execute(&db.pool)
        .await?;

    // Claim @ (100, 0, 3): only ROOT1 (log_index 1) is at-or-before it.
    // A block_number-only comparison would wrongly pick ROOT2 (log_index 5).
    sqlx::query(INSERT_DIVIDEND_CLAIMS_SQL)
        .bind(vec![HOLDER])
        .bind(vec![SOURCE])
        .bind(vec![DIV_QUOTE])
        .bind(vec![bd("100")])
        .bind(vec![0_i32])
        .bind(vec!["0xtx_claim_mid"])
        .bind(vec![100_i64])
        .bind(vec![1_700_000_002_i64])
        .bind(vec![3_i32])
        .bind(vec![0_i32])
        .bind(vec![bd("0.5")])
        .execute(&db.pool)
        .await?;

    let (root,): (Option<String>,) =
        sqlx::query_as("SELECT merkle_root FROM v2_dividend_claims WHERE holder = $1")
            .bind(HOLDER)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        root.as_deref(),
        Some(ROOT1),
        "tuple comparison must order by (block_number, tx_index, log_index), not block_number alone"
    );
    Ok(())
}

#[tokio::test]
async fn claim_without_any_root_inserts_null_root() -> Result<()> {
    let db = setup_test_db().await?;

    // No merkle roots indexed at all: claim must still land, with NULL root.
    insert_claim1(&db.pool).await?;

    let (root,): (Option<String>,) =
        sqlx::query_as("SELECT merkle_root FROM v2_dividend_claims WHERE holder = $1")
            .bind(HOLDER)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        root, None,
        "claim with no prior root must store NULL merkle_root"
    );

    let (_, _, _, _, _, _, claimed, count) = stats_row(&db.pool, SOURCE, DIV_QUOTE).await?;
    assert_eq!(
        claimed,
        bd("100"),
        "stats must still accumulate on NULL-root claim"
    );
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn claim_zero_amount_rejected_by_check() -> Result<()> {
    let db = setup_test_db().await?;
    let res = sqlx::query(INSERT_DIVIDEND_CLAIMS_SQL)
        .bind(vec![HOLDER])
        .bind(vec![SOURCE])
        .bind(vec![DIV_QUOTE])
        .bind(vec![bd("0")])
        .bind(vec![0_i32])
        .bind(vec!["0xtx_claim0"])
        .bind(vec![300_i64])
        .bind(vec![1_700_000_200_i64])
        .bind(vec![1_i32])
        .bind(vec![0_i32])
        .bind(vec![bd("0")])
        .execute(&db.pool)
        .await;
    let err = res.expect_err("zero-amount claim must violate CHECK (amount > 0)");
    assert_eq!(
        err.as_database_error().and_then(|e| e.constraint()),
        Some("chk_v2_dividend_claims_amount"),
        "expected amount CHECK constraint violation, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn backfill_rebuild_matches_trigger_accumulation() -> Result<()> {
    let db = setup_test_db().await?;
    insert_setup(&db.pool).await?;
    insert_one_deposit(&db.pool).await?;
    // Pending deposit exercises the backfill split (pending vs immediate).
    insert_one_pending_deposit(&db.pool).await?;
    insert_one_conversion(&db.pool).await?;
    insert_root1(&db.pool).await?;
    insert_claim1(&db.pool).await?;

    type StatsRows = Vec<(
        String,
        String,
        BigDecimal,
        BigDecimal,
        BigDecimal,
        BigDecimal,
        BigDecimal,
        BigDecimal,
        BigDecimal,
        i32,
    )>;
    // Includes total_pending_deposited and the GENERATED pending_swap_balance so
    // the full-row compare exercises the backfill pending split. Same column
    // list before and after the rebuild.
    const STATS_QUERY: &str = "SELECT source_token, dividend_token, total_deposited, \
         total_pending_deposited, total_consumed_quote, total_converted_received, \
         pending_swap_balance, dividend_balance, total_claimed, claim_count \
         FROM v2_dividend_vault_stats ORDER BY source_token, dividend_token";

    let before: StatsRows = sqlx::query_as(STATS_QUERY).fetch_all(&db.pool).await?;

    // Re-run the whole idempotent migration file: backfill TRUNCATEs stats and
    // rebuilds from history. Result must equal trigger accumulation.
    let sql = std::fs::read_to_string("migrations/dividend.sql")?;
    sqlx::raw_sql(&sql).execute(&db.pool).await?;

    let after: StatsRows = sqlx::query_as(STATS_QUERY).fetch_all(&db.pool).await?;
    assert_eq!(
        before, after,
        "backfill rebuild must match trigger accumulation"
    );
    Ok(())
}
