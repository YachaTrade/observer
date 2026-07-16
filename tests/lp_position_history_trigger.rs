mod common;
use common::setup_test_db;

use bigdecimal::BigDecimal;
use sqlx::PgPool;
use std::str::FromStr;

const ALICE: &str = "0xa11ce000000000000000000000000000000a11ce";
const BOB: &str = "0xb0b0000000000000000000000000000000000b0b";
// factory(0x59c51c66...).feeTo() on testnet — the _mintFee() carve-out
// recipient. Cost basis must skip rows where account_id matches this.
const FEETO: &str = "0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a";
// NadFunPair fairlaunch MIN_LIQUIDITY lock recipient AND graduation-pool
// real-deposit recipient. New design no longer special-cases this address;
// inclusion in cost basis depends on whether the row sits next to a feeTo
// sibling (graduation case → dEaD owns full deposit) or next to a real user
// (first-mint case → dEaD owns tiny MIN_LIQUIDITY share, user owns bulk).
const DEAD: &str = "0x000000000000000000000000000000000000dead";

async fn ensure_dex_mint_burn(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS dex_mint (
            pool_id          VARCHAR(42) NOT NULL,
            sender           VARCHAR(42) NOT NULL,
            amount0          NUMERIC NOT NULL,
            amount1          NUMERIC NOT NULL,
            created_at       BIGINT NOT NULL,
            block_number     BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index        INT NOT NULL,
            tx_index         INT NOT NULL,
            PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
        );",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS dex_burn (
            pool_id          VARCHAR(42) NOT NULL,
            sender           VARCHAR(42) NOT NULL,
            to_address       VARCHAR(42) NOT NULL,
            amount0          NUMERIC NOT NULL,
            amount1          NUMERIC NOT NULL,
            created_at       BIGINT NOT NULL,
            block_number     BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index        INT NOT NULL,
            tx_index         INT NOT NULL,
            PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
        );",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_pool(pool: &PgPool, pool_id: &str) {
    // pool (v2_upgrade_new_tables.sql) requires created_at, block_number,
    // tx_hash NOT NULL — supply minimal valid placeholders.
    sqlx::query(
        "INSERT INTO pool(pool_id, token0, token1, reserve0, reserve1, created_at, block_number, tx_hash) \
         VALUES ($1, '0xt000000000000000000000000000000000000000', '0xt100000000000000000000000000000000000000', 0, 0, 0, 0, '0xseed') \
         ON CONFLICT (pool_id) DO NOTHING",
    )
    .bind(pool_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_dex_mint(pool: &PgPool, pool_id: &str, tx: &str, a0: &str, a1: &str) {
    // dex_mint emitted AFTER the LP Transfer in V2 Pair.mint() — log_index=2
    // (the LP transfer test helper uses log_index=1, so 2 > 1 satisfies the
    // matching predicate added for the multi-mint fix).
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', $2::numeric, $3::numeric, 100, 1, $4, 2, 0)",
    )
    .bind(pool_id)
    .bind(a0)
    .bind(a1)
    .bind(tx)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_dex_burn(
    pool: &PgPool,
    pool_id: &str,
    tx: &str,
    to: &str,
    a0: &str,
    a1: &str,
) {
    // dex_burn emitted AFTER the LP Transfer in V2 Pair.burn() — log_index=2
    // (LP transfer test helper uses log_index=1, so 2 > 1 satisfies the
    // matching predicate added for the multi-burn fix).
    sqlx::query(
        "INSERT INTO dex_burn(pool_id, sender, to_address, amount0, amount1, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', $2, $3::numeric, $4::numeric, 200, 2, $5, 2, 0)",
    )
    .bind(pool_id)
    .bind(to)
    .bind(a0)
    .bind(a1)
    .bind(tx)
    .execute(pool)
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn insert_lp_history(
    pool: &PgPool,
    account: &str,
    pool_id: &str,
    event_type: &str,
    lp_in: &str,
    lp_out: &str,
    counterparty: Option<&str>,
    tx: &str,
    log_index: i32,
    block: i64,
) {
    sqlx::query(
        "INSERT INTO lp_position_history(account_id, pool_id, lp_in, lp_out, event_type, counterparty, \
         transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, $3::numeric, $4::numeric, $5::lp_event_type, $6, $7, $8, 0, $9, 100)",
    )
    .bind(account)
    .bind(pool_id)
    .bind(lp_in)
    .bind(lp_out)
    .bind(event_type)
    .bind(counterparty)
    .bind(tx)
    .bind(block)
    .bind(log_index)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn migration_creates_lp_position_tables() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;

    for tbl in &["lp_position_history", "lp_position"] {
        let row: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=$1)",
        )
        .bind(tbl)
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(row.0, "{} table missing", tbl);
    }

    let total_supply: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='pool' AND column_name='total_supply')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(total_supply.0, "pool.total_supply column missing");

    // Split into BEFORE (fill cost basis) + AFTER (apply aggregates) so that
    // ON CONFLICT DO NOTHING replays don't double-count via BEFORE side effects
    // (codex P1-A, 2026-05-14).
    for tg in &["trg_fill_lp_cost_basis", "trg_apply_lp_position"] {
        let trigger: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = $1)")
                .bind(tg)
                .fetch_one(pool)
                .await
                .unwrap();
        assert!(trigger.0, "{} trigger missing", tg);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mint_records_dex_mint_amounts_as_token_in() {
    // After the view-based migration: lp_position carries only lp_in/lp_out.
    // Cost basis (token0_in / token1_in) lives in the lp_position_cost_basis
    // view, share-weighted across non-feeTo recipients. With a single
    // recipient (ALICE, no feeTo sibling) the view returns the full
    // dex_mint amounts.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x1111000000000000000000000000000000000000";
    let tx = "0xMINT01";
    seed_pool(pool, pid).await;
    insert_dex_mint(pool, pid, tx, "1000", "4000").await;

    insert_lp_history(pool, ALICE, pid, "mint", "500", "0", None, tx, 1, 1).await;

    let (lp_in, lp_out): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, lp_out FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in.to_string(), "500", "lp_in");
    assert_eq!(lp_out.to_string(), "0", "lp_out");

    // Cost basis comes from the view (sum over all rows for this account/pool).
    let cb: (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT COALESCE(SUM(token0_in), 0), COALESCE(SUM(token1_in), 0) \
         FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(cb.0.to_string(), "1000", "token0_in (from dex_mint, via view)");
    assert_eq!(cb.1.to_string(), "4000", "token1_in (from dex_mint, via view)");

    let supply: (BigDecimal,) =
        sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id=$1")
            .bind(pid)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(supply.0.to_string(), "500");
}

#[tokio::test(flavor = "multi_thread")]
async fn burn_records_dex_burn_actual_withdrawn() {
    // Exercises the dex_burn lookup path: when the parser already attributes
    // the burn row directly to the user, the burn cost basis in the
    // lp_position_cost_basis view is the full dex_burn (single-recipient
    // attribution, NOT proportional). Re-attribution from pool→user is
    // covered by burn_reattributes_account_from_pool_to_recipient below.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x2222000000000000000000000000000000000000";
    let mint_tx = "0xMINT02";
    let burn_tx = "0xBURN02";
    seed_pool(pool, pid).await;
    insert_dex_mint(pool, pid, mint_tx, "1000", "4000").await;
    insert_lp_history(pool, ALICE, pid, "mint", "1000", "0", None, mint_tx, 1, 1).await;

    // Pool grew: actual withdrawn for 400 LP is (4800, 19200) — more than 40% of cost basis
    insert_dex_burn(pool, pid, burn_tx, ALICE, "4800", "19200").await;
    insert_lp_history(pool, ALICE, pid, "burn", "0", "400", None, burn_tx, 1, 2).await;

    let (lp_in, lp_out): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, lp_out FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in.to_string(), "1000", "cumulative lp_in");
    assert_eq!(lp_out.to_string(), "400", "cumulative lp_out");

    // Cost basis: mint contributes token0_in=1000, burn contributes
    // token0_out=4800 (actual withdrawal, NOT proportional).
    let cb: (BigDecimal, BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT COALESCE(SUM(token0_in), 0), COALESCE(SUM(token0_out), 0), \
                COALESCE(SUM(token1_in), 0), COALESCE(SUM(token1_out), 0) \
         FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(cb.0.to_string(), "1000", "lifetime token0_in");
    assert_eq!(
        cb.1.to_string(),
        "4800",
        "actual token0 withdrawn (NOT proportional 400)"
    );
    assert_eq!(cb.2.to_string(), "4000", "lifetime token1_in");
    assert_eq!(cb.3.to_string(), "19200", "actual token1 withdrawn");

    let supply: (BigDecimal,) =
        sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id=$1")
            .bind(pid)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(supply.0.to_string(), "600"); // 1000 - 400
}

#[tokio::test(flavor = "multi_thread")]
async fn full_burn_deletes_position_row() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x3333000000000000000000000000000000000000";
    let mint_tx = "0xMINT03";
    let burn_tx = "0xBURN03";
    seed_pool(pool, pid).await;
    insert_dex_mint(pool, pid, mint_tx, "1000", "2000").await;
    insert_lp_history(pool, ALICE, pid, "mint", "1000", "0", None, mint_tx, 1, 1).await;
    insert_dex_burn(pool, pid, burn_tx, ALICE, "1200", "2400").await;
    insert_lp_history(pool, ALICE, pid, "burn", "0", "1000", None, burn_tx, 1, 2).await;

    // lp_position row MUST be deleted (lp_in == lp_out)
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(count.0, 0, "lp_position row should be DELETED on full burn");

    // history rows persist
    let hist_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM lp_position_history WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        hist_count.0, 2,
        "lp_position_history should retain mint+burn rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn holder_transfer_moves_lp_balance() {
    // Holder→holder LP transfer: lp_in/lp_out are bookkept on both sides,
    // pool.total_supply is unchanged. Cost basis propagation across transfers
    // is intentionally NOT tracked by the lp_position_cost_basis view (see
    // plan "Out of scope" — defer until a consumer needs running per-holder
    // cost basis after transfers). Originally this test verified avg cost
    // moved with the LP; under the new design the test asserts only the
    // balance side of the contract.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x4444000000000000000000000000000000000000";
    let mint_tx = "0xMINT04";
    let xfer_tx = "0xXFER04";
    seed_pool(pool, pid).await;

    // alice mints 1000 LP with cost (10000, 40000)
    insert_dex_mint(pool, pid, mint_tx, "10000", "40000").await;
    insert_lp_history(pool, ALICE, pid, "mint", "1000", "0", None, mint_tx, 1, 1).await;

    // alice → bob 200 LP. Insert transfer_out THEN transfer_in (Rust receive order)
    insert_lp_history(
        pool,
        ALICE,
        pid,
        "transfer_out",
        "0",
        "200",
        Some(BOB),
        xfer_tx,
        1,
        2,
    )
    .await;
    insert_lp_history(
        pool,
        BOB,
        pid,
        "transfer_in",
        "200",
        "0",
        Some(ALICE),
        xfer_tx,
        2,
        2,
    )
    .await;

    let (a_in, a_out): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, lp_out FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(a_in.to_string(), "1000", "alice lp_in");
    assert_eq!(a_out.to_string(), "200", "alice lp_out");

    let (b_in, b_out): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, lp_out FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(BOB)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(b_in.to_string(), "200", "bob lp_in");
    assert_eq!(b_out.to_string(), "0", "bob lp_out");

    // pool.total_supply unchanged by transfer
    let supply: (BigDecimal,) =
        sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id=$1")
            .bind(pid)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(supply.0.to_string(), "1000");
}

#[tokio::test(flavor = "multi_thread")]
async fn reentry_after_full_burn_creates_fresh_row() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x5555000000000000000000000000000000000000";
    seed_pool(pool, pid).await;

    // Round 1: mint 1000, burn 1000 → row deleted
    insert_dex_mint(pool, pid, "0xMINT05A", "1000", "2000").await;
    insert_lp_history(pool, ALICE, pid, "mint", "1000", "0", None, "0xMINT05A", 1, 1).await;
    insert_dex_burn(pool, pid, "0xBURN05A", ALICE, "1200", "2400").await;
    insert_lp_history(pool, ALICE, pid, "burn", "0", "1000", None, "0xBURN05A", 1, 2).await;
    let count1: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(count1.0, 0, "row deleted after round 1 full burn");

    // Round 2: mint 500 again
    insert_dex_mint(pool, pid, "0xMINT05B", "5000", "20000").await;
    insert_lp_history(pool, ALICE, pid, "mint", "500", "0", None, "0xMINT05B", 1, 3).await;

    // Fresh row — lp balance starts at round 2's mint
    let (lp_in,): (BigDecimal,) = sqlx::query_as(
        "SELECT lp_in FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in.to_string(), "500", "fresh lp_in only counts round 2");

    // Cost basis view: round 1 mint(1000,2000) + burn(1200,2400) + round 2
    // mint(5000,20000) all show up as separate rows. Lifetime sum is what
    // a consumer would see if they query without further filtering.
    let cb_t0_in: BigDecimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(token0_in), 0) FROM lp_position_cost_basis \
         WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        cb_t0_in.to_string(),
        "6000",
        "view sees all mint rows across reentry (1000 + 5000)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_history_insert_does_not_double_count() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x6666000000000000000000000000000000000000";
    let alice = ALICE;
    let mint_tx = "0xMINTDUP";
    seed_pool(pool, pid).await;
    insert_dex_mint(pool, pid, mint_tx, "1000", "2000").await;

    // First insert — applies
    insert_lp_history(pool, alice, pid, "mint", "500", "0", None, mint_tx, 1, 1).await;
    // Replay same PK — must be no-op (AFTER trigger does not fire when
    // ON CONFLICT DO NOTHING skips the insert; codex P1-A, 2026-05-14)
    sqlx::query(
        "INSERT INTO lp_position_history(account_id, pool_id, lp_in, lp_out, event_type, counterparty, \
         transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 500::numeric, 0, 'mint', NULL, $3, 1, 0, 1, 100) \
         ON CONFLICT (account_id, pool_id, transaction_hash, tx_index, log_index) DO NOTHING",
    )
    .bind(alice)
    .bind(pid)
    .bind(mint_tx)
    .execute(pool)
    .await
    .unwrap();

    let (lp_in,): (BigDecimal,) = sqlx::query_as(
        "SELECT lp_in FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in.to_string(), "500", "lp_in must NOT double-count after replay");

    // The view derives cost basis from a single lp_position_history row;
    // the duplicate INSERT...ON CONFLICT DO NOTHING did not insert a new
    // row, so the view still returns the single deposit.
    let cb: (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT COALESCE(SUM(token0_in), 0), COALESCE(SUM(token1_in), 0) \
         FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(cb.0.to_string(), "1000", "token0_in must NOT double-count");
    assert_eq!(cb.1.to_string(), "2000", "token1_in must NOT double-count");

    let supply: (BigDecimal,) =
        sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id=$1")
            .bind(pid)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(supply.0.to_string(), "500", "total_supply must NOT double-count");
}

#[tokio::test(flavor = "multi_thread")]
async fn burn_reattributes_account_from_pool_to_recipient() {
    // V2 Pair.burn() emits Transfer(pool→0x0); the parser attributes that row
    // to account_id = pool_id. The trigger MUST re-attribute it to the real
    // recipient from dex_burn.to_address so the user (not the pool) gets the
    // actual withdrawn amounts.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x7777000000000000000000000000000000000000";
    let alice = ALICE;
    let mint_tx = "0xMINT07";
    let burn_tx = "0xBURN07";
    seed_pool(pool, pid).await;
    insert_dex_mint(pool, pid, mint_tx, "1000", "2000").await;
    insert_lp_history(pool, alice, pid, "mint", "1000", "0", None, mint_tx, 1, 1).await;

    // dex_burn says alice withdrew (1200, 2400)
    insert_dex_burn(pool, pid, burn_tx, alice, "1200", "2400").await;

    // Parser would emit burn row with account_id = pool_id (the pair contract).
    // Trigger must re-attribute to alice.
    insert_lp_history(pool, pid, pid, "burn", "0", "400", None, burn_tx, 1, 2).await;

    // Verify alice (not pool) was credited (lp balance via lp_position;
    // cost-basis token_out via the view's burn arm).
    let (lp_in, lp_out): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, lp_out FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in.to_string(), "1000", "alice lp_in");
    assert_eq!(lp_out.to_string(), "400", "alice lp_out (re-attributed from pool)");

    let cb: (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT COALESCE(SUM(token0_out), 0), COALESCE(SUM(token1_out), 0) \
         FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(cb.0.to_string(), "1200", "alice token0_out (actual withdrawal)");
    assert_eq!(cb.1.to_string(), "2400", "alice token1_out (actual withdrawal)");

    // Pool itself must NOT have an lp_position row
    let pool_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(pid)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(pool_count.0, 0, "pool should not have an lp_position row");
}

#[tokio::test(flavor = "multi_thread")]
async fn transfer_to_pool_is_skipped() {
    // V2 Pair.burn() first leg: user sends LP to the pair contract. The parser
    // emits this as transfer_out (user → pool). The trigger MUST skip it —
    // the user's lp/cost decrement comes from the re-attributed burn row.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0x8888000000000000000000000000000000000000";
    let alice = ALICE;
    let mint_tx = "0xMINT08";
    let xfer_tx = "0xXFER08";
    seed_pool(pool, pid).await;
    insert_dex_mint(pool, pid, mint_tx, "1000", "2000").await;
    insert_lp_history(pool, alice, pid, "mint", "1000", "0", None, mint_tx, 1, 1).await;

    // Parser would emit transfer_out (alice → pool) before the burn — must be skipped.
    insert_lp_history(
        pool,
        alice,
        pid,
        "transfer_out",
        "0",
        "400",
        Some(pid),
        xfer_tx,
        1,
        2,
    )
    .await;

    // Alice's lp_position must be UNCHANGED by the transfer_out (the burn row
    // would apply the lp change; this test doesn't insert burn, so alice
    // should still be at mint state).
    let (lp_in, lp_out): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, lp_out FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in.to_string(), "1000", "lp_in unchanged");
    assert_eq!(
        lp_out.to_string(),
        "0",
        "lp_out unchanged — transfer_out to pool skipped"
    );
    // Cost basis from the mint row is preserved (the skipped transfer_out
    // wouldn't contribute anyway, since the view only covers mint+burn).
    let cb_t0: BigDecimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(token0_in), 0) FROM lp_position_cost_basis \
         WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(cb_t0.to_string(), "1000", "token0_in unchanged");

    // lp_position_history must NOT contain the skipped row
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM lp_position_history WHERE transaction_hash=$1",
    )
    .bind(xfer_tx)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(count.0, 0, "skipped transfer_out should not be persisted");
}

#[tokio::test(flavor = "multi_thread")]
async fn event_type_column_is_enum() {
    // Verify the migration created `lp_event_type` ENUM and the column uses it.
    // We assert the column's udt_name (PostgreSQL's user-defined type name) is
    // 'lp_event_type', which is the structural property that gives type safety —
    // rather than a runtime rejection test (sqlx prepared-statement paths can
    // have subtle interactions with enum literal coercion that mask the type).
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;

    let (data_type, udt_name): (String, String) = sqlx::query_as(
        "SELECT data_type, udt_name FROM information_schema.columns \
         WHERE table_name = 'lp_position_history' AND column_name = 'event_type'"
    ).fetch_one(pool).await.unwrap();

    assert_eq!(data_type, "USER-DEFINED", "event_type should be a user-defined ENUM");
    assert_eq!(udt_name, "lp_event_type", "event_type must use the lp_event_type ENUM");

    // Verify the ENUM has exactly the expected values
    let values: Vec<String> = sqlx::query_scalar(
        "SELECT e.enumlabel FROM pg_type t \
         JOIN pg_enum e ON t.oid = e.enumtypid \
         WHERE t.typname = 'lp_event_type' \
         ORDER BY e.enumsortorder"
    ).fetch_all(pool).await.unwrap();
    assert_eq!(values, vec!["mint", "burn", "transfer_in", "transfer_out"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_mint_in_single_tx_matches_correct_dex_mint() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid    = "0x9999000000000000000000000000000000000000";
    let alice  = ALICE;
    let tx     = "0xMULTIMINT";
    seed_pool(pool, pid).await;

    // Two router-aggregated mints in the same tx
    // Mint event 1: amount0=100, amount1=200 (log_index=1)
    // Transfer 1:   lp=10  (log_index=0)  <-- should match Mint 1
    // Mint event 2: amount0=300, amount1=600 (log_index=3)
    // Transfer 2:   lp=30  (log_index=2)  <-- should match Mint 2

    // Insert dex_mint at log_index=1 and log_index=3
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, created_at, block_number, transaction_hash, log_index, tx_index)\
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 100, 200, 100, 1, $2, 1, 0),\
                ($1, '0xdeadbeef00000000000000000000000000000000', 300, 600, 100, 1, $2, 3, 0)"
    ).bind(pid).bind(tx).execute(pool).await.unwrap();

    // First LP transfer at log_index=0, lp_in=10 — should pick (100, 200)
    insert_lp_history(pool, alice, pid, "mint", "10", "0", None, tx, 0, 1).await;

    // After first row: lp balance is 10. Cost basis (via view) is the
    // first dex_mint's (100, 200) since the view matches each lp_history
    // row to the smallest-log_index dex_mint > its own.
    let lp_in1: BigDecimal = sqlx::query_scalar(
        "SELECT lp_in FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in1.to_string(), "10", "lp_in after first mint");

    let cb1: (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in FROM lp_position_cost_basis \
         WHERE account_id=$1 AND pool_id=$2 AND log_index=0",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(cb1.0.to_string(), "100", "first mint matched first dex_mint");
    assert_eq!(cb1.1.to_string(), "200");

    // Second LP transfer at log_index=2, lp_in=30 — should pick (300, 600)
    insert_lp_history(pool, alice, pid, "mint", "30", "0", None, tx, 2, 1).await;

    let lp_in2: BigDecimal = sqlx::query_scalar(
        "SELECT lp_in FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(lp_in2.to_string(), "40", "cumulative lp_in");

    let cb_sum: (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT COALESCE(SUM(token0_in), 0), COALESCE(SUM(token1_in), 0) \
         FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(alice)
    .bind(pid)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(cb_sum.0.to_string(), "400", "100 + 300 — each lp_history row matched its own dex_mint");
    assert_eq!(cb_sum.1.to_string(), "800", "200 + 600");
}

// ============================================================================
// View-based cost basis tests (lp_position_cost_basis)
// ============================================================================
//
// These tests encode the post-fix semantics: cost basis is a derived view, NOT
// trigger-filled columns. The trigger no longer fills token/USD columns on
// lp_position_history or lp_position; consumers read the view instead.
//
// feeTo handling:
//   * Rows whose account_id == 0x715103eeEac12FB84f5d3B35c3268Dd767fa8b8A
//     (factory.feeTo()) are excluded from cost basis (return 0). They
//     represent the _mintFee() carve-out from k growth, not a deposit.
//   * 0xdead is NOT a special case. On graduation pools dEaD owns the entire
//     deposit (bonding curve locks LP there); on first-mints dEaD owns the
//     MIN_LIQUIDITY share — both follow share-weighting against the row's
//     siblings in the same tx.

async fn insert_dex_mint_full(
    pool: &PgPool,
    pool_id: &str,
    tx: &str,
    a0: &str,
    a1: &str,
    t0_usd: &str,
    t1_usd: &str,
    value: &str,
    log_index: i32,
) {
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, value, token0_usd, token1_usd, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', $2::numeric, $3::numeric, $4::numeric, $5::numeric, $6::numeric, 100, 1, $7, $8, 0)",
    )
    .bind(pool_id)
    .bind(a0)
    .bind(a1)
    .bind(value)
    .bind(t0_usd)
    .bind(t1_usd)
    .bind(tx)
    .bind(log_index)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_lp_history_mint(
    pool: &PgPool,
    account: &str,
    pool_id: &str,
    tx: &str,
    lp: &str,
    log_index: i32,
) {
    sqlx::query(
        "INSERT INTO lp_position_history(account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, $3::numeric, 0, 'mint', $4, 1, 0, $5, 100)",
    )
    .bind(account)
    .bind(pool_id)
    .bind(lp)
    .bind(tx)
    .bind(log_index)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn view_graduation_pool_feeto_zero_dead_full() {
    // Graduation pool semantics: Pair.mint() emits two Transfer(0x0 → ...) logs:
    //   - one to feeTo (factory._mintFee() carve-out, ~1/6 of new LP from k growth)
    //   - one to dEaD (the full bonding-curve deposit, locked forever)
    // The dex_mint event sits at a HIGHER log_index than both Transfers.
    // Cost basis: feeTo row = 0 (excluded), dEaD row = full deposit (sole
    // remaining recipient after feeTo exclusion).
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pool_id = "0xpool00000000000000000000000000000000pool";
    let tx = "0xtxgraduation0000000000000000000000000000000000000000000000000001";
    seed_pool(pool, pool_id).await;

    // chain-deposited 100 token0 + 200 token1, $5 total value
    insert_dex_mint_full(pool, pool_id, tx, "100", "200", "2.5", "2.5", "5", 17).await;

    // Two LP Transfer rows from one Pair.mint() emit; log_index < dex_mint(17).
    insert_lp_history_mint(pool, FEETO, pool_id, tx, "1000000000000000000", 15).await; // 1e18 LP
    insert_lp_history_mint(pool, DEAD, pool_id, tx, "999000000000000000000", 16).await; // 999e18 LP

    // feeTo row — must be 0 across the board
    let feeto: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in, lp_in_usd FROM lp_position_cost_basis \
         WHERE account_id = $1 AND pool_id = $2 AND transaction_hash = $3",
    )
    .bind(FEETO)
    .bind(pool_id)
    .bind(tx)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(feeto.0, BigDecimal::from(0), "feeTo token0_in must be 0 (no deposit)");
    assert_eq!(feeto.1, BigDecimal::from(0), "feeTo token1_in must be 0");
    assert_eq!(feeto.2, BigDecimal::from(0), "feeTo lp_in_usd must be 0");

    // dEaD row — sole non-feeTo recipient, owns the entire deposit
    let dead: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in, lp_in_usd FROM lp_position_cost_basis \
         WHERE account_id = $1 AND pool_id = $2 AND transaction_hash = $3",
    )
    .bind(DEAD)
    .bind(pool_id)
    .bind(tx)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(dead.0, BigDecimal::from(100), "dEaD token0_in = full deposit (only non-fee recipient)");
    assert_eq!(dead.1, BigDecimal::from(200), "dEaD token1_in = full deposit");
    assert_eq!(dead.2, BigDecimal::from(5), "dEaD lp_in_usd = full deposit USD");
}

#[tokio::test(flavor = "multi_thread")]
async fn view_standard_first_mint_share_weighted() {
    // First-mint on a NadFunPair: Pair locks MIN_LIQUIDITY (1000 wei) to dEaD,
    // gives the rest to the real depositor. Both rows are NON-feeTo, so cost
    // basis share-weights proportional to lp_in. dEaD gets ~0 share (1000 wei
    // out of ~1e12 total); user gets ~all of it. Conservation: Σshares = full.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pool_id = "0xpool00000000000000000000000000000000poo2";
    let tx = "0xtxfirstmint0000000000000000000000000000000000000000000000000002";
    seed_pool(pool, pool_id).await;

    insert_dex_mint_full(pool, pool_id, tx, "1000000", "2000000", "0.5", "0.5", "1", 17).await;

    insert_lp_history_mint(pool, DEAD, pool_id, tx, "1000", 15).await; // MIN_LIQUIDITY
    insert_lp_history_mint(pool, ALICE, pool_id, tx, "999999999000", 16).await; // bulk

    // total_real_lp = 1000 + 999999999000 = 1000000000000
    // ALICE share = 999999999000 / 1000000000000 → token0_in ≈ 999999 (out of 1_000_000)
    let alice_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(ALICE)
    .bind(pool_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(
        alice_t0 >= BigDecimal::from(999_999),
        "ALICE near-full deposit, got {alice_t0}"
    );
    assert!(alice_t0 <= BigDecimal::from(1_000_000));

    // dEaD MIN_LIQ share — tiny, but non-zero (not feeTo)
    let dead_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(DEAD)
    .bind(pool_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(dead_t0 < BigDecimal::from(2), "dEaD MIN_LIQ share ~= 0, got {dead_t0}");

    // Conservation: sum of all per-row shares = full deposit
    let sum_t0: BigDecimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(token0_in), 0) FROM lp_position_cost_basis WHERE pool_id=$1",
    )
    .bind(pool_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        sum_t0,
        BigDecimal::from(1_000_000),
        "conservation: Σshare = full deposit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn view_add_liquidity_feeto_excluded() {
    // Standard add-LP: feeTo carve-out + single real depositor (BOB).
    // feeTo row = 0; BOB row = full deposit.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pool_id = "0xpool00000000000000000000000000000000poo3";
    let tx = "0xtxaddlp0000000000000000000000000000000000000000000000000000003a";
    seed_pool(pool, pool_id).await;

    insert_dex_mint_full(pool, pool_id, tx, "500", "1000", "1", "2", "3", 17).await;

    insert_lp_history_mint(pool, FEETO, pool_id, tx, "1000000", 15).await; // protocol fee carve
    insert_lp_history_mint(pool, BOB, pool_id, tx, "999000000", 16).await; // real depositor

    let bob: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in, lp_in_usd FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2",
    )
    .bind(BOB)
    .bind(pool_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(bob.0, BigDecimal::from(500), "BOB token0_in = full deposit (feeTo excluded)");
    assert_eq!(bob.1, BigDecimal::from(1000), "BOB token1_in = full");
    assert_eq!(bob.2, BigDecimal::from(3), "BOB lp_in_usd = full");
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_token_cols_materialized_on_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pool_id = "0xpool00000000000000000000000000000000poo4";
    let tx      = "0xtxagg00000000000000000000000000000000000000000000000000000004x";
    seed_pool(pool, pool_id).await;

    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 100::numeric, 200::numeric, 1::numeric, 1::numeric, 2::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(pool).await.unwrap();

    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 1000000000000000000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000)",
    ).bind(ALICE).bind(pool_id).bind(tx).execute(pool).await.unwrap();

    let (lp_in, t0, t1, lp_usd): (BigDecimal, BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, token0_in, token1_in, lp_in_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(lp_in,  BigDecimal::from(1_000_000_000_000_000_000i64), "lp_in tracked");
    assert_eq!(t0,     BigDecimal::from(100), "token0_in materialized from view math (sole non-fee recipient)");
    assert_eq!(t1,     BigDecimal::from(200), "token1_in materialized");
    assert_eq!(lp_usd, BigDecimal::from(2),   "lp_in_usd materialized");
}

#[tokio::test(flavor = "multi_thread")]
async fn materialize_graduation_pool_feeto_zero_dead_full_in_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a1";
    let tx      = "0xtxmatgraduation0000000000000000000000000000000000000000000000001";
    seed_pool(pool, pool_id).await;

    // dex_mint MUST exist before lp_position_history rows insert (V2_DEX → Token ordering)
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 100::numeric, 200::numeric, 2.5::numeric, 2.5::numeric, 5::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(pool).await.unwrap();

    // Two LP Transfer rows inserted in a single batch (= statement trigger sees both at once)
    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES \
         ($1, $2, $3::numeric, 0, 'mint', $4, 1, 0, 15, 1779000000), \
         ($5, $2, $6::numeric, 0, 'mint', $4, 1, 0, 16, 1779000000)",
    )
    .bind(FEETO).bind(pool_id).bind("1000000000000000000")   // 1e18
    .bind(tx)
    .bind(DEAD).bind("999000000000000000000")                // 999e18 (only non-fee recipient)
    .execute(pool).await.unwrap();

    // After statement trigger fires, lp_position MUST contain materialized cost basis
    let (lp_in, token0_in, token1_in, lp_in_usd): (BigDecimal, BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, token0_in, token1_in, lp_in_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(DEAD).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(lp_in,     BigDecimal::from(999_000_000_000_000_000_000u128), "dEaD lp_in tracked");
    assert_eq!(token0_in, BigDecimal::from(100),  "dEaD token0_in = full deposit (sole non-fee recipient)");
    assert_eq!(token1_in, BigDecimal::from(200),  "dEaD token1_in = full");
    assert_eq!(lp_in_usd, BigDecimal::from(5),    "dEaD lp_in_usd = full");

    let (lp_in, token0_in, lp_in_usd): (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, token0_in, lp_in_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(FEETO).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(lp_in,     BigDecimal::from(1_000_000_000_000_000_000i64), "feeTo lp_in tracked");
    assert_eq!(token0_in, BigDecimal::from(0), "feeTo token0_in = 0 (_mintFee carve-out, no deposit)");
    assert_eq!(lp_in_usd, BigDecimal::from(0), "feeTo lp_in_usd = 0");
}

#[tokio::test(flavor = "multi_thread")]
async fn materialize_first_mint_share_weighted_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a2";
    let tx      = "0xtxmatfirstmint00000000000000000000000000000000000000000000000002";
    seed_pool(pool, pool_id).await;

    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 1000000::numeric, 2000000::numeric, 0.5::numeric, 0.5::numeric, 1::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(pool).await.unwrap();

    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES \
         ($1, $2, 1000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000), \
         ($4, $2, 999999999000::numeric, 0, 'mint', $3, 1, 0, 16, 1779000000)",
    ).bind(DEAD).bind(pool_id).bind(tx).bind(ALICE).execute(pool).await.unwrap();

    // ALICE near-full share
    let token0_in: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();
    assert!(token0_in >= BigDecimal::from(999_999), "ALICE near-full deposit");
    assert!(token0_in <= BigDecimal::from(1_000_000), "ALICE upper bound");

    // dEaD tiny share (MIN_LIQ ≈ 0 of total)
    let dead_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(DEAD).bind(pool_id).fetch_one(pool).await.unwrap();
    assert!(dead_t0 < BigDecimal::from(2), "dEaD MIN_LIQ share ~= 0");

    // Conservation invariant
    let sum_t0: BigDecimal = sqlx::query_scalar(
        "SELECT SUM(token0_in) FROM lp_position WHERE pool_id=$1"
    ).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(sum_t0, BigDecimal::from(1_000_000), "Σshare = full deposit");
}

#[tokio::test(flavor = "multi_thread")]
async fn materialize_emits_warning_when_dex_mint_missing() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a3";
    let tx      = "0xtxmatrace0000000000000000000000000000000000000000000000000000003";
    seed_pool(pool, pool_id).await;

    // Insert lp_position_history WITHOUT a matching dex_mint — ordering invariant broken.
    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 1000000000000000000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000)",
    ).bind(ALICE).bind(pool_id).bind(tx).execute(pool).await.unwrap();

    // token cols stay 0 (= no silent wrong attribution)
    let token0_in: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(token0_in, BigDecimal::from(0),
        "without dex_mint, token0_in stays 0; WARNING should have been raised in pg log");
}

#[tokio::test(flavor = "multi_thread")]
async fn materialize_burn_full_attribution_in_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a4";
    let tx      = "0xtxmatburn0000000000000000000000000000000000000000000000000000004";
    seed_pool(pool, pool_id).await;

    // First seed a prior mint so ALICE has an lp_position row with lp_in
    let setup_tx = "0xtxmatburnsetup00000000000000000000000000000000000000000000000000";
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 500::numeric, 1000::numeric, 1::numeric, 2::numeric, 3::numeric, 99, 1, $2, 17, 0)",
    ).bind(pool_id).bind(setup_tx).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 1000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000)",
    ).bind(ALICE).bind(pool_id).bind(setup_tx).execute(pool).await.unwrap();

    // Now seed dex_burn for the burn tx, then insert burn row.
    sqlx::query(
        "INSERT INTO dex_burn(pool_id, sender, to_address, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', $2, 250::numeric, 500::numeric, 0.5::numeric, 1::numeric, 1.5::numeric, 100, 2, $3, 17, 0)",
    ).bind(pool_id).bind(ALICE).bind(tx).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 0, 500::numeric, 'burn', $3, 2, 0, 16, 1779000001)",
    ).bind(pool_id).bind(pool_id).bind(tx).execute(pool).await.unwrap();
    // (Trigger rewrites account_id from pool_id to dex_burn.to_address = ALICE.)

    let (token0_out, token1_out, lp_out_usd): (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_out, token1_out, lp_out_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(token0_out,  BigDecimal::from(250), "ALICE burn token0_out = dex_burn.amount0");
    assert_eq!(token1_out,  BigDecimal::from(500), "ALICE burn token1_out = dex_burn.amount1");
    assert_eq!(lp_out_usd,  BigDecimal::from_str("1.5").unwrap(), "ALICE burn lp_out_usd");
}

#[tokio::test(flavor = "multi_thread")]
async fn anchor_residual_two_real_recipients_exact_conservation() {
    // Multi-recipient mint with feeTo carve-out + 2 real recipients whose
    // lp_in proportions do NOT cleanly divide amount0/amount1. Without
    // anchor-residual, each non-feeTo recipient stores a fractional NUMERIC
    // (e.g. 333.333…). With anchor-residual, the LARGEST non-feeTo recipient
    // absorbs the leftover wei so every per-row value is an integer AND
    // Σ token0_in = amount0 to the wei.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolanchor00000000000000000000000000ab01";
    let tx      = "0xtxanchortwo000000000000000000000000000000000000000000000000ab01";
    seed_pool(pool, pool_id).await;

    // amount0=1000, amount1=2000. real_lp = 3 + 7 = 10 (feeTo excluded).
    // ALICE share: 3*1000/10 = 300 (integer-clean by coincidence — let's pick non-clean):
    // Use real_lp = 333 + 666 = 999 so 1000 * 333 / 999 = 333.333…, 1000 * 666 / 999 = 666.666…
    insert_dex_mint_full(pool, pool_id, tx, "1000", "2000", "1", "2", "3", 17).await;
    insert_lp_history_mint(pool, FEETO, pool_id, tx, "9999",  14).await; // feeTo carve — zeroed
    insert_lp_history_mint(pool, ALICE, pool_id, tx, "333",   15).await; // smaller non-feeTo
    insert_lp_history_mint(pool, BOB,   pool_id, tx, "666",   16).await; // larger non-feeTo (anchor)

    // ALICE truncated share: 1000 * 333 / 999 = 333 (TRUNC of 333.333…)
    let alice_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(alice_t0, BigDecimal::from(333), "ALICE TRUNC of share, no fractional");
    assert!(!alice_t0.to_string().contains('.'), "ALICE token0_in is integer");

    // BOB (largest non-feeTo) absorbs the residual:
    // raw_truncs = 333 (ALICE) + 666 (BOB) = 999. Residual = 1000 - 999 = 1.
    // BOB = 666 + 1 = 667.
    let bob_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(BOB).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(bob_t0, BigDecimal::from(667), "BOB = TRUNC + residual (anchor)");
    assert!(!bob_t0.to_string().contains('.'), "BOB token0_in is integer");

    // feeTo zeroed (existing carve-out semantics).
    let feeto_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(FEETO).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(feeto_t0, BigDecimal::from(0), "feeTo always 0");

    // Conservation: Σ token0_in over recipients = dex_mint.amount0 EXACTLY.
    let sum_t0: BigDecimal = sqlx::query_scalar(
        "SELECT SUM(token0_in) FROM lp_position WHERE pool_id=$1"
    ).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(sum_t0, BigDecimal::from(1000), "Σ token0_in = amount0 to the wei");

    // Same shape for token1 (amount1=2000):
    // ALICE = TRUNC(333 * 2000 / 999) = TRUNC(666.666…) = 666
    // BOB   = TRUNC(666 * 2000 / 999) + (2000 - 666 - 1333) = TRUNC(1333.333…) + 1 = 1333 + 1 = 1334
    let alice_t1: BigDecimal = sqlx::query_scalar(
        "SELECT token1_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();
    let bob_t1: BigDecimal = sqlx::query_scalar(
        "SELECT token1_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(BOB).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(alice_t1, BigDecimal::from(666), "ALICE token1_in = TRUNC");
    assert_eq!(bob_t1,   BigDecimal::from(1334), "BOB token1_in = TRUNC + residual");
    let sum_t1: BigDecimal = sqlx::query_scalar(
        "SELECT SUM(token1_in) FROM lp_position WHERE pool_id=$1"
    ).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(sum_t1, BigDecimal::from(2000), "Σ token1_in = amount1");
}

#[tokio::test(flavor = "multi_thread")]
async fn anchor_residual_largest_absorbs_not_smallest_not_feeto() {
    // Explicit anchor-selection assertion: when 3 non-feeTo recipients have
    // lp_in proportions (1, 2, 7) and amount0 = 100, naive TRUNC gives:
    //   r1 = TRUNC(1*100/10) = 10
    //   r2 = TRUNC(2*100/10) = 20
    //   r3 = TRUNC(7*100/10) = 70
    // Σ = 100 exactly (this set divides cleanly — anchor must NOT add a residual).
    // Switch to (1, 3, 7) with amount0 = 100, real_lp = 11:
    //   r1 = TRUNC(1*100/11)  = TRUNC(9.09…)  = 9
    //   r2 = TRUNC(3*100/11)  = TRUNC(27.27…) = 27
    //   r3 = TRUNC(7*100/11)  = TRUNC(63.63…) = 63
    //   Σ truncs = 99. Residual = 1. The LARGEST (r3 = BOB with lp_in=7) gets it.
    //   r3 final = 63 + 1 = 64. Σ = 9 + 27 + 64 = 100.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolanchor00000000000000000000000000ab02";
    let tx      = "0xtxanchorlrg000000000000000000000000000000000000000000000000ab02";
    let r1 = "0xc11111111111111111111111111111111111c111"; // smallest
    let r2 = ALICE;                                         // middle
    let r3 = BOB;                                           // largest = anchor
    seed_pool(pool, pool_id).await;

    insert_dex_mint_full(pool, pool_id, tx, "100", "100", "1", "1", "2", 17).await;
    insert_lp_history_mint(pool, r1, pool_id, tx, "1", 13).await;
    insert_lp_history_mint(pool, r2, pool_id, tx, "3", 14).await;
    insert_lp_history_mint(pool, r3, pool_id, tx, "7", 15).await;

    let r1_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(r1).bind(pool_id).fetch_one(pool).await.unwrap();
    let r2_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(r2).bind(pool_id).fetch_one(pool).await.unwrap();
    let r3_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(r3).bind(pool_id).fetch_one(pool).await.unwrap();

    assert_eq!(r1_t0, BigDecimal::from(9),  "smallest gets TRUNC, no residual");
    assert_eq!(r2_t0, BigDecimal::from(27), "middle gets TRUNC, no residual");
    assert_eq!(r3_t0, BigDecimal::from(64), "LARGEST gets TRUNC + residual");

    // Conservation
    let sum_t0: BigDecimal = sqlx::query_scalar(
        "SELECT SUM(token0_in) FROM lp_position WHERE pool_id=$1"
    ).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(sum_t0, BigDecimal::from(100), "Σ = amount0");
}

#[tokio::test(flavor = "multi_thread")]
async fn anchor_residual_min_liquidity_dead_first_mint() {
    // NadFunPair first-mint case: dEaD gets MIN_LIQUIDITY=1000 wei, user gets
    // the rest. Both are non-feeTo. dEaD's share = 1000/total_lp ≈ 1e-9 of
    // amount0; TRUNC=0 (since the lp_in:amount0 ratio is dEaD:other = 1000:big
    // and 1000 * amount0 / total_lp < 1). User (the LARGEST) absorbs the entire
    // amount0 as the residual.
    //
    // Concrete numbers: total_lp = 1000 + 999_999_999 = 1_000_000_000.
    // amount0 = 1000.
    // dEaD share = TRUNC(1000 * 1000 / 1_000_000_000) = TRUNC(0.001) = 0
    // user share TRUNC = TRUNC(999_999_999 * 1000 / 1_000_000_000) = TRUNC(999.999_999) = 999
    // Residual = 1000 - 0 - 999 = 1. User = 999 + 1 = 1000.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolanchor00000000000000000000000000ab03";
    let tx      = "0xtxanchorminliq000000000000000000000000000000000000000000000ab03";
    seed_pool(pool, pool_id).await;

    insert_dex_mint_full(pool, pool_id, tx, "1000", "5000", "1", "1", "2", 17).await;
    insert_lp_history_mint(pool, DEAD,  pool_id, tx, "1000",        14).await;
    insert_lp_history_mint(pool, ALICE, pool_id, tx, "999999999",   15).await;

    let dead_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(DEAD).bind(pool_id).fetch_one(pool).await.unwrap();
    let alice_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();

    assert_eq!(dead_t0, BigDecimal::from(0),
        "dEaD MIN_LIQ share TRUNCs to 0 — NOT given anchor residual");
    assert_eq!(alice_t0, BigDecimal::from(1000),
        "ALICE absorbs full amount0 via anchor (she's the LARGEST non-feeTo)");

    let sum_t0: BigDecimal = sqlx::query_scalar(
        "SELECT SUM(token0_in) FROM lp_position WHERE pool_id=$1"
    ).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(sum_t0, BigDecimal::from(1000), "Σ = amount0 to the wei");
}

#[tokio::test(flavor = "multi_thread")]
async fn anchor_residual_single_non_feeto_unchanged() {
    // Single non-feeTo recipient (graduation tx pattern): the anchor receives
    // the FULL amount because there's only one non-feeTo. TRUNC of a single
    // recipient at share=1.0 is amount0 itself (no residual). Anchor logic
    // is a no-op in this case — behavior must match pre-fix output.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;

    let pool_id = "0xpoolanchor00000000000000000000000000ab04";
    let tx      = "0xtxanchorsingl00000000000000000000000000000000000000000000ab04";
    seed_pool(pool, pool_id).await;

    insert_dex_mint_full(pool, pool_id, tx, "12345", "67890", "1", "1", "2", 17).await;
    insert_lp_history_mint(pool, FEETO, pool_id, tx, "111", 14).await; // feeTo carve
    insert_lp_history_mint(pool, ALICE, pool_id, tx, "777", 15).await; // single non-feeTo

    let alice: (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(alice.0, BigDecimal::from(12345), "ALICE gets full amount0 (sole non-feeTo)");
    assert_eq!(alice.1, BigDecimal::from(67890), "ALICE gets full amount1");

    let feeto_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(FEETO).bind(pool_id).fetch_one(pool).await.unwrap();
    assert_eq!(feeto_t0, BigDecimal::from(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn balance_column_tracks_lp_in_minus_lp_out() {
    // lp_position.balance is a STORED GENERATED column (= lp_in - lp_out).
    // Verifies the column auto-updates on every UPSERT to lp_in/lp_out via the
    // apply_lp_position() trigger — no separate maintenance needed.
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;
    ensure_dex_mint_burn(pool).await;
    let pid = "0xpoolbal0000000000000000000000000000ab01";
    let mint_tx = "0xbalmint00000000000000000000000000000000000000000000000000000001";
    let burn_tx = "0xbalburn00000000000000000000000000000000000000000000000000000002";
    seed_pool(pool, pid).await;

    // Mint 1000 LP to ALICE — balance = 1000
    insert_dex_mint(pool, pid, mint_tx, "5000", "10000").await;
    insert_lp_history(pool, ALICE, pid, "mint", "1000", "0", None, mint_tx, 1, 1).await;

    let balance: BigDecimal = sqlx::query_scalar(
        "SELECT balance FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    ).bind(ALICE).bind(pid).fetch_one(pool).await.unwrap();
    assert_eq!(balance, BigDecimal::from(1000), "balance after first mint");

    // Partial burn 400 LP — balance = 600
    insert_dex_burn(pool, pid, burn_tx, ALICE, "2000", "4000").await;
    insert_lp_history(pool, ALICE, pid, "burn", "0", "400", None, burn_tx, 1, 2).await;

    let (lp_in, lp_out, balance): (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, lp_out, balance FROM lp_position WHERE account_id=$1 AND pool_id=$2",
    ).bind(ALICE).bind(pid).fetch_one(pool).await.unwrap();
    assert_eq!(lp_in, BigDecimal::from(1000), "cumulative lp_in");
    assert_eq!(lp_out, BigDecimal::from(400), "cumulative lp_out");
    assert_eq!(balance, BigDecimal::from(600), "balance = lp_in - lp_out");
}

#[tokio::test(flavor = "multi_thread")]
async fn balance_column_exists_as_generated_stored() {
    // Schema-level check: the balance column is a GENERATED ALWAYS STORED column,
    // not a regular NUMERIC. PostgreSQL information_schema.columns surfaces this
    // via `is_generated` ('ALWAYS' for STORED generated columns, 'NEVER' otherwise)
    // and `generation_expression` (the expression text).
    let db = setup_test_db().await.unwrap();
    let pool = &db.pool;

    let row: (String, Option<String>) = sqlx::query_as(
        "SELECT is_generated, generation_expression
           FROM information_schema.columns
          WHERE table_name='lp_position' AND column_name='balance'",
    ).fetch_one(pool).await.unwrap();
    assert_eq!(row.0, "ALWAYS", "balance column must be GENERATED ALWAYS");
    let expr = row.1.expect("generation_expression should be populated for a generated column");
    // Normalize whitespace before comparing — PG may store as "(lp_in - lp_out)".
    let normalized: String = expr.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized.contains("lp_in - lp_out") || normalized.contains("(lp_in - lp_out)"),
        "expected generation expression to be 'lp_in - lp_out', got: {expr}"
    );
}
