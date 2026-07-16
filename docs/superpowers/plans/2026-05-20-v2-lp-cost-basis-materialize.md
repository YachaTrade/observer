# V2 LP Cost-Basis Materialize into lp_position Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Materialize the `lp_position_cost_basis` view's share-weighted, feeTo-aware cost basis computation into the `lp_position` aggregate table via a single `AFTER STATEMENT` trigger on `lp_position_history`. Consumers query `lp_position` directly with a single-table SELECT — no JOIN needed — and `PR #211 LP fee/APR` queries become trivial.

**Architecture:**
- Add a `FOR EACH STATEMENT` `AFTER INSERT` trigger on `lp_position_history` that fires once per batch. The trigger uses the **REFERENCING NEW TABLE** transition table to see all newly-inserted rows, joins them with `dex_mint` / `dex_burn` (guaranteed to exist by the V2 DEX → Token stream ordering invariant), computes share-weighted cost basis (feeTo zeroed), `UPDATE`s those `lp_position_history` rows' token / USD columns, and rebuilds the `lp_position` aggregate cost basis columns absolutely for each affected `(account_id, pool_id)` pair.
- View `lp_position_cost_basis` stays as the canonical SQL definition of the math, but the trigger inlines the same logic against the transition table. Consumers can read either, but `lp_position` is recommended (faster, single-table).
- Ordering invariant: if `dex_mint` is missing for any mint row at trigger fire time, raise a `WARNING` (matching the existing `RAISE WARNING 'LP burn without matching dex_burn'` pattern) so the operator notices breakage instead of silent zero-attribution.

**Tech Stack:** PostgreSQL plpgsql with `REFERENCING NEW TABLE` (PG ≥ 10), sqlx integration tests in Rust, two-track migration system (base `0021_lp_position.sql` for fresh DBs + `v2_upgrade_lp_position.sql` for existing prod DBs — function bodies must stay byte-identical).

---

## File Structure

| Path | Action | Responsibility |
|------|--------|----------------|
| `migrations/0021_lp_position.sql` | Modify | Add `refresh_lp_position_cost_basis()` function + statement trigger; rest unchanged from PR #216 |
| `migrations/0029_lp_position_cost_basis_view.sql` | Modify | View body stays (used internally for analytics / back-compat); remove the "view is the source of truth" header note |
| `migrations/v2_upgrade_lp_position.sql` | Modify | Mirror the new function + trigger; add one-time absolute backfill at file tail |
| `observer/tests/lp_position_history_trigger.rs` | Modify | Add 5 new tests for materialized behavior; update existing tests that asserted view-only output to read `lp_position` directly |
| `observer/tests/common/mod.rs` | No change | (Already applies `v2_upgrade_lp_position.sql` for the test runner — added in unmerged consolidation work, BUT that branch is paused; this plan must work without it. If the test harness needs the upgrade applied, add `name == "v2_upgrade_lp_position.sql"` to the include filter as part of Task 7.) |

No changes to Rust source code (`src/event/common/token/*.rs`, `src/event/v2/dex/*.rs`).

---

## Task 1: Branch + baseline cargo test

**Files:**
- New branch: `design/v2-lp-cost-basis-materialize` (in both the migrations submodule and the observer parent)
- Baseline: existing `tests/lp_position_history_trigger.rs` (15 tests) must pass against current `migrations` tip `8f6351c`

- [ ] **Step 1: Create migrations branch off origin/v2**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git fetch origin v2
git checkout -b design/v2-lp-cost-basis-materialize origin/v2
git log --oneline -1
# Expected: 8f6351c feat(lp-position): view-based cost basis ... (#36)
```

- [ ] **Step 2: Create observer branch off origin/v2**

```bash
cd /Users/gyu/project/nads-pump/observer
git fetch origin v2
git checkout -b design/v2-lp-cost-basis-materialize origin/v2
git rev-parse HEAD
# Expected: c30adf085e... (v2 tip with PR #218 merged)
```

- [ ] **Step 3: Run baseline tests**

```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --test lp_position_history_trigger -- --test-threads=1 2>&1 | tail -10
```

Expected: `test result: ok. 15 passed; 0 failed`. If anything fails, stop and triage before editing the migration.

---

## Task 2: Write failing test — graduation pool materialized into lp_position

**Files:**
- Modify: `observer/tests/lp_position_history_trigger.rs` (append new tests near the existing view tests at line ~840)

- [ ] **Step 1: Add the new test**

Append to `tests/lp_position_history_trigger.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn materialize_graduation_pool_feeto_zero_dead_full_in_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = db.pool;
    ensure_dex_mint_burn(&pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a1";
    let tx      = "0xtxmatgraduation0000000000000000000000000000000000000000000000000001";
    seed_pool(&pool, pool_id).await;

    // dex_mint MUST exist before lp_position_history rows insert (V2_DEX → Token ordering)
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 100::numeric, 200::numeric, 2.5::numeric, 2.5::numeric, 5::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

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
    .execute(&pool).await.unwrap();

    // After statement trigger fires, lp_position MUST contain materialized cost basis
    let (lp_in, token0_in, token1_in, lp_in_usd): (BigDecimal, BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, token0_in, token1_in, lp_in_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(DEAD).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(lp_in,     BigDecimal::from(999_000_000_000_000_000_000u128), "dEaD lp_in tracked");
    assert_eq!(token0_in, BigDecimal::from(100),  "dEaD token0_in = full deposit (sole non-fee recipient)");
    assert_eq!(token1_in, BigDecimal::from(200),  "dEaD token1_in = full");
    assert_eq!(lp_in_usd, BigDecimal::from(5),    "dEaD lp_in_usd = full");

    let (lp_in, token0_in, lp_in_usd): (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, token0_in, lp_in_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(FEETO).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(lp_in,     BigDecimal::from(1_000_000_000_000_000_000i64), "feeTo lp_in tracked");
    assert_eq!(token0_in, BigDecimal::from(0), "feeTo token0_in = 0 (_mintFee carve-out, no deposit)");
    assert_eq!(lp_in_usd, BigDecimal::from(0), "feeTo lp_in_usd = 0");
}
```

- [ ] **Step 2: Run the test — verify it FAILS**

```bash
cargo test --test lp_position_history_trigger materialize_graduation_pool_feeto_zero_dead_full_in_lp_position -- --test-threads=1 2>&1 | tail -15
```

Expected: FAIL with assertion on `token0_in` (= 0, but test expects 100) OR `feeTo lp_in_usd != 0` style. Whatever the failure, it must be on a `token0_in` / `lp_in_usd` assertion — not a panic. Confirms the trigger hasn't been written yet.

- [ ] **Step 3: Commit failing test**

```bash
cd /Users/gyu/project/nads-pump/observer
git add tests/lp_position_history_trigger.rs
git commit -m "test(lp-position): failing test for materialized cost basis on lp_position aggregate"
```

---

## Task 3: Write failing tests — share-weighting + race WARNING + first-mint MIN_LIQUIDITY case

**Files:**
- Modify: `observer/tests/lp_position_history_trigger.rs`

- [ ] **Step 1: Add share-weighted MIN_LIQUIDITY test**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn materialize_first_mint_share_weighted_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = db.pool;
    ensure_dex_mint_burn(&pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a2";
    let tx      = "0xtxmatfirstmint0000000000000000000000000000000000000000000000000002";
    seed_pool(&pool, pool_id).await;

    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 1000000::numeric, 2000000::numeric, 0.5::numeric, 0.5::numeric, 1::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES \
         ($1, $2, 1000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000), \
         ($4, $2, 999999999000::numeric, 0, 'mint', $3, 1, 0, 16, 1779000000)",
    ).bind(DEAD).bind(pool_id).bind(tx).bind(ALICE).execute(&pool).await.unwrap();

    // ALICE near-full share
    let token0_in: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert!(token0_in >= BigDecimal::from(999_999), "ALICE near-full deposit");
    assert!(token0_in <= BigDecimal::from(1_000_000), "ALICE upper bound");

    // dEaD tiny share (MIN_LIQ ≈ 0 of total)
    let dead_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(DEAD).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert!(dead_t0 < BigDecimal::from(2), "dEaD MIN_LIQ share ~= 0");

    // Conservation invariant
    let sum_t0: BigDecimal = sqlx::query_scalar(
        "SELECT SUM(token0_in) FROM lp_position WHERE pool_id=$1"
    ).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(sum_t0, BigDecimal::from(1_000_000), "Σshare = full deposit");
}
```

- [ ] **Step 2: Add late-arriving dex_mint test (= race WARNING)**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn materialize_emits_warning_when_dex_mint_missing() {
    let db = setup_test_db().await.unwrap();
    let pool = db.pool;
    ensure_dex_mint_burn(&pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a3";
    let tx      = "0xtxmatrace0000000000000000000000000000000000000000000000000000000003";
    seed_pool(&pool, pool_id).await;

    // Insert lp_position_history WITHOUT a matching dex_mint — ordering invariant broken.
    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 1000000000000000000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000)",
    ).bind(ALICE).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    // token cols stay 0 (= no silent wrong attribution)
    let token0_in: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(token0_in, BigDecimal::from(0),
        "without dex_mint, token0_in stays 0; WARNING should have been raised in pg log");
}
```

- [ ] **Step 3: Add burn materialization test**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn materialize_burn_full_attribution_in_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = db.pool;
    ensure_dex_mint_burn(&pool).await;

    let pool_id = "0xpoolmat000000000000000000000000000000a4";
    let tx      = "0xtxmatburn00000000000000000000000000000000000000000000000000000004";
    seed_pool(&pool, pool_id).await;

    // First seed a prior mint so ALICE has an lp_position row with lp_in
    let setup_tx = "0xtxmatburnsetup0000000000000000000000000000000000000000000000000000";
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 500::numeric, 1000::numeric, 1::numeric, 2::numeric, 3::numeric, 99, 1, $2, 17, 0)",
    ).bind(pool_id).bind(setup_tx).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 1000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000)",
    ).bind(ALICE).bind(pool_id).bind(setup_tx).execute(&pool).await.unwrap();

    // Now seed dex_burn for the burn tx, then insert burn row.
    sqlx::query(
        "INSERT INTO dex_burn(pool_id, sender, to_address, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', $2, 250::numeric, 500::numeric, 0.5::numeric, 1::numeric, 1.5::numeric, 100, 2, $3, 17, 0)",
    ).bind(pool_id).bind(ALICE).bind(tx).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 0, 500::numeric, 'burn', $3, 2, 0, 16, 1779000001)",
    ).bind(pool_id).bind(pool_id).bind(tx).execute(&pool).await.unwrap();
    // (Trigger rewrites account_id from pool_id to dex_burn.to_address = ALICE.)

    let (token0_out, token1_out, lp_out_usd): (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_out, token1_out, lp_out_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(token0_out,  BigDecimal::from(250), "ALICE burn token0_out = dex_burn.amount0");
    assert_eq!(token1_out,  BigDecimal::from(500), "ALICE burn token1_out = dex_burn.amount1");
    assert_eq!(lp_out_usd,  BigDecimal::from_str("1.5").unwrap(), "ALICE burn lp_out_usd");
}
```

You will need `use std::str::FromStr;` at the top of the test file if it isn't already imported.

- [ ] **Step 4: Run the new tests — verify they FAIL**

```bash
cargo test --test lp_position_history_trigger \
  materialize_first_mint_share_weighted_lp_position \
  materialize_emits_warning_when_dex_mint_missing \
  materialize_burn_full_attribution_in_lp_position \
  -- --test-threads=1 2>&1 | tail -25
```

Expected: 3 failures, all on `token0_in` / `token0_out` / `lp_out_usd` assertions (= still 0 because trigger not yet written).

- [ ] **Step 5: Commit failing tests**

```bash
git add tests/lp_position_history_trigger.rs
git commit -m "test(lp-position): failing tests for share-weighted materialize + race WARNING + burn"
```

---

## Task 4: Write the `refresh_lp_position_cost_basis()` trigger function in 0021

**Files:**
- Modify: `migrations/0021_lp_position.sql` (append a new function + trigger immediately after `apply_lp_position()` function, BEFORE the existing trigger declarations)

- [ ] **Step 1: Append the function + trigger after `apply_lp_position()`**

Edit `0021_lp_position.sql`. Locate the line `$$ LANGUAGE plpgsql;` that closes the `apply_lp_position()` function body (currently at line 181). Immediately after that closing line, BEFORE the `DROP TRIGGER IF EXISTS trg_lp_position_on_history …` block, insert:

```sql
-- AFTER STATEMENT: materialize the lp_position_cost_basis view definition into
-- both lp_position_history (per-row token/USD cols) and lp_position (aggregate
-- token/USD cols) for the (pool_id, transaction_hash) tuples touched in this
-- INSERT batch. Fires once per INSERT statement (sees the whole batch via the
-- NEW transition table), so share-weighted attribution is correct regardless of
-- batch row order.
--
-- Ordering invariant: V2 DEX stream completes before Token stream, so dex_mint
-- and dex_burn rows are already in the DB when this fires. If a row's matching
-- dex_mint/dex_burn is missing (= invariant broken), RAISE WARNING and leave
-- token cols at 0 — never silently mis-attribute.
CREATE OR REPLACE FUNCTION refresh_lp_position_cost_basis()
RETURNS TRIGGER AS $$
DECLARE
    feeto CONSTANT TEXT := '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a';
    affected_count INT;
BEGIN
    -- (1) MINT side: re-fill lp_position_history.token cols for every mint row
    -- in (pool_id, transaction_hash) tuples touched by this batch.
    -- Uses the same share-weighted math as the lp_position_cost_basis view.
    WITH affected_mint_txs AS (
        SELECT DISTINCT pool_id, transaction_hash
          FROM new_rows
         WHERE event_type = 'mint'
    ),
    mint_with_dm AS (
        SELECT
            ph.account_id, ph.pool_id, ph.transaction_hash, ph.tx_index, ph.log_index,
            ph.lp_in,
            dm.amount0    AS dm_amount0,
            dm.amount1    AS dm_amount1,
            dm.value      AS dm_value,
            dm.token0_usd AS dm_token0_usd,
            dm.token1_usd AS dm_token1_usd,
            dm.log_index  AS dm_log_index
          FROM lp_position_history ph
          JOIN affected_mint_txs a
            ON a.pool_id = ph.pool_id AND a.transaction_hash = ph.transaction_hash
          JOIN LATERAL (
              SELECT *
                FROM dex_mint
               WHERE pool_id = ph.pool_id
                 AND transaction_hash = ph.transaction_hash
                 AND log_index > ph.log_index
               ORDER BY log_index ASC LIMIT 1
          ) dm ON true
         WHERE ph.event_type = 'mint'
    ),
    mint_costs AS (
        SELECT
            ph.account_id, ph.pool_id, ph.transaction_hash, ph.tx_index, ph.log_index,
            CASE WHEN LOWER(ph.account_id) = feeto THEN 0
                 ELSE ph.lp_in * ph.dm_amount0 / NULLIF(r.real_lp, 0)
            END AS token0_in,
            CASE WHEN LOWER(ph.account_id) = feeto THEN 0
                 ELSE ph.lp_in * ph.dm_amount1 / NULLIF(r.real_lp, 0)
            END AS token1_in,
            CASE WHEN LOWER(ph.account_id) = feeto THEN 0
                 ELSE ROUND(ph.lp_in * COALESCE(ph.dm_token0_usd, 0) / NULLIF(r.real_lp, 0), 10)
            END AS token0_in_usd,
            CASE WHEN LOWER(ph.account_id) = feeto THEN 0
                 ELSE ROUND(ph.lp_in * COALESCE(ph.dm_token1_usd, 0) / NULLIF(r.real_lp, 0), 10)
            END AS token1_in_usd,
            CASE WHEN LOWER(ph.account_id) = feeto THEN 0
                 ELSE ROUND(ph.lp_in * COALESCE(ph.dm_value, 0) / NULLIF(r.real_lp, 0), 10)
            END AS lp_in_usd
          FROM mint_with_dm ph
          JOIN LATERAL (
              SELECT COALESCE(SUM(sib.lp_in), 0) AS real_lp
                FROM mint_with_dm sib
               WHERE sib.pool_id = ph.pool_id
                 AND sib.transaction_hash = ph.transaction_hash
                 AND sib.dm_log_index = ph.dm_log_index
                 AND LOWER(sib.account_id) <> feeto
          ) r ON true
    )
    UPDATE lp_position_history h
       SET token0_in     = c.token0_in,
           token1_in     = c.token1_in,
           token0_in_usd = c.token0_in_usd,
           token1_in_usd = c.token1_in_usd,
           lp_in_usd     = c.lp_in_usd
      FROM mint_costs c
     WHERE h.account_id       = c.account_id
       AND h.pool_id          = c.pool_id
       AND h.transaction_hash = c.transaction_hash
       AND h.tx_index         = c.tx_index
       AND h.log_index        = c.log_index;

    -- (2) BURN side: re-fill lp_position_history.token cols for burn rows
    -- in (pool_id, transaction_hash) tuples touched by this batch.
    WITH affected_burn_txs AS (
        SELECT DISTINCT pool_id, transaction_hash
          FROM new_rows
         WHERE event_type = 'burn'
    )
    UPDATE lp_position_history h
       SET token0_out     = db.amount0,
           token1_out     = db.amount1,
           token0_out_usd = ROUND(COALESCE(db.token0_usd, 0), 10),
           token1_out_usd = ROUND(COALESCE(db.token1_usd, 0), 10),
           lp_out_usd     = ROUND(COALESCE(db.value,      0), 10)
      FROM affected_burn_txs a,
           LATERAL (
              SELECT *
                FROM dex_burn
               WHERE pool_id = a.pool_id
                 AND transaction_hash = a.transaction_hash
                 AND log_index > h.log_index
               ORDER BY log_index ASC LIMIT 1
           ) db
     WHERE h.pool_id          = a.pool_id
       AND h.transaction_hash = a.transaction_hash
       AND h.event_type       = 'burn';

    -- (3) WARNING for rows that landed without a matching dex_mint or dex_burn
    -- (= ordering invariant broken: V2 DEX should have finished first).
    FOR affected_count IN
        SELECT 1 FROM new_rows n
         WHERE n.event_type = 'mint'
           AND NOT EXISTS (SELECT 1 FROM dex_mint dm
                            WHERE dm.pool_id = n.pool_id
                              AND dm.transaction_hash = n.transaction_hash)
         LIMIT 1
    LOOP
        RAISE WARNING 'LP mint without matching dex_mint at trigger time — ordering invariant broken; affected pool/tx pairs in lp_position_history this batch';
    END LOOP;
    FOR affected_count IN
        SELECT 1 FROM new_rows n
         WHERE n.event_type = 'burn'
           AND NOT EXISTS (SELECT 1 FROM dex_burn db
                            WHERE db.pool_id = n.pool_id
                              AND db.transaction_hash = n.transaction_hash)
         LIMIT 1
    LOOP
        RAISE WARNING 'LP burn without matching dex_burn at trigger time — ordering invariant broken; affected pool/tx pairs in lp_position_history this batch';
    END LOOP;

    -- (4) Aggregate rebuild: for each (account_id, pool_id) touched by this
    -- batch, recompute lp_position.token* / *_usd absolutely from history.
    -- lp_in / lp_out are NOT touched here — they're maintained by the existing
    -- apply_lp_position() per-row UPSERT. This trigger only owns cost basis.
    WITH affected_pairs AS (
        SELECT DISTINCT account_id, pool_id FROM new_rows
        UNION
        -- Also include any row whose account_id was rewritten by the BEFORE
        -- INSERT trigger (burn re-attribution): the new_rows transition table
        -- holds the original (pre-rewrite) account_id, so we need to look at
        -- the actual stored rows for any affected tx.
        SELECT DISTINCT h.account_id, h.pool_id
          FROM lp_position_history h
          JOIN (SELECT DISTINCT pool_id, transaction_hash FROM new_rows) t
            ON t.pool_id = h.pool_id AND t.transaction_hash = h.transaction_hash
    ),
    aggregates AS (
        SELECT h.account_id, h.pool_id,
               SUM(h.token0_in)      AS token0_in,
               SUM(h.token0_out)     AS token0_out,
               SUM(h.token1_in)      AS token1_in,
               SUM(h.token1_out)     AS token1_out,
               SUM(h.token0_in_usd)  AS token0_in_usd,
               SUM(h.token0_out_usd) AS token0_out_usd,
               SUM(h.token1_in_usd)  AS token1_in_usd,
               SUM(h.token1_out_usd) AS token1_out_usd,
               SUM(h.lp_in_usd)      AS lp_in_usd,
               SUM(h.lp_out_usd)     AS lp_out_usd
          FROM lp_position_history h
          JOIN affected_pairs ap
            ON ap.account_id = h.account_id AND ap.pool_id = h.pool_id
         GROUP BY h.account_id, h.pool_id
    )
    UPDATE lp_position lp
       SET token0_in      = a.token0_in,
           token0_out     = a.token0_out,
           token1_in      = a.token1_in,
           token1_out     = a.token1_out,
           token0_in_usd  = a.token0_in_usd,
           token0_out_usd = a.token0_out_usd,
           token1_in_usd  = a.token1_in_usd,
           token1_out_usd = a.token1_out_usd,
           lp_in_usd      = a.lp_in_usd,
           lp_out_usd     = a.lp_out_usd
      FROM aggregates a
     WHERE lp.account_id = a.account_id
       AND lp.pool_id    = a.pool_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

- [ ] **Step 2: Register the trigger** — after the existing two `CREATE TRIGGER` blocks (`trg_fill_lp_cost_basis` and `trg_apply_lp_position`), add:

```sql
DROP TRIGGER IF EXISTS trg_refresh_lp_position_cost_basis ON lp_position_history;
CREATE TRIGGER trg_refresh_lp_position_cost_basis
    AFTER INSERT ON lp_position_history
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_lp_position_cost_basis();
```

- [ ] **Step 3: Run the failing tests — they should now pass**

```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --test lp_position_history_trigger \
  materialize_graduation_pool_feeto_zero_dead_full_in_lp_position \
  materialize_first_mint_share_weighted_lp_position \
  materialize_burn_full_attribution_in_lp_position \
  -- --test-threads=1 2>&1 | tail -10
```

Expected: 3 passed. The `materialize_emits_warning_when_dex_mint_missing` test should also pass because `token0_in` stays 0 when dex_mint is absent.

- [ ] **Step 4: Run the WARNING test separately**

```bash
cargo test --test lp_position_history_trigger materialize_emits_warning_when_dex_mint_missing -- --test-threads=1 2>&1 | tail -10
```

Expected: PASS (token0_in = 0 because dex_mint missing, trigger raised WARNING).

- [ ] **Step 5: Run the full lp_position_history_trigger suite — no regressions**

```bash
cargo test --test lp_position_history_trigger -- --test-threads=1 2>&1 | tail -10
```

Expected: `test result: ok. 19 passed; 0 failed` (15 prior + 4 new).

- [ ] **Step 6: Commit**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git add 0021_lp_position.sql
git commit -m "feat(lp-position): materialize cost basis into lp_position via statement trigger

Adds refresh_lp_position_cost_basis() AFTER STATEMENT trigger on
lp_position_history. The trigger inlines the same share-weighted
feeTo-aware math as the lp_position_cost_basis view but writes
the result into lp_position_history.token* and lp_position.token*
columns, so consumers can query lp_position directly with a single-
table SELECT and PR #211 LP fee/APR can read the aggregate denominator
without JOINing dex_mint.

Uses REFERENCING NEW TABLE so the statement-level trigger sees all
NEW rows in the batch and computes share weights against the full
sibling set in one pass. Relies on the documented V2_DEX → Token
stream ordering invariant — if dex_mint or dex_burn is missing at
trigger fire time, raises WARNING and leaves cost basis at 0 rather
than silently mis-attributing."
```

---

## Task 5: Update existing view-based test assertions to read lp_position

**Files:**
- Modify: `observer/tests/lp_position_history_trigger.rs`

The 4 existing view tests (`view_graduation_pool_feeto_zero_dead_full`, `view_standard_first_mint_share_weighted`, `view_add_liquidity_feeto_excluded`, `aggregate_token_cols_stay_zero`) read the view. Keep three of them as-is (they test the view, which still exists as the canonical SQL). One test (`aggregate_token_cols_stay_zero`) asserted that `lp_position` token columns STAY at 0 — that's now wrong under materialization. Flip it.

- [ ] **Step 1: Rename and re-purpose `aggregate_token_cols_stay_zero`**

Locate the test (search `aggregate_token_cols_stay_zero` in `tests/lp_position_history_trigger.rs`). Replace its body so it now asserts that the materialized lp_position cols hold the correct values:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn aggregate_token_cols_materialized_on_lp_position() {
    let db = setup_test_db().await.unwrap();
    let pool = db.pool;
    ensure_dex_mint_burn(&pool).await;
    let pool_id = "0xpool00000000000000000000000000000000poo4";
    let tx      = "0xtxagg00000000000000000000000000000000000000000000000000000004x";
    seed_pool(&pool, pool_id).await;

    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 100::numeric, 200::numeric, 1::numeric, 1::numeric, 2::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO lp_position_history (account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, 1000000000000000000::numeric, 0, 'mint', $3, 1, 0, 15, 1779000000)",
    ).bind(ALICE).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    let (lp_in, t0, t1, lp_usd): (BigDecimal, BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, token0_in, token1_in, lp_in_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(lp_in,  BigDecimal::from(1_000_000_000_000_000_000i64), "lp_in tracked");
    assert_eq!(t0,     BigDecimal::from(100), "token0_in materialized from view math (sole non-fee recipient)");
    assert_eq!(t1,     BigDecimal::from(200), "token1_in materialized");
    assert_eq!(lp_usd, BigDecimal::from(2),   "lp_in_usd materialized");
}
```

- [ ] **Step 2: Run the suite — all tests pass**

```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --test lp_position_history_trigger -- --test-threads=1 2>&1 | tail -10
```

Expected: `test result: ok. 19 passed`.

- [ ] **Step 3: Commit**

```bash
git add tests/lp_position_history_trigger.rs
git commit -m "test(lp-position): flip aggregate_token_cols_stay_zero to assert materialized values"
```

---

## Task 6: One-time absolute backfill in 0021

**Files:**
- Modify: `migrations/0021_lp_position.sql` (replace the existing PR #216 zero-reset backfill with an absolute rebuild)

- [ ] **Step 1: Replace the existing backfill block** (currently sets token cols to 0)

Find the block in `0021_lp_position.sql` that starts `-- One-time backfill: trigger no longer fills token/USD columns` (around line 195 after Task 4's insertions, was around line 195 before). Replace BOTH `UPDATE lp_position_history SET …` and `UPDATE lp_position SET …` zero-resets with:

```sql
-- ----------------------------------------------------------------------
-- One-time backfill: rebuild token/USD columns on lp_position_history
-- (per-row, via the same share-weighted view math) and on lp_position
-- (aggregate, via SUM-from-history). Idempotent — re-running on
-- already-correct data is a no-op.
-- ----------------------------------------------------------------------

-- Backfill lp_position_history.token cols for ALL mint rows using the view
-- definition (which already encodes share-weighted feeTo-zero math).
UPDATE lp_position_history h
   SET token0_in     = v.token0_in,
       token1_in     = v.token1_in,
       token0_in_usd = v.token0_in_usd,
       token1_in_usd = v.token1_in_usd,
       lp_in_usd     = v.lp_in_usd
  FROM lp_position_cost_basis v
 WHERE h.event_type       = 'mint'
   AND h.account_id       = v.account_id
   AND h.pool_id          = v.pool_id
   AND h.transaction_hash = v.transaction_hash
   AND h.tx_index         = v.tx_index
   AND h.log_index        = v.log_index;

UPDATE lp_position_history h
   SET token0_out     = v.token0_out,
       token1_out     = v.token1_out,
       token0_out_usd = v.token0_out_usd,
       token1_out_usd = v.token1_out_usd,
       lp_out_usd     = v.lp_out_usd
  FROM lp_position_cost_basis v
 WHERE h.event_type       = 'burn'
   AND h.account_id       = v.account_id
   AND h.pool_id          = v.pool_id
   AND h.transaction_hash = v.transaction_hash
   AND h.tx_index         = v.tx_index
   AND h.log_index        = v.log_index;

-- Backfill lp_position aggregate from history.
UPDATE lp_position lp
   SET token0_in      = agg.token0_in,
       token0_out     = agg.token0_out,
       token1_in      = agg.token1_in,
       token1_out     = agg.token1_out,
       token0_in_usd  = agg.token0_in_usd,
       token0_out_usd = agg.token0_out_usd,
       token1_in_usd  = agg.token1_in_usd,
       token1_out_usd = agg.token1_out_usd,
       lp_in_usd      = agg.lp_in_usd,
       lp_out_usd     = agg.lp_out_usd
  FROM (
      SELECT account_id, pool_id,
             SUM(token0_in)      AS token0_in,
             SUM(token0_out)     AS token0_out,
             SUM(token1_in)      AS token1_in,
             SUM(token1_out)     AS token1_out,
             SUM(token0_in_usd)  AS token0_in_usd,
             SUM(token0_out_usd) AS token0_out_usd,
             SUM(token1_in_usd)  AS token1_in_usd,
             SUM(token1_out_usd) AS token1_out_usd,
             SUM(lp_in_usd)      AS lp_in_usd,
             SUM(lp_out_usd)     AS lp_out_usd
        FROM lp_position_history
       GROUP BY account_id, pool_id
  ) agg
 WHERE lp.account_id = agg.account_id
   AND lp.pool_id    = agg.pool_id;
```

Note: `lp_position_cost_basis` is defined in `0029_lp_position_cost_basis_view.sql` which runs LATER than `0021`. The view doesn't exist yet at this point in `0021`'s execution. This backfill block must therefore be moved to run AFTER `0029` is applied. Two options:

a. Move the backfill block to the END of `0029_lp_position_cost_basis_view.sql`.
b. Create a new file `0030_lp_position_backfill.sql` containing only this backfill.

Choose (a) — keeps related logic together (view + backfill that reads from view in the same file).

- [ ] **Step 2: Delete the backfill block from 0021 and append it to 0029**

In `0021_lp_position.sql`, delete the entire UPDATE block written above (it was just added — undo). In `0029_lp_position_cost_basis_view.sql`, append the block at the file end.

- [ ] **Step 3: Run tests to confirm 0021 still applies cleanly + backfill in 0029 works**

```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --test lp_position_history_trigger -- --test-threads=1 2>&1 | tail -10
```

Expected: `test result: ok. 19 passed`. (The test runner applies 0029 after 0021, so the backfill executes against tables created by 0021 + dex_mint/dex_burn created by 0023.)

- [ ] **Step 4: Commit**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git add 0021_lp_position.sql 0029_lp_position_cost_basis_view.sql
git commit -m "feat(lp-position): one-time absolute backfill of token cols using view math

Replaces PR #216's zero-reset backfill (which assumed cost basis would
be derived at read time) with an absolute rebuild that materializes the
view output into lp_position_history.token* and lp_position.token*
columns. Idempotent; re-running on correct data is a no-op."
```

---

## Task 7: Mirror in v2_upgrade_lp_position.sql

**Files:**
- Modify: `migrations/v2_upgrade_lp_position.sql`

- [ ] **Step 1: Copy the new `refresh_lp_position_cost_basis()` function body verbatim from 0021**

Open `v2_upgrade_lp_position.sql`. Locate the `CREATE TRIGGER trg_apply_lp_position` block (currently around line 220-223). Immediately after that line, before the next section, paste the full `CREATE OR REPLACE FUNCTION refresh_lp_position_cost_basis() RETURNS TRIGGER AS $$ ... $$ LANGUAGE plpgsql;` block from `0021_lp_position.sql`.

Also paste:

```sql
DROP TRIGGER IF EXISTS trg_refresh_lp_position_cost_basis ON lp_position_history;
CREATE TRIGGER trg_refresh_lp_position_cost_basis
    AFTER INSERT ON lp_position_history
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_lp_position_cost_basis();
```

- [ ] **Step 2: Add the same absolute backfill block AT THE END of v2_upgrade_lp_position.sql**

After the existing `CREATE VIEW lp_position_cost_basis AS …;` and the existing zero-reset backfill (which is currently in v2_upgrade as PR #216 wrote it), DELETE the zero-reset blocks and append the absolute backfill from `0029`. v2_upgrade contains everything in one file because prod already has all source tables.

- [ ] **Step 3: Verify byte-identical invariants**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
echo '--- fill_lp_cost_basis body ---'
diff <(awk '/CREATE OR REPLACE FUNCTION fill_lp_cost_basis/,/^\$\$ LANGUAGE plpgsql/' 0021_lp_position.sql) \
     <(awk '/CREATE OR REPLACE FUNCTION fill_lp_cost_basis/,/^\$\$ LANGUAGE plpgsql/' v2_upgrade_lp_position.sql)
echo '--- apply_lp_position body ---'
diff <(awk '/CREATE OR REPLACE FUNCTION apply_lp_position/,/^\$\$ LANGUAGE plpgsql/' 0021_lp_position.sql) \
     <(awk '/CREATE OR REPLACE FUNCTION apply_lp_position/,/^\$\$ LANGUAGE plpgsql/' v2_upgrade_lp_position.sql)
echo '--- refresh_lp_position_cost_basis body ---'
diff <(awk '/CREATE OR REPLACE FUNCTION refresh_lp_position_cost_basis/,/^\$\$ LANGUAGE plpgsql/' 0021_lp_position.sql) \
     <(awk '/CREATE OR REPLACE FUNCTION refresh_lp_position_cost_basis/,/^\$\$ LANGUAGE plpgsql/' v2_upgrade_lp_position.sql)
echo '--- view body ---'
diff <(awk '/^CREATE OR REPLACE VIEW lp_position_cost_basis/,/^SELECT \* FROM mint_costs/' 0029_lp_position_cost_basis_view.sql 2>/dev/null || awk '/^DROP VIEW IF EXISTS lp_position_cost_basis/,/^SELECT \* FROM mint_costs/' 0029_lp_position_cost_basis_view.sql) \
     <(awk '/^DROP VIEW IF EXISTS lp_position_cost_basis/,/^SELECT \* FROM mint_costs/' v2_upgrade_lp_position.sql)
```

All 4 diffs must print nothing.

- [ ] **Step 4: Make sure test harness applies v2_upgrade_lp_position.sql**

Check `tests/common/mod.rs` around the filter list (search `v2_upgrade_new_tables.sql`). If `v2_upgrade_lp_position.sql` is NOT in the include list, add it so the harness applies it on fresh test DB setup. (If it's already there, no-op.)

```bash
grep 'v2_upgrade_lp_position' /Users/gyu/project/nads-pump/observer/tests/common/mod.rs
# If empty, edit common/mod.rs to add: || name == "v2_upgrade_lp_position.sql"
```

If added:

```bash
cd /Users/gyu/project/nads-pump/observer
git add tests/common/mod.rs
git commit -m "test: apply v2_upgrade_lp_position.sql in test harness for trigger/view coverage"
```

- [ ] **Step 5: Run all tests**

```bash
cargo test --test lp_position_history_trigger -- --test-threads=1 2>&1 | tail -10
cargo test --test v2_controllers -- --test-threads=1 2>&1 | tail -10
```

Expected: both pass with no regressions.

- [ ] **Step 6: Commit migrations side**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git add v2_upgrade_lp_position.sql
git commit -m "feat(lp-position): mirror materialize trigger + absolute backfill in v2_upgrade twin"
```

---

## Task 8: codex review + PRs + merge

- [ ] **Step 1: `/codex review` on migrations branch**

```bash
TMPERR=$(mktemp /tmp/codex-err-XXXXXX.txt)
cd /Users/gyu/project/nads-pump/observer/migrations
timeout 360 codex review --base origin/v2 -c 'model_reasoning_effort="high"' < /dev/null 2>"$TMPERR"
rm -f "$TMPERR"
```

Apply AUTO-FIX items immediately. For ASK items, confirm with the controller before applying.

- [ ] **Step 2: Push + open migrations PR**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git push -u origin design/v2-lp-cost-basis-materialize
gh pr create --repo Naddotfun/migrations --base v2 --head design/v2-lp-cost-basis-materialize \
  --title "feat(lp-position): materialize cost basis into lp_position via statement trigger" \
  --body "<paste PR body from plan summary>"
```

- [ ] **Step 3: After PR merge, observer gitlink bump**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git checkout v2
git pull origin v2 --ff-only
NEW_TIP=$(git rev-parse HEAD)
cd ..
git -C migrations checkout "$NEW_TIP"
git add migrations docs/superpowers/plans/2026-05-20-v2-lp-cost-basis-materialize.md
git commit -m "feat(lp-position): bump migrations to materialize tip ($NEW_TIP)"
```

- [ ] **Step 4: `/codex review` observer branch + push + PR**

```bash
TMPERR=$(mktemp /tmp/codex-err-XXXXXX.txt)
cd /Users/gyu/project/nads-pump/observer
timeout 360 codex review --base origin/v2 -c 'model_reasoning_effort="high"' < /dev/null 2>"$TMPERR"
rm -f "$TMPERR"

git push -u origin design/v2-lp-cost-basis-materialize
gh pr create --repo Naddotfun/observer --base v2 --head design/v2-lp-cost-basis-materialize \
  --title "feat(lp-position): materialize cost basis into lp_position (gitlink bump + tests)" \
  --body "<paste body>"
```

- [ ] **Step 5: Merge observer PR**

```bash
gh pr merge <NN> --repo Naddotfun/observer --squash --delete-branch
```

---

## Out of scope (follow-ups tracked separately)

- **PR #211 LP fee/APR**: now simplified — read denominator from `lp_position.lp_in_usd - lp_position.lp_out_usd` directly. Spec doc update needed before that phase starts.
- **View deprecation**: optionally drop `lp_position_cost_basis` view in a future migration once all callers are migrated to `lp_position`. For now keep — used by backfill + as canonical math reference.
- **Race-window dashboard**: if the trigger's `RAISE WARNING` fires regularly in prod, add a counter to convert the warning into a Prometheus metric. Out of scope for this PR; tracked under #7 in the broader LP debugging task list.
