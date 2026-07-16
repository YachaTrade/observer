# V2 LP Cost-Basis View Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix mint cost-basis attribution bug by moving cost-basis computation from BEFORE INSERT trigger to a derived view that handles share-weighted attribution and `feeTo` exclusion. PR #35's `dEaD` special-case is reversed: on graduation pools, `0xdead` is the *real-deposit* recipient and `0x715103eeEac12FB84f5d3B35c3268Dd767fa8b8A` (factory.feeTo()) is the `_mintFee()` carve-out that should be excluded from cost-basis attribution. Trigger becomes balance-only; cost basis is a read-time view.

**Architecture:**
- `fill_lp_cost_basis()` trigger keeps account-rewrite for `burn` and counterparty=pool drops for transfers, but **no longer fills any token/USD columns**.
- `apply_lp_position()` keeps `pool.total_supply` ± and `lp_position` lp_in/lp_out UPSERT only; token/USD column accumulation removed.
- New view `lp_position_cost_basis` computes per-row token/USD cost basis for `mint` (share-weighted across non-feeTo recipients in the same tx) and `burn` (full attribution to the single dex_burn recipient).
- Token/USD columns on `lp_position_history` and `lp_position` stay (no DROP, schema unchanged) but are backfilled to 0 and remain 0 going forward. Consumers must read the view.
- `feeTo` = `0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a` (verified via testnet `Factory(0x59c51c66b79c68f63d5446940cd13b6968788e36).feeTo()`). Hardcoded as SQL constant for v1; can be moved to a config table in a follow-up if the factory ever rotates.

**Tech Stack:** PostgreSQL plpgsql + VIEW, sqlx integration tests in Rust, two-track migration system (base `0021_lp_position.sql` for fresh DBs + `v2_upgrade_lp_position.sql` for existing prod DBs — function bodies must stay byte-identical).

---

## File Structure

| Path | Action | Responsibility |
|------|--------|----------------|
| `migrations/0021_lp_position.sql` | Modify | Base schema + simplified triggers + view + backfill |
| `migrations/v2_upgrade_lp_position.sql` | Modify | Idempotent prod-upgrade twin (mirror trigger bodies + view + backfill) |
| `observer/tests/lp_position_history_trigger.rs` | Modify | Update 13 assertions reading lp_position token cols; add new view-based tests |

No changes to observer Rust source code — token columns are not read or written by Rust, only by tests.

---

## Task 1: Side branch + baseline cargo test

**Files:**
- New branch: `design/v2-lp-cost-basis-view` (in migrations submodule, off `origin/v2`)
- Baseline run: existing tests should still pass against migrations `eb2ddbb` (PR #35 tip)

- [ ] **Step 1: Create migrations branch off origin/v2**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git fetch origin v2
git checkout -b design/v2-lp-cost-basis-view origin/v2
```

- [ ] **Step 2: Confirm migrations tip = eb2ddbb (PR #35 merged)**

```bash
git -C /Users/gyu/project/nads-pump/observer/migrations log --oneline -1
```

Expected: `eb2ddbb fix(lp-position): ROUND USD columns to 10dp ... (#35)`

- [ ] **Step 3: Run existing test against current tip to establish baseline**

```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --test lp_position_history_trigger -- --test-threads=1 2>&1 | tail -30
```

Expected: existing tests pass. (If they don't, fix infrastructure before editing the migration.)

---

## Task 2: Write failing tests for new behavior (TDD red)

**Files:**
- Modify: `observer/tests/lp_position_history_trigger.rs`

Add new tests that encode the desired semantics **before** changing any SQL. They MUST fail against the current migration.

- [ ] **Step 1: Add feeTo constant + test helpers near top of file (after BOB const)**

```rust
const FEETO: &str = "0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a";
const DEAD:  &str = "0x000000000000000000000000000000000000dead";
```

- [ ] **Step 2: Add test `view_graduation_pool_feeto_zero_dead_full`**

Encode the working-pool semantics: feeTo gets cost = 0, dEaD gets full deposit.

```rust
#[sqlx::test]
async fn view_graduation_pool_feeto_zero_dead_full(pool: PgPool) {
    setup_test_db(&pool).await;
    ensure_dex_mint_burn(&pool).await;
    let pool_id = "0xpool00000000000000000000000000000000pool";
    let tx      = "0xtxgraduation0000000000000000000000000000000000000000000000000001";
    seed_pool(&pool, pool_id).await;

    // dex_mint: chain-deposited 100 token0 + 200 token1 (in wei), 5 USD value
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 100::numeric, 200::numeric, 2.5::numeric, 2.5::numeric, 5::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    // Two LP Transfer rows from one Pair.mint() emit (log_index < dex_mint.log_index=17)
    // feeTo gets small share, dEaD gets bulk
    insert_lp_history_mint(&pool, FEETO, pool_id, tx, "1000000000000000000",     15).await;  // 1e18 LP
    insert_lp_history_mint(&pool, DEAD,  pool_id, tx, "999000000000000000000",   16).await;  // 999e18 LP

    let row: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in, lp_in_usd FROM lp_position_cost_basis \
         WHERE account_id = $1 AND pool_id = $2 AND transaction_hash = $3"
    ).bind(FEETO).bind(pool_id).bind(tx).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0, BigDecimal::from(0), "feeTo token0_in must be 0 (no deposit)");
    assert_eq!(row.1, BigDecimal::from(0), "feeTo token1_in must be 0");
    assert_eq!(row.2, BigDecimal::from(0), "feeTo lp_in_usd must be 0");

    let row: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in, lp_in_usd FROM lp_position_cost_basis \
         WHERE account_id = $1 AND pool_id = $2 AND transaction_hash = $3"
    ).bind(DEAD).bind(pool_id).bind(tx).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0, BigDecimal::from(100), "dEaD token0_in = full deposit (only non-fee recipient)");
    assert_eq!(row.1, BigDecimal::from(200), "dEaD token1_in = full deposit");
    assert_eq!(row.2, BigDecimal::from(5),   "dEaD lp_in_usd = full deposit USD");
}
```

Helper to add (above `#[sqlx::test]`):

```rust
async fn insert_lp_history_mint(pool: &PgPool, account: &str, pool_id: &str, tx: &str, lp: &str, log_index: i32) {
    sqlx::query(
        "INSERT INTO lp_position_history(account_id, pool_id, lp_in, lp_out, event_type, transaction_hash, block_number, tx_index, log_index, created_at) \
         VALUES ($1, $2, $3::numeric, 0, 'mint', $4, 1, 0, $5, 1779000000)"
    ).bind(account).bind(pool_id).bind(lp).bind(tx).bind(log_index).execute(pool).await.unwrap();
}
```

- [ ] **Step 3: Add test `view_standard_first_mint_share_weighted`**

First-mint case: 1000-wei MIN_LIQUIDITY to dEaD + bulk to user. Both non-feeTo. Share-weighted.

```rust
#[sqlx::test]
async fn view_standard_first_mint_share_weighted(pool: PgPool) {
    setup_test_db(&pool).await;
    ensure_dex_mint_burn(&pool).await;
    let pool_id = "0xpool00000000000000000000000000000000poo2";
    let tx      = "0xtxfirstmint0000000000000000000000000000000000000000000000000002";
    seed_pool(&pool, pool_id).await;

    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 1000000::numeric, 2000000::numeric, 0.5::numeric, 0.5::numeric, 1::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    insert_lp_history_mint(&pool, DEAD,  pool_id, tx, "1000",            15).await;  // MIN_LIQUIDITY
    insert_lp_history_mint(&pool, ALICE, pool_id, tx, "999999999000",    16).await;  // bulk

    // total_real_lp = 1000 + 999999999000 = 999999999000 + 1000
    // ALICE share ≈ 1.0
    let alice_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(&pool).await.unwrap();
    // Roughly = 1000000 * (999999999000 / (1000 + 999999999000)) ≈ 999_999.000..
    // Verify ≥ 999_999 and ≤ 1_000_000
    assert!(alice_t0 >= BigDecimal::from(999_999), "ALICE near-full deposit, got {alice_t0}");
    assert!(alice_t0 <= BigDecimal::from(1_000_000));

    // dEaD gets a tiny share — but is included since not feeTo (different from graduation case)
    let dead_t0: BigDecimal = sqlx::query_scalar(
        "SELECT token0_in FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2"
    ).bind(DEAD).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert!(dead_t0 < BigDecimal::from(2), "dEaD MIN_LIQ share ~= 0, got {dead_t0}");

    // Conservation: sum = full amount0
    let sum_t0: BigDecimal = sqlx::query_scalar(
        "SELECT SUM(token0_in) FROM lp_position_cost_basis WHERE pool_id=$1"
    ).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(sum_t0, BigDecimal::from(1_000_000), "conservation: Σshare = full deposit");
}
```

- [ ] **Step 4: Add test `view_add_liquidity_feeto_excluded`**

Standard add-LP case: feeTo + single user. User gets full deposit.

```rust
#[sqlx::test]
async fn view_add_liquidity_feeto_excluded(pool: PgPool) {
    setup_test_db(&pool).await;
    ensure_dex_mint_burn(&pool).await;
    let pool_id = "0xpool00000000000000000000000000000000poo3";
    let tx      = "0xtxaddlp0000000000000000000000000000000000000000000000000000003a";
    seed_pool(&pool, pool_id).await;

    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 500::numeric, 1000::numeric, 1::numeric, 2::numeric, 3::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    insert_lp_history_mint(&pool, FEETO, pool_id, tx, "1000000", 15).await;     // protocol fee carve
    insert_lp_history_mint(&pool, BOB,   pool_id, tx, "999000000", 16).await;   // real depositor

    let (t0, t1, usd): (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT token0_in, token1_in, lp_in_usd FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2"
    ).bind(BOB).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(t0,  BigDecimal::from(500),  "BOB token0_in = full deposit (feeTo excluded)");
    assert_eq!(t1,  BigDecimal::from(1000), "BOB token1_in = full");
    assert_eq!(usd, BigDecimal::from(3),    "BOB lp_in_usd = full");
}
```

- [ ] **Step 5: Add test `aggregate_token_cols_stay_zero`**

`apply_lp_position` no longer accumulates token cols.

```rust
#[sqlx::test]
async fn aggregate_token_cols_stay_zero(pool: PgPool) {
    setup_test_db(&pool).await;
    ensure_dex_mint_burn(&pool).await;
    let pool_id = "0xpool00000000000000000000000000000000poo4";
    let tx      = "0xtxagg00000000000000000000000000000000000000000000000000000004x";
    seed_pool(&pool, pool_id).await;

    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1, token0_usd, token1_usd, value, created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, '0xdeadbeef00000000000000000000000000000000', 100::numeric, 200::numeric, 1::numeric, 1::numeric, 2::numeric, 100, 1, $2, 17, 0)",
    ).bind(pool_id).bind(tx).execute(&pool).await.unwrap();

    insert_lp_history_mint(&pool, ALICE, pool_id, tx, "1000000000000000000", 15).await;

    let (lp_in, t0, t1, lp_usd): (BigDecimal, BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT lp_in, token0_in, token1_in, lp_in_usd FROM lp_position WHERE account_id=$1 AND pool_id=$2"
    ).bind(ALICE).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(lp_in,  BigDecimal::from(1_000_000_000_000_000_000_i64), "lp_in still tracked");
    assert_eq!(t0,     BigDecimal::from(0), "token0_in NOT accumulated by trigger");
    assert_eq!(t1,     BigDecimal::from(0), "token1_in NOT accumulated");
    assert_eq!(lp_usd, BigDecimal::from(0), "lp_in_usd NOT accumulated");
}
```

- [ ] **Step 6: Run all new tests — verify they FAIL (no view, trigger still fills cols)**

```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --test lp_position_history_trigger view_ aggregate_token_cols_stay_zero -- --test-threads=1 2>&1 | tail -40
```

Expected: failures referencing missing view `lp_position_cost_basis` or non-zero token cols on `lp_position`.

- [ ] **Step 7: Commit failing tests**

```bash
cd /Users/gyu/project/nads-pump/observer
git checkout -b design/v2-lp-cost-basis-view origin/v2
git add tests/lp_position_history_trigger.rs
git commit -m "test(lp-position): failing tests for view-based cost basis + feeTo exclusion"
```

---

## Task 3: Update existing tests to match new semantics

The existing 13 assertions read `lp_position.token0_in / token1_in / token0_out / token1_out / lp_in_usd` and expect non-zero values. Under the new design these are all 0 on `lp_position`; cost basis is in the view.

**Files:**
- Modify: `observer/tests/lp_position_history_trigger.rs` (lines 194, 247, 291, 367, 384, 421, 435, 481, 527, 542, 587, 669, 679 per prior grep)

- [ ] **Step 1: For each spot, switch SELECT target from `lp_position` to `lp_position_cost_basis` aggregated view, OR drop token-col assertions and keep lp_in/lp_out only**

Strategy: where the test's intent is "this account ended up with cost basis X", change to query a `SUM(...) FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2`. Where the test's intent is "balance bookkeeping works", drop the token-col fields from the SELECT.

Edit each of the 13 spots. Concrete example for line 194 (look up the actual assertion in the file before editing — pattern is `SELECT lp_in, lp_out, token0_in, ... FROM lp_position`):

```rust
// BEFORE
let (lp_in, lp_out, token0_in, token0_out, token1_in, token1_out): (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) =
    sqlx::query_as("SELECT lp_in, lp_out, token0_in, token0_out, token1_in, token1_out FROM lp_position WHERE account_id=$1 AND pool_id=$2")
    .bind(...).bind(...).fetch_one(&pool).await.unwrap();
assert_eq!(token0_in, BigDecimal::from(expected));

// AFTER (option 1: drop token-col assertions, balance test only)
let (lp_in, lp_out): (BigDecimal, BigDecimal) =
    sqlx::query_as("SELECT lp_in, lp_out FROM lp_position WHERE account_id=$1 AND pool_id=$2")
    .bind(...).bind(...).fetch_one(&pool).await.unwrap();

// AFTER (option 2: assert cost basis via view sum)
let token0_in: BigDecimal = sqlx::query_scalar(
    "SELECT COALESCE(SUM(token0_in), 0) FROM lp_position_cost_basis WHERE account_id=$1 AND pool_id=$2"
).bind(...).bind(...).fetch_one(&pool).await.unwrap();
assert_eq!(token0_in, BigDecimal::from(expected));
```

- [ ] **Step 2: Run all tests; expect all baseline + new tests to pass once Task 4 lands. For now they will still fail since view doesn't exist yet — that's fine, commit anyway**

```bash
cd /Users/gyu/project/nads-pump/observer
git add tests/lp_position_history_trigger.rs
git commit -m "test(lp-position): update existing tests to read cost basis from view"
```

---

## Task 4: Rewrite `fill_lp_cost_basis()` trigger (drop cost-basis fill, keep account-rewrite + drops)

**Files:**
- Modify: `migrations/0021_lp_position.sql:103-216` (function body)

- [ ] **Step 1: Replace function body — keep burn account_id rewrite and counterparty=pool drops, remove ALL token/USD fills**

```sql
CREATE OR REPLACE FUNCTION fill_lp_cost_basis()
RETURNS TRIGGER AS $$
DECLARE
    burn_row RECORD;
BEGIN
    -- mint: nothing to fill. Cost basis lives in lp_position_cost_basis view.
    -- (Token/USD columns on lp_position_history stay at their default 0.)
    --
    -- burn: re-attribute the row to the real LP-burner. The Transfer(pool→0x0)
    -- log carries from=pair (the contract itself), not the user; dex_burn.to_address
    -- is the real recipient that LP was burned on behalf of.
    IF NEW.event_type = 'burn' THEN
        SELECT * INTO burn_row
          FROM dex_burn
         WHERE pool_id = NEW.pool_id
           AND transaction_hash = NEW.transaction_hash
           AND log_index > NEW.log_index
         ORDER BY log_index ASC LIMIT 1;
        IF FOUND THEN
            NEW.account_id := burn_row.to_address;
        ELSE
            RAISE WARNING 'LP burn without matching dex_burn: pool=% tx=% (attributed to %)',
                NEW.pool_id, NEW.transaction_hash, NEW.account_id;
        END IF;

    ELSIF NEW.event_type = 'transfer_out' THEN
        -- Drop the user→pair leg of burn(); the burn row that follows in the
        -- same tx (re-attributed above) carries the user's lp_out.
        IF NEW.counterparty = NEW.pool_id THEN
            RETURN NULL;
        END IF;

    ELSIF NEW.event_type = 'transfer_in' THEN
        -- Drop the pair-receives-LP phantom row (first leg of burn).
        IF NEW.account_id = NEW.pool_id THEN
            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

- [ ] **Step 2: Verify SQL syntactically valid by dry-running against a scratch DB or via psql --dry-run substitute (e.g., load the file into a transaction and ROLLBACK)**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
# scratch psql verification deferred to Task 7 (cargo test exercises the trigger)
```

---

## Task 5: Simplify `apply_lp_position()` (drop USD/token accumulation, keep lp_in/out + total_supply)

**Files:**
- Modify: `migrations/0021_lp_position.sql:220-270`

- [ ] **Step 1: Replace function body — keep only lp_in/lp_out + pool.total_supply**

```sql
CREATE OR REPLACE FUNCTION apply_lp_position()
RETURNS TRIGGER AS $$
BEGIN
    -- pool.total_supply ± only for mint/burn (transfer is zero-sum)
    IF NEW.event_type = 'mint' THEN
        UPDATE pool SET total_supply = total_supply + NEW.lp_in WHERE pool_id = NEW.pool_id;
    ELSIF NEW.event_type = 'burn' THEN
        UPDATE pool SET total_supply = total_supply - NEW.lp_out WHERE pool_id = NEW.pool_id;
    END IF;

    -- UPSERT lp_position: lp balance only. Cost basis is in lp_position_cost_basis view.
    INSERT INTO lp_position (account_id, pool_id, lp_in, lp_out, created_at, updated_at)
    VALUES (NEW.account_id, NEW.pool_id, NEW.lp_in, NEW.lp_out, NEW.created_at, NEW.created_at)
    ON CONFLICT (account_id, pool_id) DO UPDATE SET
        lp_in      = lp_position.lp_in  + EXCLUDED.lp_in,
        lp_out     = lp_position.lp_out + EXCLUDED.lp_out,
        updated_at = EXCLUDED.updated_at;

    -- Delete the row if balance reached zero
    DELETE FROM lp_position
     WHERE account_id = NEW.account_id
       AND pool_id    = NEW.pool_id
       AND lp_in      = lp_out;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

---

## Task 6: Create `lp_position_cost_basis` VIEW

**Files:**
- Modify: `migrations/0021_lp_position.sql` (add view after triggers, before backfill UPDATEs)

- [ ] **Step 1: Add view definition**

```sql
-- ----------------------------------------------------------------------
-- View: lp_position_cost_basis
-- ----------------------------------------------------------------------
-- Per-row cost basis for mint and burn events. Replaces the old trigger-
-- filled token/USD columns on lp_position_history.
--
-- Mint cost basis:
--   * If account is feeTo (_mintFee carve-out from k growth, not a deposit)
--     → all token/USD = 0.
--   * Else → share-weighted from dex_mint: per-row token_in = amount *
--     (this_lp_in / Σ non-feeTo lp_in for the same (pool, tx)).
--     Conservation: Σ over recipients = full deposit.
--
-- Burn cost basis:
--   * Full attribution to the dex_burn row (already single-recipient per tx).
--
-- feeTo address is hardcoded for now. Move to a config table if the
-- factory ever rotates feeTo.
-- ----------------------------------------------------------------------
CREATE OR REPLACE VIEW lp_position_cost_basis AS
WITH mint_costs AS (
    SELECT
        ph.account_id,
        ph.pool_id,
        ph.transaction_hash,
        ph.tx_index,
        ph.log_index,
        ph.event_type,
        CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ph.lp_in / NULLIF(r.real_lp, 0) * dm.amount0
        END AS token0_in,
        CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ph.lp_in / NULLIF(r.real_lp, 0) * dm.amount1
        END AS token1_in,
        CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ROUND(ph.lp_in / NULLIF(r.real_lp, 0) * COALESCE(dm.token0_usd, 0), 10)
        END AS token0_in_usd,
        CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ROUND(ph.lp_in / NULLIF(r.real_lp, 0) * COALESCE(dm.token1_usd, 0), 10)
        END AS token1_in_usd,
        CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ROUND(ph.lp_in / NULLIF(r.real_lp, 0) * COALESCE(dm.value, 0), 10)
        END AS lp_in_usd,
        0::NUMERIC AS token0_out,
        0::NUMERIC AS token1_out,
        0::NUMERIC AS token0_out_usd,
        0::NUMERIC AS token1_out_usd,
        0::NUMERIC AS lp_out_usd
    FROM lp_position_history ph
    JOIN dex_mint dm
      ON dm.pool_id = ph.pool_id
     AND dm.transaction_hash = ph.transaction_hash
    JOIN LATERAL (
        SELECT COALESCE(SUM(lp_in), 0) AS real_lp
          FROM lp_position_history
         WHERE pool_id = ph.pool_id
           AND transaction_hash = ph.transaction_hash
           AND event_type = 'mint'
           AND LOWER(account_id) <> '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a'
    ) r ON true
    WHERE ph.event_type = 'mint'
),
burn_costs AS (
    SELECT
        ph.account_id,
        ph.pool_id,
        ph.transaction_hash,
        ph.tx_index,
        ph.log_index,
        ph.event_type,
        0::NUMERIC AS token0_in,
        0::NUMERIC AS token1_in,
        0::NUMERIC AS token0_in_usd,
        0::NUMERIC AS token1_in_usd,
        0::NUMERIC AS lp_in_usd,
        db.amount0 AS token0_out,
        db.amount1 AS token1_out,
        ROUND(COALESCE(db.token0_usd, 0), 10) AS token0_out_usd,
        ROUND(COALESCE(db.token1_usd, 0), 10) AS token1_out_usd,
        ROUND(COALESCE(db.value,      0), 10) AS lp_out_usd
    FROM lp_position_history ph
    JOIN dex_burn db
      ON db.pool_id = ph.pool_id
     AND db.transaction_hash = ph.transaction_hash
    WHERE ph.event_type = 'burn'
)
SELECT * FROM mint_costs
UNION ALL
SELECT * FROM burn_costs;
```

- [ ] **Step 2: Adjust the existing `CREATE TYPE lp_event_type` ordering note in 0021 if needed (no change expected since view is added at the file tail)**

---

## Task 7: Backfill — zero out existing cost basis columns

**Files:**
- Modify: `migrations/0021_lp_position.sql` (replace the existing PR #35 backfill block)

- [ ] **Step 1: Replace PR #35 dEaD backfill with full reset**

```sql
-- ----------------------------------------------------------------------
-- One-time backfill: with the trigger no longer filling token/USD columns
-- on lp_position_history or lp_position, any historical values written by
-- prior trigger revisions are stale. Reset to 0. Cost basis is now read
-- exclusively via the lp_position_cost_basis view.
-- ----------------------------------------------------------------------
UPDATE lp_position_history SET
    token0_in      = 0, token0_out      = 0,
    token1_in      = 0, token1_out      = 0,
    token0_in_usd  = 0, token0_out_usd  = 0,
    token1_in_usd  = 0, token1_out_usd  = 0,
    lp_in_usd      = 0, lp_out_usd      = 0;

UPDATE lp_position SET
    token0_in      = 0, token0_out      = 0,
    token1_in      = 0, token1_out      = 0,
    token0_in_usd  = 0, token0_out_usd  = 0,
    token1_in_usd  = 0, token1_out_usd  = 0,
    lp_in_usd      = 0, lp_out_usd      = 0;
```

- [ ] **Step 2: Remove the PR #35 leftover ROUND backfill (now redundant — all cols are 0)**

Delete lines 288–302 (the two `UPDATE … SET … = ROUND(...)` blocks) from `0021_lp_position.sql`.

---

## Task 8: Mirror everything in v2_upgrade_lp_position.sql

**Files:**
- Modify: `migrations/v2_upgrade_lp_position.sql`

- [ ] **Step 1: Copy the new `fill_lp_cost_basis()` body from 0021 verbatim (per the byte-identical invariant)**
- [ ] **Step 2: Copy the new `apply_lp_position()` body from 0021 verbatim**
- [ ] **Step 3: Copy the `lp_position_cost_basis` view definition verbatim**
- [ ] **Step 4: Replace the existing PR #35 dEaD-zero backfill with the full-reset backfill from Task 7**
- [ ] **Step 5: Verify diff matches 0021 trigger bodies via awk slicing:**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
# fill_lp_cost_basis body diff
diff <(awk '/CREATE OR REPLACE FUNCTION fill_lp_cost_basis/,/LANGUAGE plpgsql/' 0021_lp_position.sql) \
     <(awk '/CREATE OR REPLACE FUNCTION fill_lp_cost_basis/,/LANGUAGE plpgsql/' v2_upgrade_lp_position.sql)
# apply_lp_position body diff
diff <(awk '/CREATE OR REPLACE FUNCTION apply_lp_position/,/LANGUAGE plpgsql/' 0021_lp_position.sql) \
     <(awk '/CREATE OR REPLACE FUNCTION apply_lp_position/,/LANGUAGE plpgsql/' v2_upgrade_lp_position.sql)
```

Both diffs must print nothing.

---

## Task 9: Run all tests against the new migration

- [ ] **Step 1: Tell cargo-sqlx to point at the migrations branch**

The observer test harness reads migrations from the submodule. The current submodule pointer (`eb2ddbb`) has PR #35; we need it at the new working branch tip.

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
git status                          # working-dir changes from Tasks 2-8
git add 0021_lp_position.sql v2_upgrade_lp_position.sql
git commit -m "feat(lp-position): view-based cost basis with share-weighted feeTo exclusion

PR #35 zeroed the wrong row on graduation pools: 0xdead is the real
deposit recipient (bonding-curve graduation locks LP there), while
0x715103eeEac12FB84f5d3B35c3268Dd767fa8b8A (= factory.feeTo()) is the
_mintFee() carve-out from k growth and carries no token deposit.

Trigger no longer fills any token/USD cost basis columns. Cost basis
is now a derived value via the lp_position_cost_basis view, which
share-weights dex_mint amounts across non-feeTo recipients and emits
0 for the feeTo row. Burn cost basis stays full-attribution from the
single dex_burn row.

apply_lp_position trigger reduced to lp_in/lp_out UPSERT + pool.total_supply
maintenance. Existing token/USD columns on lp_position_history /
lp_position are backfilled to 0 and remain 0 — consumers read the view.

v2_upgrade twin mirrored. Function bodies stay byte-identical to 0021."
git push -u origin design/v2-lp-cost-basis-view
```

- [ ] **Step 2: Bump observer submodule pointer to this new commit (locally, do NOT commit yet)**

```bash
cd /Users/gyu/project/nads-pump/observer
git fetch --recurse-submodules
NEW_TIP=$(git -C migrations rev-parse HEAD)
git -C migrations checkout "$NEW_TIP"
```

- [ ] **Step 3: Run full lp_position trigger test suite**

```bash
cargo test --test lp_position_history_trigger -- --test-threads=1 2>&1 | tail -80
```

Expected: all tests pass — both new view-based tests and the rewritten existing tests.

If any test fails, iterate on the SQL or test until green.

---

## Task 10: Open migrations PR (supersedes PR #35's dEaD logic)

- [ ] **Step 1: Open PR against migrations/v2**

```bash
gh pr create --repo Naddotfun/migrations --base v2 --head design/v2-lp-cost-basis-view \
  --title "feat(lp-position): view-based cost basis, reverses PR #35 dEaD inversion" \
  --body "$(cat <<'EOF'
## Summary
- PR #35 zeroed the wrong row: on graduation pools `0xdead` is the *real* deposit recipient (bonding curve locks LP there). `0x715103eeEac12FB84f5d3B35c3268Dd767fa8b8A` (= `factory(0x59c51c66...).feeTo()`) is the actual `_mintFee()` carve-out.
- Trigger no longer fills any token / USD cost-basis columns. Cost basis is now derived via the new `lp_position_cost_basis` view (share-weighted across non-feeTo recipients for mint, single-recipient for burn).
- `apply_lp_position` reduced to lp_in / lp_out UPSERT + `pool.total_supply` maintenance.
- Existing token / USD columns on `lp_position_history` and `lp_position` are backfilled to 0 and remain 0 going forward — consumers read the view.
- v2_upgrade twin mirrored. Function bodies stay byte-identical to `0021_lp_position.sql`.

## Test plan
- [ ] `cargo test --test lp_position_history_trigger` passes (rewritten + new view-based tests).
- [ ] On a copy of testnet DB: `SELECT * FROM lp_position_cost_basis WHERE account_id IN ('0x000…dead', '0x715103ee…') AND pool_id = '0x7Ccc3baE…'` — feeTo row token cols = 0, dEaD row token cols = full dex_mint amounts.
- [ ] On a copy of testnet DB: `SELECT token0_in FROM lp_position` returns 0 for all rows after backfill.
EOF
)"
```

- [ ] **Step 2: Run `/codex review` per the CLAUDE.md absolute rule**

```bash
cd /Users/gyu/project/nads-pump/observer/migrations
codex review --base v2 -c 'model_reasoning_effort="high"'
```

- [ ] **Step 3: Apply any AUTO-FIX items from codex; confirm ASK items with user before applying**

- [ ] **Step 4: Wait for migrations PR merge**

---

## Task 11: Observer gitlink bump (supersedes PR #215)

- [ ] **Step 1: Mark old PR #215 as superseded with a comment, then close**

```bash
gh pr comment 215 --repo Naddotfun/observer --body "Superseded by upcoming gitlink bump for migrations PR fixing the dEaD/feeTo inversion. See migrations branch design/v2-lp-cost-basis-view."
gh pr close 215 --repo Naddotfun/observer --delete-branch
```

- [ ] **Step 2: Create new gitlink-bump branch off origin/v2 and update submodule pointer**

```bash
cd /Users/gyu/project/nads-pump/observer
git fetch origin v2
git checkout -b chore/bump-migrations-view-based-cost-basis origin/v2
NEW_TIP=$(git -C migrations rev-parse origin/v2)
git -C migrations checkout "$NEW_TIP"
git add migrations
git commit -m "chore: bump migrations to v2 tip (view-based LP cost basis)

Picks up the migrations PR that:
- Reverses PR #35's dEaD-zero (graduation pools had real deposit on dEaD).
- Adds lp_position_cost_basis view with share-weighted attribution.
- Excludes 0x715103eeEac12FB84f5d3B35c3268Dd767fa8b8A (factory.feeTo()) from cost basis.
- Zeros stale token/USD columns on lp_position_history / lp_position."
git push -u origin chore/bump-migrations-view-based-cost-basis
gh pr create --repo Naddotfun/observer --base v2 --head chore/bump-migrations-view-based-cost-basis \
  --title "chore: bump migrations (view-based LP cost basis, supersedes #215)" --body "$(cat <<'EOF'
## Summary
- Bumps migrations submodule pointer to the v2 tip containing the view-based LP cost-basis fix.
- Supersedes PR #215 (which would have shipped PR #35's inverted dEaD-zero logic to prod).
- Observer Rust source unchanged — only tests adjusted to read cost basis from \`lp_position_cost_basis\` view.

## Why
PR #35 special-cased \`account_id = 0xdead\` and zeroed its token columns, assuming dEaD always represents locked-minimum-liquidity. On graduation pools (e.g. \`0x7Ccc3baE…\`), dEaD is actually the *real-deposit* recipient (bonding curve locks LP there) and the small-share row belongs to \`factory.feeTo()\` (\`0x715103eeEac12FB84f5d3B35c3268Dd767fa8b8A\`), the \`_mintFee()\` carve-out. Merging PR #215 would have wiped the only correct cost-basis row in prod.

## Test plan
- [ ] \`cargo test --test lp_position_history_trigger\` passes.
- [ ] Spot-check on testnet: \`SELECT account_id, token0_in FROM lp_position_cost_basis WHERE pool_id='0x7Ccc3baE4e3885D15caC8C064d13F6a1582DdFB7'\` — feeTo row = 0, dEaD row = full \`dex_mint.amount0\`.
EOF
)"
```

- [ ] **Step 3: `/codex review` on the observer PR (light review — gitlink bump)**

- [ ] **Step 4: Merge after green CI**

---

## Out of scope (follow-ups)

- **PR #211 LP fee/APR spec rewrite**: the spec currently reads `lp_position.token0_in_usd` etc. for fee accrual denominator. Replace with `SUM(lp_in_usd) FROM lp_position_cost_basis`. Tracked under `docs/superpowers/plans/2026-05-20-v2-lp-fee-apr-tracking.md` — needs an update pass before that phase starts.
- **Transfer cost-basis tracking**: under this design, holder-to-holder LP transfers do not propagate cost basis (token_out/token_in on transfer rows stay 0 in the view). If `/api/holders/:account/positions` needs running per-holder cost basis after transfers, add a recursive CTE in the view or a materialized variant. Defer until a consumer demands it.
- **feeTo rotation**: hardcoded SQL constant. Move to a `protocol_config(factory, fee_to)` table if the factory ever calls `setFeeTo`.

