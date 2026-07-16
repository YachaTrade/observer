# V2 LP Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** V2 NadFunPair LP holder별 잔액과 cost basis(공급 token0/token1 누적량)를 추적해서 "내 LP" 화면을 RPC 콜 없이 SQL로 산출 가능하게 만든다.

**Architecture:** 기존 `src/event/common/token/stream.rs`가 이미 체인 전역 Transfer 시그니처를 페치하고 whitelist로 드롭하는 패턴 활용. `parse_log`에 분기 한 줄 추가해서 Pair 주소로 들어온 Transfer는 LP 핸들러로 라우팅. `lp_transfer_history` raw 테이블에 적재 → PG trigger가 `lp_position`(cost basis 포함) + `pool.total_supply`를 동시 갱신. Mint 시점 cost basis 매칭은 `Sync/Receive Manager`에서 Token이 V2Dex보다 늦게 처리되도록 dependency 추가해서 같은 tx의 `dex_mint`가 trigger 발화 시점에 무조건 존재하도록 강제.

**Tech Stack:** Rust (alloy-rs, tokio, sqlx), PostgreSQL (plpgsql trigger), Redis (whitelist cache), 표준 `go test`-style table-driven tests via `cargo test --features integration-tests -- --test-threads=1` for DB integration.

**Spec:** `docs/superpowers/specs/2026-05-13-v2-lp-tracking-design.md`

---

## File Structure

**Create:**
- `migrations/0021_lp_position.sql` — base schema (lp_position, lp_transfer_history, pool ALTER, trigger)
- `migrations/v2_upgrade_lp_position.sql` — idempotent upgrade for existing prod DBs
- `src/event/common/token/lp_transfer.rs` — LP Transfer parse + dispatch helpers
- `src/db/postgres/controller/lp_transfer.rs` — `LpTransferController` with batch insert SQL
- `tests/lp_position_trigger.rs` — DB integration tests for the PL/pgSQL trigger
- `tests/lp_transfer_dispatch.rs` — unit tests for the parse_log branch decision

**Modify:**
- `src/event/common/token/stream.rs` — `parse_log` branch + LpTransfer event flow + `fetch_logs` add LP Transfer batch insert
- `src/event/common/token/mod.rs` (or wherever TokenEvent variants live) — add `TokenEvent::LpTransfer`
- `src/types/v2/dex.rs` (or new sibling file) — add `LpTransferData` struct
- `src/db/postgres/controller/mod.rs` — register `LpTransferController`
- `src/sync/receive.rs` — Token waits for V2Dex (in addition to existing Curve dep)

**Test files alongside:**
- Use existing test conventions: `#[cfg(test)] mod tests` for unit, `tests/` dir for integration with `#[sqlx::test]`-style or the project's existing pattern.

---

## Pre-flight: confirm assumptions

- [ ] **Step 0: Confirm Transfer event is in Pair ABI**

```bash
grep -n '"name": "Transfer"' abi/v2/NadFunPair.json
```

Expected: a line near `:781` showing `"name": "Transfer"` (standard ERC20 signature: `Transfer(address indexed from, address indexed to, uint256 value)`).

- [ ] **Step 0.1: Confirm sol macro exposes Transfer on the V2 Pair binding**

```bash
grep -rn 'V2INadFunPair::Transfer\|sol!.*NadFunPair' src/ | head -10
```

Expected: at least one `sol!` invocation that includes `NadFunPair.json`. If `V2INadFunPair::Transfer::SIGNATURE` is not yet referenced anywhere, that's fine — we'll use it in Task 3.

- [ ] **Step 0.2: Confirm `dex_mint` table has `(pool_id, transaction_hash)` queryable**

```bash
grep -n "CREATE TABLE IF NOT EXISTS dex_mint" migrations/v2_upgrade_new_tables.sql migrations/0014_dex.sql
```

Expected: `dex_mint` table exists with columns `pool_id, sender, amount0, amount1, transaction_hash, log_index, tx_index, block_number, created_at`.

---

## Task 1: Create base migration with schema + trigger

**Files:**
- Create: `migrations/0021_lp_position.sql`

- [ ] **Step 1: Write the failing test (DB migration smoke)**

Create `tests/lp_position_trigger.rs`:

```rust
use sqlx::PgPool;

async fn setup_pool() -> PgPool {
    let url = std::env::var("DATABASE_URL_TEST")
        .expect("set DATABASE_URL_TEST to a disposable test database");
    let pool = sqlx::PgPool::connect(&url).await.expect("connect");
    // Apply base + LP migration. Project uses sqlx migrator OR manual SQL file load.
    // Adapt this to whatever existing tests/*.rs uses — the pattern is in tests/dex_swap.rs etc.
    pool
}

#[tokio::test]
async fn migration_creates_lp_tables() {
    let pool = setup_pool().await;
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='lp_position')"
    ).fetch_one(&pool).await.unwrap();
    assert!(row.0, "lp_position table missing");

    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='lp_transfer_history')"
    ).fetch_one(&pool).await.unwrap();
    assert!(row.0, "lp_transfer_history table missing");

    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='pool' AND column_name='total_supply')"
    ).fetch_one(&pool).await.unwrap();
    assert!(row.0, "pool.total_supply column missing");
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cargo test --test lp_position_trigger migration_creates_lp_tables -- --nocapture
```

Expected: FAIL — `lp_position` table not found, or migration file missing.

- [ ] **Step 3: Write the migration file**

Create `migrations/0021_lp_position.sql`:

```sql
-- LP position tracking for V2 NadFunPair
-- spec: docs/superpowers/specs/2026-05-13-v2-lp-tracking-design.md

CREATE TABLE IF NOT EXISTS lp_position (
    pool_id       VARCHAR(42) NOT NULL,
    account_id    VARCHAR(42) NOT NULL,
    balance       NUMERIC(78,0) NOT NULL DEFAULT 0,
    cost_amount0  NUMERIC(78,0) NOT NULL DEFAULT 0,
    cost_amount1  NUMERIC(78,0) NOT NULL DEFAULT 0,
    updated_at    BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, account_id),
    CHECK (balance >= 0)
);
CREATE INDEX IF NOT EXISTS idx_lp_position_account ON lp_position(account_id);

CREATE TABLE IF NOT EXISTS lp_transfer_history (
    pool_id          VARCHAR(42) NOT NULL,
    from_address     VARCHAR(42) NOT NULL,
    to_address       VARCHAR(42) NOT NULL,
    amount           NUMERIC(78,0) NOT NULL,
    block_number     BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    tx_index         INT NOT NULL,
    log_index        INT NOT NULL,
    created_at       BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_lp_xfer_pool_block ON lp_transfer_history(pool_id, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_lp_xfer_from       ON lp_transfer_history(from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_lp_xfer_to         ON lp_transfer_history(to_address, block_number DESC);

ALTER TABLE pool ADD COLUMN IF NOT EXISTS total_supply NUMERIC(78,0) NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION apply_lp_transfer() RETURNS TRIGGER AS $$
DECLARE
    ZERO  CONSTANT VARCHAR(42) := '0x0000000000000000000000000000000000000000';
    v_a0  NUMERIC(78,0);
    v_a1  NUMERIC(78,0);
    v_balance_before NUMERIC(78,0);
    v_cost0 NUMERIC(78,0);
    v_cost1 NUMERIC(78,0);
    v_ratio NUMERIC;
    v_moved0 NUMERIC(78,0);
    v_moved1 NUMERIC(78,0);
BEGIN
    -- guard: 0 -> 0 ignore
    IF NEW.from_address = ZERO AND NEW.to_address = ZERO THEN
        RAISE WARNING 'LP transfer with zero/zero addresses: tx=%', NEW.transaction_hash;
        RETURN NEW;
    END IF;

    -- MINT branch
    IF NEW.from_address = ZERO THEN
        UPDATE pool SET total_supply = total_supply + NEW.amount WHERE pool_id = NEW.pool_id;

        SELECT amount0, amount1 INTO v_a0, v_a1
        FROM dex_mint
        WHERE pool_id = NEW.pool_id AND transaction_hash = NEW.transaction_hash
        ORDER BY log_index
        LIMIT 1;

        IF v_a0 IS NULL THEN
            RAISE WARNING 'LP mint without matching dex_mint: pool=% tx=%',
                NEW.pool_id, NEW.transaction_hash;
            v_a0 := 0;
            v_a1 := 0;
        END IF;

        INSERT INTO lp_position(pool_id, account_id, balance, cost_amount0, cost_amount1, updated_at)
        VALUES (NEW.pool_id, NEW.to_address, NEW.amount, v_a0, v_a1, NEW.created_at)
        ON CONFLICT (pool_id, account_id) DO UPDATE
        SET balance      = lp_position.balance      + EXCLUDED.balance,
            cost_amount0 = lp_position.cost_amount0 + EXCLUDED.cost_amount0,
            cost_amount1 = lp_position.cost_amount1 + EXCLUDED.cost_amount1,
            updated_at   = EXCLUDED.updated_at;
        RETURN NEW;
    END IF;

    -- BURN branch
    IF NEW.to_address = ZERO THEN
        UPDATE pool SET total_supply = total_supply - NEW.amount WHERE pool_id = NEW.pool_id;

        SELECT balance, cost_amount0, cost_amount1
          INTO v_balance_before, v_cost0, v_cost1
        FROM lp_position
        WHERE pool_id = NEW.pool_id AND account_id = NEW.from_address
        FOR UPDATE;

        IF v_balance_before IS NULL OR v_balance_before = 0 THEN
            RAISE WARNING 'LP burn from zero/missing balance: pool=% holder=% tx=%',
                NEW.pool_id, NEW.from_address, NEW.transaction_hash;
            -- CHECK constraint will block subsequent UPDATE; surface loudly
        END IF;

        v_ratio := NEW.amount::NUMERIC / NULLIF(v_balance_before, 0);
        v_moved0 := (COALESCE(v_cost0, 0) * COALESCE(v_ratio, 0))::NUMERIC(78,0);
        v_moved1 := (COALESCE(v_cost1, 0) * COALESCE(v_ratio, 0))::NUMERIC(78,0);

        UPDATE lp_position
        SET balance      = balance - NEW.amount,
            cost_amount0 = cost_amount0 - v_moved0,
            cost_amount1 = cost_amount1 - v_moved1,
            updated_at   = NEW.created_at
        WHERE pool_id = NEW.pool_id AND account_id = NEW.from_address;
        RETURN NEW;
    END IF;

    -- HOLDER -> HOLDER branch
    SELECT balance, cost_amount0, cost_amount1
      INTO v_balance_before, v_cost0, v_cost1
    FROM lp_position
    WHERE pool_id = NEW.pool_id AND account_id = NEW.from_address
    FOR UPDATE;

    IF v_balance_before IS NULL OR v_balance_before = 0 THEN
        RAISE WARNING 'LP transfer from zero/missing balance: pool=% from=% tx=%',
            NEW.pool_id, NEW.from_address, NEW.transaction_hash;
    END IF;

    v_ratio := NEW.amount::NUMERIC / NULLIF(v_balance_before, 0);
    v_moved0 := (COALESCE(v_cost0, 0) * COALESCE(v_ratio, 0))::NUMERIC(78,0);
    v_moved1 := (COALESCE(v_cost1, 0) * COALESCE(v_ratio, 0))::NUMERIC(78,0);

    UPDATE lp_position
    SET balance      = balance - NEW.amount,
        cost_amount0 = cost_amount0 - v_moved0,
        cost_amount1 = cost_amount1 - v_moved1,
        updated_at   = NEW.created_at
    WHERE pool_id = NEW.pool_id AND account_id = NEW.from_address;

    INSERT INTO lp_position(pool_id, account_id, balance, cost_amount0, cost_amount1, updated_at)
    VALUES (NEW.pool_id, NEW.to_address, NEW.amount, v_moved0, v_moved1, NEW.created_at)
    ON CONFLICT (pool_id, account_id) DO UPDATE
    SET balance      = lp_position.balance      + EXCLUDED.balance,
        cost_amount0 = lp_position.cost_amount0 + EXCLUDED.cost_amount0,
        cost_amount1 = lp_position.cost_amount1 + EXCLUDED.cost_amount1,
        updated_at   = EXCLUDED.updated_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_apply_lp_transfer ON lp_transfer_history;
CREATE TRIGGER trg_apply_lp_transfer
    AFTER INSERT ON lp_transfer_history
    FOR EACH ROW EXECUTE FUNCTION apply_lp_transfer();
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cargo test --test lp_position_trigger migration_creates_lp_tables -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add migrations/0021_lp_position.sql tests/lp_position_trigger.rs
git commit -m "feat(migrations): add lp_position schema + cost-basis trigger"
```

---

## Task 2: Idempotent upgrade migration for existing prod DBs

**Files:**
- Create: `migrations/v2_upgrade_lp_position.sql`

The migrations submodule has two tracks (per CLAUDE.md memory): base files for fresh DBs and `v2_upgrade_*.sql` for existing prod. The base file from Task 1 is for fresh DBs; this task creates the idempotent twin.

- [ ] **Step 1: Write the failing test (idempotent re-apply)**

Append to `tests/lp_position_trigger.rs`:

```rust
#[tokio::test]
async fn upgrade_migration_is_idempotent() {
    let pool = setup_pool().await;
    // Apply v2_upgrade_lp_position.sql twice. Second application must not fail.
    let sql = std::fs::read_to_string("migrations/v2_upgrade_lp_position.sql").unwrap();
    sqlx::query(&sql).execute(&pool).await.unwrap();
    sqlx::query(&sql).execute(&pool).await.unwrap();   // re-apply must be no-op
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cargo test --test lp_position_trigger upgrade_migration_is_idempotent -- --nocapture
```

Expected: FAIL — file not found.

- [ ] **Step 3: Create the upgrade file**

```sql
-- v2_upgrade_lp_position.sql
-- idempotent upgrade for existing v2 prod DBs
-- spec: docs/superpowers/specs/2026-05-13-v2-lp-tracking-design.md

CREATE TABLE IF NOT EXISTS lp_position (
    pool_id       VARCHAR(42) NOT NULL,
    account_id    VARCHAR(42) NOT NULL,
    balance       NUMERIC(78,0) NOT NULL DEFAULT 0,
    cost_amount0  NUMERIC(78,0) NOT NULL DEFAULT 0,
    cost_amount1  NUMERIC(78,0) NOT NULL DEFAULT 0,
    updated_at    BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, account_id)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'lp_position_balance_check' AND conrelid = 'lp_position'::regclass
    ) THEN
        ALTER TABLE lp_position ADD CONSTRAINT lp_position_balance_check CHECK (balance >= 0);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_lp_position_account ON lp_position(account_id);

CREATE TABLE IF NOT EXISTS lp_transfer_history (
    pool_id          VARCHAR(42) NOT NULL,
    from_address     VARCHAR(42) NOT NULL,
    to_address       VARCHAR(42) NOT NULL,
    amount           NUMERIC(78,0) NOT NULL,
    block_number     BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    tx_index         INT NOT NULL,
    log_index        INT NOT NULL,
    created_at       BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_lp_xfer_pool_block ON lp_transfer_history(pool_id, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_lp_xfer_from       ON lp_transfer_history(from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_lp_xfer_to         ON lp_transfer_history(to_address, block_number DESC);

ALTER TABLE pool ADD COLUMN IF NOT EXISTS total_supply NUMERIC(78,0) NOT NULL DEFAULT 0;

-- function + trigger are CREATE OR REPLACE / DROP+CREATE so this is naturally idempotent
-- (paste the same `apply_lp_transfer` function body from migrations/0021_lp_position.sql here)
CREATE OR REPLACE FUNCTION apply_lp_transfer() RETURNS TRIGGER AS $$
-- ... (identical to Task 1 body — DO NOT abbreviate, copy verbatim) ...
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_apply_lp_transfer ON lp_transfer_history;
CREATE TRIGGER trg_apply_lp_transfer
    AFTER INSERT ON lp_transfer_history
    FOR EACH ROW EXECUTE FUNCTION apply_lp_transfer();
```

**Note for the implementer:** when copying the function body, paste it verbatim from `migrations/0021_lp_position.sql` to keep base and upgrade in lockstep. Run a `diff` to confirm equivalence.

- [ ] **Step 4: Run the test to verify it passes**

```bash
cargo test --test lp_position_trigger upgrade_migration_is_idempotent -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Confirm base + upgrade function bodies are identical**

```bash
diff <(awk '/CREATE OR REPLACE FUNCTION apply_lp_transfer/,/LANGUAGE plpgsql/' migrations/0021_lp_position.sql) \
     <(awk '/CREATE OR REPLACE FUNCTION apply_lp_transfer/,/LANGUAGE plpgsql/' migrations/v2_upgrade_lp_position.sql)
```

Expected: empty diff.

- [ ] **Step 6: Commit**

```bash
git add migrations/v2_upgrade_lp_position.sql tests/lp_position_trigger.rs
git commit -m "feat(migrations): add v2_upgrade_lp_position.sql idempotent upgrade"
```

---

## Task 3: Trigger correctness — MINT path

**Files:**
- Test: `tests/lp_position_trigger.rs`

- [ ] **Step 1: Write the failing test (MINT with dex_mint present)**

Append to `tests/lp_position_trigger.rs`:

```rust
async fn seed_pool(pool: &PgPool, pool_id: &str) {
    sqlx::query(
        "INSERT INTO pool(pool_id, token0, token1, reserve0, reserve1)
         VALUES ($1, '0xaaa...0', '0xbbb...1', 0, 0)
         ON CONFLICT (pool_id) DO NOTHING"
    ).bind(pool_id).execute(pool).await.unwrap();
}

async fn insert_dex_mint(pool: &PgPool, pool_id: &str, tx: &str, a0: &str, a1: &str) {
    sqlx::query(
        "INSERT INTO dex_mint(pool_id, sender, amount0, amount1,
            created_at, block_number, transaction_hash, log_index, tx_index)
         VALUES ($1, '0xsender', $2::numeric, $3::numeric, 100, 1, $4, 0, 0)"
    ).bind(pool_id).bind(a0).bind(a1).bind(tx).execute(pool).await.unwrap();
}

#[tokio::test]
async fn mint_creates_position_with_cost_basis() {
    let pool = setup_pool().await;
    let pid = "0xpool00000000000000000000000000000000pool";
    let tx  = "0xtx0000000000000000000000000000000000000000000000000000000000mint";
    seed_pool(&pool, pid).await;
    insert_dex_mint(&pool, pid, tx, "1000", "2000").await;

    sqlx::query(
        "INSERT INTO lp_transfer_history(
            pool_id, from_address, to_address, amount,
            block_number, transaction_hash, tx_index, log_index, created_at)
         VALUES ($1, '0x0000000000000000000000000000000000000000',
                 '0xholder0000000000000000000000000000holder', 500::numeric,
                 1, $2, 0, 1, 100)"
    ).bind(pid).bind(tx).execute(&pool).await.unwrap();

    let row: (sqlx::types::BigDecimal, sqlx::types::BigDecimal, sqlx::types::BigDecimal) =
        sqlx::query_as(
            "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
             WHERE pool_id = $1 AND account_id = '0xholder0000000000000000000000000000holder'"
        ).bind(pid).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0.to_string(), "500");
    assert_eq!(row.1.to_string(), "1000");
    assert_eq!(row.2.to_string(), "2000");

    let supply: (sqlx::types::BigDecimal,) =
        sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id = $1")
            .bind(pid).fetch_one(&pool).await.unwrap();
    assert_eq!(supply.0.to_string(), "500");
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test --test lp_position_trigger mint_creates_position_with_cost_basis -- --nocapture
```

Expected: PASS (the trigger from Task 1 already implements this — this test guards the contract).

- [ ] **Step 3: Add the MINT-without-dex_mint warning case**

```rust
#[tokio::test]
async fn mint_without_dex_mint_records_zero_cost() {
    let pool = setup_pool().await;
    let pid = "0xpool11111111111111111111111111111111pool";
    let tx  = "0xtx111111111111111111111111111111111111111111111111111111111111";
    seed_pool(&pool, pid).await;
    // intentionally skip insert_dex_mint

    sqlx::query(
        "INSERT INTO lp_transfer_history(
            pool_id, from_address, to_address, amount,
            block_number, transaction_hash, tx_index, log_index, created_at)
         VALUES ($1, '0x0000000000000000000000000000000000000000',
                 '0xholder1111111111111111111111111111holder', 500::numeric,
                 1, $2, 0, 0, 100)"
    ).bind(pid).bind(tx).execute(&pool).await.unwrap();

    let row: (sqlx::types::BigDecimal, sqlx::types::BigDecimal, sqlx::types::BigDecimal) =
        sqlx::query_as(
            "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
             WHERE pool_id = $1 AND account_id = '0xholder1111111111111111111111111111holder'"
        ).bind(pid).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0.to_string(), "500");
    assert_eq!(row.1.to_string(), "0");
    assert_eq!(row.2.to_string(), "0");
    // WARNING was emitted to server log — manually inspectable via pg log
}
```

- [ ] **Step 4: Run test**

```bash
cargo test --test lp_position_trigger mint_without_dex_mint_records_zero_cost -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/lp_position_trigger.rs
git commit -m "test(lp): cover MINT trigger paths (with + without dex_mint)"
```

---

## Task 4: Trigger correctness — BURN path

**Files:**
- Test: `tests/lp_position_trigger.rs`

- [ ] **Step 1: Write partial burn test**

Append:

```rust
#[tokio::test]
async fn partial_burn_reduces_balance_and_cost_proportionally() {
    let pool = setup_pool().await;
    let pid = "0xpool22222222222222222222222222222222pool";
    let mint_tx = "0xtxmint222222222222222222222222222222222222222222222222222222222";
    let burn_tx = "0xtxburn222222222222222222222222222222222222222222222222222222222";
    seed_pool(&pool, pid).await;
    insert_dex_mint(&pool, pid, mint_tx, "1000", "2000").await;

    // mint 1000 LP
    sqlx::query(
        "INSERT INTO lp_transfer_history VALUES (
            $1, '0x0000000000000000000000000000000000000000',
            '0xholder2222222222222222222222222222holder',
            1000::numeric, 1, $2, 0, 0, 100)"
    ).bind(pid).bind(mint_tx).execute(&pool).await.unwrap();

    // burn 400 LP (40%) — cost should drop by 40%
    sqlx::query(
        "INSERT INTO lp_transfer_history VALUES (
            $1, '0xholder2222222222222222222222222222holder',
            '0x0000000000000000000000000000000000000000',
            400::numeric, 2, $2, 0, 0, 200)"
    ).bind(pid).bind(burn_tx).execute(&pool).await.unwrap();

    let row: (sqlx::types::BigDecimal, sqlx::types::BigDecimal, sqlx::types::BigDecimal) =
        sqlx::query_as(
            "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
             WHERE pool_id = $1 AND account_id = '0xholder2222222222222222222222222222holder'"
        ).bind(pid).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0.to_string(), "600");
    assert_eq!(row.1.to_string(), "600");   // 1000 - (1000*0.4)
    assert_eq!(row.2.to_string(), "1200");  // 2000 - (2000*0.4)

    let supply: (sqlx::types::BigDecimal,) =
        sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id = $1")
            .bind(pid).fetch_one(&pool).await.unwrap();
    assert_eq!(supply.0.to_string(), "600");
}
```

- [ ] **Step 2: Full burn case**

```rust
#[tokio::test]
async fn full_burn_zeroes_position() {
    let pool = setup_pool().await;
    let pid = "0xpool33333333333333333333333333333333pool";
    let mint_tx = "0xtxmint333333333333333333333333333333333333333333333333333333333";
    let burn_tx = "0xtxburn333333333333333333333333333333333333333333333333333333333";
    seed_pool(&pool, pid).await;
    insert_dex_mint(&pool, pid, mint_tx, "1000", "2000").await;

    sqlx::query(
        "INSERT INTO lp_transfer_history VALUES (
            $1, '0x0000000000000000000000000000000000000000',
            '0xholder33333333333333333333333333333holder',
            1000::numeric, 1, $2, 0, 0, 100)"
    ).bind(pid).bind(mint_tx).execute(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO lp_transfer_history VALUES (
            $1, '0xholder33333333333333333333333333333holder',
            '0x0000000000000000000000000000000000000000',
            1000::numeric, 2, $2, 0, 0, 200)"
    ).bind(pid).bind(burn_tx).execute(&pool).await.unwrap();

    let row: (sqlx::types::BigDecimal, sqlx::types::BigDecimal, sqlx::types::BigDecimal) =
        sqlx::query_as(
            "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
             WHERE pool_id = $1 AND account_id = '0xholder33333333333333333333333333333holder'"
        ).bind(pid).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0.to_string(), "0");
    assert_eq!(row.1.to_string(), "0");
    assert_eq!(row.2.to_string(), "0");
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test --test lp_position_trigger -- partial_burn_reduces_balance_and_cost_proportionally full_burn_zeroes_position --nocapture
```

Expected: PASS for both.

- [ ] **Step 4: Commit**

```bash
git add tests/lp_position_trigger.rs
git commit -m "test(lp): cover BURN trigger paths (partial + full)"
```

---

## Task 5: Trigger correctness — HOLDER → HOLDER path

**Files:**
- Test: `tests/lp_position_trigger.rs`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn holder_to_holder_moves_cost_proportionally() {
    let pool = setup_pool().await;
    let pid = "0xpool44444444444444444444444444444444pool";
    let mint_tx = "0xtxmint444444444444444444444444444444444444444444444444444444444";
    let xfer_tx = "0xtxxfer444444444444444444444444444444444444444444444444444444444";
    let alice = "0xalice4444444444444444444444444444alicee";
    let bob   = "0xbob44444444444444444444444444444444bobb";

    seed_pool(&pool, pid).await;
    insert_dex_mint(&pool, pid, mint_tx, "1000", "4000").await;

    // alice mints 1000 LP, cost (1000, 4000)
    sqlx::query(
        "INSERT INTO lp_transfer_history VALUES (
            $1, '0x0000000000000000000000000000000000000000',
            $2, 1000::numeric, 1, $3, 0, 0, 100)"
    ).bind(pid).bind(alice).bind(mint_tx).execute(&pool).await.unwrap();

    // alice → bob 250 LP (25%) → cost moves (250, 1000)
    sqlx::query(
        "INSERT INTO lp_transfer_history VALUES (
            $1, $2, $3, 250::numeric, 2, $4, 0, 0, 200)"
    ).bind(pid).bind(alice).bind(bob).bind(xfer_tx).execute(&pool).await.unwrap();

    let a: (sqlx::types::BigDecimal, sqlx::types::BigDecimal, sqlx::types::BigDecimal) =
        sqlx::query_as(
            "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
             WHERE pool_id = $1 AND account_id = $2"
        ).bind(pid).bind(alice).fetch_one(&pool).await.unwrap();
    assert_eq!(a.0.to_string(), "750");
    assert_eq!(a.1.to_string(), "750");
    assert_eq!(a.2.to_string(), "3000");

    let b: (sqlx::types::BigDecimal, sqlx::types::BigDecimal, sqlx::types::BigDecimal) =
        sqlx::query_as(
            "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
             WHERE pool_id = $1 AND account_id = $2"
        ).bind(pid).bind(bob).fetch_one(&pool).await.unwrap();
    assert_eq!(b.0.to_string(), "250");
    assert_eq!(b.1.to_string(), "250");
    assert_eq!(b.2.to_string(), "1000");

    let supply: (sqlx::types::BigDecimal,) =
        sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id = $1")
            .bind(pid).fetch_one(&pool).await.unwrap();
    assert_eq!(supply.0.to_string(), "1000");  // mint only, transfer doesn't move supply
}
```

- [ ] **Step 2: Run**

```bash
cargo test --test lp_position_trigger holder_to_holder_moves_cost_proportionally -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/lp_position_trigger.rs
git commit -m "test(lp): cover holder-to-holder transfer trigger path"
```

---

## Task 6: Define `LpTransferData` type

**Files:**
- Modify: `src/types/v2/dex.rs`

- [ ] **Step 1: Write the failing test**

In `src/types/v2/dex.rs` `#[cfg(test)] mod tests`:

```rust
#[test]
fn lp_transfer_data_round_trip() {
    use crate::types::v2::dex::LpTransferData;
    use std::sync::Arc;
    use bigdecimal::BigDecimal;

    let d = LpTransferData {
        pool_id:         Arc::new("0xpool".into()),
        from_address:    Arc::new("0xfrom".into()),
        to_address:      Arc::new("0xto".into()),
        amount:          Arc::new(BigDecimal::from(1234)),
        block_number:    42,
        block_timestamp: 100,
        transaction_hash: Arc::new("0xtx".into()),
        tx_index:        1,
        log_index:       2,
    };

    assert_eq!(*d.pool_id, "0xpool");
    assert_eq!(d.block_number, 42);
}
```

- [ ] **Step 2: Run**

```bash
cargo test --lib types::v2::dex -- --nocapture
```

Expected: FAIL — `LpTransferData not defined`.

- [ ] **Step 3: Add the struct**

In `src/types/v2/dex.rs`:

```rust
use std::sync::Arc;
use bigdecimal::BigDecimal;

#[derive(Debug, Clone)]
pub struct LpTransferData {
    pub pool_id:          Arc<String>,
    pub from_address:     Arc<String>,
    pub to_address:       Arc<String>,
    pub amount:           Arc<BigDecimal>,
    pub block_number:     u64,
    pub block_timestamp:  u64,
    pub transaction_hash: Arc<String>,
    pub tx_index:         u32,
    pub log_index:        u32,
}
```

- [ ] **Step 4: Run**

```bash
cargo test --lib types::v2::dex lp_transfer_data_round_trip -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/types/v2/dex.rs
git commit -m "feat(types): add LpTransferData for V2 Pair LP transfers"
```

---

## Task 7: Add `LpTransferController` with batch insert

**Files:**
- Create: `src/db/postgres/controller/lp_transfer.rs`
- Modify: `src/db/postgres/controller/mod.rs` — register `pub mod lp_transfer;`

- [ ] **Step 1: Write the failing test**

Create `tests/lp_transfer_controller.rs`:

```rust
use std::sync::Arc;
use bigdecimal::BigDecimal;
use observer::db::postgres::{PostgresDatabase, controller::lp_transfer::{LpTransferController, LpTransferData}};

#[tokio::test]
async fn batch_insert_lp_transfers_triggers_position_update() {
    let url = std::env::var("DATABASE_URL_TEST").unwrap();
    let db = PostgresDatabase::new(&url).await.expect("connect");
    let ctrl = LpTransferController::new(Arc::new(db.clone()));

    // seed pool + dex_mint
    sqlx::query("INSERT INTO pool(pool_id, token0, token1, reserve0, reserve1)
                 VALUES ('0xp7', '0xt0', '0xt1', 0, 0)
                 ON CONFLICT (pool_id) DO NOTHING")
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO dex_mint(pool_id, sender, amount0, amount1,
                 created_at, block_number, transaction_hash, log_index, tx_index)
                 VALUES ('0xp7', '0xs', 100, 200, 100, 1, '0xtxA', 0, 0)
                 ON CONFLICT DO NOTHING")
        .execute(db.pool()).await.unwrap();

    let data = vec![LpTransferData {
        pool_id:          Arc::new("0xp7".into()),
        from_address:     Arc::new("0x0000000000000000000000000000000000000000".into()),
        to_address:       Arc::new("0xholderA".into()),
        amount:           Arc::new(BigDecimal::from(50)),
        block_number:     1,
        block_timestamp:  100,
        transaction_hash: Arc::new("0xtxA".into()),
        tx_index:         0,
        log_index:        1,
    }];

    ctrl.batch_insert(&data).await.unwrap();

    let row: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
         WHERE pool_id='0xp7' AND account_id='0xholderA'"
    ).fetch_one(db.pool()).await.unwrap();
    assert_eq!(row.0.to_string(), "50");
    assert_eq!(row.1.to_string(), "100");
    assert_eq!(row.2.to_string(), "200");
}
```

- [ ] **Step 2: Run**

```bash
cargo test --test lp_transfer_controller -- --nocapture
```

Expected: FAIL — `lp_transfer` module missing.

- [ ] **Step 3: Create the controller**

Create `src/db/postgres/controller/lp_transfer.rs`. **Use existing `dex_swap.rs`** (see `src/db/postgres/controller/dex_swap.rs:12-26`) as the pattern; the prepared SQL uses `UNNEST` arrays.

```rust
use std::sync::Arc;

use anyhow::Result;
use bigdecimal::BigDecimal;
use tracing::{error, warn};

use crate::db::postgres::PostgresDatabase;
pub use crate::types::v2::dex::LpTransferData;

pub const BATCH_INSERT_LP_TRANSFERS_SQL: &str = r#"
    INSERT INTO lp_transfer_history (
        pool_id, from_address, to_address, amount,
        block_number, transaction_hash, tx_index, log_index, created_at
    )
    SELECT * FROM UNNEST(
        $1::varchar(42)[], $2::varchar(42)[], $3::varchar(42)[],
        $4::numeric[],
        $5::bigint[], $6::text[], $7::int[], $8::int[], $9::bigint[]
    )
    ON CONFLICT (pool_id, transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub struct LpTransferController {
    db: Arc<PostgresDatabase>,
}

impl LpTransferController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self { Self { db } }

    pub async fn batch_insert(&self, items: &[LpTransferData]) -> Result<()> {
        if items.is_empty() { return Ok(()); }

        let pool_ids:      Vec<&str>       = items.iter().map(|x| x.pool_id.as_str()).collect();
        let from_addrs:    Vec<&str>       = items.iter().map(|x| x.from_address.as_str()).collect();
        let to_addrs:      Vec<&str>       = items.iter().map(|x| x.to_address.as_str()).collect();
        let amounts:       Vec<BigDecimal> = items.iter().map(|x| (*x.amount).clone()).collect();
        let block_numbers: Vec<i64>        = items.iter().map(|x| x.block_number as i64).collect();
        let tx_hashes:     Vec<&str>       = items.iter().map(|x| x.transaction_hash.as_str()).collect();
        let tx_indexes:    Vec<i32>        = items.iter().map(|x| x.tx_index as i32).collect();
        let log_indexes:   Vec<i32>        = items.iter().map(|x| x.log_index as i32).collect();
        let timestamps:    Vec<i64>        = items.iter().map(|x| x.block_timestamp as i64).collect();

        sqlx::query(BATCH_INSERT_LP_TRANSFERS_SQL)
            .bind(&pool_ids)
            .bind(&from_addrs)
            .bind(&to_addrs)
            .bind(&amounts)
            .bind(&block_numbers)
            .bind(&tx_hashes)
            .bind(&tx_indexes)
            .bind(&log_indexes)
            .bind(&timestamps)
            .execute(self.db.pool())
            .await
            .map_err(|e| {
                error!("[LP_XFER] batch insert failed: {}", e);
                anyhow::anyhow!(e)
            })?;
        Ok(())
    }
}
```

- [ ] **Step 4: Register module**

In `src/db/postgres/controller/mod.rs` add:

```rust
pub mod lp_transfer;
```

- [ ] **Step 5: Run**

```bash
cargo test --test lp_transfer_controller -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/db/postgres/controller/lp_transfer.rs src/db/postgres/controller/mod.rs tests/lp_transfer_controller.rs
git commit -m "feat(db): add LpTransferController with batch insert"
```

---

## Task 8: Pair Transfer parse helper

**Files:**
- Create: `src/event/common/token/lp_transfer.rs`
- Modify: `src/event/common/token/mod.rs` — `pub mod lp_transfer;`

The existing `token/stream.rs::parse_transfer_log` decodes ERC20 Transfer logs.
We need a similar helper that decodes via the **Pair** binding (same ERC20 layout
but distinct call site for clarity) and returns `LpTransferData`.

- [ ] **Step 1: Write the failing test**

Create `tests/lp_transfer_dispatch.rs`:

```rust
use alloy::primitives::{Address, U256, Log as PrimLog, LogData};
use alloy::sol_types::SolEvent;
use observer::event::common::token::lp_transfer::parse_lp_transfer_log;

#[test]
fn parse_lp_transfer_decodes_to_struct() {
    use crate::abi::V2INadFunPair;   // wherever the sol! binding lives
    let pool: Address = "0xpool11111111111111111111111111111111pool".parse().unwrap();
    let from: Address = "0x0000000000000000000000000000000000000000".parse().unwrap();
    let to:   Address = "0xholder11111111111111111111111111111holder".parse().unwrap();
    let value = U256::from(1234u64);

    // Build a synthetic log mimicking what alloy provides
    let event = V2INadFunPair::Transfer { from, to, value };
    let prim_log = PrimLog { address: pool, data: event.encode_log_data() };
    let rpc_log: alloy::rpc::types::Log = prim_log.into();

    let parsed = parse_lp_transfer_log(&rpc_log, 42, 100, 1, 2).expect("parse ok");
    assert_eq!(parsed.pool_id.as_str().to_lowercase(),
               pool.to_string().to_lowercase());
    assert_eq!(parsed.amount.to_string(), "1234");
}
```

(The implementer may need to adapt synthetic-log construction to the project's
actual test helpers. The pattern is in `tests/` for existing event types — search
for `Log {` literal usage.)

- [ ] **Step 2: Run**

```bash
cargo test --test lp_transfer_dispatch parse_lp_transfer_decodes_to_struct -- --nocapture
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

Create `src/event/common/token/lp_transfer.rs`:

```rust
use std::sync::Arc;

use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use bigdecimal::BigDecimal;

use crate::abi::V2INadFunPair;
use crate::types::v2::dex::LpTransferData;
use crate::utils::to_big_decimal;

/// Decode a V2 NadFunPair Transfer log into LpTransferData.
/// Returns None on decode failure or `from == to` (same-account no-op).
pub fn parse_lp_transfer_log(
    log: &Log,
    block_number: u64,
    block_timestamp: u64,
    tx_index: u32,
    log_index: u32,
) -> Option<LpTransferData> {
    let decoded = log.log_decode::<V2INadFunPair::Transfer>().ok()?;
    let V2INadFunPair::Transfer { from, to, value } = decoded.inner.data;
    if from == to { return None; }

    let tx_hash = log.transaction_hash?.to_string();
    let pool    = log.address().to_string();

    Some(LpTransferData {
        pool_id:          Arc::new(pool),
        from_address:     Arc::new(from.to_string()),
        to_address:       Arc::new(to.to_string()),
        amount:           Arc::new(to_big_decimal(value)),
        block_number,
        block_timestamp,
        transaction_hash: Arc::new(tx_hash),
        tx_index,
        log_index,
    })
}
```

- [ ] **Step 4: Register module**

In `src/event/common/token/mod.rs`:

```rust
pub mod lp_transfer;
```

- [ ] **Step 5: Run**

```bash
cargo test --test lp_transfer_dispatch parse_lp_transfer_decodes_to_struct -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/event/common/token/lp_transfer.rs src/event/common/token/mod.rs tests/lp_transfer_dispatch.rs
git commit -m "feat(event): add LP Transfer log parser"
```

---

## Task 9: Wire `parse_log` branch — route Pair Transfers to LP path

**Files:**
- Modify: `src/event/common/token/stream.rs` — `parse_log` (around line 302-339)

- [ ] **Step 1: Write the failing test (dispatch decision)**

Append to `tests/lp_transfer_dispatch.rs`:

```rust
// Pseudocode-level — adapt to project's CacheManager mock pattern.
// Goal: assert that when check_white_list_token=false AND check_dex_pool=true,
//       parse_log returns an LpTransfer event (not None).
//       When check_white_list_token=false AND check_dex_pool=false, returns None.

#[tokio::test]
async fn parse_log_routes_pair_transfers_to_lp_handler() {
    // Implementer: replicate the existing test pattern used for the whitelist
    // dispatch in src/event/common/token/stream.rs tests, if any.
    // If no existing pattern, use the cache mock from src/db/cache/mock.rs (or
    // create a minimal stub). The assertion is:
    //   cache_manager stubbed: token whitelist -> false, dex_pool -> true
    //   parse_log(pair_transfer_log) -> Vec contains TokenEvent::LpTransfer { ... }
}
```

**Note:** If the project does not have a CacheManager mock yet, mark this as an integration test instead and skip the unit-level dispatch test — go straight to the integration path in Task 12.

- [ ] **Step 2: Run**

```bash
cargo test --test lp_transfer_dispatch parse_log_routes_pair_transfers_to_lp_handler -- --nocapture
```

Expected: FAIL.

- [ ] **Step 3: Modify `parse_log`**

In `src/event/common/token/stream.rs`, around the existing whitelist-check block (line 322-339), change:

```rust
    // whitelist 체크
    let token_addr_str = token_address.to_string();
    let is_whitelist = match cache_manager.check_white_list_token(&token_addr_str).await {
        Ok(v) => v,
        Err(_) => return (None, Vec::new()),
    };

    if !is_whitelist {
        return (None, Vec::new());
    }
```

to:

```rust
    // whitelist 체크
    let token_addr_str = token_address.to_string();
    let is_whitelist = match cache_manager.check_white_list_token(&token_addr_str).await {
        Ok(v) => v,
        Err(_) => return (None, Vec::new()),
    };

    if !is_whitelist {
        // V2 Pair는 ERC20이라 Transfer 시그니처가 같음.
        // whitelist에 없지만 dex_pool에 등록된 주소면 LP Transfer로 라우팅.
        let is_pair = cache_manager.check_dex_pool(&token_addr_str).await.unwrap_or(false);
        if is_pair && log.topic0() == Some(&IToken::Transfer::SIGNATURE_HASH) {
            return parse_pair_transfer_dispatch(log, &cache_manager).await;
        }
        return (None, Vec::new());
    }
```

Add the dispatch helper at the bottom of the same file:

```rust
async fn parse_pair_transfer_dispatch(
    log: Log,
    _cache_manager: &Arc<CacheManager>,
) -> (Option<ParsedLog>, Vec<TokenEvent>) {
    use crate::event::common::token::lp_transfer::parse_lp_transfer_log;

    let meta = match LogMeta::from_log(&log, RpcClient::instance().ok()?).await {
        Ok(m) => m,
        Err(_) => return (None, Vec::new()),
    };

    let parsed = match parse_lp_transfer_log(
        &log,
        meta.block_number,
        meta.block_timestamp,
        meta.tx_index,
        meta.log_index,
    ) {
        Some(p) => p,
        None => return (None, Vec::new()),
    };

    (None, vec![TokenEvent::LpTransfer(parsed)])
}
```

- [ ] **Step 4: Add `TokenEvent::LpTransfer` variant**

In the file that defines `TokenEvent` (search `enum TokenEvent`):

```rust
pub enum TokenEvent {
    Balance(TokenBalance),
    Burn(TokenBurn),
    // ... existing variants ...
    LpTransfer(crate::types::v2::dex::LpTransferData),
}
```

Update any exhaustive match arms in the file to handle `LpTransfer` (typically just `_ => {}` is sufficient for code paths that don't care, but check `separate_token_events` at `token/stream.rs:341` and similar).

- [ ] **Step 5: Run**

```bash
cargo build
cargo test --test lp_transfer_dispatch -- --nocapture
```

Expected: build succeeds; test PASS or appropriate SKIP per Step 1 note.

- [ ] **Step 6: Commit**

```bash
git add src/event/common/token/stream.rs src/event/common/token/mod.rs   # wherever TokenEvent lives
git commit -m "feat(event): route V2 Pair Transfer logs to LP handler"
```

---

## Task 10: Receive — persist LpTransfer events

**Files:**
- Modify: `src/event/common/token/receive.rs` (the receive side of the Token stream — find via `grep -rn "TokenEvent::Burn\|TokenEvent::Balance" src/event/common/token/`)

- [ ] **Step 1: Write the failing test**

Append to `tests/lp_transfer_controller.rs`:

```rust
#[tokio::test]
async fn receive_token_events_persists_lp_transfers() {
    // Construct a Vec<TokenEvent> with one Balance + one LpTransfer.
    // Invoke the existing receive_events entry (or whatever the receive-side
    // public API is in src/event/common/token/receive.rs). Assert:
    //   - lp_transfer_history row exists
    //   - lp_position updated (trigger fired)
    //   - existing balance/burn handling still works
}
```

Implementer: follow the pattern of an existing receive test if any (search `tests/` for `receive_`). If none, create the minimal harness.

- [ ] **Step 2: Run**

```bash
cargo test --test lp_transfer_controller receive_token_events_persists_lp_transfers -- --nocapture
```

Expected: FAIL — receive does not yet branch on `LpTransfer`.

- [ ] **Step 3: Modify receive**

In the file that handles `TokenEvent` dispatch on the receive side, add an
`LpTransfer` branch that buffers into a `Vec<LpTransferData>` and calls
`LpTransferController::batch_insert` in the parallel `tokio::join!` block,
mirroring the existing balance/burn batch insert pattern.

(Concrete code: locate the existing `match event` block and add)

```rust
match event {
    // ... existing arms ...
    TokenEvent::LpTransfer(d) => {
        lp_transfer_batch.push(d);
    }
}
```

…and after the loop, parallel insert:

```rust
let lp_transfer_ctrl = LpTransferController::new(db.clone());
let lp_result = async {
    if !lp_transfer_batch.is_empty() {
        lp_transfer_ctrl.batch_insert(&lp_transfer_batch).await
    } else { Ok(()) }
};
```

Add `lp_result` to the existing `tokio::join!` tuple and `error!` on failure.

- [ ] **Step 4: Run**

```bash
cargo test --test lp_transfer_controller -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/event/common/token/receive.rs tests/lp_transfer_controller.rs
git commit -m "feat(event): persist LP Transfer events on receive"
```

---

## Task 11: Enforce sync ordering — Token waits for V2Dex

**Files:**
- Modify: `src/sync/receive.rs:140-169` (the `EventType::Token | EventType::LpManager | ...` arm)

The MINT trigger lookup in `dex_mint` relies on Token being processed **after**
V2Dex for the same block range. Today, Token only waits for `Curve`. We add
`V2Dex` as a dependency.

- [ ] **Step 1: Write the failing test**

Create `tests/sync_receive_ordering.rs`:

```rust
use observer::sync::{EventType, receive::ReceiveManager};

#[tokio::test]
async fn token_waits_for_v2dex_completion() {
    let mgr = &*observer::sync::receive::RECEIVE_MANAGER;
    let block: u64 = 100;

    // simulate Curve done but V2Dex behind
    mgr.set_last_processed_block(EventType::Curve, block, block + 5).await;
    mgr.set_last_processed_block(EventType::V2Dex, block - 5, block + 5).await;

    // check_last_processed_block(Token) must NOT return immediately (it waits up to 60s by current code)
    // For a unit test, replace the 60s timeout with something testable — see Step 3.
    let start = std::time::Instant::now();
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        mgr.check_last_processed_block(block, EventType::Token)
    ).await.expect_err("Token should be blocked while V2Dex is behind");

    // Now advance V2Dex; Token should unblock quickly
    mgr.set_last_processed_block(EventType::V2Dex, block, block + 5).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        mgr.check_last_processed_block(block, EventType::Token)
    ).await.expect("Token should unblock after V2Dex catches up");
}
```

- [ ] **Step 2: Run**

```bash
cargo test --test sync_receive_ordering token_waits_for_v2dex_completion -- --nocapture
```

Expected: FAIL — current code does not block Token on V2Dex.

- [ ] **Step 3: Modify `check_last_processed_block`**

In `src/sync/receive.rs:140-169`, the existing arm:

```rust
            EventType::Token
            | EventType::LpManager
            | EventType::Reward
            | EventType::Creator
            | EventType::Distributor => {
                let mode = *self.mode.lock().await;
                match mode {
                    ReceiveType::Live => {
                        self.wait_for_dependency(
                            start, timeout, block, event_type,
                            &[(EventType::Curve, 1)],
                        ).await;
                    }
                    ReceiveType::Sync => {
                        self.wait_for_dependency(
                            start, timeout, block, event_type,
                            &[(EventType::Curve, 1)],
                        ).await;
                    }
                }
            }
```

Split Token out and add V2Dex dependency (other event types unchanged):

```rust
            EventType::Token => {
                // LP cost-basis trigger requires dex_mint of same tx to exist
                // before lp_transfer_history insert. Enforce Token after V2Dex.
                self.wait_for_dependency(
                    start, timeout, block, EventType::Token,
                    &[(EventType::Curve, 1), (EventType::V2Dex, 1)],
                ).await;
            }
            EventType::LpManager
            | EventType::Reward
            | EventType::Creator
            | EventType::Distributor => {
                self.wait_for_dependency(
                    start, timeout, block, event_type,
                    &[(EventType::Curve, 1)],
                ).await;
            }
```

- [ ] **Step 4: Run**

```bash
cargo test --test sync_receive_ordering -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sync/receive.rs tests/sync_receive_ordering.rs
git commit -m "feat(sync): Token receive waits for V2Dex (LP cost-basis ordering)"
```

---

## Task 12: End-to-end smoke — Mint → Transfer → Burn

**Files:**
- Test: `tests/lp_position_e2e.rs`

- [ ] **Step 1: Write the failing test**

```rust
use std::sync::Arc;
use bigdecimal::BigDecimal;
use observer::db::postgres::{PostgresDatabase, controller::lp_transfer::{LpTransferController, LpTransferData}};

#[tokio::test]
async fn e2e_mint_transfer_burn_consistency() {
    let url = std::env::var("DATABASE_URL_TEST").unwrap();
    let db = Arc::new(PostgresDatabase::new(&url).await.unwrap());
    let ctrl = LpTransferController::new(db.clone());

    let pid = "0xpe2e000000000000000000000000000000000pe";
    sqlx::query("INSERT INTO pool(pool_id, token0, token1, reserve0, reserve1)
                 VALUES ($1, '0xt0', '0xt1', 0, 0) ON CONFLICT DO NOTHING")
        .bind(pid).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO dex_mint(pool_id, sender, amount0, amount1,
                 created_at, block_number, transaction_hash, log_index, tx_index)
                 VALUES ($1, '0xs', 10000, 40000, 100, 1, '0xMINT', 0, 0)
                 ON CONFLICT DO NOTHING")
        .bind(pid).execute(db.pool()).await.unwrap();

    let alice = "0xeoaA000000000000000000000000000000000eoaA";
    let bob   = "0xeoaB000000000000000000000000000000000eoaB";

    let events = vec![
        // mint to alice
        LpTransferData { pool_id: Arc::new(pid.into()),
            from_address: Arc::new("0x0000000000000000000000000000000000000000".into()),
            to_address: Arc::new(alice.into()),
            amount: Arc::new(BigDecimal::from(1000)),
            block_number: 1, block_timestamp: 100,
            transaction_hash: Arc::new("0xMINT".into()),
            tx_index: 0, log_index: 1 },
        // alice -> bob 200
        LpTransferData { pool_id: Arc::new(pid.into()),
            from_address: Arc::new(alice.into()),
            to_address: Arc::new(bob.into()),
            amount: Arc::new(BigDecimal::from(200)),
            block_number: 2, block_timestamp: 200,
            transaction_hash: Arc::new("0xXFER".into()),
            tx_index: 0, log_index: 0 },
        // bob burn 200
        LpTransferData { pool_id: Arc::new(pid.into()),
            from_address: Arc::new(bob.into()),
            to_address: Arc::new("0x0000000000000000000000000000000000000000".into()),
            amount: Arc::new(BigDecimal::from(200)),
            block_number: 3, block_timestamp: 300,
            transaction_hash: Arc::new("0xBURN".into()),
            tx_index: 0, log_index: 0 },
    ];
    ctrl.batch_insert(&events).await.unwrap();

    // alice: balance 800, cost (8000, 32000)
    let a: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
         WHERE pool_id=$1 AND account_id=$2"
    ).bind(pid).bind(alice).fetch_one(db.pool()).await.unwrap();
    assert_eq!(a.0.to_string(), "800");
    assert_eq!(a.1.to_string(), "8000");
    assert_eq!(a.2.to_string(), "32000");

    // bob: balance 0, cost 0
    let b: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT balance, cost_amount0, cost_amount1 FROM lp_position
         WHERE pool_id=$1 AND account_id=$2"
    ).bind(pid).bind(bob).fetch_one(db.pool()).await.unwrap();
    assert_eq!(b.0.to_string(), "0");
    assert_eq!(b.1.to_string(), "0");
    assert_eq!(b.2.to_string(), "0");

    // pool total_supply = 800
    let s: (BigDecimal,) = sqlx::query_as("SELECT total_supply FROM pool WHERE pool_id=$1")
        .bind(pid).fetch_one(db.pool()).await.unwrap();
    assert_eq!(s.0.to_string(), "800");
}
```

- [ ] **Step 2: Run**

```bash
cargo test --test lp_position_e2e -- --nocapture
```

Expected: PASS (all preceding infrastructure should make this green).

- [ ] **Step 3: Commit**

```bash
git add tests/lp_position_e2e.rs
git commit -m "test(lp): e2e mint→transfer→burn consistency"
```

---

## Task 13: `/codex review` gate + PR

**Files:** none — workflow.

- [ ] **Step 1: Run codex review on the branch**

```bash
# from repo root
gh pr create --draft  # or use whatever the project's draft-PR flow is
# then:
codex review
```

(CLAUDE.md absolute rule — every PR must run `/codex review`.)

- [ ] **Step 2: Address findings**

Apply AUTO-FIX items immediately. For ASK items, ask the user before applying.

- [ ] **Step 3: Verify full test suite**

```bash
cargo test --workspace
cargo test --test lp_position_trigger
cargo test --test lp_transfer_controller
cargo test --test lp_transfer_dispatch
cargo test --test sync_receive_ordering
cargo test --test lp_position_e2e
```

Expected: all PASS.

- [ ] **Step 4: Bump migrations submodule pointer in parent repo**

If migrations submodule received the new SQL files in its own PR:

```bash
cd migrations && git checkout v2 && git pull && cd ..
git add migrations
git commit -m "chore: bump migrations submodule for lp_position"
```

- [ ] **Step 5: Move PR to ready, ship**

```bash
gh pr ready
```

---

## Self-Review Notes

**Spec coverage:**
- Data model — Tasks 1-2
- Trigger MINT/BURN/HOLDER paths — Tasks 3, 4, 5
- `parse_log` branch — Task 9
- LpTransfer event type — Task 9 (variant) + Task 6 (data struct)
- Receive persistence — Task 10
- Sync/Receive Manager ordering — Task 11 (the F2 finding)
- E2E — Task 12
- PR + codex review — Task 13

**Known gaps in this plan (intentional, deferred to operations):**
- Backfill of existing pool holders — operations script, not in this PR.
- Reorg handling — observer-wide policy, not in this PR.
- APR/TVL — separate phase.
