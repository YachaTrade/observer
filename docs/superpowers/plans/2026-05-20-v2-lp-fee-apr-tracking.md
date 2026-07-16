# V2 LP Fee Accrual & APR Tracking — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** V2 NadFunPair 풀의 LP fee 누적량을 `k = r0 × r1` invariant 로 측정해 hourly bucket 으로 적재하고, `pool_apr` view 로 24h/7d/30d APR 노출용 데이터를 제공한다.

**Architecture:** Pure DB layer. `dex_sync` AFTER INSERT statement-level trigger 가 직전 sync 대비 `√k` 변화율을 계산해 `pool_fee_hourly` 에 UPSERT, `pool.last_sqrt_k` baseline 을 갱신. mint/burn 끼인 sync 는 baseline 만 갱신, fee 누적 skip. Rust 코드 변경 0. SQL + 통합 테스트만.

**Tech Stack:** PostgreSQL 15+ (statement-level trigger, transition tables, FILTER), sqlx, Rust integration tests.

**Spec:** `docs/superpowers/specs/2026-05-20-v2-lp-fee-apr-tracking-design.md`

**Base branch:** `v2` (이미 `design/v2-lp-fee-apr-tracking` 가 v2 위에 rebase 됨, spec commit `93ad8a3` 존재). 이 plan 의 모든 commit 은 같은 branch 에 쌓는다.

**External dependency (noted, not blocking):** dex_sync.token0_usd/token1_usd 와 pool.value 가 v2 base 시점에 default 0. `design/v2-onchain-price-graph` 작업이 v2 에 머지된 뒤 USD-side fee 가 의미 있는 값으로 채워진다. 본 plan 의 알고리즘은 TVL=0 graceful (fee_usd=0 누적, fee_token0/1 정상 누적, sample_count 정상) 이므로 dependency 머지 전에도 안전하게 작동.

---

## File Structure

**Create:**
- `migrations/0027_pool_fee_hourly.sql` — 신규 스키마 + 트리거 + 뷰 (fresh DB)
- `migrations/v2_upgrade_pool_fee_hourly.sql` — 동일 SQL, prod idempotent
- `tests/pool_fee_accrual.rs` — 통합 테스트

**Modify:**
- `tests/common/mod.rs` — `call_batch_insert_dex_syncs/mints/burns` helper 추가 (없는 경우)

---

## Task 1 — 스키마 & view 골격 (트리거는 빈 함수)

**Files:**
- Create: `migrations/0027_pool_fee_hourly.sql`
- Create: `migrations/v2_upgrade_pool_fee_hourly.sql`

이 task 는 **테이블/뷰/빈 트리거** 만 만든다. 트리거 본문(fee 산정)은 Task 4 부터 TDD 로 채운다. 빈 트리거는 baseline 갱신만 하는 no-op (fee 누적 0).

- [ ] **Step 1: `migrations/0027_pool_fee_hourly.sql` 작성**

전체 파일 내용:

```sql
-- 0027_pool_fee_hourly.sql
--
-- V2 LP fee accrual & APR tracking. See:
--   docs/superpowers/specs/2026-05-20-v2-lp-fee-apr-tracking-design.md
--
-- (1) pool: baseline columns
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sqrt_k        NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_at       BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_block    BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_tx_index INT     NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_log_index INT    NOT NULL DEFAULT 0;

-- (2) hourly bucket
CREATE TABLE IF NOT EXISTS pool_fee_hourly (
    pool_id        VARCHAR(42) NOT NULL,
    bucket_hour    BIGINT      NOT NULL,
    fee_token0     NUMERIC     NOT NULL DEFAULT 0,
    fee_token1     NUMERIC     NOT NULL DEFAULT 0,
    fee_usd        NUMERIC     NOT NULL DEFAULT 0,
    tvl_usd_sum    NUMERIC     NOT NULL DEFAULT 0,
    sample_count   INT         NOT NULL DEFAULT 0,
    updated_at     BIGINT      NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, bucket_hour)
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_hourly_pool_hour
    ON pool_fee_hourly (pool_id, bucket_hour DESC);
CREATE INDEX IF NOT EXISTS idx_pool_fee_hourly_hour
    ON pool_fee_hourly (bucket_hour DESC);

-- (3) trigger function: stub (baseline 갱신만, fee 산정은 Task 4+ 에서 추가)
CREATE OR REPLACE FUNCTION update_pool_fee_accrual()
RETURNS TRIGGER AS $$
BEGIN
    -- Stub: 다음 task 에서 본문 채움. 현재는 baseline 만 갱신 (fee 누적 없음).
    UPDATE pool p
       SET last_sqrt_k         = sqrt(d.reserve0 * d.reserve1),
           last_sync_at        = d.created_at,
           last_sync_block     = d.block_number,
           last_sync_tx_index  = d.tx_index,
           last_sync_log_index = d.log_index
      FROM (
          SELECT DISTINCT ON (pool_id)
                 pool_id, reserve0, reserve1, created_at, block_number, tx_index, log_index
            FROM new_dex_syncs
           ORDER BY pool_id, block_number DESC, tx_index DESC, log_index DESC
      ) d
     WHERE p.pool_id = d.pool_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 트리거 이름은 알파벳상 reserve trigger 보다 먼저 실행되도록 'a_' prefix.
DROP TRIGGER IF EXISTS a_trg_update_pool_fee_accrual ON dex_sync;
CREATE TRIGGER a_trg_update_pool_fee_accrual
    AFTER INSERT ON dex_sync
    REFERENCING NEW TABLE AS new_dex_syncs
    FOR EACH STATEMENT
    EXECUTE FUNCTION update_pool_fee_accrual();

-- (4) pool_apr view
CREATE OR REPLACE VIEW pool_apr AS
WITH now_h AS (SELECT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600)::BIGINT AS h)
SELECT
    f.pool_id,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24)        AS fee_24h_usd,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)      AS fee_7d_usd,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)     AS fee_30d_usd,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24)     AS fee_24h_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24)     AS fee_24h_token1,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)   AS fee_7d_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)   AS fee_7d_token1,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)  AS fee_30d_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)  AS fee_30d_token1,
    SUM(f.tvl_usd_sum) FILTER (WHERE f.bucket_hour >= now_h.h - 24)
        / NULLIF(SUM(f.sample_count) FILTER (WHERE f.bucket_hour >= now_h.h - 24), 0)
        AS tvl_24h_usd_avg,
    SUM(f.tvl_usd_sum) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)
        / NULLIF(SUM(f.sample_count) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7), 0)
        AS tvl_7d_usd_avg,
    SUM(f.tvl_usd_sum) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)
        / NULLIF(SUM(f.sample_count) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30), 0)
        AS tvl_30d_usd_avg
FROM pool_fee_hourly f
CROSS JOIN now_h
WHERE f.bucket_hour >= now_h.h - 24*30
GROUP BY f.pool_id, now_h.h;
```

- [ ] **Step 2: `migrations/v2_upgrade_pool_fee_hourly.sql` 작성**

내용은 0027 과 동일 (모두 idempotent: `IF NOT EXISTS`, `CREATE OR REPLACE`, `DROP TRIGGER IF EXISTS`). 파일 헤더만 다름:

```sql
-- v2_upgrade_pool_fee_hourly.sql
--
-- Idempotent prod upgrade for the schema/trigger/view added by
-- migrations/0027_pool_fee_hourly.sql. Apply manually on prod where the
-- numbered migration cannot run (pre-existing DB state).
--
-- (copy of 0027 body below — keep in sync)

<same body as 0027 from "(1) pool: baseline columns" downward>
```

- [ ] **Step 3: testcontainers 로 migration 적용 확인 (sanity test)**

`tests/common/mod.rs` 의 `apply_baseline_migrations` 가 `migrations/NNNN_*.sql` 파일을 자동 발견/적용하므로, 별도 psql 실행 불필요. 임시 sanity test 로 검증:

`tests/pool_fee_accrual.rs` 신규 (Task 3 에서 본격화하지만 여기서 minimal smoke):

```rust
mod common;

use anyhow::Result;
use common::setup_test_db;

#[tokio::test]
async fn migration_creates_pool_fee_hourly_table() -> Result<()> {
    let db = setup_test_db().await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'pool_fee_hourly')",
    )
    .fetch_one(&db.pool).await?;
    assert!(exists, "pool_fee_hourly table should exist after migrations");

    let has_last_sqrt_k: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema='public' AND table_name='pool' AND column_name='last_sqrt_k')",
    )
    .fetch_one(&db.pool).await?;
    assert!(has_last_sqrt_k, "pool.last_sqrt_k column should exist");

    let view_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.views \
         WHERE table_schema='public' AND table_name='pool_apr')",
    )
    .fetch_one(&db.pool).await?;
    assert!(view_exists, "pool_apr view should exist");
    Ok(())
}
```

Run:
```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --test pool_fee_accrual migration_creates_pool_fee_hourly_table -- --nocapture 2>&1 | tail -15
```

Expected: PASS. (Docker 필요 — testcontainers 가 ephemeral Postgres 띄움.)

이 테스트는 Task 3 의 `first_sync_sets_baseline_no_fee` 가 추가되면 그대로 두어도 되고 (스키마 sanity 보장 좋음), 또는 그쪽이 같은 보장을 포함한다면 삭제해도 됨.

- [ ] **Step 4: Commit**

```bash
git add migrations/0027_pool_fee_hourly.sql migrations/v2_upgrade_pool_fee_hourly.sql
git commit -m "feat(pool): schema + view skeleton for LP fee hourly tracking

Schema: pool.last_sqrt_k/last_sync_* baseline columns,
pool_fee_hourly bucket table, pool_apr view.
Trigger function is a stub that only updates baseline.
Fee accrual logic added in follow-up commits."
```

---

## Task 2 — 테스트 헬퍼 추가

**Files:**
- Modify: `tests/common/mod.rs` — `call_batch_insert_dex_syncs/mints/burns` helper 추가 (없는 경우)

- [ ] **Step 1: 헬퍼 존재 여부 확인**

Run:
```bash
grep -n "call_batch_insert_dex_sync\|call_batch_insert_dex_mint\|call_batch_insert_dex_burn" \
    tests/common/mod.rs
```

이미 있으면 해당 helper 의 시그니처만 확인하고 Step 2 skip. 없으면 Step 2 진행.

- [ ] **Step 2: helper 3개 추가**

`tests/common/mod.rs` 의 다른 `call_*` helper 와 같은 스타일로, 파일 끝에 추가:

```rust
/// Call `BATCH_INSERT_DEX_SYNCS_SQL` with a single sync tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_dex_syncs(
    pool: &PgPool,
    pool_id: &str,
    reserve0: &str,
    reserve1: &str,
    created_at: i64,
    block_number: i64,
    transaction_hash: &str,
    log_index: i32,
    tx_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::dex_swap::BATCH_INSERT_DEX_SYNCS_SQL)
        .bind(&vec![pool_id])
        .bind(&vec![parse(reserve0)])
        .bind(&vec![parse(reserve1)])
        .bind(&vec![created_at])
        .bind(&vec![block_number])
        .bind(&vec![transaction_hash])
        .bind(&vec![log_index])
        .bind(&vec![tx_index])
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_DEX_SYNCS_SQL")?;
    Ok(())
}

/// Call `BATCH_INSERT_DEX_MINTS_SQL` with a single mint tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_dex_mints(
    pool: &PgPool,
    pool_id: &str,
    sender: &str,
    amount0: &str,
    amount1: &str,
    created_at: i64,
    block_number: i64,
    transaction_hash: &str,
    log_index: i32,
    tx_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::dex_swap::BATCH_INSERT_DEX_MINTS_SQL)
        .bind(&vec![pool_id])
        .bind(&vec![sender])
        .bind(&vec![parse(amount0)])
        .bind(&vec![parse(amount1)])
        .bind(&vec![created_at])
        .bind(&vec![block_number])
        .bind(&vec![transaction_hash])
        .bind(&vec![log_index])
        .bind(&vec![tx_index])
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_DEX_MINTS_SQL")?;
    Ok(())
}

/// Call `BATCH_INSERT_DEX_BURNS_SQL` with a single burn tuple.
#[allow(clippy::too_many_arguments)]
pub async fn call_batch_insert_dex_burns(
    pool: &PgPool,
    pool_id: &str,
    sender: &str,
    to_address: &str,
    amount0: &str,
    amount1: &str,
    created_at: i64,
    block_number: i64,
    transaction_hash: &str,
    log_index: i32,
    tx_index: i32,
) -> Result<()> {
    use std::str::FromStr;
    let parse = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
    sqlx::query(observer::db::postgres::controller::dex_swap::BATCH_INSERT_DEX_BURNS_SQL)
        .bind(&vec![pool_id])
        .bind(&vec![sender])
        .bind(&vec![to_address])
        .bind(&vec![parse(amount0)])
        .bind(&vec![parse(amount1)])
        .bind(&vec![created_at])
        .bind(&vec![block_number])
        .bind(&vec![transaction_hash])
        .bind(&vec![log_index])
        .bind(&vec![tx_index])
        .execute(pool)
        .await
        .context("failed to execute BATCH_INSERT_DEX_BURNS_SQL")?;
    Ok(())
}

/// Helper: seed a pool row directly. dex_sync trigger 가 `pool` 테이블의 row 를
/// 참조하므로, 통합 테스트는 pool row 를 먼저 만들어 두어야 한다.
pub async fn insert_test_pool(
    pool: &PgPool,
    pool_id: &str,
    token0: &str,
    token1: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO pool (pool_id, token0, token1, created_at, block_number, tx_hash) \
         VALUES ($1, $2, $3, 0, 0, '') ON CONFLICT (pool_id) DO NOTHING",
    )
    .bind(pool_id)
    .bind(token0)
    .bind(token1)
    .execute(pool)
    .await
    .context("failed to insert test pool")?;
    Ok(())
}
```

- [ ] **Step 3: 빌드 확인**

Run:
```bash
cargo test --no-run --test pool_fee_accrual 2>&1 | tail -5 || true
cargo test --no-run --tests 2>&1 | tail -5
```

Expected: 컴파일 통과 (test 파일 없어도 OK — 다른 tests 가 helper 사용 안 해도 컴파일은 됨).

- [ ] **Step 4: Commit**

```bash
git add tests/common/mod.rs
git commit -m "test(common): add dex_sync/mint/burn batch insert + pool seed helpers"
```

---

## Task 3 — Smoke test: 첫 sync → baseline 만 잡힘, fee = 0

**Files:**
- Create: `tests/pool_fee_accrual.rs`

이 task 부터 트리거 본문을 TDD 로 채워나간다. Stub 트리거 (Task 1) 가 이미 baseline 갱신만 하므로 첫 시나리오는 그 동작 검증.

- [ ] **Step 1: 테스트 파일 골격 + 첫 테스트 작성**

`tests/pool_fee_accrual.rs` 신규:

```rust
//! Integration tests for the V2 LP fee accrual trigger
//! (`update_pool_fee_accrual` on `dex_sync`).
//!
//! Spec: docs/superpowers/specs/2026-05-20-v2-lp-fee-apr-tracking-design.md

mod common;

use anyhow::Result;
use bigdecimal::BigDecimal;
use common::{
    call_batch_insert_dex_syncs, insert_test_pool, setup_test_db,
};
use std::str::FromStr;

const POOL: &str = "0xpool00000000000000000000000000000000001";
const T0: &str = "0xtoken00000000000000000000000000000000001";
const T1: &str = "0xtoken00000000000000000000000000000000002";

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

#[tokio::test]
async fn first_sync_sets_baseline_no_fee() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    // First sync — baseline 갱신만, fee 없음.
    call_batch_insert_dex_syncs(
        &db.pool, POOL,
        "1000", "1000",                       // r0, r1 → k = 1_000_000, √k = 1000
        1_700_000_000, 1, "0xtx1", 0, 0,
    ).await?;

    let (last_sqrt_k, last_at): (BigDecimal, i64) = sqlx::query_as(
        "SELECT last_sqrt_k, last_sync_at FROM pool WHERE pool_id = $1",
    )
    .bind(POOL)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(last_sqrt_k, bd("1000"));
    assert_eq!(last_at, 1_700_000_000);

    let bucket_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pool_fee_hourly WHERE pool_id = $1",
    )
    .bind(POOL)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(bucket_count, 0, "첫 sync 는 bucket row 생성하지 않음");
    Ok(())
}
```

- [ ] **Step 2: 테스트 실행 — pass 예상 (stub 트리거가 이미 baseline 만 갱신)**

Run:
```bash
cargo test --test pool_fee_accrual first_sync_sets_baseline_no_fee -- --nocapture 2>&1 | tail -10
```

Expected: PASS. (stub 트리거 동작 검증)

- [ ] **Step 3: Commit**

```bash
git add tests/pool_fee_accrual.rs
git commit -m "test(pool): verify first dex_sync sets baseline without fee accrual"
```

---

## Task 4 — 연속 swap 2개 → fee 누적 (트리거 본문 1차 구현)

**Files:**
- Modify: `migrations/0027_pool_fee_hourly.sql` — 트리거 함수 본문 채움
- Modify: `migrations/v2_upgrade_pool_fee_hourly.sql` — 같은 본문 sync
- Modify: `tests/pool_fee_accrual.rs` — 시나리오 테스트 추가

- [ ] **Step 1: 실패 테스트 작성**

`tests/pool_fee_accrual.rs` 끝에 추가:

```rust
#[tokio::test]
async fn two_consecutive_syncs_accumulate_fee() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    // 첫 sync: r0=r1=1000 → √k = 1000
    call_batch_insert_dex_syncs(
        &db.pool, POOL, "1000", "1000",
        1_700_000_000, 1, "0xtx1", 0, 0,
    ).await?;

    // 두 번째 sync: swap fee 로 k 증가 — r0=1100, r1=1000 → k = 1_100_000, √k ≈ 1048.808...
    // share_growth = 1048.808/1000 - 1 ≈ 0.048808
    // dex_sync.token0_usd/token1_usd 는 default 0 → fee_usd = 0, fee_token0/1 은 양수.
    // fee_token0 = share_growth * 1100 ≈ 53.689
    // fee_token1 = share_growth * 1000 ≈ 48.808
    call_batch_insert_dex_syncs(
        &db.pool, POOL, "1100", "1000",
        1_700_000_100, 2, "0xtx2", 0, 0,
    ).await?;

    let (fee_token0, fee_token1, fee_usd, sample_count): (BigDecimal, BigDecimal, BigDecimal, i32) =
        sqlx::query_as(
            "SELECT fee_token0, fee_token1, fee_usd, sample_count \
             FROM pool_fee_hourly WHERE pool_id = $1",
        )
        .bind(POOL)
        .fetch_one(&db.pool)
        .await?;

    // numeric precision 은 평가 시 5자리까지 검사
    let f0 = fee_token0.to_string().parse::<f64>().unwrap();
    let f1 = fee_token1.to_string().parse::<f64>().unwrap();
    assert!((f0 - 53.689).abs() < 0.1, "fee_token0 = {} (expected ~53.689)", f0);
    assert!((f1 - 48.808).abs() < 0.1, "fee_token1 = {} (expected ~48.808)", f1);
    assert_eq!(fee_usd, bd("0"), "TVL=0 환경 → fee_usd = 0");
    assert_eq!(sample_count, 1);

    // baseline 도 두 번째 sync 로 진행됐는지
    let last_sqrt_k: BigDecimal =
        sqlx::query_scalar("SELECT last_sqrt_k FROM pool WHERE pool_id = $1")
            .bind(POOL).fetch_one(&db.pool).await?;
    let lsk = last_sqrt_k.to_string().parse::<f64>().unwrap();
    assert!((lsk - 1048.808).abs() < 0.1);
    Ok(())
}
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run:
```bash
cargo test --test pool_fee_accrual two_consecutive_syncs_accumulate_fee -- --nocapture 2>&1 | tail -15
```

Expected: FAIL — `pool_fee_hourly` 에 row 없음 (stub 트리거가 bucket 누적 안 함).

- [ ] **Step 3: 트리거 본문 구현 (mint/burn 검출은 Task 5)**

`migrations/0027_pool_fee_hourly.sql` 의 `update_pool_fee_accrual` 함수 본문을 통째로 교체:

```sql
CREATE OR REPLACE FUNCTION update_pool_fee_accrual()
RETURNS TRIGGER AS $$
BEGIN
    -- 1) ordered syncs per pool with cumulative baseline tracking
    WITH ordered AS (
        SELECT
            s.pool_id, s.reserve0, s.reserve1, s.created_at,
            s.block_number, s.tx_index, s.log_index,
            s.token0_usd, s.token1_usd,
            sqrt(s.reserve0 * s.reserve1) AS sqrt_k_new,
            -- prev 는 (a) 같은 batch 안 직전 sync 의 sqrt_k_new, 없으면 (b) pool.last_sqrt_k
            COALESCE(
                LAG(sqrt(s.reserve0 * s.reserve1)) OVER w,
                (SELECT last_sqrt_k FROM pool WHERE pool_id = s.pool_id)
            ) AS sqrt_k_old
        FROM new_dex_syncs s
        WINDOW w AS (
            PARTITION BY s.pool_id
            ORDER BY s.block_number, s.tx_index, s.log_index
        )
    ),
    -- 2) fee 산정: sqrt_k_old > 0 AND sqrt_k_new > sqrt_k_old
    fee_rows AS (
        SELECT
            pool_id,
            (created_at / 3600)::BIGINT                      AS bucket_hour,
            (sqrt_k_new / sqrt_k_old - 1)                    AS share_growth,
            (sqrt_k_new / sqrt_k_old - 1) * reserve0         AS fee_token0,
            (sqrt_k_new / sqrt_k_old - 1) * reserve1         AS fee_token1,
            (sqrt_k_new / sqrt_k_old - 1) * (token0_usd + token1_usd) AS fee_usd,
            (token0_usd + token1_usd)                        AS tvl_usd_at_evt
        FROM ordered
        WHERE sqrt_k_old > 0 AND sqrt_k_new > sqrt_k_old
    )
    -- 3) bucket 누적
    INSERT INTO pool_fee_hourly (
        pool_id, bucket_hour, fee_token0, fee_token1, fee_usd, tvl_usd_sum, sample_count, updated_at
    )
    SELECT
        pool_id, bucket_hour,
        SUM(fee_token0), SUM(fee_token1), SUM(fee_usd),
        SUM(tvl_usd_at_evt), COUNT(*)::INT,
        EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
    FROM fee_rows
    GROUP BY pool_id, bucket_hour
    ON CONFLICT (pool_id, bucket_hour) DO UPDATE SET
        fee_token0   = pool_fee_hourly.fee_token0   + EXCLUDED.fee_token0,
        fee_token1   = pool_fee_hourly.fee_token1   + EXCLUDED.fee_token1,
        fee_usd      = pool_fee_hourly.fee_usd      + EXCLUDED.fee_usd,
        tvl_usd_sum  = pool_fee_hourly.tvl_usd_sum  + EXCLUDED.tvl_usd_sum,
        sample_count = pool_fee_hourly.sample_count + EXCLUDED.sample_count,
        updated_at   = EXCLUDED.updated_at;

    -- 4) baseline 갱신: pool 별 마지막 sync (block/tx/log 최대)
    UPDATE pool p
       SET last_sqrt_k         = d.sqrt_k_new,
           last_sync_at        = d.created_at,
           last_sync_block     = d.block_number,
           last_sync_tx_index  = d.tx_index,
           last_sync_log_index = d.log_index
      FROM (
          SELECT DISTINCT ON (pool_id)
                 pool_id, sqrt(reserve0 * reserve1) AS sqrt_k_new,
                 created_at, block_number, tx_index, log_index
            FROM new_dex_syncs
           ORDER BY pool_id, block_number DESC, tx_index DESC, log_index DESC
      ) d
     WHERE p.pool_id = d.pool_id;

    -- 5) WARNING for sqrt_k 감소 (mint/burn 누락 의심)
    --    (Task 5 의 mint/burn 검출이 들어오면 사실상 발생 안 함)
    PERFORM 1 FROM new_dex_syncs s
      JOIN pool p ON p.pool_id = s.pool_id
     WHERE p.last_sqrt_k > 0
       AND sqrt(s.reserve0 * s.reserve1) < p.last_sqrt_k;
    IF FOUND THEN
        RAISE WARNING 'pool_fee_accrual: sqrt_k decreased without mint/burn (mint/burn missing?)';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

`migrations/v2_upgrade_pool_fee_hourly.sql` 의 같은 함수도 동일 본문으로 교체 (두 파일 동기 유지).

- [ ] **Step 4: 테스트 실행 → 통과 확인**

`setup_test_db` 가 매 테스트마다 ephemeral container 를 새로 띄우고 최신 migrations 폴더를 자동 적용하므로 별도 psql 명령 불필요.

Run:
```bash
cargo test --test pool_fee_accrual two_consecutive_syncs_accumulate_fee -- --nocapture 2>&1 | tail -15
```

Expected: PASS.

- [ ] **Step 5: 기존 first_sync 테스트도 회귀 없는지 확인**

Run:
```bash
cargo test --test pool_fee_accrual -- --nocapture 2>&1 | tail -15
```

Expected: 두 테스트 PASS.

- [ ] **Step 6: Commit**

```bash
git add migrations/0027_pool_fee_hourly.sql migrations/v2_upgrade_pool_fee_hourly.sql tests/pool_fee_accrual.rs
git commit -m "feat(pool): accumulate LP fee via sqrt(k) ratio on dex_sync

Trigger compares each sync's sqrt(r0*r1) against the previous baseline
(intra-batch LAG, falling back to pool.last_sqrt_k). When sqrt_k grows,
UPSERTs share-growth-equivalent fee_token0/1, fee_usd (= growth * TVL),
and TVL sample into pool_fee_hourly bucket. Baseline is then advanced
to the last sync per pool.

Mint/burn filtering deferred to follow-up commit."
```

---

## Task 5 — Mint/burn 끼인 sync 는 fee skip (baseline 만 갱신)

**Files:**
- Modify: `migrations/0027_pool_fee_hourly.sql` — fee_rows CTE 에 mint/burn 제외 조건 추가
- Modify: `migrations/v2_upgrade_pool_fee_hourly.sql` — sync
- Modify: `tests/pool_fee_accrual.rs` — mint, burn 시나리오 테스트 2개 추가

- [ ] **Step 1: 실패 테스트 2개 작성**

`tests/pool_fee_accrual.rs` 끝에 추가:

```rust
#[tokio::test]
async fn mint_blocked_sync_skips_fee_accrual() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    // 첫 sync: baseline
    call_batch_insert_dex_syncs(
        &db.pool, POOL, "1000", "1000",
        1_700_000_000, 1, "0xtx1", 0, 0,
    ).await?;

    // 같은 tx 에 mint + sync — mint 가 reserve 를 키워 k 가 커지지만 fee 아님.
    // 순서 중요: F2 finding (token stream 이 v2dex 다음) — 본 테스트는 trigger 진입
    // 시점에 dex_mint 가 이미 존재한다는 invariant 를 시뮬레이션하기 위해 mint 먼저 insert.
    common::call_batch_insert_dex_mints(
        &db.pool, POOL, "0xminter000000000000000000000000000000001",
        "100", "100",
        1_700_000_100, 2, "0xtx2", 0, 0,
    ).await?;
    call_batch_insert_dex_syncs(
        &db.pool, POOL, "1100", "1100",
        1_700_000_100, 2, "0xtx2", 1, 0,
    ).await?;

    let bucket_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pool_fee_hourly WHERE pool_id = $1",
    )
    .bind(POOL).fetch_one(&db.pool).await?;

    assert_eq!(bucket_count, 0, "mint 가 끼인 sync 는 fee 누적 skip");

    // baseline 은 두 번째 sync 로 진행됐어야 함 — 다음 swap 의 fee 산정 기준선이므로.
    let last_sqrt_k: BigDecimal =
        sqlx::query_scalar("SELECT last_sqrt_k FROM pool WHERE pool_id = $1")
            .bind(POOL).fetch_one(&db.pool).await?;
    let lsk = last_sqrt_k.to_string().parse::<f64>().unwrap();
    assert!((lsk - 1100.0).abs() < 0.1, "baseline 은 mint 후 reserve 로 갱신: √(1100*1100)=1100");
    Ok(())
}

#[tokio::test]
async fn burn_blocked_sync_skips_fee_accrual() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    // 첫 sync
    call_batch_insert_dex_syncs(
        &db.pool, POOL, "1000", "1000",
        1_700_000_000, 1, "0xtx1", 0, 0,
    ).await?;

    // 같은 tx 에 burn + sync — burn 은 reserve 를 줄여 k 감소.
    common::call_batch_insert_dex_burns(
        &db.pool, POOL, "0xburner000000000000000000000000000000001",
        "0xto000000000000000000000000000000000000001",
        "100", "100",
        1_700_000_100, 2, "0xtx2", 0, 0,
    ).await?;
    call_batch_insert_dex_syncs(
        &db.pool, POOL, "900", "900",
        1_700_000_100, 2, "0xtx2", 1, 0,
    ).await?;

    let bucket_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pool_fee_hourly WHERE pool_id = $1",
    )
    .bind(POOL).fetch_one(&db.pool).await?;
    assert_eq!(bucket_count, 0, "burn 가 끼인 sync 는 fee 누적 skip");
    Ok(())
}
```

(`common::` 가 mod common 으로 import 됐는지 확인 — 이미 `mod common;` 선언되어 있으므로 `common::call_batch_insert_dex_mints` 형태로 호출 가능.)

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run:
```bash
cargo test --test pool_fee_accrual mint_blocked_sync_skips_fee_accrual -- --nocapture 2>&1 | tail -10
cargo test --test pool_fee_accrual burn_blocked_sync_skips_fee_accrual -- --nocapture 2>&1 | tail -10
```

Expected: 첫 번째 FAIL (mint 가 있어도 현재 트리거가 fee 누적해 row 1 개 생성). 두 번째 PASS (burn 으로 sqrt_k 감소 → 현재도 fee 누적 skip — `sqrt_k_new > sqrt_k_old` 조건). 두 번째는 즉시 통과해도 OK.

- [ ] **Step 3: 트리거 본문에 mint/burn 제외 조건 추가**

`migrations/0027_pool_fee_hourly.sql` 의 트리거 함수에서 `ordered` CTE 의 `FROM new_dex_syncs s` 를 다음으로 교체:

```sql
        FROM new_dex_syncs s
        LEFT JOIN dex_mint m
            ON m.pool_id = s.pool_id
           AND m.transaction_hash = s.transaction_hash
        LEFT JOIN dex_burn b
            ON b.pool_id = s.pool_id
           AND b.transaction_hash = s.transaction_hash
        WHERE m.pool_id IS NULL AND b.pool_id IS NULL
```

(baseline UPDATE 의 `FROM new_dex_syncs` 는 그대로 유지 — mint/burn 끼인 sync 도 baseline 은 갱신해야 다음 swap 산정 기준선이 맞음.)

`migrations/v2_upgrade_pool_fee_hourly.sql` 도 동일하게.

- [ ] **Step 4: 테스트**

Run:
```bash
cargo test --test pool_fee_accrual -- --nocapture 2>&1 | tail -20
```

Expected: 4 테스트 모두 PASS (first_sync, two_consecutive, mint_blocked, burn_blocked).

- [ ] **Step 5: Commit**

```bash
git add migrations/0027_pool_fee_hourly.sql migrations/v2_upgrade_pool_fee_hourly.sql tests/pool_fee_accrual.rs
git commit -m "feat(pool): exclude mint/burn-blocked syncs from fee accrual

LEFT JOIN dex_mint/dex_burn on (pool_id, transaction_hash) — if a sync
shares its tx with a mint or burn, skip fee accumulation but still
advance the sqrt_k baseline so subsequent swaps measure correctly.
Relies on F2 (token stream ordering) from the V2 LP tracking spec to
guarantee dex_mint/dex_burn are persisted before dex_sync arrives."
```

---

## Task 6 — Same-batch 다중 sync ordering 검증

**Files:**
- Modify: `tests/pool_fee_accrual.rs`

- [ ] **Step 1: 테스트 추가**

`tests/pool_fee_accrual.rs` 끝:

```rust
#[tokio::test]
async fn multi_sync_in_one_batch_cumulative_growth() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    // 첫 sync 로 baseline 잡기
    call_batch_insert_dex_syncs(
        &db.pool, POOL, "1000", "1000",
        1_700_000_000, 1, "0xtx0", 0, 0,
    ).await?;

    // 같은 batch (한 statement) 에 두 sync 동시 insert.
    // sync A: r=1100,1000 → √k≈1048.81
    // sync B: r=1200,1050 → √k≈1122.50
    // expected fee_token0 (cumulative): (1048.81/1000 - 1)*1100 + (1122.50/1048.81 - 1)*1200
    //                                 ≈ 53.69 + 84.32 ≈ 138.01
    let arr_pool: Vec<&str> = vec![POOL, POOL];
    let arr_r0: Vec<BigDecimal> = vec![bd("1100"), bd("1200")];
    let arr_r1: Vec<BigDecimal> = vec![bd("1000"), bd("1050")];
    let arr_ts: Vec<i64> = vec![1_700_000_100, 1_700_000_200];
    let arr_blk: Vec<i64> = vec![2, 3];
    let arr_tx: Vec<&str> = vec!["0xtxA", "0xtxB"];
    let arr_log: Vec<i32> = vec![0, 0];
    let arr_txidx: Vec<i32> = vec![0, 0];
    sqlx::query(observer::db::postgres::controller::dex_swap::BATCH_INSERT_DEX_SYNCS_SQL)
        .bind(&arr_pool).bind(&arr_r0).bind(&arr_r1)
        .bind(&arr_ts).bind(&arr_blk)
        .bind(&arr_tx).bind(&arr_log).bind(&arr_txidx)
        .execute(&db.pool).await?;

    let fee_token0: BigDecimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(fee_token0), 0) FROM pool_fee_hourly WHERE pool_id = $1",
    )
    .bind(POOL).fetch_one(&db.pool).await?;
    let f0 = fee_token0.to_string().parse::<f64>().unwrap();
    assert!((f0 - 138.01).abs() < 0.5, "cumulative fee_token0 = {} (expected ~138.01)", f0);
    Ok(())
}
```

- [ ] **Step 2: 실행 → 통과 확인**

Run:
```bash
cargo test --test pool_fee_accrual multi_sync_in_one_batch_cumulative_growth -- --nocapture 2>&1 | tail -10
```

Expected: PASS — Task 4 의 LAG window function 이 batch 안 cumulative 산정 보장.

만약 FAIL: LAG ordering 검토 (`PARTITION BY pool_id ORDER BY block, tx_index, log_index`) — sub-row 가 같은 block_number 일 때 tx_index 가 진짜 다른지 등.

- [ ] **Step 3: Commit**

```bash
git add tests/pool_fee_accrual.rs
git commit -m "test(pool): verify cumulative sqrt_k tracking across multi-sync batch"
```

---

## Task 7 — Hourly bucket 경계 + TVL 누적 검증

**Files:**
- Modify: `tests/pool_fee_accrual.rs`

- [ ] **Step 1: 테스트 추가**

```rust
#[tokio::test]
async fn syncs_split_across_hour_boundary() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    // hour bucket H0 = 1_700_000_000 / 3600 = 472222
    // hour bucket H1 = 1_700_003_600 / 3600 = 472223
    call_batch_insert_dex_syncs(&db.pool, POOL, "1000", "1000",
        1_700_000_000, 1, "0xtx0", 0, 0).await?;
    call_batch_insert_dex_syncs(&db.pool, POOL, "1100", "1000",
        1_700_000_100, 2, "0xtx1", 0, 0).await?;   // H0 에 누적
    call_batch_insert_dex_syncs(&db.pool, POOL, "1200", "1000",
        1_700_003_700, 3, "0xtx2", 0, 0).await?;   // H1 에 누적

    let rows: Vec<(i64, i32)> = sqlx::query_as(
        "SELECT bucket_hour, sample_count FROM pool_fee_hourly \
         WHERE pool_id = $1 ORDER BY bucket_hour",
    )
    .bind(POOL).fetch_all(&db.pool).await?;
    assert_eq!(rows.len(), 2, "두 시간 bucket 에 row 생성");
    assert_eq!(rows[0].0, 1_700_000_000 / 3600);
    assert_eq!(rows[0].1, 1);
    assert_eq!(rows[1].0, 1_700_003_700 / 3600);
    assert_eq!(rows[1].1, 1);
    Ok(())
}

#[tokio::test]
async fn tvl_usd_sum_uses_per_side_usd_columns() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    call_batch_insert_dex_syncs(&db.pool, POOL, "1000", "1000",
        1_700_000_000, 1, "0xtx0", 0, 0).await?;

    // 두 번째 sync 를 직접 SQL 로 insert — token0_usd/token1_usd 지정
    sqlx::query(
        "INSERT INTO dex_sync (pool_id, reserve0, reserve1, value, token0_usd, token1_usd, \
            created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(POOL)
    .bind(bd("1100")).bind(bd("1000"))
    .bind(bd("0"))                        // value (legacy) — 알고리즘에서 쓰지 않음
    .bind(bd("200")).bind(bd("100"))      // token0_usd, token1_usd → TVL = 300
    .bind(1_700_000_100_i64).bind(2_i64).bind("0xtx1")
    .bind(0_i32).bind(0_i32)
    .execute(&db.pool).await?;

    let (fee_usd, tvl_sum): (BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT fee_usd, tvl_usd_sum FROM pool_fee_hourly WHERE pool_id = $1",
    )
    .bind(POOL).fetch_one(&db.pool).await?;

    let fee_usd_f = fee_usd.to_string().parse::<f64>().unwrap();
    let tvl_sum_f = tvl_sum.to_string().parse::<f64>().unwrap();
    // share_growth ≈ 0.048808, TVL = 300 → fee_usd ≈ 14.64
    assert!((fee_usd_f - 14.64).abs() < 0.5, "fee_usd = {} (expected ~14.64)", fee_usd_f);
    assert!((tvl_sum_f - 300.0).abs() < 0.1);
    Ok(())
}
```

- [ ] **Step 2: 실행 → 통과 확인**

Run:
```bash
cargo test --test pool_fee_accrual syncs_split_across_hour_boundary tvl_usd_sum_uses_per_side_usd_columns -- --nocapture 2>&1 | tail -15
```

Expected: 두 테스트 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/pool_fee_accrual.rs
git commit -m "test(pool): hour-boundary split + per-side USD TVL accumulation"
```

---

## Task 8 — pool_apr view 검증

**Files:**
- Modify: `tests/pool_fee_accrual.rs`

- [ ] **Step 1: 테스트 추가**

```rust
#[tokio::test]
async fn pool_apr_view_returns_windowed_fee_and_tvl() -> Result<()> {
    let db = setup_test_db().await?;
    insert_test_pool(&db.pool, POOL, T0, T1).await?;

    // bucket_hour 가 "최근 24h 안" 이 되도록 현재시각 기준으로 sync.
    let now_h = (chrono::Utc::now().timestamp() / 3600) as i64;
    let ts_recent = now_h * 3600 + 60;
    let ts_recent_2 = now_h * 3600 + 120;

    call_batch_insert_dex_syncs(&db.pool, POOL, "1000", "1000",
        ts_recent, 1, "0xtx0", 0, 0).await?;
    sqlx::query(
        "INSERT INTO dex_sync (pool_id, reserve0, reserve1, value, token0_usd, token1_usd, \
            created_at, block_number, transaction_hash, log_index, tx_index) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(POOL).bind(bd("1100")).bind(bd("1000"))
    .bind(bd("0")).bind(bd("200")).bind(bd("100"))
    .bind(ts_recent_2).bind(2_i64).bind("0xtx1")
    .bind(0_i32).bind(0_i32)
    .execute(&db.pool).await?;

    let row: (BigDecimal, BigDecimal, BigDecimal) = sqlx::query_as(
        "SELECT fee_24h_usd, tvl_24h_usd_avg, fee_24h_token0 \
           FROM pool_apr WHERE pool_id = $1",
    )
    .bind(POOL).fetch_one(&db.pool).await?;

    let fee = row.0.to_string().parse::<f64>().unwrap();
    let tvl_avg = row.1.to_string().parse::<f64>().unwrap();
    let f0 = row.2.to_string().parse::<f64>().unwrap();

    assert!((fee - 14.64).abs() < 0.5, "fee_24h_usd = {} (expected ~14.64)", fee);
    assert!((tvl_avg - 300.0).abs() < 0.1, "tvl_24h_usd_avg = {} (expected 300)", tvl_avg);
    assert!((f0 - 53.69).abs() < 0.5, "fee_24h_token0 = {} (expected ~53.69)", f0);
    Ok(())
}
```

(상단에 `use chrono;` 또는 `chrono::Utc` 가 필요하면 `tests/common/mod.rs` 의 dependency 와 동일 — `Cargo.toml [dev-dependencies]` 에 `chrono` 이미 있는지 확인. 없으면 `std::time::SystemTime` 사용:)

```rust
let now_h = (std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() / 3600) as i64;
```

(chrono 가 없다면 위 SystemTime 버전으로 교체.)

- [ ] **Step 2: 실행 → 통과 확인**

Run:
```bash
cargo test --test pool_fee_accrual pool_apr_view_returns_windowed_fee_and_tvl -- --nocapture 2>&1 | tail -15
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/pool_fee_accrual.rs
git commit -m "test(pool): pool_apr view exposes windowed fee + avg TVL"
```

---

## Task 9 — 전체 회귀 + push

**Files:** —

- [ ] **Step 1: 전체 테스트 회귀**

Run:
```bash
cd /Users/gyu/project/nads-pump/observer
cargo test --tests 2>&1 | tail -30
```

Expected: 모든 통합 테스트 PASS. 새로 추가한 `pool_fee_accrual` 의 모든 케이스 + 기존 테스트 회귀 없음.

- [ ] **Step 2: `/codex review` (project rule)**

Run:
```
/codex review
```

AUTO-FIX 즉시 적용. ASK 항목 user 확인 후 적용.

- [ ] **Step 3: codex 리뷰 fix 가 있으면 별도 commit**

(없으면 skip)

- [ ] **Step 4: branch push**

Run:
```bash
git push origin design/v2-lp-fee-apr-tracking
```

- [ ] **Step 5: 기존 PR (#211) 에 구현 commit 들 자동 포함됨**

PR #211 (spec only) 가 같은 branch 라서 push 만 하면 구현 commit 까지 PR 에 합쳐짐. PR description 을 업데이트:

```bash
gh pr edit 211 --body "$(cat <<'EOF'
## Summary

V2 NadFunPair 풀의 LP fee 누적량을 chain invariant (k = r0 × r1) 변화로 측정해
hourly bucket 으로 적재하고, pool_apr view 로 24h/7d/30d APR 노출용 데이터를 제공.

## Commits

- spec: design 문서
- schema + view skeleton
- trigger: sqrt(k) ratio 로 fee 누적
- mint/burn 끼인 sync 는 fee skip, baseline 만 갱신
- 통합 테스트 7건

## Spec

`docs/superpowers/specs/2026-05-20-v2-lp-fee-apr-tracking-design.md`

## External dependency

dex_sync.token0_usd/token1_usd 와 pool.value 의 USD population 은
design/v2-onchain-price-graph 의 후속 작업. 본 PR 의 알고리즘은
TVL=0 graceful — fee_token0/1 은 정상 누적, fee_usd 는 USD population 작업
머지 후 의미 있는 값으로 채워짐.

## Test plan

- [ ] cargo test --tests 전체 PASS
- [ ] /codex review AUTO-FIX 반영
- [ ] testnet 24h 관찰 (fee_token0/1 누적 + apr_24h NULL/0 → USD merge 후 의미 있는 값)
EOF
)"
```

---

## Self-Review

### Spec coverage

| Spec 섹션 | Plan task |
|---|---|
| Schema: pool ALTER + pool_fee_hourly | Task 1 |
| Algorithm: √k ratio, share_growth, fee_token/usd | Task 4 |
| Mint/burn 끼임 skip | Task 5 |
| 첫 sync edge case | Task 3 |
| Same-batch ordering | Task 6 |
| Hourly bucket | Task 7 |
| Per-side USD TVL | Task 7 |
| pool_apr view | Task 1 (정의) + Task 8 (검증) |
| TVL=0 graceful | Task 4 (테스트 내 assertion) |
| sqrt_k 감소 WARNING | Task 4 (트리거 본문 내 RAISE WARNING) |

빈 곳: spec 의 "trigger 간 ordering" — Task 1 의 `a_` prefix 로 해결. 빠진 항목 없음.

### Placeholder scan

- `<same body as 0027 from "(1)" downward>` (Task 1 Step 2) — 명시적 "copy body" 지시. 엔지니어가 0027 본문을 그대로 복사하도록 의도. 명확함, placeholder 아님.
- 모든 SQL/Rust 코드 블록 inline 으로 완성됨. TBD/TODO 없음.

### Type consistency

- 트리거 함수명: 일관 (`update_pool_fee_accrual`)
- 트리거명: 일관 (`a_trg_update_pool_fee_accrual`)
- 테이블/컬럼명: spec 과 plan 모두 `pool_fee_hourly`, `fee_token0/1`, `fee_usd`, `tvl_usd_sum`, `sample_count`
- helper 함수명: `call_batch_insert_dex_syncs/mints/burns`, `insert_test_pool` — Task 2 에서 정의 후 Task 3+ 에서 사용 일관

### Scope

Single phase, single PR. 외부 dependency 명시. 적정.
