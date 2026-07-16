# V2 LP Fee Accrual & APR Tracking — Design Spec

**Date:** 2026-05-20
**Branch:** design/v2-lp-fee-apr-tracking (예정)
**Scope:** V2 NadFunPair 풀의 LP fee 누적량을 chain invariant 로 측정해 시계열로 저장하고, APR 노출 형식(컬럼/뷰)을 정의한다. API 엔드포인트 구현 / 프론트 노출 / 백필은 본 phase 범위 밖.

## Goal

V2 DEX 풀에 LP 공급한 사용자의 수익률(APR)을 데이터로 제공한다. 핵심은 별도 LP-fee 이벤트가 emit 되지 않는 V2 표준 패턴에서, **pair contract reserve 에 retained 되는 fee 를 chain invariant (`k = reserve0 × reserve1`) 변화로 직접 측정**한다는 점.

산출 결과:

1. 풀별 **시계열 fee accrual** (token0/token1 raw + USD, hourly bucket)
2. 풀별 **평균 TVL** (hourly bucket 내 sync 시점 단순 평균)
3. 위 두 가지를 조합해 `apr_24h / apr_7d / apr_30d` 를 산출하는 **read-side view**

소비자 (websocket-server / 백엔드 API) 는 view 만 SELECT 하면 APR 노출 가능.

## Non-Goals (explicitly deferred)

- **API 엔드포인트 / WebSocket 구독** — observer 는 데이터 + view 만 제공. 노출은 다른 컴포넌트.
- **mint/burn segment 끼인 sync 의 부분 fee 추출** — mint/burn 이 끼인 sync 는 baseline 만 갱신, fee 누적 skip. 같은 tx 안에서 mint/burn 영향과 fee 를 분리하는 정교한 알고리즘은 후속 phase.
- **시간가중 TVL** — 현재는 bucket 내 sync 시점 단순 평균. sync 빈도가 시간에 따라 편향되면 약간 부정확. v2 phase.
- **백필** — 마이그레이션 시점 이후 dex_sync 만 반영. 기존 풀의 초반 24h/7d/30d 윈도우는 점진적으로 정확해진다. 필요 시 one-shot 스크립트 별도.
- **reorg 처리** — observer 전체 정책에 따른다. 본 모듈도 미대응(알려진 한계).
- **V1 (Capricorn CL) LP fee 추적** — V2 NadFunPair 한정.
- **Impermanent Loss 계산** — 응용 레이어.
- **Creator/protocol fee** — `v2_fee_collect_history` / `v2_fee_settle_history` 로 이미 별도 인덱싱 중. 본 spec 은 pair reserve 에 retained 되는 LP 몫만 다룬다.

## Key Findings During Brainstorming

### F1. nad.fun V2 에는 별도 LP-fee 이벤트가 없다

`FeeCollector.Collect/Settle/Setup/FeeToClaim` 은 모두 **creator + curve_protocol + dex_protocol** 분배용. LP 가 가져가는 swap fee 몫은 Uniswap V2 표준대로 pair 의 reserve 에 retained 된다 — 별도 이벤트 없이.

따라서 LP fee 를 측정하려면 `dex_sync` reserve 변화에서 mint/burn 영향을 분리해 swap-driven 변화만 추출해야 한다.

### F2. constant-product invariant 로 fee 를 측정할 수 있다

V2 invariant 상 mint/burn 가 없으면 매 swap 은 `k = r0 × r1` 를 **단조 증가** 시킨다 (retained fee 때문). LP share 1 단위의 invariant 가치는 `√k`. 따라서 두 연속 sync 사이 (mint/burn 없을 때):

- `ratio = √k_new / √k_old`
- `share_growth = ratio - 1`
- `fee_usd = share_growth × pool_tvl_usd_at_event`

가격이 어떻게 움직여도 (구성비가 바뀌어도) k 자체는 fee 가 아니면 안 바뀌므로 robust.

**Token 단위 fee 의 한계**: dex_sync 만으로는 어느 쪽 token 으로 fee 가 들어왔는지(어느 쪽이 in 이었는지) 분리할 수 없다. 본 spec 의 `fee_token0/1` 컬럼은 정확한 retained-token 누적이 아니라 **share-growth equivalent**:

- `fee_token0_equiv = share_growth × r0_new`
- `fee_token1_equiv = share_growth × r1_new`

= "현시점에 LP 가 그 share 증가분만큼 인출하면 추가로 받는 token 양". 단일 swap 단위 정확 token 추출은 `dex_swap.amount{0,1}_in` + LP fee rate 기반의 후속 phase 로 분리.

### F3. mint/burn 끼인 sync 는 baseline 만 갱신

`dex_mint` / `dex_burn` 가 emit 된 tx 의 sync 는 reserve 변화에 fee 성분과 mint/burn 성분이 섞인다. 본 phase 는 단순화를 위해 **그런 sync 는 fee 누적 skip 하고 `pool.last_sqrt_k` baseline 만 갱신**한다. 손실되는 fee 는 그 한 swap 분뿐이라 windowed 평균에 미치는 영향은 미미.

`dex_mint` / `dex_burn` 인덱싱은 이미 V2 LP tracking phase 에서 완료 (`migrations/0023_dex_event_tables.sql`).

### F4. 기존 트리거 패턴 재사용 가능

`migrations/0024_pool_volume_trigger.sql` 의 `update_pool_volume` 이 statement-level trigger 로 batch INSERT 한 번에 group-by-pool 로 누적하는 패턴 확립. 본 phase 트리거도 동일 스타일로 작성.

## Data Model

### Schema changes (new migration)

```sql
-- (1) pool: fee 산정 baseline 컬럼 추가
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sqrt_k     NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_at    BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_block BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_tx_index  INT NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_log_index INT NOT NULL DEFAULT 0;
-- (block, tx_index, log_index) 트리플 보유 이유: 같은 batch 안 여러 sync 의 ordering 처리.

-- (2) hourly bucket
CREATE TABLE IF NOT EXISTS pool_fee_hourly (
    pool_id        VARCHAR(42) NOT NULL,
    bucket_hour    BIGINT      NOT NULL,                   -- floor(created_at / 3600)
    fee_token0     NUMERIC     NOT NULL DEFAULT 0,         -- share-growth equivalent token0 (Algorithm 참고)
    fee_token1     NUMERIC     NOT NULL DEFAULT 0,         -- share-growth equivalent token1
    fee_usd        NUMERIC     NOT NULL DEFAULT 0,         -- USD 환산 (가격 없으면 0)
    tvl_usd_sum    NUMERIC     NOT NULL DEFAULT 0,         -- sample 별 TVL 합 (avg 계산용)
    sample_count   INT         NOT NULL DEFAULT 0,         -- 누적된 sync 개수 (mint/burn skip 제외)
    updated_at     BIGINT      NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, bucket_hour)
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_hourly_pool_hour
    ON pool_fee_hourly (pool_id, bucket_hour DESC);
CREATE INDEX IF NOT EXISTS idx_pool_fee_hourly_hour
    ON pool_fee_hourly (bucket_hour DESC);
```

마이그레이션 파일: `0027_pool_fee_hourly.sql` (다음 번호). prod 운영 중 DB 용 대응 마이그레이션은 `v2_upgrade_pool_fee_hourly.sql` 에 동일 idempotent SQL.

## Algorithm

### Trigger: `update_pool_fee_accrual`

`dex_sync` AFTER INSERT statement trigger. transition table `new_dex_syncs` 의 모든 row 를 한 statement 안에서 처리.

처리 절차:

1. **Ordering**: `new_dex_syncs` 를 `(pool_id, block_number, tx_index, log_index)` ASC 로 정렬.
2. **Mint/burn 끼임 체크**: 각 sync 가 같은 `(pool_id, block_number, transaction_hash)` 안의 `dex_mint` / `dex_burn` 와 함께 있는지 LEFT JOIN 으로 확인. 매칭되면 그 sync 는 **fee 누적 skip**, baseline (`pool.last_sqrt_k`, `last_sync_*`) 만 갱신. SQL 예시 (trigger 안):
   ```sql
   FROM new_dex_syncs s
   LEFT JOIN dex_mint m
       ON m.pool_id = s.pool_id
      AND m.transaction_hash = s.transaction_hash
   LEFT JOIN dex_burn b
       ON b.pool_id = s.pool_id
      AND b.transaction_hash = s.transaction_hash
   WHERE m.pool_id IS NULL AND b.pool_id IS NULL  -- fee 누적 대상
   ```
   주의: V2 LP tracking phase 의 stream ordering finding (F2 of 2026-05-13 spec) — token stream 이 v2dex 다음에 받게 강제되어 있어, dex_sync insert 시점에 같은 tx 의 dex_mint/dex_burn 은 이미 DB 에 있음 → LEFT JOIN race-free.
3. **Baseline race**: 같은 batch 안 같은 pool 의 sync 가 여러 개면 ordering 유지 채로 cumulative 계산. 직전 sync 의 (computed) sqrt_k 를 다음 sync 의 baseline 으로 사용.
4. **Fee 계산** (mint/burn 안 끼인 경우):
   ```
   sqrt_k_new = sqrt(reserve0_new * reserve1_new)
   sqrt_k_old = pool.last_sqrt_k (또는 batch 안 직전 sync 의 sqrt_k_new)
   IF sqrt_k_old > 0 AND sqrt_k_new > sqrt_k_old:
       ratio          = sqrt_k_new / sqrt_k_old
       share_growth   = ratio - 1
       fee_usd        = share_growth * (token0_usd + token1_usd)  -- sync 시점 TVL
       fee_token0     = share_growth * reserve0_new               -- share-growth equiv
       fee_token1     = share_growth * reserve1_new
       tvl_usd_at_evt = token0_usd + token1_usd
   ELIF sqrt_k_new <= sqrt_k_old:
       -- mint/burn 누락 의심, 또는 분 단위 precision noise → fee skip, WARNING 로그, baseline 만 갱신
   ELSE:
       -- 첫 sync: baseline 만, fee = 0
   ```
5. **Bucket 누적**: `bucket_hour = floor(created_at / 3600)` 로 `pool_fee_hourly` 에 UPSERT (`fee_token0/1`, `fee_usd`, `tvl_usd_sum` 은 ADD, `sample_count` 는 +1).
6. **Pool baseline 갱신**: 그룹의 마지막 sync 값으로 `pool.last_sqrt_k`, `last_sync_at`, `last_sync_block`, `last_sync_tx_index`, `last_sync_log_index` 를 UPDATE.

### Edge cases

- **첫 sync** (pool.last_sqrt_k = 0): baseline 만 잡고 fee = 0.
- **TVL = 0** (token0_usd + token1_usd = 0, 가격 미상): `fee_usd = 0` 누적, `fee_token0/1` 은 정상 누적, `sample_count` 는 +1, `tvl_usd_sum` 은 +0.
- **sqrt_k_new < sqrt_k_old** (mint/burn 누락 또는 reorg/잘못된 데이터): fee 누적 skip, baseline 만 갱신, WARNING 로그.
- **같은 (block, tx) 안 sync 가 mint/burn 와 같이 있을 때**: 본 spec 은 baseline 만 갱신, fee skip.

### Order constraint with existing triggers

- `update_pool_reserves` (기존, dex_sync 에 reserve/value 반영) 와 `update_pool_fee_accrual` 둘 다 dex_sync 의 AFTER INSERT trigger 가 될 수 있다.
- **fee_accrual trigger 가 reserve trigger 보다 먼저 실행**되어야 한다 — `pool.last_sqrt_k` 의 직전 값과 새 sync 의 reserve 를 비교해야 하므로. trigger 이름 알파벳 순서 (`a_update_pool_fee_accrual` < `update_pool_reserves`) 또는 명시적 ordering 으로 보장.
- 또는 fee_accrual trigger 안에서 `pool` 테이블을 안 읽고 transition table 만으로 batch 자체에서 baseline 을 추적하면 trigger 간 ordering 의존 제거 가능. 구현 시 후자 우선.

## Read API Contract

### View: `pool_apr`

소비자가 그대로 SELECT 하는 안정 인터페이스. 마이그레이션과 함께 배포.

```sql
CREATE OR REPLACE VIEW pool_apr AS
WITH now_h AS (SELECT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600)::BIGINT AS h)
SELECT
    f.pool_id,
    -- USD fee accrued
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24)        AS fee_24h_usd,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)      AS fee_7d_usd,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)     AS fee_30d_usd,
    -- token share-growth equivalent (24h / 7d / 30d)
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24)     AS fee_24h_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24)     AS fee_24h_token1,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)   AS fee_7d_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)   AS fee_7d_token1,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)  AS fee_30d_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)  AS fee_30d_token1,
    -- avg TVL (sample 단순 평균)
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

APR 환산은 소비자가:

```
apr_24h = fee_24h_usd / tvl_24h_usd_avg * 365 * 100   -- % 단위
apr_7d  = fee_7d_usd  / tvl_7d_usd_avg  / 7  * 365 * 100
apr_30d = fee_30d_usd / tvl_30d_usd_avg / 30 * 365 * 100
```

(view 안에서 APR 까지 계산 vs raw 노출 결정 — 본 phase 는 **raw 노출**. APR 정의 (% 단위, 시간 환산 등) 변경 시 view 만 바꾸기 위해 그리고 분모 = 0 케이스를 소비자가 명시적으로 다루도록.)

### 노출 컬럼 (소비자 측 표준)

API/WS 가 expose 할 때 표준 필드명:

- `apr_24h`, `apr_7d`, `apr_30d` (% , float)
- `fee_24h_usd`, `fee_7d_usd`, `fee_30d_usd`
- `tvl_24h_avg_usd`, `tvl_7d_avg_usd`, `tvl_30d_avg_usd`

소비자가 view 의 raw 컬럼에서 위 형식으로 변환. 본 spec 은 컬럼명만 못 박는다.

## Code Changes (observer)

본 spec 은 **DB 레이어만**. application 코드 변경 거의 없음:

- `src/db/postgres/controller/v2/pool.rs` (혹은 dex 컨트롤러) 에 `pool_fee_hourly` 관련 read helper 한두 개 추가 (필요 시).
- `migrations/0027_pool_fee_hourly.sql` 신규.
- `migrations/v2_upgrade_pool_fee_hourly.sql` 신규 (prod idempotent).
- 트리거가 모든 무거운 일을 함. Rust 쪽 batch INSERT 경로 (`BATCH_INSERT_DEX_SYNCS_SQL`) 변경 없음.

## Testing Strategy

> Project rule: `/tdd` (TDD via superpowers:test-driven-development). 본 phase 도 동일.

### Unit (Rust)

- 없음 — 트리거가 SQL 안에서 작동.

### Integration (SQL / sqlx)

`tests/db/pool_fee_accrual_test.rs` 신규. 시나리오:

1. **첫 sync** → fee = 0, baseline 잡힘.
2. **연속 swap 2개** → `√k_new/√k_old - 1` 비율과 일치하는 `fee_usd` 누적.
3. **mint 끼인 sync** → 그 sync 는 fee skip, baseline 만 갱신. 이후 swap fee 정상 누적.
4. **burn 끼인 sync** → 동일.
5. **가격 미상 (token0_usd = token1_usd = 0)** → `fee_token0/1` 정상, `fee_usd = 0`, `tvl_usd_sum += 0`.
6. **같은 batch 안 여러 sync** → ordering 보장, cumulative 계산 정확.
7. **sqrt_k 감소 (negative fee)** → skip + WARNING. `fee_usd >= 0` invariant 보장.
8. **hourly bucket 경계 넘김** → 두 bucket 에 나눠 누적.
9. **`pool_apr` view** → seed 데이터로 fee_24h / tvl_24h_avg / APR 계산 검증.

### Coverage 목표

- 트리거 분기 100%.
- view 의 윈도우별 SUM/AVG 정확성 검증 (24h/7d/30d 경계).

## Migration & Rollout

1. PR 1: 본 spec (이 파일) commit → docs PR.
2. PR 2: `0027_pool_fee_hourly.sql` + `v2_upgrade_pool_fee_hourly.sql` + 트리거 + 통합 테스트.
3. testnet 배포 → 24h 관찰 → mainnet.
4. 백필 필요 시 별도 one-shot 스크립트 (out of scope of this spec).

## Open Questions / Future Work

- mint/burn segment 끼인 sync 의 부분 fee 추출 (v2 phase): mint amount0/1, burn amount0/1 가 dex_mint/dex_burn 에 이미 있으므로, 그 영향을 reserve 에서 빼고 남은 부분을 fee 로 산정 가능.
- 시간가중 TVL 로 업그레이드 시 `pool_fee_hourly` 에 `tvl_usd_time_sum` (Δt × tvl 누적) 컬럼 추가.
- `pool_apr` view 에 fee tier 별 분해 (`creator_fee` 등 protocol fee 와 합산해 "total APR") — 별도 phase.
