# V2 LP Tracking — Design Spec

**Date:** 2026-05-13
**Branch:** v2
**Scope:** V2 NadFunPair LP holder tracking with cost basis. APR/TVL is OUT OF SCOPE for this phase.

## Goal

Track LP token holdings per (pool, account) on V2 DEX pools (NadFunPair), so the
"내 LP" UI can show:

1. 사용자가 어떤 풀에 LP를 보유 중인지
2. 초기에 얼마만큼 유동성 공급했는지 (cost basis: token0/token1)
3. 현재 풀 비율로 환산한 내 몫 (= balance × reserve / total_supply)
4. 증감 = 현재 몫 - 공급량 (수수료 누적 + transfer 영향 반영)

온체인 RPC 콜 없이 SQL 한 쿼리로 위 데이터 모두 산출 가능해야 함.

## Non-Goals (explicitly deferred)

- **APR / TVL / volume_24h** — 별도 phase (사용자가 분리 결정)
- **Realized/Unrealized P&L USD** — cost는 token0/1 raw로 저장. USD 환산은 응용 레이어.
- **Impermanent Loss 계산** — 응용 레이어. 본 phase는 IL 산출에 필요한 데이터(공급량 + 현재 몫)만 제공.
- **누적 fee earned** — 별도 phase.
- **Holder 리더보드 / 풀별 holder 카운트 API** — 데이터는 모임. 엔드포인트는 별건.
- **기존 풀 holder 백필** — 본 phase 출시 시점 이후 활동만 정확. 필요 시 one-shot 스크립트로 별도.
- **Reorg 처리** — observer 전체 정책에 따름. 본 모듈도 미대응(알려진 한계).
- **V1 (Capricorn CL) LP 추적** — V2 NadFunPair 한정.

## Key Findings During Brainstorming

본 설계는 코드베이스 두 발견에 결정적으로 의존:

### F1. Transfer 페치는 이미 일어나고 있다

`src/event/common/token/stream.rs:228`이 이미 체인 전역의 ERC20 Transfer 시그니처를 페치하고, `parse_log`(line 322-339)에서 Redis whitelist로 드롭하는 패턴. RPC 폭발 우려 없음.

→ 우리가 할 일은 `parse_log`에 분기 한 줄 추가: 토큰 whitelist 실패 시 `check_dex_pool` 확인 → Pair이면 LP Transfer 핸들러로 라우팅.

별도 RPC 호출 / 별도 필터 / 별도 stream 0개.

### F2. Mint 매칭 race는 Sync/Receive Manager 순서로 해결

token stream과 v2dex stream이 별도 스레드라 receive 단 매칭은 불가. 그러나 sync/receive manager에서 **token을 v2dex 다음에 처리**하도록 순서 강제 가능. 그러면 `lp_transfer_history` 인서트 시점에 같은 tx의 `dex_mint` row가 무조건 DB에 존재 → trigger lookup으로 충분, 양방향 매칭 불필요.

## Data Model

### Schema changes

```sql
-- (1) LP holder position with cost basis
CREATE TABLE lp_position (
    pool_id       VARCHAR(42) NOT NULL,
    account_id    VARCHAR(42) NOT NULL,
    balance       NUMERIC(78,0) NOT NULL DEFAULT 0,
    cost_amount0  NUMERIC(78,0) NOT NULL DEFAULT 0,
    cost_amount1  NUMERIC(78,0) NOT NULL DEFAULT 0,
    updated_at    BIGINT NOT NULL,
    PRIMARY KEY (pool_id, account_id),
    CHECK (balance >= 0)
);
CREATE INDEX idx_lp_position_account ON lp_position(account_id);

-- (2) LP Transfer raw history
--     PK convention matches existing dex_swap/dex_mint/dex_burn
CREATE TABLE lp_transfer_history (
    pool_id          VARCHAR(42) NOT NULL,
    from_address     VARCHAR(42) NOT NULL,
    to_address       VARCHAR(42) NOT NULL,
    amount           NUMERIC(78,0) NOT NULL,
    block_number     BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    tx_index         INT NOT NULL,
    log_index        INT NOT NULL,
    created_at       BIGINT NOT NULL,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX idx_lp_xfer_pool_block ON lp_transfer_history(pool_id, block_number DESC);
CREATE INDEX idx_lp_xfer_from       ON lp_transfer_history(from_address, block_number DESC);
CREATE INDEX idx_lp_xfer_to         ON lp_transfer_history(to_address, block_number DESC);

-- (3) total_supply (only new pool column needed; reserves already Sync-driven)
ALTER TABLE pool ADD COLUMN IF NOT EXISTS total_supply NUMERIC(78,0) NOT NULL DEFAULT 0;
```

Cost basis semantics:
- `cost_amount0/1` represents the cumulative token0/1 deposit associated with the
  **current** `balance`. It moves proportionally on transfers and burns.

### Trigger

`AFTER INSERT ON lp_transfer_history` — three branches by event kind:

#### Branch MINT (from = 0x0, to ≠ 0x0)
```
pool.total_supply += amount
SELECT amount0, amount1 FROM dex_mint
  WHERE pool_id = NEW.pool_id AND transaction_hash = NEW.transaction_hash
  ORDER BY log_index LIMIT 1
  -- guaranteed to exist (manager ordering)
UPSERT lp_position(NEW.to_address):
  balance      += NEW.amount
  cost_amount0 += amount0
  cost_amount1 += amount1
```

#### Branch BURN (to = 0x0, from ≠ 0x0)
```
pool.total_supply -= amount
SELECT balance, cost0, cost1 FROM lp_position(NEW.from_address) FOR UPDATE
ratio := NEW.amount / balance_before
UPDATE lp_position(NEW.from_address):
  balance      -= NEW.amount
  cost_amount0 -= cost0 * ratio
  cost_amount1 -= cost1 * ratio
```

#### Branch HOLDER → HOLDER (from ≠ 0x0, to ≠ 0x0)
```
SELECT balance, cost0, cost1 FROM lp_position(NEW.from_address) FOR UPDATE
ratio := NEW.amount / balance_before
moved_cost0 := cost0 * ratio
moved_cost1 := cost1 * ratio

UPDATE lp_position(NEW.from_address):
  balance -= amount; cost_amount0 -= moved_cost0; cost_amount1 -= moved_cost1

UPSERT lp_position(NEW.to_address):
  balance += amount; cost_amount0 += moved_cost0; cost_amount1 += moved_cost1
```

#### Edge cases
- `from = 0x0 AND to = 0x0`: WARNING log, no-op.
- `from balance is NULL or 0` on BURN/holder-transfer: indicates we started indexing
  after the holder accumulated balance pre-cutover. Trigger uses `COALESCE(balance, 0)`
  → balance goes negative, blocked by `CHECK (balance >= 0)`. **By design**: this
  loudly signals incomplete backfill instead of silently corrupting cost basis.
  Operations: re-seed via one-shot backfill (out of scope here).

## Indexing Changes

### `src/event/common/token/stream.rs::parse_log`

Add one branch after existing whitelist check:

```rust
let is_whitelist = cache_manager.check_white_list_token(&token_addr_str).await?;
if !is_whitelist {
    // NEW: check if this is a V2 Pair (LP token)
    if cache_manager.check_dex_pool(&token_addr_str).await? {
        return parse_lp_transfer_log(log, ...);   // returns (None, LpTransferEvent)
    }
    return (None, Vec::new());
}
```

### New event flow

- Add `TokenEvent::LpTransfer(LpTransferData)` variant (or similar sibling channel).
- `lp_transfer_history` is the only persistence target. lp_position + pool.total_supply
  are mutated by the trigger.

### Sync/Receive manager ordering (CRITICAL)

Token stream's receive step MUST run **after** v2dex stream's receive step for the
same block range. Concretely: the manager that coordinates these streams must enforce
ordering such that `dex_mint` / `dex_burn` for a block range are committed before
the corresponding `lp_transfer_history` rows for that block range.

If this ordering invariant is violated, the trigger MINT branch's `dex_mint` lookup
returns NULL, and cost_amount0/1 default to 0 — silently lossy. **The plan phase must
include a verification step that documents and tests this ordering.**

## Migrations

- Base file: `migrations/0021_lp_position.sql` (full schema for fresh DBs)
- Idempotent upgrade: `v2_upgrade_lp_position.sql` (`IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`)
- Worked in the `migrations` submodule. Parent repo bumps the gitlink in the same PR.

## API surface (informative — not implemented here)

User-facing "내 LP" query, no RPC required:

```sql
SELECT
    lp.pool_id,
    p.token0, p.token1,
    lp.balance                                                   AS my_lp,
    lp.cost_amount0                                              AS supplied_token0,
    lp.cost_amount1                                              AS supplied_token1,
    lp.balance * p.reserve0 / NULLIF(p.total_supply, 0)          AS share_token0,
    lp.balance * p.reserve1 / NULLIF(p.total_supply, 0)          AS share_token1
FROM lp_position lp
JOIN pool p ON p.pool_id = lp.pool_id
WHERE lp.account_id = $1 AND lp.balance > 0;
```

`share_token{0,1} - supplied_token{0,1}` = 사용자 입장의 증감(수수료 누적 + transfer 영향 반영).

## Testing Strategy (TDD; coverage 80%+)

### Unit
- LP Transfer log decoding round-trip
- `parse_log` 분기: whitelist-fail + pool-yes → LpTransfer; whitelist-fail + pool-no → drop

### DB trigger (integration)
- MINT: dex_mint先 → lp_transfer_history insert → cost basis 정확
- MINT without prior dex_mint: cost_amount0/1 = 0 (loud signal via WARNING)
- BURN: 부분 burn 시 cost가 비례 차감
- BURN: 전체 burn 시 balance=0, cost=0
- Holder → holder: cost가 비례 이동, 합산 보존
- from=to=0x0 zero-zero transfer: no-op
- from balance NULL on burn: CHECK constraint blocks (loud signal)
- total_supply: mint +, burn - 누적 일관성

### E2E
- PairCreated → Mint(user A) → Transfer(A→B 절반) → Burn(B 전부) 시나리오:
  - A: balance > 0, cost = 절반 남음
  - B: balance = 0, row 존재 또는 삭제 정책 명확화 필요 (현재 설계: row 유지, balance=0)
- Sync/receive manager ordering: dex_mint이 lp_transfer보다 늦게 들어오는 상황을 강제로 만들어 트리거가 어떻게 실패하는지 회귀 테스트

## PR Plan

**PR-A (단일 PR로 충분):**
- migrations (base + v2_upgrade)
- types: `V2LpTransfer` (or `TokenEvent::LpTransfer`)
- cache: `check_dex_pool` 활용 (이미 존재) — 새 메서드 없음
- token stream parse_log 분기 추가
- LpTransfer 채널 + `lp_transfer_history` 배치 인서트 컨트롤러
- sync/receive manager 순서 강제 (token after v2dex)
- 트리거 + 검증 테스트

각 PR은 `/codex review` 통과 후 머지 (CLAUDE.md 절대 룰).

## Known Limitations (intentional)

1. 출시 시점 이후 활동만 정확. 기존 풀 holder는 백필 phase 별도.
2. Reorg 미대응 — observer 전체 정책에 합류.
3. 코스트 베이시스가 token0/1 raw amount. USD 환산은 응용 레이어 (vault USD enrich 패턴과 분리).
4. APR/TVL은 본 phase에 없음 — 다음 phase.

## Open Questions

(없음 — 모든 결정 사항 본 문서에 명시. 구현 phase에서 발견되는 미스매치는
deviation으로 별도 처리.)
