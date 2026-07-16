# design/v2-onchain-price-graph

## Purpose

`dex_swap.value` 와 `pool.volume` 을 indexer 내부에서 USD-단위로 계산한다. 외부 oracle 의존을 **WMON USD price 1개** 로 압축하고, 나머지 가격은 chain 의 swap/sync reserve 에서 추론.

PR #209 의 N3 revert (indexer 가 oracle 의존 회피) 의 후속 — Pyth 는 WMON 만 쓰고, token-token swap 가격은 chain 자체에서 자연 추출.

## Core algorithm — 한 문단

```
on swap/sync at pool (t0, t1, reserve0, reserve1, block):
    if t0 == WMON:
        token_price_cache[t1][block] = reserve0 / reserve1     # t1 의 WMON 단위 가격
    elif t1 == WMON:
        token_price_cache[t0][block] = reserve1 / reserve0
    elif token_price_cache[t0] exists:
        token_price_cache[t1][block] = (reserve0 * t0_price) / reserve1
    elif token_price_cache[t1] exists:
        token_price_cache[t0][block] = (reserve1 * t1_price) / reserve0
    # else: 둘 다 모름 → 이 pool 의 swap value 는 0 (orphan)

# 매 RawSwap 처리 시:
wmon_usd = pyth(WMON, block)                          # 기존 price_cache
token_wmon = token_price_cache[token][block]          # 신규 cache
value_usd = (amount / 10^decimals) * token_wmon * wmon_usd
```

순서 invariant: 한 batch 의 RawSync/RawSwap event 를 `(block_number, log_index)` 로 sort 후 직렬 inference. 같은 (block, tx) 안에서 Swap → Sync 순서로 emit (Uniswap V2 spec) 이므로 sort 만 하면 자연 보장.

## Components

```rust
// 신규
token_price_cache: DashMap<token_id, DashMap<block, Arc<BigDecimal>>>
token_price_insertion_order: RwLock<HashMap<token_id, VecDeque<block>>>  // cleanup용

// 기존 (그대로)
price_cache: DashMap<quote_id, DashMap<block, BigDecimal>>  // Pyth USD per quote
```

cleanup: `remove_token_prices_before_or_equal_all_tokens(block - 1000)` — 기존 C4 패턴 그대로 재사용 + token 별 newest 1개 항상 보존 (long-inactive token 의 first-swap-after-gap 가 orphan 되는 것 방지).

oracle 표면적: WMON USD price 만 (`price_cache` via Pyth). 나머지 토큰 가격은 chain 자체에서 자연 회복.

**재시작 warm-up**: cache 자체는 메모리만이지만 startup 직후 `warm_up_token_price_cache(sentinel_block)` 가 `pool` 테이블의 현재 reserve 들로 forward propagation 을 fixpoint 까지 1회 실행. 재시작 후 첫 batch 부터 정상 USD value. live RawSync inference 가 자연 덮어씀.

## Implementation plan

1. ✅ **Phase 1**: `token_price_cache` 인프라 — `src/db/cache/mod.rs`
   - DashMap + insertion-order RwLock (TOCTOU-safe)
   - `insert_token_price`, `get_token_price`, `get_latest_token_price_before`, `remove_token_prices_before_or_equal_all_tokens`
   - `get_token_decimals_factor` helper (quote_token / dex_token union, fallback 18)
2. ✅ **Phase 2**: `process_raw_dex_events` 재구조
   - Pre-fetch pool meta (token0, token1, d0, d1) per unique pool (CacheManager.get_pool_pair + pool table fallback)
   - Sort events by `(block_number, log_index)`, single-thread inference loop
   - RawSync → `update_token_price_from_sync` (4-case propagation, zero guards)
   - RawSwap → `compute_swap_value_usd` 결과를 `dex_swap.value` 에 박음
   - V2 dex receive 끝에 `remove_token_prices_before_or_equal_all_tokens(to_block - 1000)` cleanup
3. **Phase 3**: PR draft + codex review

**Schema drift 확인 (2026-05-19)**: prod 의 `pool` 테이블에는 `volume NUMERIC NOT NULL DEFAULT 0` 컬럼이 이미 있음 (`\d pool` 로 확인). 그러나 migrations submodule 의 `0014_dex.sql` / `v2_upgrade_new_tables.sql` 정의에는 누락 — 누군가 manual ALTER 또는 별도 migration 으로 prod 만 갱신됨. 동반 migrations submodule PR 로 정의 동기화 (idempotent ALTER). 

`dex_swap.value` 에 chain-implied USD value 박고, **`pool.volume` 도 같은 CTE 안에서 자동 누적** — replay-idempotent.

## Open: 안전망들 (운영 후 도입)

처음 구현엔 포함 X. 운영하면서 진짜로 깨지는 거 보고 하나씩 추가.

| ID | 무엇 | 진짜 발생 빈도 | 영향 | 대응 |
|---|---|---|---|---|
| S1 | swap-implied vs reserve-implied 선택 | 큰 slippage swap 가끔 | 작음 | reserve-implied 로 시작, 운영 안 좋으면 swap-implied 비교 |
| S2 | batch 내 다중 swap race | 같은 block 다중 pool swap 흔함 | 작음 (한 swap 동안 stale) | sort 만으로 충분, 더 엄밀한 ordering 보장은 deferred |
| S3 | orphan flag (`value_unknown` 컬럼) | 신규 토큰 출시 시 | 중간 (운영 통계 불명확) | schema 변경 동반, 후행 PR |
| S4 | BigDecimal scale / zero guard | zero swap 거의 X | 거의 X | `safe_ratio` helper 처음부터 포함 (실수 방지) |
| S5 | cache 분리 (price_cache + token_price_cache) | 코드 구조 | 영향 X | 분리로 시작 — 두 cache 다른 의미 |
| S6 | reorg detection (H2) | Monad 빈도 낮음 | 큼 (1번 발생 시 영구 오염) | nice-to-have, 운영 데이터 보고 결정 |
| S7 | cache 영속화 (Redis/DB) | 재시작 가끔 | 작음 | 메모리만 — 재시작 후 warm-up |

S4, S5 는 첫 구현에 들어감 (cost 거의 0). S1~S3, S6, S7 은 deferred.

## Verification — 어떻게 확인하나

### 1. 컴파일 / 정적 검사
- `cargo build` clean
- `cargo check --tests` clean
- `/codex review --base v2` clean (round 3 통과 — P1 idempotency / P2 keep-newest 두 round 해소 후)

### 2. 단위 테스트 (후속 작업)
PR 내 추가 권장 (현재는 deferred — 사용자 결정 따라):
- `update_token_price_from_sync` 4-case 가지치기
  - t0=WMON → t1 price = r0/r1
  - t1=WMON → t0 price = r1/r0
  - t0 known → t1 price = (r0 * t0_price) / r1
  - t1 known → t0 price = (r1 * t1_price) / r0
  - 둘 다 모름 → 변화 X (orphan)
- `insert_token_price` same-block dedupe — 같은 block 두 번 호출 후 order queue 크기 = 1
- `remove_token_prices_before_or_equal_all_tokens` keep-newest — 1000 block 너머 모든 entry cleanup 후에도 token 별 최소 1개 보존
- `compute_swap_value_usd` orphan fallback — pool meta 미스 / Pyth WMON 미스 시 0 반환
- `get_token_decimals_factor` — quote_token / dex_token / fallback 18 각 분기

### 3. 통합 검증 (수동 / staging)
- testnet 인덱서 배포 후 hours-level 관측:
  - `SELECT COUNT(*), AVG(value), MIN(value), MAX(value) FROM dex_swap WHERE block_number > X` → 0 아닌 분포 확인
  - `SELECT COUNT(*) FROM dex_swap WHERE value = 0` 비율 (= orphan rate) 확인. 신규 토큰 출시 직후만 spike 되어야 함
  - 비-WMON pair (USDC-USDT 같은) 의 swap row 가 정상 USD value 들고 있는지 sample row 점검

### 4. 운영 관측 포인트 (prod)
- **Pyth WMON USD feed 가용성** — feed miss 시 그 block 의 모든 RawSwap value = 0. `value=0 count / block` 시계열에서 spike 감지
- **Orphan rate** — `value=0` row 비율. 평소 < 5% 권장. 갑자기 올라가면 가격 graph 가 어느 토큰 chain 끊김 (pool 폐쇄, listing 사고, 등)
- **token_price_cache 메모리** — 운영 시 `get_token_price_cache_size()` metric 추가 권장. 1000-block 윈도우 안에서 stable 해야 (1000 × N tokens). 증가 추세면 cleanup 비정상 의심
- **Pool meta lookup miss** — `get_pool_pair` 가 None 반환 빈도. 평소 매우 낮아야 — 신규 PairCreated 처리 직전 race 만 의도된 케이스
- **batch 처리 시간** — inference 단계가 single-thread 라 batch 크면 직렬 5-10ms 추가. p99 receive time 증가 ≤ 10ms 인지 확인

### 5. 회귀 감지
- 기존 V1 dex / V1 curve / V2 curve receive 경로의 `price_cache` 누적이 영향 안 받았는지 (이번 PR 은 token_price_cache 신규 추가, 기존 quote price_cache 손대지 않음)
- `cache_manager.get_token_decimals_factor` 가 hot path 에 PG hit 가능 — 캐시 hit 률을 받쳐주는지 (chain-immutable 이라 첫 lookup 후 영원히 hit)
- Redis 장애 시 행동 (memory_cache 가 fallback 못 가져오면 orphan rate spike 로 감지)

### 6. Failure mode 와 대응
| 시나리오 | 어떻게 감지 | 어떻게 복구 |
|---|---|---|
| Pyth WMON feed 장기 미스 | dashboard 의 `value=0` 비율 100% spike | Pyth provider 점검, 운영자 manual 대응 |
| chain reorg 발생 (H2 미구현) | reorg 후 swap value 영구 오염 (skip 한 stale price 기반) | 알림 X — 운영자가 외부 모니터링 (e.g. block hash mismatch) 으로 인지. 영향 받은 dex_swap row backfill (re-inference 도구 필요 — Phase 6) |
| 신규 token 가격 path 형성 전 swap | orphan rate 일시 증가, 해당 token 의 dex_swap.value=0 | WMON pair 가 첫 swap 으로 graph 진입 후 자연 회복. 과거 value=0 row 는 backfill 대상 |
| token_price_cache 메모리 leak | metric size 무한 증가 | cleanup 호출 흐름 점검 (V2 dex receive 마지막에 `remove_token_prices_*_all_tokens`) |
| Pool meta lookup 실패 (PairCreated race) | 해당 swap value = 0 | 다음 batch 가 PairCreated 처리 완료 후엔 정상화. orphan rate 일시 spike |

## Rollout 순서

1. **PR #209** (`fix/v2-critical-defects`) 머지 — N3 revert 정책 v2 에 들어감
2. **migrations submodule PR**: `pool.volume` 정의 동기화 (idempotent `ADD COLUMN IF NOT EXISTS`). prod 는 이미 컬럼 있어 영향 X, 새 환경 (testnet / dev / staging) 은 정의대로 생성. 머지 후 observer parent repo 의 submodule pointer bump
3. **이 PR (#210) v2 에 rebase** — N3 revert 코드 라인이 이번 PR 의 inference 와 충돌 가능. 이번 PR 코드가 wins
4. **이 PR 머지** — chain-implied `dex_swap.value` + 자동 `pool.volume` 누적 (CTE) 동시 활성화
5. **운영 관측 (위 §4)** 기준으로 24~72시간 — orphan rate, Pyth miss, 메모리 안정성, `pool.volume` 분포 확인

각 단계에서 깨질 시: 직전 단계로 revert. 가격 시스템 자체는 토글 가능한 cache 가 아니라 receive 로직에 박혀 있어 코드 revert 가 toggle 임. feature flag 도입은 deferred 안전망 (필요 시 추가).

## Out of scope

- Pyth 외 다른 oracle 도입 — 별도 RFC
- LP token / stable pool 특수 가격 모델 — V2 constant-product 만 1차 대상
- Historical backfill (배포 전 swap 의 value 재계산) — 별도 도구
- `dex_swap` → `market.volume` 트리거 추가 (graduated 토큰의 raw DEX swap 도 market 단위 누적) — 별도 결정 필요

## Outcome

(머지 시 작성)
