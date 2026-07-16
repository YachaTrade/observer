# feat/price-usd-block-bucket

## Purpose
price_usd(DefiLlama) 스트림을 Pyth `price` 스트림과 동일한 **25-블록 버킷** 구조로 맞춘다.
현재는 60s wall-clock throttle(`should_refetch`)로 `/current`만 호출 → 과거 블록 backfill 시 "현재가"가 박힘.
목표: ① 버킷당 1회 fetch, ② 과거 버킷은 DefiLlama **historical** 엔드포인트로 그 시점 가격, ③ `price`+`price_usd` 병렬 적재가 버킷-step 값으로 들어오는지 검증.

multi-anchor 설계(`docs/plans/2026-06-16-multi-anchor-orphan-defillama-bridge-design.md`)의 **선행 작업**. 이 브랜치는 인제스트 구조만 다루고, 앵커/캐시/forward-prop은 건드리지 않음.

## DefiLlama API 라이브 검증 (2026-06-16)
스키마는 current/historical 동일: `{"coins": {"monad:0x..": {decimals, symbol, price, confidence, timestamp}}}` → 기존 `parse_current` 재사용(추가 `timestamp` 필드는 무시됨).

| 호출 | 결과 |
|---|---|
| `GET /prices/current/{coins}` | MON price=0.0226 conf=0.99 ✅ |
| `GET /prices/historical/{ts}/{coins}?searchWidth=600` | **`{"coins":{}}` 빈값** — ±10분 내 스냅샷 없음 |
| `GET /prices/historical/{ts}/{coins}` (searchWidth 기본 6h) | MON=0.022429(현재 0.0226과 다름=과거값 정확), XAU=4288.87, conf 0.99 ✅, comma-batched ✅ |

**교훈:**
- DefiLlama 스냅샷은 ~10초 간격이 아님(듬성듬성). 버킷ts 질의 시 **넉넉한 searchWidth 필수**(좁으면 빈값). `HISTORICAL_SEARCH_WIDTH_SECS = 3600`(1h) 채택 — 시대는 정확, 해상도는 버킷보다 거침(backfill 의미상 OK).
- 인접 버킷이 같은 스냅샷을 받을 수 있음 → 정확(carry-forward). 호출 절감용 `batchHistorical`는 후속 최적화.
- `MODE=testnet`이면 `build_provider`가 **MockProvider(고정 0.03/0.99)** 사용 → 진짜 DefiLlama 검증은 **MODE=mainnet** 필요.

## 설계 — Pyth 버킷 루프 미러 (tip=A 동작, backfill=정확 통합)
배치를 25-블록 버킷으로 묶고, `last_fetched_bucket`보다 새 버킷만 fetch. tip 버킷(now에 가까움)은 `/current`, 과거 버킷은 `/historical/{bucket_ts}`. fetch 결과를 conf≥0.9 필터+carry-forward 후 버킷 멤버 블록에 dense stamp.
- tip(라이브): 배치당 새 버킷 1개 → fetch 1회.
- backfill: 배치 내 버킷 N개 → 버킷마다 historical 1회(429 백오프로 rate-limit).

## 인터페이스 contract (Codex 구현 대상 — 시그니처 고정)

신규 `src/event/common/price_usd/bucket.rs` (순수 함수, 단위 테스트 대상):
```rust
pub const BUCKET_BLOCK_INTERVAL: u64 = 25;

/// block - block % 25
pub fn bucket_of(block: u64) -> u64;

#[derive(Debug, Clone, PartialEq)]
pub struct BucketGroup {
    pub bucket_block: u64,        // bucket_of(member)
    pub bucket_ts: u64,          // 버킷 내 첫 멤버 블록의 timestamp
    pub blocks: Vec<(u64, u64)>, // (block_number, block_timestamp), 오름차순
}

/// 오름차순 (block, ts) 들을 25-블록 버킷으로 묶음. bucket_block 오름차순.
/// bucket_ts = 각 버킷 내 첫(최소 block) 멤버의 ts.
pub fn group_into_buckets(blocks: &[(u64, u64)]) -> Vec<BucketGroup>;

#[derive(Debug, Clone, PartialEq)]
pub enum FetchKind { Current, Historical(u64) /* bucket_ts */ }

/// now - bucket_ts <= tip_threshold_secs → Current, else Historical(bucket_ts)
pub fn select_fetch(bucket_ts: u64, now: u64, tip_threshold_secs: u64) -> FetchKind;

/// last_fetched 보다 strictly 큰 bucket_block 만 반환(forward scan dedupe).
pub fn buckets_to_fetch(grouped: &[BucketGroup], last_fetched: Option<u64>) -> Vec<BucketGroup>;
```

provider 트레이트 확장 (`provider/mod.rs`) + DefiLlama/Mock 양쪽 구현:
```rust
async fn fetch_historical(
    &self, coin_refs: &[String], timestamp: u64, search_width_secs: u64,
) -> Result<HashMap<String, PriceUsdPoint>>;
```
- DefiLlama: `GET /prices/historical/{timestamp}/{joined}?searchWidth={search_width_secs}`, 응답은 `parse_current` 재사용, 기존 429 백오프/청크 로직 동일 적용.
- Mock: `fetch_current`과 같은 고정값 반환(테스트는 별도 recording mock 사용).

stream seam (`stream.rs`, 얇은 async — recording-mock 테스트 대상):
```rust
async fn fetch_bucket(
    provider: &dyn PriceUsdProvider, coin_refs: &[String],
    kind: &FetchKind, search_width_secs: u64,
) -> Result<HashMap<String, PriceUsdPoint>>;
// Current → fetch_current(coin_refs); Historical(ts) → fetch_historical(coin_refs, ts, search_width_secs)
```

`stream_events` 재배선: `should_refetch`/`REFRESH_INTERVAL_SECS` 제거, `last_fetch_at` → `last_fetched_bucket: Option<u64>`. 루프에서 `group_into_buckets` → `buckets_to_fetch` → 버킷별 `select_fetch`+`fetch_bucket`+`apply_fresh_prices`(conf 필터/carry-forward 유지) → `build_dense_rows`. 신규 const: `TIP_THRESHOLD_SECS=120`, `HISTORICAL_SEARCH_WIDTH_SECS=3600`.

## 테스트 (RED=Opus / GREEN=Codex)
- `bucket.rs` 단위: `bucket_of` 경계(0,24,25,49,50); `group_into_buckets`(48..=51 → {25:[48,49]},{50:[50,51]}, bucket_ts=첫 멤버 ts); `select_fetch`(tip→Current, 과거→Historical(ts)); `buckets_to_fetch`(last_fetched 필터, None→전체).
- `stream.rs` 단위: `fetch_bucket`이 FetchKind에 따라 옳은 provider 메서드를 옳은 ts로 호출(recording mock); `apply_fresh_prices` conf<0.9 → carry-forward, conf≥0.9 → 갱신.
- 라이브 검증: MODE=mainnet으로 인덱서 기동 → `price`+`price_usd` 병렬 적재, 과거 버킷이 그 시점 가격(현재가 아님)으로 채워지는지 spot-check.

## Changes
- `tests/price_usd_bucketing.rs` (신규, RED→GREEN): 순수 버킷 로직 12 테스트.
- `src/event/common/price_usd/bucket.rs` (신규): `BUCKET_BLOCK_INTERVAL=25`, `bucket_of`, `BucketGroup`, `group_into_buckets`, `FetchKind`, `select_fetch`, `buckets_to_fetch`.
- `provider/mod.rs`: 트레이트에 `fetch_historical(coin_refs, timestamp, search_width_secs)` 추가.
- `provider/defillama.rs`: `/historical/{ts}/{coins}?searchWidth=` 구현 + 공유 retry 로직 `request_with_retry`로 추출(중복 제거).
- `provider/mock.rs`: `fetch_historical` → `fetch_current` 위임(고정값).
- `stream.rs`: 60s wall-clock throttle 제거, `last_fetched_bucket` 기반 버킷-단위 패스. 새 버킷마다 `select_fetch`로 current/historical 선택, 버킷별 dense stamp(carry-forward). consts `TIP_THRESHOLD_SECS=120`, `HISTORICAL_SEARCH_WIDTH_SECS=3600`.
- `should_refetch`는 유지(orphan pub util, 기존 테스트 커버, 후속 정리 대상).

**구현 주체**: Codex delegation이 과도하게 느려(추론 단계서 5분+ 무산출) 사용자 중단 지시 → **Opus 인라인 구현(fallback 경로)**. diff 교차검증은 `/codex review` 권장.

**검증**: `cargo build` 클린, `cargo test --test price_usd_bucketing`(12) + `price_usd_logic`(8) GREEN, 내 파일 clippy 클린. 라이브(MODE=mainnet) 병렬 적재 검증은 대기.

**주의**: 인라인 작업 중 `cargo fmt`(전체)가 67파일 재포맷 → 무관 60파일 v2로 revert, 내 변경만 유지. (tasks/lessons.md 기록)

## Outcome
- (PR/머지 시)
