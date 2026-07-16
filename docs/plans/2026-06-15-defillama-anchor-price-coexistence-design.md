# whitelist_token DefiLlama 가격 + quote_token Pyth 공존 설계

> 작성: 2026-06-15 / 설계: Opus / 구현: Codex (모델 라우팅 준수)
> v2 정정: 테이블 단위 분담으로 재설계 (quote_token=Pyth 인덱싱 / whitelist_token=DefiLlama 표시).

## 1. 배경 / 문제

두 가격 경로가 목적도 소비자도 다름이 확인됨:

| 테이블 | 소비처 | 목적 | 가격 소스 |
|---|---|---|---|
| `quote_token` | **observer `src/`** (main/config/cache/dex) | **인덱싱**: forward-propagation 루트, 이벤트 `usd_value` | Pyth (`pyth_feed_id`) |
| `whitelist_token` | **observer 외부(다운스트림)** — `balance_usd` 계산 | **표시/CMS**: 화이트리스트 토큰의 보유 USD 가치 | (현재) Pyth `price_feed_id` |

- `whitelist_token`은 observer `src/`에서 안 읽힘(grep 확인). CMS 큐레이션 테이블(`0031`/`0032`)이고, `price_feed_id` NULL이면 다운스트림 `balance_usd`가 null.
- 사고: `whitelist_token.price_feed_id`에 Pyth가 없는 토큰(**LV**)에 MON/USD 피드(`0x31491744…`)를 placeholder로 박아 LV를 $0.0517 대신 MON 시세 $0.0223으로 **~57% 저평가**(silent). LVMON도 동일(LVMON≈MON ~0.6% 근사).
- 근본 원인: `whitelist_token`을 **Pyth feed_id 수동 큐레이션**으로 가격하려니, Pyth 미커버 토큰(LV/LVMON 등)에서 깨짐.

## 2. 결정 — 테이블 단위 분담 (공존)

- **`quote_token` → Pyth (유지·무변경)**: 인덱싱 가격 경로. WMON/MON/USDC/USDT0/AUSD 등 quote는 Pyth 전용 피드가 검증됨. **루트앵커 MON도 Pyth 잔류** → 인덱싱 가격 그래프 안전.
- **`whitelist_token` → DefiLlama (신규)**: 표시/`balance_usd`용. 주소 기반(`monad:{token_id}`)이라 Pyth feed_id 큐레이션 surface 자체가 사라짐 → LV/LVMON 류 placeholder 사고 근본 차단.

핵심: **두 경로는 서로 독립.** DefiLlama는 인덱싱 `price_cache`/`get_quote_usd_price`를 건드리지 않는다. observer는 `whitelist_token` 가격의 **PRODUCER**일 뿐(소비는 다운스트림).

### 2.1 검증된 커버리지 (2026-06-15, monad mainnet)

현재 `whitelist_token` **실제 8행** (주소 확정됨). 전부 DefiLlama `monad:{token_id}` conf 0.99 응답:

| sort | token_id | symbol | 현 price_feed_id | DefiLlama 실가 |
|---|---|---|---|---|
| 1 | 0x0000000000000000000000000000000000000000 | MON | MON/USD `0x31491744…` | $0.02226 |
| 2 | 0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A | WMON | MON/USD `0x31491744…` | $0.02226 |
| 3 | 0x754704Bc059F8C67012fEd69BC8A327a5aafb603 | USDC | USDC/USD `0xeaa020…` | $0.99978 |
| 4 | 0xe7cd86e13AC4309349F30B3435a9d337750fC82D | USDT | USDT/USD `0x2b89b9…` | $0.99927 |
| 5 | 0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a | AUSD | AUSD/USD `0xd9912df…` | $0.99977 |
| 6 | 0x91b81bfbe3A747230F0529Aa28d8b2Bc898E6D56 | LVMON | **MON placeholder** `0x31491744…` | $0.022394 |
| 7 | 0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242 | WETH | ETH/USD `0xff6149…` | $1715.83 |
| 3 | 0x1001fF13bf368Aa4fa85F21043648079F00E1001 | LV | **MON placeholder** `0x31491744…` (~57% 저평가) | $0.051648 |

- 주소 사용자 확정 — LVMON 실주소 `0x91b81b…`(구 0031 seed `0xBe3fa505…`는 outdated), WMON/USDC/USDT도 이미 seed됨.
- **XAUt0(`0x01bFF41798a0BcF287b996046Ca68b395DbC1071`)는 현재 whitelist 미포함** → 추가 시 refresher가 자동 픽업(아래 §5.1, 하드코딩 없음).
- DefiLlama는 **enabled whitelist 전부**(현 8행)를 주소 기반으로 가격 → feed_id 큐레이션 불필요, LV placeholder 사고 근본 해소.

## 3. 아키텍처

```
[Pyth Hermes] (기존, 무변경)        [DefiLlama coins API] (신규)
      │                                   │ 60s batch (current)
      ▼                                   │ batchHistorical (백필)
 price 테이블 → price_cache               ▼
      │ (인덱싱)                    price_usd (신규 테이블)
      ▼                                   │
 get_quote_usd_price / forward-prop       ▼
 (quote_token, 무변경)            다운스트림 balance_usd (observer 외부)
```

- 인덱싱 경로(왼쪽): 손대지 않음.
- DefiLlama 경로(오른쪽): observer가 `whitelist_token` enabled 행을 가격. **block-driven**(price 스트림 미러)으로 block마다 `price_usd` 행 적재(공백 block 없음), 단 DefiLlama 호출은 60s throttle. 다운스트림이 표시/`balance_usd`에 사용.

## 4. 신규 테이블 (별도)

```sql
CREATE TABLE IF NOT EXISTS price_usd (
    token_id     VARCHAR(42) NOT NULL,
    block_number BIGINT      NOT NULL,
    price        NUMERIC     NOT NULL,   -- USD 단가
    confidence   NUMERIC,                -- DefiLlama confidence (0~1)
    created_at   BIGINT      NOT NULL,   -- block_timestamp (기존 price 테이블과 동일 의미)
    PRIMARY KEY (token_id, block_number)
);
CREATE INDEX IF NOT EXISTS idx_price_usd_token_block ON price_usd (token_id, block_number DESC);
```

- **block-키 dense** — 기존 Pyth `price(quote_id, block_number, price, created_at)` 구조를 그대로 미러(+`confidence`). price 스트림이 **모든 block에 행을 쓰듯**(25블록 bucket으로 Pyth 1회 fetch → bucket 내 전 block에 행 push), price_usd도 **block마다 행** → 공백 block 없음, 다운스트림이 `block_number`로 직접 join 가능.
- DefiLlama 식별자는 **저장 안 함** — 코드에서 `'monad:' + token_id`로 조립(전 토큰 monad:addr로 검증됨). 향후 coingecko-only 토큰 생기면 `whitelist_token` override 컬럼 추가.
- `created_at` = 해당 block의 `block_timestamp` (price 테이블과 동일 의미, `get_block_timestamp`로 획득).
- **DefiLlama는 1/min만 쿼리**(rate limit) → 그 1개 가격을 직전 fill 이후 경과한 block 전부에 적용(§5). price 스트림의 "bucket 캐시 hit이면 fetch skip"과 같은 발상(여기선 60s throttle).
- **READ** (다운스트림 `balance_usd`): exact block join(`price_usd.block_number = balance.block_number`) 또는 carry-forward(`block_number <= target ORDER BY block_number DESC LIMIT 1`). 둘 다 가능(dense라 exact도 거의 항상 hit).
- 별도 테이블이라 인덱싱 `price`/`price_cache`는 무영향(분리 유지).
- 행 규모: block당 1행 × 토큰 수 → 기존 `price` 테이블(quote당 block마다 1행)과 동일 차수. 이미 수용된 패턴.

## 5. 인그레스

### 5.1 Live (block-driven, DefiLlama 1/min) — price 스트림 패턴 미러
- price 스트림과 **동일 구조**: `STREAM_MANAGER` block range로 `from_block..=to_block` 진행(chain 따라감), 각 block의 timestamp는 `get_block_timestamp`. 별도 `EventType`(예: `EventType::PriceUsd`) 부여.
- **DefiLlama fetch는 60s throttle**: 마지막 호출 후 60s 안이면 직전 가격 재사용, 지났으면 enabled whitelist → `monad:{id}` batch 1콜로 갱신.
- 그 cycle의 block 전부에 (재사용 또는 갱신된) 가격으로 **block마다 행 INSERT** → 공백 block 없음(= "공백시간 block도 다 채움").
- "현재 indexed block"·timestamp는 price 스트림 기존 머신러리 재사용 → Codex HIGH(block 스탬핑 소스/ts→block 변환) 자동 해소.
- confidence<0.9 등 → 그 토큰 skip(§8.1), 직전 가격으로 carry-forward.

### 5.2 Backfill (과거 데이터, Pyth처럼 가능)
- 위 스트림이 옛 `from_block`부터 catch-up(별도 시작 block 지정) → 자연 백필. block→`get_block_timestamp`→DefiLlama `historical/{ts}` 조회.
- 호출 절감: 같은 시간 bucket(예: 25블록/분 단위)은 DefiLlama 1회로 묶고 그 구간 block에 fill(price 스트림의 bucket과 동일). 또는 `batchHistorical`로 다중 시점 묶음.
- block 기준으로 진행하므로 **ts→block 역변환 불필요**.
- caveat: 오래된 데이터 granularity coarse(CoinGecko). 표시/balance_usd엔 충분.

### 5.3 Rate limit (free tier로 충분)
- 키 불필요. whitelist ~10종 batch 1콜/분 = ~43k/월 → 한도 1% 미만.
- 한도 비공개(Cloudflare) → 반응형: `429`/`Retry-After` + 지수 백오프, batch 우선, 실패 시 직전행 유지(다운스트림은 최신행 읽으므로 자동 carry-forward).

## 6. quote_token / 인덱싱 경로 — 무변경 (명시)

- `quote_token`·Pyth·`price_cache`·`get_quote_usd_price`·forward-propagation·이벤트 `usd_value`(dividend deposit/conversion 등) **전부 그대로**.
- 본 작업은 인덱싱 정확도에 영향 없음. 순수 표시(`balance_usd`) 가격 소스 교체.

## 7. whitelist_token 정리

- `price_feed_id`(Pyth placeholder) 경로 **deprecate**: DefiLlama로 가격되는 토큰은 이 컬럼에 의존 안 함. LV/LVMON MON-placeholder 사고가 근본 해소(컬럼 NULL 정리 또는 다운스트림이 `price_usd` 우선 참조).
- 다운스트림(`balance_usd` 계산)이 `whitelist_token.price_feed_id`(+Pyth) 대신 `price_usd` 최신행을 읽도록 전환 — **observer 외부 변경 필요**(별도 PR/조율, 본 설계 범위 밖이나 머지순서 명시).

## 8. 가드레일

1. **confidence 게이트** (<0.9 skip) — thin token(LV) DEX 노이즈 차단.
2. **carry-forward fill** — DefiLlama fetch 실패/skip이어도 해당 block들은 **직전 good 가격으로 채움**(공백 block 방지 = 사용자 요구). 직전 가격 없으면(콜드스타트) 그 토큰만 해당 block 비움.
3. **429/backoff/batch** — rate limit 방어. batch는 URL 길이/항목 수로 청크 분할(상한 비공개 → 보수적). DefiLlama 호출은 60s throttle.
4. **EIP-55 유지** — `token_id` lowercasing 금지. DefiLlama 식별자(`monad:{token_id}`)는 응답 키와 case-insensitive 매칭하되 저장은 원본 케이싱.

### 8.1 DefiLlama edge 정책 (Codex MEDIUM) — skip = fresh 미반영, 해당 block은 직전 good 가격으로 fill
| 상황 | 처리 |
|---|---|
| `{"coins":{}}` / 해당 토큰 항목 없음 | 신규가 미반영 + WARN, block은 직전 good 가격으로 fill |
| `confidence` 필드 누락 | 동일 (직전 good fill) |
| `confidence < 0.9` | 동일 (직전 good fill) |
| `429` / 네트워크 실패 | 백오프, block은 직전 good 가격으로 fill |
| 직전 가격도 없는 토큰(콜드스타트) | 그 토큰 해당 block **미기록(price 없음)** — 다운스트림 balance_usd는 null(0 아님) |

## 9. 도메인 invariant 갱신

CLAUDE.md의 *"외부 oracle 의존은 Pyth 하나뿐"* → **"인덱싱 quote 가격=Pyth, 표시용 whitelist 가격=DefiLlama(free tier), 그 외 토큰=on-chain forward-propagation"** 으로 개정.

## 10. 결정 필요 / 후속(deferred)

- [x] ~~LVMON seed 주소 정합성~~ → 확정: `0x91b81bfbe3A747230F0529Aa28d8b2Bc898E6D56` (사용자 확인). 구 0031 seed `0xBe3fa505…`는 outdated.
- [x] ~~WMON/USDC/USDT 미seed~~ → 이미 현 whitelist에 seed됨 (0031 NOTE는 옛 상태).
- [x] ~~DefiLlama 식별자 저장~~ → 확정: 저장 안 함, 코드에서 `'monad:'+token_id` 조립. coingecko-only 토큰 생기면 그때 override 추가.
- [x] ~~테이블 형태/키~~ → 확정: 별도 `price_usd` 테이블, **block-키 dense**(PK `(token_id, block_number)`, `created_at`=block_timestamp) — 기존 `price` 테이블 미러. block마다 행(공백 block 없음), DefiLlama는 1/min throttle. Codex HIGH(스탬핑 소스/ts→block)는 price 스트림의 block-driven 머신러리(STREAM_MANAGER+get_block_timestamp) 재사용으로 해소.
- [ ] XAUt0(`0x01bFF4…`) whitelist 추가 시점(미포함) — refresher는 자동 픽업.
- [ ] 다운스트림 `balance_usd` 전환 PR (observer 외부) — `price_feed_id`(Pyth) → `price_usd`(DefiLlama). 머지순서 조율.
- (deferred) 인덱싱 `usd_value`의 비-quote dividend claim(임의 ERC20) 가격 — **본 작업과 별개**. 필요 시 후속에서 DefiLlama 표시가격을 인덱싱 경로로도 끌어올지 결정.

## 11. 마이그레이션 (Two-track)

- `price_usd`: fresh DB용 base + 운영 DB용 idempotent `v2_upgrade_*.sql` 양 트랙. `CREATE TABLE IF NOT EXISTS`.
- **whitelist_token seed 보정 (Codex MEDIUM)**: 마이그레이션은 현재 2행(옛 LVMON `0xBe3fa505…`)만 seed → prod CMS 상태(8행)와 불일치. fresh DB/dev 패리티 위해 **현 8행(올바른 LVMON `0x91b81b…` 포함) seed/보정 마이그레이션 추가**. whitelist는 CMS 관리이므로 운영 트랙은 데이터 덮어쓰기 주의(ON CONFLICT 정책 명시).
- (인접) `quote_token`에 USDC/USDT/AUSD 등 비-native quote 추가 시 `is_native=FALSE` 필수 — 본 작업과 별개지만 Pyth 경로 오인 방지(Codex MEDIUM).
- migrations submodule PR → 머지 후 observer gitlink bump (PR 본문 머지순서 명시).

## 12. 테스트 계획 (TDD)

RED(Opus) → GREEN(Codex):
- DefiLlama 응답 파싱(정상/`{"coins":{}}`/confidence 누락) → 저장/skip 분기.
- 60s refresher: enabled whitelist 행만 조회, batch 콜 구성, `price_usd` INSERT.
- confidence < 0.9 → skip + WARN.
- 429/실패 시 직전행 보존(다운스트림 carry-forward).
- batchHistorical 파싱 → 시점별 저장.
- 통합(testcontainers): 신규 테이블 마이그레이션 + refresher 1회.
- 회귀: 인덱싱 경로(quote_token/price_cache/get_quote_usd_price)는 영향 없음 확인.

## 13. 모델 라우팅

- 설계 문서·TDD RED·마이그레이션 pointer·PR = **Opus**.
- `src/**` 구현(refresher/파서/config/바이너리) + build/test = **Codex** (delegation contract). 구현 전 Codex가 본 spec 리뷰(cross-model).
