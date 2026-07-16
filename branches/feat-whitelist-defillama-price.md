# feat/whitelist-defillama-price

## Purpose

whitelist_token 집합의 USD 가격을 **DefiLlama coins API**(free tier)로 받아 신규 `price_usd` 테이블에 적재한다. Pyth 기반 인덱싱 경로(`quote_token` → `price_cache` → `get_quote_usd_price` → forward-propagation)와 **완전 분리**된 표시(`balance_usd`)용 가격 소스. 동기: Pyth 미커버 토큰(LV/LVMON/XAUt0)의 가격, 그리고 api-server의 요청별 라이브 DefiLlama 호출을 observer 배치(60s) + DB read로 대체.

설계: `docs/plans/2026-06-15-defillama-anchor-price-coexistence-design.md`

## Changes

- **신규 `price_usd` 테이블** (migrations 서브모듈 `0034_price_usd.sql` + `v2_upgrade_price_usd.sql`): block-키 dense, `(token_id, block_number, price, confidence, created_at)`, PK `(token_id, block_number)` — 기존 Pyth `price` 구조 미러 +confidence.
- **`src/event/common/price_usd/`** (price 모듈 미러): `parse_current`/`coin_ref`/`should_refetch`/`build_dense_rows`, DefiLlamaProvider(reqwest, 429/Retry-After/5xx 백오프, coin_ref 50 청크) + mock, block-driven stream(60s throttle, dense fill, confidence≥0.9 게이트, carry-forward last-good, cold-start skip, EIP-55 보존), receive → controller.
- **`src/db/postgres/controller/price_usd.rs`**: UNNEST 배치 insert, ON CONFLICT (token_id, block_number) DO NOTHING.
- **`EventType::PriceUsd`** 배선(sync/mod, stream, receive) + main.rs spawn.
- **테스트**: `tests/price_usd_logic.rs`(8, 순수 로직) + `tests/price_usd.rs`(4, 통합 스키마/read 계약).

모델 라우팅: 설계/RED/migration = Opus, 구현(GREEN) = Codex(gpt-5.5), 검증+딥리뷰 = Opus.

커밋: `d3e8ade`(docs) → `80e8737`(RED) → `f84cb2f`(feat GREEN).

## Outcome

(작성 예정 — PR 머지 시 PR 링크/머지 커밋/머지순서 결과)

### 머지순서 의존
- migrations PR(`0034_price_usd.sql`) 선행 머지 → observer gitlink bump → observer PR 머지.

### 후속 (별도 트랙)
- **api-server 전환**: whitelist USD를 라이브 DefiLlama 대신 `price_usd` read로 (price_usd의 실효 소비자).
- **multi-anchor rooting**: DEX forward-propagation의 WMON 단일 root → 풀별 quote 앵커 root(앵커 USD를 price_usd/quote Pyth에서 lookup). 별도 설계+TDD.
- **price_feed_id 정리**: prod 드롭 완료 → api-server CMS(`cms/mod.rs`) price_feed_id 참조 제거 필요(현재 prod에서 깨질 위험) + migration 정합성(0032 ADD 제거/drop).
- **whitelist seed 보정**: migration의 stale 2행(옛 LVMON `0xBe3fa505…`) → 현 9행(+XAUt0).
