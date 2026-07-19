# feat/market-type-nadfun-uniswapv3

## Purpose

`market_type` 저장 값을 GIWA 계약에 맞게 변경: 본딩 커브 `CURVE` → `NADFUN`, DEX `DEX` → `UNISWAPV3`. market/swap CHECK 제약·부분 인덱스·runtime 리터럴·테스트·문서 일괄 갱신. `point_type`('CURVE'/'DEX')와 fee_type('curve_buy'/'curve_sell'), 레거시 `V2_*` 읽기 호환 분기는 불변.

## Changes

- `d296797` feat: market_type 저장 값 rename — `CURVE`→`NADFUN`, `DEX`→`UNISWAPV3`
  - migrations: `0002` market CHECK `('NADFUN','UNISWAPV3')` + 부분 인덱스 predicate, `0004` swap CHECK 동일
  - src: `giwa_market_type` 매핑, curve/dex receive 리터럴, `model.rs`, cache 쿼리(`= 'UNISWAPV3'`, `IN ('UNISWAPV3','V2_DEX')`) 및 match arm
  - tests: group_a/b 리터럴 27곳, docs: README·event-indexing 계약 문구
- 검증: 전체 스위트 통과. 실패 5건은 전부 rename 무관 — `pool_batch_update_reserves` 3건(베이스라인 기존 결함, bind 9 vs 11), `src/cache.rs` doc-test 2건(미접촉 파일 기존 결함). `mint`/`sniping`은 병렬 플레이크로 단독 통과 확인.
- 실행: Codex(gpt-5.6-sol medium) / 검증·커밋: orchestrator.
- 다운스트림 후속: giwa/api-server(22곳), giwa/websocket-server(wire enum + 테스트) 별도 작업 필요.

## Outcome

- 2026-07-19 `main`에 로컬 merge (no-squash). 핵심 커밋 `d296797`. 검증: 전체 스위트 통과(무관 기존 결함 5건 제외), fresh PG에서 NADFUN/UNISWAPV3 CHECK 동작 확인. 통합 migration `giwa/migrations e3f787a` 동기화.
- 후속: giwa/api-server·giwa/websocket-server 다운스트림 rename 별도 진행.
