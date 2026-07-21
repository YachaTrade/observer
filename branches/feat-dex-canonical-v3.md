# feat/dex-canonical-v3

## Purpose

dex 모듈을 Monad Capricorn CL에서 GIWA canonical Uniswap V3 풀 + GiwaRouter Buy/Sell 인덱싱으로 전환한다. 현재 dex 스트림이 잡는 ICapricornCLPool/IDexRouter 이벤트는 GIWA에서 발생하지 않아 dex 인덱싱이 동작하지 않던 상태를 바로잡는다.

## Changes

- `dfab0c3` chore: ABI 교체 — GiwaRouter.json / IUniswapV3Pool.json 추가, Capricorn 4종(ICapricornCLPool·IDexRouter·ICapricornCLFactory·ILens) 제거. Capricorn 풀 ABI = canonical V3 ABI 시그니처 동일성 스크립트로 확인
- `c28d25a` feat: stream.rs 이벤트 소스 교체 — 풀 파싱/reserve 합성/slot0 로직 유지(ABI 동일), 라우터를 GiwaRouter.Buy/Sell로. `graduated==true`만 처리(false=커브 매매는 curve 핸들러 담당, 이중 저장 방지). graduated 필드 round-trip 회귀 테스트 추가
- `8527e8a` docs: DEX_ROUTER env를 GiwaRouter 주소로 재정의(주석)
- `197eb8b` docs: README/event-indexing/dex.md를 canonical V3 기준으로
- 검증: build + lib 49 passed(신규 graduated round-trip 포함), group_b 기존 결함 외 통과. 실행 Codex(gpt-5.6-sol medium), 검증·테스트 보강·커밋 orchestrator

## Outcome

- 2026-07-21 `main`에 no-ff merge. 이 브랜치는 이번 세션 GIWA 작업 전체를 선형으로 담음: token.version/chain 제거(3c70f10) → vault/dividend 인덱싱 복원(8da4835) → v1/v2 평탄화(b9f0512) → v2_ 접두어 제거 + 테스트 하니스를 GIWA 스키마 서브모듈로 전환(d84dcf0, 9457e2c) → dex canonical V3(dfab0c3~e64eb69) → fee/point 인덱싱 제거(730c091) → point 컨트롤러/테스트 제거 + 스키마 bump(d98a095).
- 검증: 전체 스위트 통과(기존 결함 pool_batch_update_reserves 3 · cache doc-test 2 제외). 스키마 서브모듈 = giwa/migrations 82b2b69(point 도메인 제거, 121테이블).
- 배포 의존성: observer·api-server·migrations 동시 배포 필요. api-server는 point 4테이블 읽기 제거가 같은 배포에 포함돼야 함(별도 에이전트).
