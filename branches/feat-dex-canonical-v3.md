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

- (머지 시 작성)
