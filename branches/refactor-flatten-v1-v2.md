# refactor/flatten-v1-v2

## Purpose

GIWA는 단일 컨트랙트 스택이므로, Monad 시절 두 세대를 병행하려고 만든 v1/v2 네임스페이스 분리를 코드 레이아웃에서 제거한다. DB 식별자는 건드리지 않는다(별도 후속 작업).

## Changes

- `src/event/{v1/dex,v1/lp_manager,v2/curve,v2/vault,v2/vault_registry,v2/usd_enrich.rs}` → `src/event/` 직하 (단 `common/`은 체인 무관 스트림 묶음이라 유지)
- `src/types/{v1,v2}/*` → `src/types/`, `controller/v2/*` → `controller/`, `abi/{v1,v2}/*` → `abi/`
- 식별자 접두어 제거: `V2VaultEventHandler`→`VaultEventHandler`, `event_v2_curve`→`event_curve` 등. 살아있는 V1 접두어 선언은 없었음
- 충돌 해결: `types/v1/curve.rs`는 메타데이터·차트·DB 모델이 여전히 소비하므로 삭제하지 않고 `types/legacy_curve.rs`로 유지, 라이브 BondingCurve 타입이 `types/curve.rs` 차지
- 테스트 파일명 정리: `v2_controllers`→`sniping_controllers`, `v2_dividend`→`dividend_controllers`, `dividend_via_v2vault`→`dividend_via_vault`
- 동결 유지: DB 테이블/컬럼/인덱스/트리거 이름의 `v2_` 접두어, market_type 값, env·체크포인트 이름, `migrations/`
- 검증: build + 전체 스위트 통과(기존 결함 pool_batch_update_reserves 3 · cache doc-test 2 제외). 실행 Codex(gpt-5.6-sol xhigh), 검증·커밋 orchestrator

## Outcome

- (머지 시 작성)
