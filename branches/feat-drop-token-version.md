# feat/drop-token-version

## Purpose

GIWA는 단일 버전·단일 체인 배포이므로 `token.version`('V2')과 `token.chain`('GIWA') discriminator 쓰기를 코드에서 제거한다. observer `migrations/`는 레거시 테스트 전용이라 불가침 — GIWA 스키마 SSOT(`~/project/giwa/migrations`)에서 컬럼 자체를 drop.

## Changes

- `token.rs`: 배치 INSERT에서 version 배열 + chain `'GIWA'` 리터럴 제거, 파라미터 $1~$23 배열 + $24 WNATIVE로 재배열, `TokenBatchData.version` 필드 삭제
- `receive.rs`: `version: "V2"` 구성 제거
- tests: 배치 헬퍼/픽스처에서 version 제거 (legacy 스키마 DEFAULT 'V1'/'MON'이 채움)
- docs: README·event-indexing 쓰기 계약에서 version/chain 삭제
- 연계: giwa/migrations `99327c9`(컬럼 drop), websocket-server `ce96186`, api-server `35d5e79`
- 검증: build + lib 23 + giwa_runtime_contract 5 + group_b(기존 결함 3 제외) + token_chain. 실행: Codex(gpt-5.6-sol medium), 검증·커밋: orchestrator

## Outcome

- (머지 시 작성)
