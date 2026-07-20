# feat/vault-indexing

## Purpose

GIWA 전환 때 제거됐던(`f8a4515`, `22f50ce` @ nads-pump/observer) vault·vault_registry·dividend 인덱싱 스택을 복원한다. api-server가 vault/dividend 엔드포인트에서 해당 테이블을 읽으므로 인덱서가 채워야 한다.

## Changes

- 삭제 직전 커밋 `f283630`(nads-pump/observer)에서 26개 파일 복원: 이벤트 스트림 2종(`vault`, `vault_registry`), ABI 6종, 타입/컨트롤러/`usd_enrich`/`vault_metadata`, 테스트 3종, 문서 3종
- GIWA 규약 적응: `V2Vault`→`Vault`(체크포인트 `vault`), env 접두어 제거(`BURN_VAULT`/`LP_VAULT`/`CREATOR_FEE_VAULT`/`GIFT_VAULT`/`DIVIDEND_VAULT`/`VAULT_REGISTRY`, 전부 optional), `token.version`/`chain` 미사용 반영
- 런타임 6핸들러 → **8핸들러**: Vault는 Curve 뒤 대기, VaultRegistry는 독립(admin 주도). README·event-indexing·.env.example 갱신
- 전제: `giwa/migrations 9eee340`이 vault·dividend·0015 스키마를 복원해둠
- 검증: build + lib 47 + giwa_runtime_contract 13 + v2_dividend 11 + dividend_via_v2vault 1 + vault_registry_type 6 + v2_controllers 2, 그 외 스위트 전부 통과. 실패 5건은 기존 결함(pool_batch_update_reserves 3, cache doc-test 2)
- 실행 Codex(gpt-5.6-sol xhigh), 검증·커밋 orchestrator

## Outcome

- (머지 시 작성)
