# refactor/prune-priceusd-whitelist-treasury

## Purpose

GIWA observer를 더 lean하게: price_usd(DefiLlama), whitelist_token, treasury, 미사용 dex 테이블(dex_swap/dex_sync/dex_token/fee_config)을 인덱싱·스키마에서 제거. LP cost-basis용 pool/dex_mint/dex_burn은 유지.

## Changes

- **PriceUsd 핸들러 전체 제거** (모듈·컨트롤러·EventType·main spawn·price_usd 테스트 5개). 핸들러 8→7 (Curve/Dex/LpManager/Vault/VaultRegistry/Token/Price)
- **Token 필터 변경**: whitelist_token 멤버십 → `token` 테이블 멤버십(`SELECT EXISTS`, Redis 캐시). Curve의 premature premark 제거
- **dividend USD**: quote > DefiLlama(price_usd) > chain → quote > chain
- **dex_token/fee_config**: postgres/redis 헬퍼 제거, 토큰 decimals는 quote_token + 18 fallback
- treasury 헬퍼/테스트 제거
- **유지**: Dex 핸들러, pool 등록, dex_mint/dex_burn, LP cost-basis, mint/burn/swap/market 쓰기
- 스키마: `giwa/migrations 25bf794`(121→98테이블, 삭제대상 트리거/함수 포함). 서브모듈 bump
- 검증: build + 전체 스위트 통과(기존 결함 pool_batch_update_reserves 3 제외). runtime_contract 14. 실행 Codex(gpt-5.6-sol high), 검증 orchestrator

## Outcome

- (머지 시 작성)

## 배포 의존성

observer·api-server·migrations 동시 배포. api-server가 price_usd/whitelist_token/account_x/dex_token/fee_config/treasury를 읽으므로 그 읽기 제거가 같은 배포에 포함돼야 함(별도 에이전트).
