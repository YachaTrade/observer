# refactor/drop-v2-table-prefix

## Purpose

DB 식별자에서 Monad 시절 `v2_` 접두어를 제거한다(단일 컨트랙트 세대이므로 무의미). 아울러 테스트 하니스를 레거시 스키마 사본 대신 GIWA 스키마 SSOT에 연결한다.

## Changes

- SQL 문자열의 테이블/인덱스/트리거/함수 이름에서 `v2_` 제거 (`giwa/migrations 27711aa`와 짝)
- 죽은 `v2_lp_allocate_history` 참조 제거 — 스키마에서도 삭제됨(그 이름은 실사용 v1 테이블 소유)
- **테스트 하니스 전환**: 레거시 `migrations/` 디렉터리를 삭제하고 `YachaTrade/migrations` 서브모듈로 교체. `apply_baseline_migrations`가 `migrations/0001_init.sql`(= 프로덕션이 적용하는 그 파일)을 적용하므로, 스키마-코드 불일치가 배포가 아니라 테스트에서 잡힌다
- `read_schema_section()` 헬퍼 추가 — 통합본의 `-- >>> <name>` 마커로 한 블록만 재실행(dividend 백필 idempotency 테스트용)
- 테스트 픽스처 정리: `insert_token`의 version 컬럼 제거, group_b의 chain/version 단언 삭제, `token_chain.rs` 삭제(GIWA 스키마에 chain 컬럼 없음)
- 검증: build + 전체 스위트 통과(기존 결함 pool_batch_update_reserves 3 · cache doc-test 2 제외)

## Outcome

- (머지 시 작성)

## 배포 의존성

observer · api-server · giwa/migrations 세 리포가 **함께** 배포돼야 한다. 부분 롤아웃 시 해당 테이블 대상 쿼리가 전부 실패한다.
