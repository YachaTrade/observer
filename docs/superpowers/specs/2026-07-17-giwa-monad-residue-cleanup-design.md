# GIWA Sepolia 배포를 위한 Monad 잔재 정리 — 설계

- 날짜: 2026-07-17
- 상태: 승인됨 (접근안 A: 설정 주도 정리)
- 타깃: GIWA Sepolia 테스트넷 (`MODE=testnet`)

## 배경

이 프로젝트는 Monad에서 운영하던 인덱서를 GIWA chain(ETH L2, OP Stack)에 새로 배포하기 위해
`chore: initialize GIWA observer baseline`으로 스쿼시된 상태다. 핵심 코드(이벤트 핸들러,
DB 계약 `token.chain='GIWA'` 등)는 이미 GIWA 기준이지만, 배포 계약(env), CI, migration
시드/DEFAULT, DefiLlama 조회 키에 Monad 잔재가 남아 있어 신규 배포 시 잘못된 데이터가
생성되거나 운영자가 헤매게 된다.

## 범위

**포함**: DefiLlama 체인 슬러그 env화, migration의 Monad 주소 상수 교체, env 계약
`WMON`→`WETH` rename, CI 정리, `check.sh` 파라미터화, `.env.example` 신설, 오해를 부르는
주석 최소 수정, README/event-indexing 문서 갱신.

**제외**: 실제 인프라 배포, `0010_raffle.sql`의 `monad_airdrop` 테이블(src 미사용 레거시,
스키마 계약상 불변), `docs/plans/`·`branches/` 히스토리 문서, `src/db/cache/mod.rs` 등
내부 WMON 개념 주석 전면 재작성(diff 노이즈 대비 가치 낮음).

## 확정된 상수

| 항목 | 값 |
| --- | --- |
| Wrapped native (GIWA) | WETH predeploy `0x4200000000000000000000000000000000000006` |
| Pyth ETH/USD feed | `0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace` |
| GIWA Sepolia RPC (기본값/예시) | `https://sepolia-rpc.giwa.io` |
| DefiLlama 슬러그 기본값 | `ethereum` |

## 변경 항목

### 1. DefiLlama 체인 슬러그 env화

`src/event/common/price_usd/mod.rs`의 `coin_ref()`가 `"monad:{token_id}"`를 하드코딩한다.
`DEFILLAMA_CHAIN_SLUG` env(기본값 `ethereum`)를 lazy static으로 읽어 `"{slug}:{token_id}"`를
생성하도록 바꾼다.

- GIWA Sepolia는 `MODE=testnet` → `MockProvider` 경로라 런타임 영향 없음. 향후 메인넷에서
  whitelist 토큰을 ethereum 메인넷 주소로 매핑하는 기존 `price_source_id` 설계
  (2026-06-16 whitelist-testnet-address-mapping)와 정합.
- 테스트 갱신: `tests/price_usd_logic.rs`의 `monad:` 픽스처와
  `coin_ref_builds_monad_prefixed_preserving_case` 테스트, `tests/price_usd_price_source.rs`의
  `coin_ref` 기대값을 기본 슬러그(`ethereum`) 기준으로 수정. EIP-55 케이싱 보존 단언은 유지.

### 2. Migration의 Monad quote 주소 상수 일괄 교체

테스트 하니스는 migration `.sql` 파일을 직접 읽어 적용하고(checksum 없음), 프로덕션도
migration을 자동 적용하지 않으므로 신규 GIWA DB 기준 제자리 수정이 안전하다.

**교체 규칙**: 컬럼 `DEFAULT`와 시드 `INSERT`에 등장하는 Monad WMON 주소
`0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A`(소문자 변형 포함)는 WETH predeploy로 교체.
LVMON(`0xBe3fa50514D9617ce645a02B34F595541AF02b6b`) 시드 행은 제거. 반면 **백필
`UPDATE`/`WHERE`의 주소 리터럴은 그대로 둔다** — Monad DB 업그레이드용 히스토리 스크립트로,
빈 GIWA DB에서는 no-op이다.

대상 파일:

- `0019_quote_token.sql`: MON/LVMON 시드 2행 → WETH 1행
  (`'Wrapped Ether'`, `'WETH'`, 18, ETH/USD feed, 이미지 URI는 기존 스토리지 규칙에 맞춰
  `https://storage.nadapp.net/quote/weth.webp`). 헤더 주석의 WMON 표현 갱신.
- `0031_whitelist_token.sql`: MON/LVMON 시드 → WETH 1행(sort_order 1).
- `0002_token.sql`, `0007_price.sql`, `0015_v2_events.sql`: `quote_id DEFAULT` 교체.
- `v2_upgrade_new_tables.sql`, `v2_upgrade_alter.sql`: `DEFAULT` 절과 시드 `INSERT`만 교체,
  백필 구문은 유지.
- `0028_quote_token_is_native.sql`: DDL 변경 없음(DEFAULT TRUE 유지 — WETH가 native라
  의미 동일). 주석의 MON/WMON/LVMON 서술만 WETH 기준으로 갱신.
- 구현 중 `0033`, `0035` 등 나머지 migration도 주소 상수 재검색으로 누락 확인.

### 3. env 계약 rename: `WMON` → `WETH`

- `src/config.rs`: `env::var("WMON")` → `env::var("WETH")`, panic 메시지·주변 주석 갱신.
  내부 static 이름 `WNATIVE_ADDRESS`는 유지.
- `tests/common/mod.rs`: 기본값 세팅 env 이름 변경(값은 임의 유효 주소면 충분).
- 문서: README와 `docs/event-indexing.md`의 deployment variables 목록에 현재 누락된
  `WETH`, `MODE`, `MAIN_RPC_URL`/`SUB_RPC_URL_1`/`SUB_RPC_URL_2`를 추가.

### 4. CI 정리 — `.github/workflows/ci.yml`

- `MAIN_RPC_URL` → `https://sepolia-rpc.giwa.io` (테스트는 RPC를 실제 호출하지 않음).
- 코드가 읽지 않는 죽은 env 제거: `BONDING_CURVE_FACTORY`, `KEYSTORE_PASSWORD`,
  `KEY_VALUE_DATABASE_DIR`, `END_BLOCK`.
- `START_BLOCK` 등 실제로 읽는 변수는 유지.

### 5. `check.sh` 파라미터화

하드코딩된 Monad RPC IP(`http://64.31.48.109:8080`)를
`RPC_URL="${RPC_URL:-https://sepolia-rpc.giwa.io}"`로 교체.

### 6. `.env.example` 신설

GIWA Sepolia 프로파일 기준 전체 템플릿. 코드가 `env::var`로 읽는 변수 전수를 필수/선택
구분과 한 줄 설명으로 나열한다:

- 필수: `DATABASE_URL`, `REDIS_URL`, `MAIN_RPC_URL`, `SUB_RPC_URL_1`, `SUB_RPC_URL_2`,
  `START_BLOCK`, `BLOCK_BATCH_SIZE`, `BLOCK_INTERVAL`, `BLOCK_OFFSET`, `DEFAULT_DELAY`,
  `RPC_TIME_OUT`, `PROVIDER_CHECK_INTERVAL`, `METRICS_REPORT_INTERVAL`, `BONDING_CURVE`,
  `DEX_FACTORY`, `DEX_ROUTER`, `LP_MANAGER`, `WETH`, `CREATE_FEE_AMOUNT`,
  `GRADUATE_FEE_AMOUNT`, `BONDING_CURVE_FEE_RATE`, `DEX_ROUTER_FEE_RATE`
- 선택(기본값 있음): `MODE`(testnet 권장), `STREAM_TIMEOUT`, `METRICS_PORT`,
  `PYTH_API_URL`, `VANITY_ADDRESS_SUFFIX`, `DEFILLAMA_CHAIN_SLUG`, `PG_*`
- 컨트랙트 주소는 `0x...` placeholder, `WETH`는 predeploy 실주소 기입.
- `.gitignore`의 `.env.*` 패턴에 걸리므로 `!.env.example` 예외 추가.

### 7. 주석 잔재 최소 수정

`src/event/common/price/stream.rs`의 "Monad block-time variance (0.3–0.5 s)" 주석을
체인 중립 표현으로 수정. 그 외 내부 주석은 범위 제외 원칙 유지.

## 검증

1. `cargo fmt --all -- --check`
2. `cargo clippy -- -D warnings`
3. `cargo test --lib` (config/coin_ref 단위 테스트 포함)
4. Docker 가용 시: `cargo test --test giwa_runtime_contract` 및
   `price_usd*` 통합 테스트 — migration 교체 후 시드/DEFAULT가 테스트 픽스처와 충돌하지
   않는지 확인.

## 리스크와 비고

- **migration 제자리 수정**: 기존 Monad 운영 DB에는 절대 재적용하지 않는 전제(신규 GIWA
  배포 전용 리포). sqlx-cli `migrate run`을 쓰는 로컬 DB가 있다면 리셋 필요.
- **DefiLlama 슬러그**: GIWA가 DefiLlama에 등록되기 전까지는 whitelist 토큰을 ethereum
  메인넷 주소로 매핑해 사용하는 운영 전제. 테스트넷에서는 mock이라 무관.
- **주소 실측 확인**: `BONDING_CURVE` 등 GIWA에 배포된 실제 컨트랙트 주소는 이 작업 범위
  밖이며 배포 시 운영자가 `.env`에 기입한다.
