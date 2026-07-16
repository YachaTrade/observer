# GIWA Observer Handoff

작성일: 2026-07-17

## 현재 상태

| 항목 | 값 |
| --- | --- |
| 저장소 경로 | `~/project/giwa/observer` |
| 기본 브랜치 | `main` |
| 초기 baseline | `aac0dde chore: initialize GIWA observer baseline` |
| 원본 Observer 상태 | `8586f3b fix: enforce generic Curve runtime contract` |
| 원본 migration 상태 | `a6612de feat: add token chain discriminator` |
| Git remote | 미설정 |
| migration 구조 | submodule이 아닌 저장소 내 일반 디렉터리 |

이 저장소는 GIWA 배포만 인덱싱하는 새 기준점이다. 런타임과 공개 체크포인트에는 V1/V2 구분이 없고, 내부 Rust 모듈과 ABI 경로에만 구현 출처가 남아 있다.

## 인덱싱 범위

런타임은 정확히 다음 6개 스트림을 실행한다.

| 공개 이벤트 | 실제 구현 출처 | 체크포인트 |
| --- | --- | --- |
| Curve | 기존 v2 BondingCurve | `curve` |
| Dex | 기존 v1 Capricorn DEX | `dex` |
| LpManager | 기존 v1 LPManager | `lp_manager` |
| Token | 공통 ERC-20 처리 | `token` |
| Price | 공통 quote 가격 처리 | `price` |
| PriceUsd | 공통 token USD 가격 처리 | `price_usd` |

처리 흐름은 다음과 같다.

```text
RPC logs / provider data
        -> Stream
        -> monitored channel
        -> Receive
        -> PostgreSQL + Redis
```

스트림 순서는 다음 규칙을 가진다.

```text
Price --------> Curve --------> Dex
                    |----------> LpManager
                    |----------> Token (strict wait)

PriceUsd (independent)
```

- Curve receive는 Price를 최대 60초 기다린다. 시간이 초과되면 경고 후 진행하므로 가격 provider와 cache 상태를 모니터링해야 한다.
- Dex와 LpManager의 블록 범위는 Curve가 앞선 뒤 진행하며, receive 단계에서도 Curve를 최대 60초 기다린다.
- Token은 Curve를 엄격하게 기다린다. 시간 초과 후 잘못된 상태로 진행하지 않는다.
- Price와 PriceUsd는 다른 스트림을 기다리지 않는다.

세부 이벤트 규약은 [event-indexing](docs/event-indexing.md), [Curve](docs/event/curve.md), [Dex](docs/event/dex.md), [LpManager](docs/event/lp-manager.md)를 참고한다.

## 제거된 범위

다음 Observer 스택은 이 저장소에서 제거됐다.

- 이전 v1 Curve, Reward, Creator, Distributor
- v2 Dex, Fee contract stream, LPManager
- Vault, VaultRegistry, DividendVault 전체 런타임
- 제거된 스택 전용 ABI, controller, test, 현재 기능 문서

Vault 제거는 Observer 코드 제거만 의미한다. 기존 DB 테이블, 데이터, migration, seed는 삭제하지 않았다.

## DB 쓰기 규약

새 GIWA 토큰 생성은 다음 값을 명시적으로 저장한다.

```text
token.version = 'V2'
token.chain   = 'GIWA'
```

그 외 주요 값은 다음과 같다.

- Curve 상태와 거래의 `market_type`: `CURVE`
- 졸업 이후와 Dex 거래의 `market_type`: `DEX`
- Curve 매수 수수료 이력: `curve_buy`
- Curve 매도 수수료 이력: `curve_sell`
- `token_id`는 계속 단독 primary key다.
- 다른 테이블에는 `chain`을 추가하지 않는다.

기존 MON 데이터는 그대로 둔다.

- `migrations/0036_token_chain.sql`은 기존 `token.chain IS NULL` 행을 `MON`으로 채운다.
- `token.chain` 기본값은 `MON`이며 `NOT NULL`이다.
- 기존 `V2_CURVE`, `V2_DEX`, `v2_*` 값은 변환하지 않는다.
- Vault/Dividend 및 제거된 스트림의 기존 테이블과 행을 drop, truncate, rewrite하지 않는다.
- 기존 DB에 migration 전체를 처음부터 재실행하지 말고, 현재 배포 절차에서 미적용 migration만 적용한다.

Observer binary는 migration을 자동 적용하지 않는다. 배포 전에 별도 migration 절차로 적용해야 한다.

시작 전에 `quote_token` 테이블에 최소 1개 이상의 quote 설정이 있어야 한다. 각 행의 `quote_id`, `pyth_feed_id`, `decimals`를 시작 시 읽으며, 행이 없으면 프로세스가 중단된다.

## 환경변수

`.env`는 저장소에 포함되어 있지 않다. 실제 값은 배포 secret에서 공급해야 한다.

### GIWA contract 및 fee

아래 8개 이름은 버전 prefix 없이 사용한다.

```dotenv
BONDING_CURVE=0x...
DEX_FACTORY=0x...
DEX_ROUTER=0x...
LP_MANAGER=0x...

CREATE_FEE_AMOUNT=...
GRADUATE_FEE_AMOUNT=...
BONDING_CURVE_FEE_RATE=...
DEX_ROUTER_FEE_RATE=...
```

주소는 시작 시 EIP-55 형태로 정규화된다. 누락되거나 잘못된 주소면 즉시 실패한다.

### 필수 인프라 설정

```dotenv
DATABASE_URL=postgres://...
REDIS_URL=redis://...

MAIN_RPC_URL=https://...
SUB_RPC_URL_1=https://...
SUB_RPC_URL_2=https://...
WMON=0x...

START_BLOCK=...
BLOCK_BATCH_SIZE=...
BLOCK_INTERVAL=...
BLOCK_OFFSET=...

DEFAULT_DELAY=...
RPC_TIME_OUT=...
PROVIDER_CHECK_INTERVAL=...
METRICS_REPORT_INTERVAL=...

PG_MAX_CONNECTIONS=...
PG_MIN_CONNECTIONS=...
PG_MAX_LIFETIME=...
PG_ACQUIRE_TIMEOUT=...
PG_IDLE_TIMEOUT=...
PG_STATEMENT_CACHE_CAPACITY=...
PG_SSL_MODE=disable

DEFAULT_IMAGE_1=https://...
DEFAULT_IMAGE_2=https://...
DEFAULT_IMAGE_3=https://...
DEFAULT_IMAGE_4=https://...
DEFAULT_IMAGE_5=https://...
```

`PG_SSL_MODE` 허용값은 `disable`, `prefer`, `require`, `verify-ca`, `verify-full`이다. `DEFAULT_IMAGE_1`부터 `DEFAULT_IMAGE_5`까지는 신규 account 생성 시 무작위로 선택되므로 모두 준비해야 한다.

### 선택 설정과 기본값

| 변수 | 기본값 |
| --- | --- |
| `MODE` | `mainnet` |
| `STREAM_TIMEOUT` | `5000` ms |
| `METRICS_PORT` | `8080` |
| `VANITY_ADDRESS_SUFFIX` | `7777` |
| `PYTH_API_URL` | `https://hermes.pyth.network/v2/updates/price` |

## 시작 순서와 운영상 주의점

프로세스 시작 순서는 다음과 같다.

1. contract 주소 설정을 검증하고 정규화한다.
2. PostgreSQL과 Redis 연결을 초기화한다.
3. PostgreSQL의 `quote_token` 설정을 메모리에 적재한다.
4. Observer가 소유한 Redis cache prefix를 삭제하고 DB 기준으로 다시 채운다.
5. RPC provider 3개를 초기화한다.
6. 시작 블록과 스트림 범위를 계산한다.
7. 가격 cache를 준비한 뒤 6개 스트림과 metrics 서버를 실행한다.

Redis는 시작할 때 전체 keyspace를 SCAN하고 Observer 전용 prefix의 key를 지운다. 같은 Redis를 다른 서비스와 공유할 수는 있지만, Observer prefix에 외부 서비스 데이터를 저장하면 안 된다.

`START_BLOCK=0`은 `balance_history`의 최신 블록을 이용해 재개한다. 신규 DB처럼 기준 행이 없으면 시작할 수 없으므로 최초 배포 시에는 명시적인 시작 블록을 주는 편이 안전하다.

Prometheus endpoint는 `METRICS_PORT`의 `/metrics`에 노출된다.

## 로컬 검증

필수 정적/단위 검증:

```bash
cargo check --lib --bin observer
cargo test --lib
cargo test --test giwa_runtime_contract
```

현재 새 저장소에서 확인한 결과:

- `cargo test --lib`: 23 passed
- `cargo test --test giwa_runtime_contract`: 5 passed

DB와 Redis, 실제 환경변수가 준비된 후 실행:

```bash
cargo run --release
```

## 알려진 baseline 문제

아래 항목은 GIWA 분리 작업 이전부터 있던 문제이며 이번 범위에서 수정하지 않았다.

1. `cargo test --tests`
   - `group_b_controllers`의 pool reserve 테스트 3개가 실패한다.
   - prepared statement는 11개 bind를 요구하지만 테스트가 9개만 전달한다.
   - 실패 테스트: `pool_batch_update_reserves`, `pool_batch_update_reserves_freshness_breaks_timestamp_tie`, `pool_batch_update_reserves_stale_sync_rejected`.
2. `cargo test --all-targets --no-run`
   - `benches/sort_benchmark.rs`가 선언되지 않은 `criterion`, `rayon`에 의존한다.
3. 저장소 전체 `cargo fmt --all -- --check`
   - 기존 파일의 포맷 차이가 남아 있다. GIWA 변경 파일만 focused formatting으로 검증했다.
4. `.github/workflows/ci.yml`
   - 제거된 예전 환경변수를 포함하고 현재 필수 GIWA 설정이 빠져 있다.
   - CI는 전체 fmt, clippy, test를 실행하므로 위 baseline 문제가 정리되기 전에는 그대로 green이 되지 않을 수 있다.

이 문제들을 수정할 때 GIWA 기능 변경과 별도 커밋으로 분리하는 것을 권장한다.

## 배포 전 체크리스트

- [ ] Git remote를 설정한다.
- [ ] GIWA contract 4개 주소와 WMON 주소를 확정한다.
- [ ] RPC 3개 endpoint를 확정한다.
- [ ] PostgreSQL과 Redis secret을 설정한다.
- [ ] PostgreSQL pool 관련 환경변수를 설정한다.
- [ ] `DEFAULT_IMAGE_1..5`를 설정한다.
- [ ] 기존 DB에 `0036_token_chain.sql` 적용 여부를 확인한다.
- [ ] `quote_token` seed와 decimals, Pyth feed를 확인한다.
- [ ] 기존 MON token 행의 `chain='MON'`을 확인한다.
- [ ] canary 시작 블록을 정하고 `START_BLOCK`을 설정한다.
- [ ] 시작 로그에서 6개 generic checkpoint만 생성되는지 확인한다.
- [ ] 신규 token이 `version='V2'`, `chain='GIWA'`, `market_type='CURVE'`로 저장되는지 확인한다.
- [ ] Graduate 이후 `market_type='DEX'`로 변경되는지 확인한다.
- [ ] Curve fee history가 `curve_buy`/`curve_sell`로 저장되는지 확인한다.
- [ ] Vault/Dividend 과거 테이블과 데이터가 그대로 남아 있는지 확인한다.
- [ ] `/metrics`와 RPC/Postgres/Redis health 로그를 확인한다.

## 주요 파일

| 파일 | 역할 |
| --- | --- |
| `src/main.rs` | 초기화 순서와 6개 handler wiring |
| `src/config.rs` | contract, fee, stream, metrics 설정 |
| `src/sync/receive.rs` | receive 단계의 스트림 의존성 |
| `src/sync/stream.rs` | 블록 범위와 stream checkpoint 정책 |
| `src/event/v2/curve/` | 활성 Curve 구현 출처 |
| `src/event/v1/dex/` | 활성 Dex 구현 출처 |
| `src/event/v1/lp_manager/` | 활성 LpManager 구현 출처 |
| `src/event/common/` | Token, Price, PriceUsd 구현 |
| `src/db/postgres/controller/token.rs` | GIWA token 및 초기 market insert |
| `migrations/0036_token_chain.sql` | `token.chain` backfill/default/not-null |
| `tests/giwa_runtime_contract.rs` | 6개 runtime wiring과 generic 설정 계약 |
| `tests/token_chain.rs` | chain migration 동작 검증 |

## 유지해야 할 결정

- 공개 런타임 이름에 V1/V2 prefix를 다시 추가하지 않는다.
- Curve는 현재 v2 구현 출처, Dex와 LpManager는 현재 v1 구현 출처를 유지한다.
- `token.version='V2'`와 `token.chain='GIWA'`를 명시적으로 쓴다.
- `token_id` 단독 primary key를 유지한다.
- 기존 MON 및 versioned market/fee 값을 정규화하지 않는다.
- Vault를 포함한 과거 DB 구조와 migration을 삭제하지 않는다.
- 제거된 stream을 feature flag나 optional env로 되살리지 않는다.

설계 배경은 [GIWA single-version design](docs/superpowers/specs/2026-07-16-giwa-single-version-indexing-design.md)에 정리되어 있다.
