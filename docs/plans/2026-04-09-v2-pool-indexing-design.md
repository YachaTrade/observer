# V2 Pool & Factory 인덱싱 설계

## 배경

V2는 Launchpad(BondingCurve) + DEX(Uniswap V2 스타일) 동시 운영.
- Launchpad 토큰: BondingCurve Create → Curve 거래 → Graduate → DEX 거래
- Pure DEX pair: NadFunFactory PairCreated → 바로 DEX 거래
- 기존 token/market 테이블은 launchpad 전용 → DEX는 별도 테이블로 분리

## 설계 결정

### 테이블 분리 전략

- **Launchpad**: 기존 `token` + `market` 테이블 유지. 졸업 후에도 여기서 관리.
- **DEX**: `pool` + `dex_token` 테이블 신규. 별도 페이지.
- **졸업 토큰**: market에도 있고 pool에도 있음 (양쪽 조회 가능). dex_token에는 안 넣음.

### 신규 테이블

```sql
CREATE TABLE dex_token (
    token_id    VARCHAR(42) PRIMARY KEY,
    name        VARCHAR NOT NULL DEFAULT '',
    symbol      VARCHAR NOT NULL DEFAULT '',
    decimals    INT NOT NULL DEFAULT 18,
    image_uri   VARCHAR NOT NULL DEFAULT '',
    created_at  BIGINT NOT NULL
);

CREATE TABLE pool (
    pool_id      VARCHAR(42) PRIMARY KEY,
    token0       VARCHAR(42) NOT NULL,
    token1       VARCHAR(42) NOT NULL,
    reserve0     NUMERIC NOT NULL DEFAULT 0,
    reserve1     NUMERIC NOT NULL DEFAULT 0,
    price        NUMERIC NOT NULL DEFAULT 0,
    created_at   BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash      VARCHAR NOT NULL
);

CREATE INDEX idx_pool_token0 ON pool (token0);
CREATE INDEX idx_pool_token1 ON pool (token1);
```

### 기존 테이블 변경 없음

token, market 테이블은 그대로.

## 이벤트 흐름

### Launchpad 토큰
```
BondingCurve Create → token INSERT + market INSERT (기존 그대로)
Curve Buy/Sell/Sync → market UPDATE
Graduate → market UPDATE (V2_DEX) + pool INSERT (졸업 pair)
Dex Swap/Sync → market UPDATE + pool UPDATE
```

### Pure DEX pair
```
NadFunFactory PairCreated(token, pair, creator, pairIndex)
  → token0/token1 RPC call (pair.token0(), pair.token1())
  → 각 토큰이 launchpad token 테이블에 없으면 → dex_token INSERT (ERC20 fetch)
  → pool INSERT
```

## 프론트엔드 조회

- Launchpad 페이지 → token + market
- DEX 페이지 → pool + dex_token (졸업 토큰은 token 테이블 JOIN)

## 핸들러 구조

```
src/event/v2/factory/
  ├── mod.rs      # V2FactoryEventHandler
  ├── stream.rs   # PairCreated 이벤트 구독
  └── receive.rs  # pool INSERT + dex_token fetch/INSERT
```

## 구현 순서

1. DB Controller: pool + dex_token CRUD (pool.rs, dex_token.rs)
2. Types: V2FactoryEvent, V2PairCreated
3. Factory 핸들러: stream + receive + EventType 등록
4. Curve 수정: Graduate 시 pool INSERT 추가
5. Dex 수정: Sync 시 pool UPDATE 추가
