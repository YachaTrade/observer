# PnL (Transfer 기반) 설계

## 개요

Token Transfer + WMON Transfer를 기반으로 모든 토큰 흐름을 추적한다.
Swap 이벤트 파싱 없이, Transfer만으로 모든 상황을 커버한다.

## 핵심 원칙

1. **tx_sender = User**: 트랜잭션 발신자가 거래 주체
2. **Pool 기준 흐름**: Router wrap/unwrap 무관, Pool과의 흐름만 추적
3. **현금 흐름 기반**: Native/USD 수입/지출로 PnL 계산

---

## 추적 대상

```
1. Token Transfer (화이트리스트 토큰)
2. WMON Transfer (WNATIVE_ADDRESS)
```

---

## 판단 로직

### TX 분석

```
1. tx_sender 확인 (= User)
2. Token Transfer 방향 (User 기준)
3. WMON Transfer 방향 (Pool 기준)
4. 케이스 판단
```

### 케이스별 판단

```
┌─────────────────────────────────────────────────────────────────┐
│ Token 방향 (User 기준) │ WMON 방향 (Pool 기준) │ 결과           │
├─────────────────────────────────────────────────────────────────┤
│ Pool → User (IN)       │ ??? → Pool (IN)       │ Buy            │
│ User → Pool (OUT)      │ Pool → ??? (OUT)      │ Sell           │
│ User → Pool (OUT)      │ ??? → Pool (IN)       │ LP Mint        │
│ Pool → User (IN)       │ Pool → ??? (OUT)      │ LP Burn        │
│ EOA → EOA              │ 없음                  │ Transfer       │
│ ??? → User             │ 없음                  │ Airdrop        │
└─────────────────────────────────────────────────────────────────┘
```

### 간단 정리

```
Token과 WMON 방향이 반대 → Trade (Buy/Sell)
Token과 WMON 방향이 같음 → LP (Mint/Burn)
WMON 없음 → Transfer 또는 Airdrop
```

---

## 실제 TX 흐름

### 매수 (Buy)

```
User → Router.swapExactETHForTokens()

1. User → Router (native MON)
2. Router → WMON.deposit() (wrap)
3. WMON Transfer: Router → Pool
4. Token Transfer: Pool → User

분석:
- tx_sender = User
- Token: Pool → User (IN)
- WMON: ??? → Pool (IN)
- 방향 반대 → Buy

Position 업데이트:
- native_out += wmon_amount
- usd_out += usd_value
- token_in += token_amount
```

### 매도 (Sell)

```
User → Router.swapExactTokensForETH()

1. Token Transfer: User → Pool
2. WMON Transfer: Pool → Router
3. Router → WMON.withdraw() (unwrap)
4. Router → User (native MON)

분석:
- tx_sender = User
- Token: User → Pool (OUT)
- WMON: Pool → ??? (OUT)
- 방향 반대 → Sell

Position 업데이트:
- native_in += wmon_amount
- usd_in += usd_value
- token_out += token_amount
```

### LP 추가 (Mint)

```
User → Router.addLiquidity()

1. Token Transfer: User → Pool
2. WMON Transfer: Router → Pool

분석:
- tx_sender = User
- Token: User → Pool (OUT)
- WMON: ??? → Pool (IN)
- 방향 같음 (둘 다 Pool로) → LP Mint

Position 업데이트:
- native_out += wmon_amount
- usd_out += usd_value
- token_out += token_amount
```

### LP 제거 (Burn)

```
User → Router.removeLiquidity()

1. Token Transfer: Pool → User
2. WMON Transfer: Pool → Router

분석:
- tx_sender = User
- Token: Pool → User (IN)
- WMON: Pool → ??? (OUT)
- 방향 같음 (둘 다 Pool에서) → LP Burn

Position 업데이트:
- native_in += wmon_amount
- usd_in += usd_value
- token_in += token_amount
```

### 지갑 전송 (Transfer)

```
User → Token.transfer(to, amount)

1. Token Transfer: User → OtherEOA

분석:
- tx_sender = User
- Token: EOA → EOA
- WMON: 없음
- → Transfer

Position 업데이트:
- Sender: token_out += amount
- Receiver: token_in += amount
```

---

## DB 구조

### transfer 테이블

```sql
CREATE TABLE IF NOT EXISTS transfer (
    id BIGSERIAL PRIMARY KEY,
    tx_hash VARCHAR(66) NOT NULL,
    tx_sender VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL,
    block_number BIGINT NOT NULL,
    log_index INT NOT NULL,
    transaction_index INT NOT NULL,
    created_at BIGINT NOT NULL,

    UNIQUE(tx_hash, log_index)
);

CREATE INDEX idx_transfer_tx ON transfer(tx_hash);
CREATE INDEX idx_transfer_sender ON transfer(tx_sender);
CREATE INDEX idx_transfer_token ON transfer(token_id);
CREATE INDEX idx_transfer_block ON transfer(block_number);
```

### position 테이블 (현금 흐름 기반)

```sql
CREATE TABLE IF NOT EXISTS position (
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,

    -- Native 흐름
    native_in NUMERIC NOT NULL DEFAULT 0,      -- 수입 (매도, LP 제거 시 받음)
    native_out NUMERIC NOT NULL DEFAULT 0,     -- 지출 (매수, LP 추가 시 지불)

    -- USD 흐름
    usd_in NUMERIC NOT NULL DEFAULT 0,         -- 수입 (USD)
    usd_out NUMERIC NOT NULL DEFAULT 0,        -- 지출 (USD)

    -- Token 흐름
    token_in NUMERIC NOT NULL DEFAULT 0,       -- 획득 (매수, LP 제거, Transfer 받음)
    token_out NUMERIC NOT NULL DEFAULT 0,      -- 지출 (매도, LP 추가, Transfer 보냄)

    -- 메타데이터
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,

    PRIMARY KEY (account_id, token_id)
);

CREATE INDEX idx_position_account ON position(account_id);
CREATE INDEX idx_position_token ON position(token_id);
```

---

## Position 업데이트 요약

| 케이스 | native_in | native_out | usd_in | usd_out | token_in | token_out |
|--------|-----------|------------|--------|---------|----------|-----------|
| Buy | - | +wmon | - | +usd | +token | - |
| Sell | +wmon | - | +usd | - | - | +token |
| LP Mint | - | +wmon | - | +usd | - | +token |
| LP Burn | +wmon | - | +usd | - | +token | - |
| Transfer Out | - | - | - | - | - | +token |
| Transfer In | - | - | - | - | +token | - |

---

## PnL 계산

### Realized PnL (확정 손익)

```sql
realized_pnl = native_in - native_out
realized_pnl_usd = usd_in - usd_out
```

### 현재 보유량

```sql
current_balance = token_in - token_out
```

### 평균 매수가

```sql
avg_price = native_out / NULLIF(token_in, 0)
avg_price_usd = usd_out / NULLIF(token_in, 0)
```

### Unrealized PnL (미실현 손익)

```sql
unrealized_pnl = (current_price * current_balance) - (avg_price * current_balance)
unrealized_pnl_usd = (current_price_usd * current_balance) - (avg_price_usd * current_balance)
```

### Total PnL

```sql
total_pnl = realized_pnl + unrealized_pnl
total_pnl_usd = realized_pnl_usd + unrealized_pnl_usd
```

---

## 조회 쿼리

```sql
SELECT
    p.account_id,
    p.token_id,

    -- 원본 데이터
    p.native_in,
    p.native_out,
    p.usd_in,
    p.usd_out,
    p.token_in,
    p.token_out,

    -- Realized PnL
    (p.native_in - p.native_out) AS realized_pnl,
    (p.usd_in - p.usd_out) AS realized_pnl_usd,

    -- 현재 보유량
    (p.token_in - p.token_out) AS current_balance,

    -- 평균 매수가
    p.native_out / NULLIF(p.token_in, 0) AS avg_price,
    p.usd_out / NULLIF(p.token_in, 0) AS avg_price_usd,

    -- Unrealized PnL (market 테이블 조인 필요)
    (m.price * (p.token_in - p.token_out)) -
    ((p.native_out / NULLIF(p.token_in, 0)) * (p.token_in - p.token_out)) AS unrealized_pnl,

    -- Total PnL
    (p.native_in - p.native_out) +
    ((m.price * (p.token_in - p.token_out)) -
    ((p.native_out / NULLIF(p.token_in, 0)) * (p.token_in - p.token_out))) AS total_pnl

FROM position p
LEFT JOIN market m ON p.token_id = m.token_id
WHERE p.account_id = :account_id;
```

---

## 예시

### 시나리오 1: 단순 매수/매도 (이익)

```
1. A가 100개 매수 (1 WMON, $2000)
2. A가 100개 매도 (1.5 WMON, $3600)
```

**A의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 1.5 |
| native_out | 1 |
| usd_in | 3600 |
| usd_out | 2000 |
| token_in | 100 |
| token_out | 100 |

**PnL:**
```
realized_pnl = 1.5 - 1 = +0.5 WMON ✓
realized_pnl_usd = 3600 - 2000 = +$1600 ✓
current_balance = 0
```

---

### 시나리오 2: 단순 매수/매도 (손실)

```
1. A가 100개 매수 (1 WMON, $2000)
2. A가 100개 매도 (0.6 WMON, $1200)
```

**A의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 0.6 |
| native_out | 1 |
| usd_in | 1200 |
| usd_out | 2000 |
| token_in | 100 |
| token_out | 100 |

**PnL:**
```
realized_pnl = 0.6 - 1 = -0.4 WMON (손실) ✓
realized_pnl_usd = 1200 - 2000 = -$800 ✓
current_balance = 0
```

---

### 시나리오 3: 부분 매도 + 미실현 손익

```
1. A가 100개 매수 (1 WMON, $2000)
2. A가 50개 매도 (0.75 WMON, $1800)
3. 현재 토큰 가격: 0.02 WMON/개
```

**A의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 0.75 |
| native_out | 1 |
| usd_in | 1800 |
| usd_out | 2000 |
| token_in | 100 |
| token_out | 50 |

**PnL:**
```
realized_pnl = 0.75 - 1 = -0.25 WMON
current_balance = 100 - 50 = 50개
avg_price = 1 / 100 = 0.01 WMON/개
unrealized_pnl = (0.02 * 50) - (0.01 * 50) = 1 - 0.5 = +0.5 WMON
total_pnl = -0.25 + 0.5 = +0.25 WMON ✓
```

---

### 시나리오 4: 지갑 간 전송 후 매도

```
1. A가 100개 매수 (1 WMON, $2000)
2. A가 B에게 100개 전송
3. B가 100개 매도 (1.5 WMON, $3600)
```

**A의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 0 |
| native_out | 1 |
| usd_in | 0 |
| usd_out | 2000 |
| token_in | 100 |
| token_out | 100 |

**A의 PnL:**
```
realized_pnl = 0 - 1 = -1 WMON (손실)
current_balance = 0
```

**B의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 1.5 |
| native_out | 0 |
| usd_in | 3600 |
| usd_out | 0 |
| token_in | 100 |
| token_out | 100 |

**B의 PnL:**
```
realized_pnl = 1.5 - 0 = +1.5 WMON (전부 이익, cost=0)
current_balance = 0
```

**전체 시스템 PnL:**
```
A: -1 WMON
B: +1.5 WMON
실제 순이익: 1.5 - 1 = +0.5 WMON ✓
```

---

### 시나리오 5: 에어드랍 받고 매도

```
1. A가 에어드랍으로 100개 받음 (cost = 0)
2. A가 100개 매도 (1 WMON, $2400)
```

**A의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 1 |
| native_out | 0 |
| usd_in | 2400 |
| usd_out | 0 |
| token_in | 100 |
| token_out | 100 |

**PnL:**
```
realized_pnl = 1 - 0 = +1 WMON (전부 이익) ✓
realized_pnl_usd = 2400 - 0 = +$2400 ✓
current_balance = 0
```

---

### 시나리오 6: LP 추가/제거 (Impermanent Loss)

```
1. A가 100개 매수 (1 WMON, $2000)
2. A가 LP 추가: 100개 + 1 WMON ($2000)
3. A가 LP 제거: 80개 + 1.2 WMON ($2880) ← IL 발생
4. A가 80개 매도 (1.5 WMON, $3600)
```

**A의 Position:**

| 필드 | 값 | 설명 |
|------|-----|------|
| native_in | 1.2 + 1.5 = 2.7 | LP 제거 + 매도 |
| native_out | 1 + 1 = 2 | 매수 + LP 추가 |
| usd_in | 2880 + 3600 = 6480 | |
| usd_out | 2000 + 2000 = 4000 | |
| token_in | 100 + 80 = 180 | 매수 + LP 제거 |
| token_out | 100 + 80 = 180 | LP 추가 + 매도 |

**PnL:**
```
realized_pnl = 2.7 - 2 = +0.7 WMON ✓
realized_pnl_usd = 6480 - 4000 = +$2480 ✓
current_balance = 180 - 180 = 0
```

**분석:**
- 토큰 20개 손실 (100 → 80)
- 대신 WMON 0.2개 이득 (1 → 1.2)
- 이것이 Impermanent Loss, 하지만 현금 흐름 기반이라 자동 반영됨

---

### 시나리오 7: 복합 시나리오

```
1. A가 100개 매수 (1 WMON)
2. A가 B에게 50개 전송
3. A가 50개 매도 (0.75 WMON)
4. B가 50개 매도 (0.8 WMON)
```

**A의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 0.75 |
| native_out | 1 |
| token_in | 100 |
| token_out | 100 |

**A의 PnL:** `0.75 - 1 = -0.25 WMON`

**B의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 0.8 |
| native_out | 0 |
| token_in | 50 |
| token_out | 50 |

**B의 PnL:** `0.8 - 0 = +0.8 WMON`

**전체:**
```
지출: 1 WMON (A 매수)
수입: 0.75 + 0.8 = 1.55 WMON (A 매도 + B 매도)
순이익: +0.55 WMON

A + B PnL: -0.25 + 0.8 = +0.55 WMON ✓
```

---

### 시나리오 8: 여러 번 매수 후 일부 매도

```
1. A가 100개 매수 (1 WMON)
2. A가 200개 매수 (3 WMON)
3. A가 150개 매도 (3 WMON)
```

**A의 Position:**

| 필드 | 값 |
|------|-----|
| native_in | 3 |
| native_out | 4 |
| token_in | 300 |
| token_out | 150 |

**PnL:**
```
realized_pnl = 3 - 4 = -1 WMON
current_balance = 300 - 150 = 150개
avg_price = 4 / 300 = 0.0133 WMON/개

미실현 손익 (현재가 0.02 WMON/개 가정):
unrealized_pnl = (0.02 * 150) - (0.0133 * 150) = 3 - 2 = +1 WMON

total_pnl = -1 + 1 = 0 WMON
```

---

## 구현 완료

### Stream 로직 (src/event/token/stream.rs)

```rust
// 1. TX별 tx_sender 조회
let tx_sender = client.get_transaction_by_hash(hash).inner.signer();

// 2. WMON Pool Flow 계산
for wmon in wmon_transfers {
    if all_pools.contains(wmon.from_address) {
        sender_native_in += wmon.amount;  // Pool에서 나감 = 매도 수익
    }
    if all_pools.contains(wmon.to_address) {
        sender_native_out += wmon.amount; // Pool로 들어감 = 매수 비용
    }
}

// 3. EOA만 position_history 기록
if from_is_eoa {
    native_in = if tx_sender == from { sender_native_in } else { 0 };
    record(from, token_out, native_in);
}
if to_is_eoa {
    native_out = if tx_sender == to { sender_native_out } else { 0 };
    record(to, token_in, native_out);
}
```

### EOA 캐싱 (Redis, 30일 TTL)

```
Key: eoa:{address}
Value: bool (true = EOA, false = Contract)
TTL: 30 days (2,592,000 seconds)
```

**체크 로직:**
1. Redis 캐시 확인
2. 없으면 RPC 호출 (`eth_getCode`)
3. code 비어있으면 EOA
4. 결과 Redis에 캐싱

**자동 필터링되는 주소:**
- BondingCurve
- DEX Pool
- DEX Router
- BondingCurve Router
- LP Manager
- Factory
- 기타 모든 Contract

### DB 테이블

#### position_history (INSERT만, 중복 방지)
```sql
PRIMARY KEY (account_id, token_id, transaction_hash, tx_index, log_index)
ON CONFLICT DO NOTHING
```

#### position (Trigger 자동 업데이트)
```sql
-- position_history INSERT 시 trigger로 position 자동 갱신
UPDATE position SET
    native_in = native_in + NEW.native_in,
    native_out = native_out + NEW.native_out,
    ...
```

---

## 장점

| 항목 | 설명 |
|------|------|
| 모든 케이스 커버 | Buy, Sell, LP, Transfer, Airdrop |
| Swap 파싱 불필요 | Transfer만으로 추론 |
| Router 무관 | Pool 기준 WMON 흐름만 추적 |
| EOA 자동 필터링 | Contract 주소 자동 제외 |
| 단순한 계산 | in - out = PnL |
| USD 트래킹 | 거래 시점 USD 가치 기록 |
| Idempotent | position_history PK로 중복 방지 |

## 필요 정보

| 항목 | 출처 |
|------|------|
| WMON 주소 | config.WNATIVE_ADDRESS |
| Pool 주소 | BondingCurve + cache_manager.get_token_pool() |
| tx_sender | get_transaction_by_hash().inner.signer() |
| EOA 여부 | cache_manager.check_is_eoa() (Redis 캐싱) |
| USD 가격 | cache_manager.get_price() |

---

## RPC 호출

TX당:
- `get_transaction_by_hash` × 1 (tx_sender 조회)
- `get_transaction_receipt` × 1 (block_number, gas 조회)
- `get_native_balance_at_block` × 2 (before/after)
- `get_code` × N (EOA 체크, Redis 캐싱으로 최소화)

## Native 흐름 계산 (Balance Check 방식)

```rust
// 1. balance 변화량 계산
let balance_before = get_native_balance_at_block(sender, block - 1);
let balance_after = get_native_balance_at_block(sender, block);
let balance_change = balance_after - balance_before;

// 2. gas 비용 계산
let gas_cost = receipt.gas_used * receipt.effective_gas_price;

// 3. gas 제외한 순수 매수/매도 금액
if balance_change < 0 {
    // 매수: 지출 (balance 감소) → native_out = |change| - gas
    native_out = abs(balance_change) - gas_cost;
} else {
    // 매도: 수입 (balance 증가) → native_in = change + gas
    native_in = balance_change + gas_cost;
}
```

**특징:**
- WMON Pool flow 추적 불필요
- Gas 자동 분리
- 순수 매수/매도 금액만 기록

---

## Fee Tracking

### 개요

Buy 이벤트에서 발생하는 Fee를 추적한다.
`fee_history` → `fee` 테이블로 자동 누적 (Trigger).
PnL 조회 시 `position`과 `fee`를 JOIN해서 최종 PnL 계산.

---

### Fee 발생 이벤트

| 이벤트 | Fee Rate | 설명 |
|--------|----------|------|
| Create | 10 MON (고정) | 토큰 생성 비용 |
| CurveBuy | 1% | BondingCurve 매수 수수료 |
| SwapBuy | 1% | DEX Pool 매수 수수료 |
| DexRouterBuy | 0.5% | DexRouter 매수 수수료 |

**Sell 이벤트는 Fee 추적 안함** (Curve/Router는 MON fee, Pool은 Token fee로 복잡)

---

### Fee 계산 공식

```
1. Create
   fee_native = DEPLOY_FE_AMOUNT (10 wei)
   fee_usd = (fee_native / 10^18) * native_price

2. CurveBuy
   fee_native = amount_in * 1 / 100
   fee_usd = (fee_native / 10^18) * native_price

3. SwapBuy (Pool)
   fee_native = amount_in / 100
   fee_usd = (fee_native / 10^18) * native_price

   ※ Pool Swap 이벤트의 amount_in은 fee 포함 금액
   ※ fee = amount_in * 1% = amount_in / 100

4. DexRouterBuy
   fee_native = amount_in * 0.5 / 100
   fee_usd = (fee_native / 10^18) * native_price
```

---

### Account ID 기준

| 이벤트 | account_id |
|--------|------------|
| Create | `creator` (이벤트 필드) |
| CurveBuy | `tx_sender` (트랜잭션 서명자) |
| SwapBuy | `tx_sender` |
| DexRouterBuy | `tx_sender` |

---

### DB 구조

#### fee_history (개별 fee 이벤트)

```sql
CREATE TABLE IF NOT EXISTS fee_history (
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INT NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    native_amount NUMERIC NOT NULL,      -- WMON 기준 fee
    usd_amount NUMERIC NOT NULL,         -- USD 기준 fee
    fee_type VARCHAR(20) NOT NULL,       -- 'create', 'curve_buy', 'swap_buy', 'dex_router_buy'
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,

    PRIMARY KEY (transaction_hash, log_index)
);

CREATE INDEX idx_fee_history_account_token ON fee_history(account_id, token_id);
```

#### fee (account, token별 누적)

```sql
CREATE TABLE IF NOT EXISTS fee (
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    native_amount NUMERIC NOT NULL DEFAULT 0,  -- 누적 fee (WMON)
    usd_amount NUMERIC NOT NULL DEFAULT 0,     -- 누적 fee (USD)
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,

    PRIMARY KEY (account_id, token_id)
);
```

#### Trigger (자동 누적)

```sql
CREATE OR REPLACE FUNCTION update_fee_on_history()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO fee (account_id, token_id, native_amount, usd_amount, created_at, updated_at)
    VALUES (NEW.account_id, NEW.token_id, NEW.native_amount, NEW.usd_amount, NEW.created_at, NEW.created_at)
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        native_amount = fee.native_amount + EXCLUDED.native_amount,
        usd_amount = fee.usd_amount + EXCLUDED.usd_amount,
        updated_at = EXCLUDED.updated_at;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_fee_on_history
AFTER INSERT ON fee_history
FOR EACH ROW
EXECUTE FUNCTION update_fee_on_history();
```

---

### PnL 계산 (Fee 반영)

```sql
SELECT
    p.account_id,
    p.token_id,

    -- Position 데이터
    p.native_in,
    p.native_out,
    p.usd_in,
    p.usd_out,

    -- Fee 데이터
    COALESCE(f.native_amount, 0) AS total_fee_native,
    COALESCE(f.usd_amount, 0) AS total_fee_usd,

    -- Realized PnL (Fee 차감 전)
    (p.native_in - p.native_out) AS realized_pnl_before_fee,
    (p.usd_in - p.usd_out) AS realized_pnl_usd_before_fee,

    -- Realized PnL (Fee 차감 후)
    (p.native_in - p.native_out - COALESCE(f.native_amount, 0)) AS realized_pnl,
    (p.usd_in - p.usd_out - COALESCE(f.usd_amount, 0)) AS realized_pnl_usd

FROM position p
LEFT JOIN fee f ON p.account_id = f.account_id AND p.token_id = f.token_id
WHERE p.account_id = :account_id;
```

---

### 예시

#### 시나리오: Buy → Sell (Fee 반영)

```
1. A가 100개 매수 (1 WMON, CurveBuy)
   - native_out = 1 WMON
   - fee = 0.01 WMON (1%)

2. A가 100개 매도 (1.5 WMON)
   - native_in = 1.5 WMON
```

**Position:**
| 필드 | 값 |
|------|-----|
| native_in | 1.5 |
| native_out | 1 |

**Fee:**
| 필드 | 값 |
|------|-----|
| native_amount | 0.01 |

**PnL 계산:**
```
realized_pnl_before_fee = 1.5 - 1 = +0.5 WMON
realized_pnl = 0.5 - 0.01 = +0.49 WMON ✓
```

---

### 구현 파일

| 파일 | 역할 |
|------|------|
| `types/fee.rs` | FeeType enum, FeeHistoryEvent 구조체 |
| `db/postgres/controller/fee.rs` | batch_insert_fee_history() |
| `event/curve/receive.rs` | Create, CurveBuy fee 수집 |
| `event/dex/receive.rs` | SwapBuy, DexRouterBuy fee 수집 |
| `migrations/0015_fee_history.sql` | 테이블, 인덱스, 트리거 생성 |

---

## Cost Basis Transfer (EOA→EOA)

### 개요

EOA간 토큰 전송 시 Cost Basis를 비례 이전한다.
기존에는 전송 시 sender의 cost가 그대로 남고, receiver는 cost=0으로 처리되어 PnL이 왜곡되었다.

### 문제점 (기존)

```
A: 100개 매수 (1 WMON) → native_out = 1
A → B: 100개 전송
B: 100개 매도 (1.5 WMON)

결과:
A PnL: -1 WMON (전부 손실, cost가 남아있음)
B PnL: +1.5 WMON (전부 이익, cost = 0)

실제 순이익: 0.5 WMON인데, A와 B의 PnL 합은 0.5 WMON으로 맞지만 개별 PnL이 왜곡됨
```

### 해결책

Cost Basis를 비례 이전:
```
A: 100개 매수 (1 WMON)
A → B: 50개 전송 (50%)

Cost 계산:
- avg_cost = native_out / token_in = 1 / 100 = 0.01 WMON/개
- transfer_cost = avg_cost * transfer_amount = 0.01 * 50 = 0.5 WMON

A position_history: native_in = 0.5 (cost 회수), token_out = 50
B position_history: native_out = 0.5 (cost 수령), token_in = 50

결과:
A: native_out=1, native_in=0.5 → 남은 cost = 0.5 WMON
B: native_out=0.5 → cost = 0.5 WMON
```

---

### Transfer Type

position_history에 transfer_type 컬럼 추가하여 거래 유형 추적:

| transfer_type | 설명 | token 방향 | native 방향 |
|---------------|------|------------|-------------|
| `buy` | 매수 | IN | OUT |
| `sell` | 매도 | OUT | IN |
| `transfer_out` | EOA→EOA 보내기 | OUT | - |
| `transfer_in` | EOA→EOA 받기 | IN | - |
| `lp_add` | LP 추가 | OUT | OUT |
| `lp_remove` | LP 제거 | IN | IN |
| `airdrop` | 에어드랍 | IN | - |
| `other` | 기타 | - | - |

---

### Transfer Type 판별 로직 (stream.rs)

```rust
// from (토큰 보내는 쪽)
match (is_eoa_to_eoa, has_native_in, has_native_out) {
    (true, _, _) => TransferOut,      // EOA→EOA
    (false, true, _) => Sell,         // 토큰 팔고 MON 받음
    (false, _, true) => LpAdd,        // 토큰도 주고 MON도 줌
    _ => Other,
}

// to (토큰 받는 쪽)
match (is_eoa_to_eoa, has_native_out, has_native_in, from_is_eoa) {
    (true, _, _, _) => TransferIn,    // EOA→EOA
    (false, true, _, _) => Buy,       // MON 주고 토큰 받음
    (false, _, true, _) => LpRemove,  // 토큰도 받고 MON도 받음
    (false, _, _, false) => Airdrop,  // Contract → EOA, no WMON
    _ => Other,
}
```

---

### sender_address

EOA→EOA transfer 시 sender 주소 기록 (cost basis 조회용):

| transfer_type | sender_address |
|---------------|----------------|
| `transfer_out` | NULL (자기가 sender) |
| `transfer_in` | 보낸 사람 주소 |
| 그 외 | NULL |

---

### DB Trigger 로직

```sql
-- transfer_out: 자신의 position에서 cost 계산 → native_in에 기록
IF NEW.transfer_type = 'transfer_out' THEN
    SELECT native_out, usd_out, token_in, token_out INTO sender_position
    FROM position WHERE account_id = NEW.account_id AND token_id = NEW.token_id;

    avg_cost = sender_position.native_out / sender_position.token_in;
    transfer_cost = avg_cost * NEW.token_out;

    NEW.native_in := transfer_cost;  -- cost 회수
    NEW.usd_in := transfer_cost_usd;
END IF;

-- transfer_in: sender의 position에서 cost 계산 → native_out에 기록
IF NEW.transfer_type = 'transfer_in' AND NEW.sender_address IS NOT NULL THEN
    SELECT native_out, usd_out, token_in, token_out INTO sender_position
    FROM position WHERE account_id = NEW.sender_address AND token_id = NEW.token_id;

    avg_cost = sender_position.native_out / sender_position.token_in;
    transfer_cost = avg_cost * NEW.token_in;

    NEW.native_out := transfer_cost;  -- cost 수령
    NEW.usd_out := transfer_cost_usd;
END IF;
```

---

### 예시: 부분 전송 후 매도

```
1. A가 100개 매수 (1 WMON)
2. A가 B에게 50개 전송
3. A가 50개 매도 (0.6 WMON)
4. B가 50개 매도 (0.7 WMON)
```

**A의 Position History:**

| tx | transfer_type | token_in | token_out | native_in | native_out |
|----|---------------|----------|-----------|-----------|------------|
| 매수 | buy | 100 | 0 | 0 | 1 |
| 전송 | transfer_out | 0 | 50 | 0.5 | 0 |
| 매도 | sell | 0 | 50 | 0.6 | 0 |

**A의 Position (누적):**
- token_in = 100, token_out = 100
- native_in = 1.1, native_out = 1
- **A PnL = 1.1 - 1 = +0.1 WMON**

**B의 Position History:**

| tx | transfer_type | token_in | token_out | native_in | native_out |
|----|---------------|----------|-----------|-----------|------------|
| 수령 | transfer_in | 50 | 0 | 0 | 0.5 |
| 매도 | sell | 0 | 50 | 0.7 | 0 |

**B의 Position (누적):**
- token_in = 50, token_out = 50
- native_in = 0.7, native_out = 0.5
- **B PnL = 0.7 - 0.5 = +0.2 WMON**

**검증:**
```
전체 지출: 1 WMON (A 매수)
전체 수입: 0.6 + 0.7 = 1.3 WMON (A 매도 + B 매도)
실제 순이익: 0.3 WMON

A + B PnL: 0.1 + 0.2 = 0.3 WMON ✓
```

---

### 구현 파일

| 파일 | 역할 |
|------|------|
| `types/token.rs` | TransferType enum, sender_address 필드 |
| `db/postgres/controller/position.rs` | transfer_type, sender_address INSERT |
| `event/token/stream.rs` | transfer_type 판별 로직 |
| `migrations/0017_transfer_cost_basis.sql` | 컬럼 추가, Trigger 생성 |
