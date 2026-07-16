# getCode 기반 Sender Resolution Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** event.sender의 코드 타입(EOA/EIP-7702 delegated/contract)으로 실제 행위자를 판별. contract인 경우 tx receipt의 ERC20 Transfer에서 유저 추출.

**Architecture:** 기존 EIP-7702 tx type 체크 제거. `CacheManager`에 `resolve_actor` 메서드 추가 — getCode로 event.sender 타입 판별 후, contract이면 receipt에서 Transfer 이벤트 분석. 각 이벤트 파서는 `resolve_actor`만 호출.

**Tech Stack:** Rust, alloy 1.0.24, Redis cache

---

### Task 1: CacheManager — TxSenderInfo 제거, check_is_eoa 수정, resolve_actor 추가

**Files:**
- Modify: `src/db/cache/mod.rs:24-28` (TxSenderInfo 제거)
- Modify: `src/db/cache/mod.rs:787-823` (check_is_eoa에 EIP-7702 체크 추가)
- Modify: `src/db/cache/mod.rs:830-905` (get_tx_sender 원복 + resolve_actor 추가)
- Modify: `src/db/redis/mod.rs` (eip7702 캐싱 메서드 제거)

**Step 1: TxSenderInfo 제거, get_tx_sender를 원래 시그니처로 원복**

`src/db/cache/mod.rs`에서:

1. `TxSenderInfo` 구조체 삭제 (line 24-28)
2. `get_tx_sender` 반환타입을 `Result<Option<alloy::primitives::Address>>`로 원복
3. `is_eip7702` 관련 로직 전부 제거

```rust
/// TX sender 조회 (Redis 캐시 우선, 없으면 RPC 호출 후 캐싱)
pub async fn get_tx_sender(
    &self,
    tx_hash: &str,
) -> Result<Option<alloy::primitives::Address>> {
    // Redis 캐시 확인
    match self.redis.get_tx_sender(tx_hash).await {
        Ok(Some(sender)) => {
            debug!("TX sender found in Redis: tx_hash={}, sender={}", tx_hash, sender);
            return sender
                .parse::<alloy::primitives::Address>()
                .map(Some)
                .map_err(|e| anyhow!("Invalid cached sender address: {}", e));
        }
        Ok(None) => {
            debug!("TX sender not found in Redis: tx_hash={}", tx_hash);
        }
        Err(e) => {
            error!("Error getting TX sender from Redis: {}", e);
        }
    }

    // RPC로 tx sender 조회
    let client = RpcClient::instance()?;
    let hash = tx_hash
        .parse::<alloy::primitives::TxHash>()
        .map_err(|e| anyhow!("Invalid tx_hash: {}", e))?;

    match client.get_transaction_by_hash(hash).await {
        Ok(Some(tx)) => {
            let sender = tx.inner.signer();

            // Redis에 캐싱
            if let Err(e) = self
                .redis
                .insert_tx_sender(tx_hash, &sender.to_string())
                .await
            {
                warn!("Failed to cache TX sender in Redis: {}", e);
            }

            debug!(
                "TX sender fetched via RPC: tx_hash={}, sender={}",
                tx_hash, sender
            );
            Ok(Some(sender))
        }
        Ok(None) => {
            debug!("Transaction not found: tx_hash={}", tx_hash);
            Ok(None)
        }
        Err(e) => {
            error!("Failed to get transaction by hash: {}", e);
            Err(e)
        }
    }
}
```

**Step 2: check_is_eoa에 EIP-7702 delegated EOA 체크 추가**

`src/db/cache/mod.rs`에서 `check_is_eoa` (line 787-823)를 `check_is_eoa_or_delegated`로 변경:

```rust
/// 주소가 EOA 또는 EIP-7702 delegated EOA인지 확인
/// - code 없음 → EOA (true)
/// - code가 0xef0100... → EIP-7702 delegated EOA (true)
/// - 그 외 code → contract (false)
pub async fn check_is_eoa_or_delegated(&self, address: &str) -> Result<bool> {
    // Redis 캐시 확인
    match self.redis.check_is_eoa(address).await {
        Ok(Some(is_eoa)) => {
            debug!(
                "EOA status found in Redis: address={}, is_eoa={}",
                address, is_eoa
            );
            return Ok(is_eoa);
        }
        Ok(None) => {
            debug!("EOA status not found in Redis: address={}", address);
        }
        Err(e) => {
            error!("Error checking EOA status in Redis: {}", e);
        }
    }

    // RPC로 코드 조회
    let client = RpcClient::instance()?;
    let addr = address
        .parse::<alloy::primitives::Address>()
        .map_err(|e| anyhow!("Invalid address: {}", e))?;

    let code = client.get_code(addr).await?;
    // EOA (no code) 또는 EIP-7702 delegated EOA (0xef0100 prefix)
    let is_eoa_or_delegated = code.is_empty()
        || (code.len() == 23 && code[0] == 0xef && code[1] == 0x01 && code[2] == 0x00);

    // Redis에 캐싱
    if let Err(e) = self.redis.insert_is_eoa(address, is_eoa_or_delegated).await {
        warn!("Failed to cache EOA status in Redis: {}", e);
    }

    debug!(
        "EOA status checked via RPC: address={}, is_eoa_or_delegated={}, code_len={}",
        address, is_eoa_or_delegated, code.len()
    );
    Ok(is_eoa_or_delegated)
}
```

기존 `check_is_eoa`도 유지 (token/stream.rs에서 사용 중). 새 메서드를 추가하는 방식.

**Step 3: resolve_actor 메서드 추가**

`src/db/cache/mod.rs`에 추가:

```rust
/// 이벤트의 실제 행위자(actor)를 판별
/// 1. event_sender가 EOA/EIP-7702 delegated → event_sender 반환
/// 2. event_sender가 contract → tx receipt에서 ERC20 Transfer 분석
///    - is_buy=true: Transfer.to 중 EOA/delegated = 유저
///    - is_buy=false: Transfer.from 중 EOA/delegated = 유저
/// 3. fallback: tx.origin 반환
pub async fn resolve_actor(
    &self,
    tx_hash: &str,
    event_sender: &str,
    token: &str,
    is_buy: bool,
) -> Result<String> {
    // 1. event_sender가 EOA/delegated인지 확인
    match self.check_is_eoa_or_delegated(event_sender).await {
        Ok(true) => return Ok(event_sender.to_string()),
        Ok(false) => {
            debug!(
                "event_sender is contract, resolving from receipt: tx={}, sender={}",
                tx_hash, event_sender
            );
        }
        Err(e) => {
            warn!("Failed to check EOA status for {}: {}", event_sender, e);
            // fallback to event_sender
            return Ok(event_sender.to_string());
        }
    }

    // 2. tx receipt에서 ERC20 Transfer 분석
    let client = RpcClient::instance()?;
    let hash = tx_hash
        .parse::<alloy::primitives::TxHash>()
        .map_err(|e| anyhow!("Invalid tx_hash: {}", e))?;

    let token_addr = token
        .parse::<alloy::primitives::Address>()
        .map_err(|e| anyhow!("Invalid token address: {}", e))?;

    // ERC20 Transfer event signature: Transfer(address,address,uint256)
    let transfer_sig: alloy::primitives::B256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        .parse()
        .unwrap();

    if let Ok(Some(receipt)) = client.get_transaction_receipt(hash).await {
        for log in &receipt.inner.logs() {
            // 해당 토큰의 Transfer 이벤트만 필터
            if log.address() != token_addr {
                continue;
            }
            if log.topic0() != Some(&transfer_sig) {
                continue;
            }
            if log.topics().len() < 3 {
                continue;
            }

            // Transfer(from, to, amount) - from과 to는 indexed
            let from = format!("0x{}", &log.topics()[1][12..].iter().map(|b| format!("{:02x}", b)).collect::<String>());
            let to = format!("0x{}", &log.topics()[2][12..].iter().map(|b| format!("{:02x}", b)).collect::<String>());

            let candidate = if is_buy { &to } else { &from };

            // candidate가 EOA/delegated인지 확인
            if let Ok(true) = self.check_is_eoa_or_delegated(candidate).await {
                debug!(
                    "Resolved actor from Transfer event: tx={}, actor={}, is_buy={}",
                    tx_hash, candidate, is_buy
                );
                return Ok(candidate.to_string());
            }
        }
    }

    // 3. Fallback: tx.origin
    match self.get_tx_sender(tx_hash).await {
        Ok(Some(sender)) => {
            debug!("Fallback to tx.origin: tx={}, sender={}", tx_hash, sender);
            Ok(sender.to_string())
        }
        _ => {
            warn!("All resolution methods failed for tx={}, using event_sender", tx_hash);
            Ok(event_sender.to_string())
        }
    }
}
```

**Step 4: Redis eip7702 메서드 제거**

`src/db/redis/mod.rs`에서 삭제:
- `PREFIX_TX_EIP7702` 상수
- `insert_tx_is_eip7702` 메서드
- `get_tx_is_eip7702` 메서드

**Step 5: 빌드 확인**

Run: `cargo check 2>&1 | head -30`
Expected: 호출부 에러 (다음 Task에서 수정)

**Step 6: Commit**

```bash
git add src/db/cache/mod.rs src/db/redis/mod.rs
git commit -m "feat: replace EIP-7702 tx type check with getCode-based actor resolution"
```

---

### Task 2: Curve stream — resolve_actor 사용

**Files:**
- Modify: `src/event/curve/stream.rs:260-357`

**Step 1: CurveBuy 파서 수정**

기존 EIP-7702 분기를 `resolve_actor` 호출로 교체:

```rust
Some(&IBondingCurve::CurveBuy::SIGNATURE_HASH) => {
    let curve = log.address().to_string();
    let IBondingCurve::CurveBuy {
        sender: event_sender,
        amountIn,
        amountOut,
        token,
    } = log.log_decode()?.inner.data;
    let token = token.to_string();

    let sender = cache_manager
        .resolve_actor(&transaction_hash, &event_sender.to_string(), &token, true)
        .await
        .unwrap_or_else(|e| {
            error!("[CURVE] Failed to resolve actor for CurveBuy: {}", e);
            event_sender.to_string()
        });

    let buy = Buy {
        sender: Arc::new(sender.clone()),
        to: None,
        amount_in: Arc::new(to_big_decimal(amountIn)),
        amount_out: Arc::new(to_big_decimal(amountOut)),
        token: Arc::new(token),
        market: Arc::new(curve.to_string()),
        market_type: MarketType::CURVE,
        transaction_hash: Arc::new(transaction_hash),
        block_number,
        block_timestamp,
        log_index,
        transaction_index,
        tx_sender: Arc::new(sender),
    };

    Ok(CurveEvent::Buy(buy))
}
```

**Step 2: CurveSell 파서 동일 패턴 (is_buy=false)**

```rust
let sender = cache_manager
    .resolve_actor(&transaction_hash, &event_sender.to_string(), &token, false)
    .await
    .unwrap_or_else(|e| {
        error!("[CURVE] Failed to resolve actor for CurveSell: {}", e);
        event_sender.to_string()
    });
```

**Step 3: CreateCurve — get_tx_sender 원복**

```rust
let tx_sender = match cache_manager.get_tx_sender(&transaction_hash).await {
    Ok(Some(sender)) => sender.to_string(),
    _ => creator.to_string(),
};
```

**Step 4: 빌드 확인**

Run: `cargo check 2>&1 | head -30`

**Step 5: Commit**

```bash
git add src/event/curve/stream.rs
git commit -m "feat: use resolve_actor for curve events"
```

---

### Task 3: DEX stream — resolve_actor 사용

**Files:**
- Modify: `src/event/dex/stream.rs:210-250, 570-650`

**Step 1: DEX Swap 파서 수정**

```rust
let ICapricornCLPool::Swap {
    sender: event_sender,
    recipient,
    amount0,
    amount1,
    sqrtPriceX96,
    liquidity,
    tick,
    ..
} = log.log_decode()?.inner.data;

// Swap 방향 먼저 판단 (resolve_actor에 is_buy 필요)
let token0_is_mon = token0 == *WNATIVE_ADDRESS;
let is_buy = match (token0_is_mon, amount0.is_positive()) {
    (true, true) => true,   // native in, ERC20 out => Buy
    (true, false) => false,  // native out, ERC20 in => Sell
    (false, true) => false,  // ERC20 in, native out => Sell
    (false, false) => true,  // ERC20 out, native in => Buy
};

let swap_token = if token0_is_mon { &token1 } else { &token0 };

let sender = cache_manager
    .resolve_actor(&transaction_hash, &event_sender.to_string(), swap_token, is_buy)
    .await
    .unwrap_or_else(|e| {
        error!("[DEX] Failed to resolve actor for Swap: {}", e);
        event_sender.to_string()
    });
```

주의: `token0_is_mon`과 `is_buy` 판단 로직을 sender resolution 전에 배치해야 함. 기존 코드에서 이 로직은 sender 이후에 있으므로 순서 조정 필요.

**Step 2: DexRouterBuy 파서 수정**

```rust
let IDexRouter::DexRouterBuy {
    sender: event_sender,
    token,
    amountIn,
    amountOut,
} = log.log_decode()?.inner.data;

let sender = cache_manager
    .resolve_actor(&transaction_hash, &event_sender.to_string(), &token.to_string(), true)
    .await
    .unwrap_or_else(|e| {
        error!("[DEX] Failed to resolve actor for DexRouterBuy: {}", e);
        event_sender.to_string()
    });

let buy = DexRouterBuy {
    token: Arc::new(token.to_string()),
    sender: Arc::new(sender.clone()),
    amount_in: Arc::new(to_big_decimal(amountIn)),
    amount_out: Arc::new(to_big_decimal(amountOut)),
    transaction_hash: Arc::new(transaction_hash),
    block_timestamp,
    block_number,
    log_index,
    transaction_index,
    tx_sender: Arc::new(sender),
};
```

**Step 3: DexRouterSell 동일 패턴 (is_buy=false)**

**Step 4: token/stream.rs — get_tx_sender 원복**

`src/event/token/stream.rs:764`에서 `.sender` 제거 (TxSenderInfo 없으므로):

```rust
Ok(Some(sender)) => Some((hash_string, sender)),
```

**Step 5: 빌드 확인**

Run: `cargo check`

**Step 6: Commit**

```bash
git add src/event/dex/stream.rs src/event/token/stream.rs
git commit -m "feat: use resolve_actor for dex events"
```

---

### Task 4: 빌드 및 테스트 검증

**Step 1: 전체 빌드**

Run: `cargo build`

**Step 2: 테스트**

Run: `cargo test --lib --bins`

**Step 3: 문서 업데이트**

`docs/plans/2026-03-18-eip7702-sender-resolution.md` 업데이트 — 최종 구현 방식 반영.
