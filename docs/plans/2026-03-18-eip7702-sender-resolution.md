# EIP-7702 Sender Resolution

## 배경

Observer가 buy/sell/swap 이벤트의 행위자를 `tx.origin` (`tx.inner.signer()`)으로 판단하고 있었음.
유저가 EIP-7702 (delegated EOA) 또는 smart wallet을 사용하면 릴레이어/번들러가 행위자로 잘못 기록되는 문제 발생.

## 해결 방식

```
if tx.type == 0x04 (EIP-7702):
    actor = event.sender (msg.sender, 컨트랙트가 emit한 값)
else:
    actor = tx.origin (기존 로직 유지)
```

- **EIP-7702 우선 대응**. Smart wallet (ERC-4337) 은 추후 별도 대응.
- 기존 일반 EOA 유저는 동작 변경 없음.

## 이벤트별 sender 결정 로직

| 이벤트 | event.sender 의미 | EIP-7702일 때 | 일반 tx일 때 |
|--------|-------------------|---------------|-------------|
| CurveBuy | msg.sender (유저 EOA) | event.sender | tx.origin |
| CurveSell | msg.sender (유저 EOA) | event.sender | tx.origin |
| CreateCurve | creator 파라미터 (별도 필드) | tx.origin | tx.origin |
| DEX Swap | msg.sender (Router 컨트랙트) | event.sender | tx.origin |
| DexRouterBuy | msg.sender (유저 EOA) | event.sender | tx.origin |
| DexRouterSell | msg.sender (유저 EOA) | event.sender | tx.origin |

**CreateCurve 예외**: CurveCreate 이벤트에는 `creator` 필드가 별도로 존재하며 이것이 실제 생성자 주소. `tx_sender`는 tx.origin 그대로 사용.

## 구현 구조

### 1. TxSenderInfo (src/db/cache/mod.rs)

```rust
pub struct TxSenderInfo {
    pub sender: Address,      // tx.origin (트랜잭션 서명자)
    pub is_eip7702: bool,     // tx.type == 4 여부
}
```

`CacheManager::get_tx_sender` → `Result<Option<TxSenderInfo>>`

### 2. EIP-7702 감지 (src/db/cache/mod.rs)

```rust
let is_eip7702 = tx.inner.tx_type() == 4;
```

### 3. Redis 캐싱 (src/db/redis/mod.rs)

- `tx_sender:{hash}` — 기존 sender 캐싱 (TTL 1h)
- `tx_eip7702:{hash}` — EIP-7702 플래그 캐싱 (TTL 1h, EIP-7702일 때만 저장)

### 4. 이벤트 파서 분기 패턴

```rust
let sender = match cache_manager.get_tx_sender(&transaction_hash).await {
    Ok(Some(info)) => {
        if info.is_eip7702 {
            event_sender.to_string()    // msg.sender (실제 유저)
        } else {
            info.sender.to_string()     // tx.origin (기존 동작)
        }
    }
    _ => event_sender.to_string(),      // fallback: event.sender
};
```

## 변경된 파일

| 파일 | 변경 내용 |
|------|----------|
| `src/db/cache/mod.rs` | `TxSenderInfo` 구조체, `get_tx_sender` 반환타입 변경 |
| `src/db/redis/mod.rs` | `insert_tx_is_eip7702`, `get_tx_is_eip7702` 추가 |
| `src/event/curve/stream.rs` | CurveBuy/CurveSell EIP-7702 분기, CreateCurve 타입 호환 |
| `src/event/dex/stream.rs` | Swap/RouterBuy/RouterSell EIP-7702 분기 |
| `src/event/token/stream.rs` | `TxSenderInfo` 타입 호환 수정 |

## 향후 고려사항

- **Smart Wallet (ERC-4337)**: 번들러가 tx를 제출하므로 tx.origin이 번들러. event.sender는 스마트 월렛 주소 (유저 지갑). 필요 시 동일 패턴으로 대응 가능.
- **Redis 최적화**: 현재 cache hit 시 2회 Redis 호출 (sender + is_eip7702). 트래픽 증가 시 MGET으로 1회로 최적화 가능.
- **봇/Aggregator 컨트랙트**: 제3자 컨트랙트가 bonding curve를 대신 호출하면 tx.origin도 event.sender도 실제 유저가 아님. 컨트랙트에 beneficiary 파라미터 추가가 필요.
