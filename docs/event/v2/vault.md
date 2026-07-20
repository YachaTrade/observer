# Vault (v2 구현)

**EventType**: `Vault`
**체크포인트**: `vault`
**컨트랙트**: BurnVault, LPVault, CreatorFeeVault, GiftVault, DividendVault (5개)
**ABI**: `abi/v2/BurnVault.json`, `abi/v2/LPVault.json`, `abi/v2/CreatorFeeVault.json`, `abi/v2/GiftVault.json`, `abi/v2/DividendVault.json`
**블록 의존성**: Curve를 1블록 오프셋으로 대기

주소 설정(모두 선택): `BURN_VAULT`, `LP_VAULT`, `CREATOR_FEE_VAULT`, `GIFT_VAULT`, `DIVIDEND_VAULT`

DividendVault 이벤트도 같은 Vault 스트림에서 처리한다. 배당 이벤트와 DB 처리는 [Dividend](dividend.md)에서 설명한다.

---

## 볼트 타입 판별

이벤트의 `log.address()`를 보고 어느 볼트에서 발생했는지 판별한다:
- BurnVault 주소 → `VaultType::Burn`
- LPVault 주소 → `VaultType::Lp`
- CreatorFeeVault 주소 → `VaultType::CreatorFee`
- 나머지 → `VaultType::Gift`

일부 이벤트는 시그니처가 동일해서 (Deposit, Claim) 볼트 타입으로 구분한다.

---

## VaultBurn (토큰 소각)

BurnVault 또는 GiftVault에서 토큰을 소각할 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| pair | address | pair 주소 |
| quoteIn | uint256 | 소각에 사용된 quote 금액 |
| tokenBurned | uint256 | 소각된 토큰 수량 |

### DB 저장
- `v2_vault_burns` 테이블: vault_type (BURN/GIFT), token_id, quote_in, token_burned

---

## LpInject (LP 유동성 주입)

LPVault에서 유동성을 추가할 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| pair | address | pair 주소 |
| quoteUsed | uint256 | 사용된 quote 금액 |
| tokenUsed | uint256 | 사용된 토큰 수량 |
| lpBurned | uint256 | 소각된 LP 토큰 수량 |

### DB 저장
- `v2_vault_lp_injections` 테이블

---

## CreatorDeposit (크리에이터 수수료 예치)

크리에이터 수수료가 CreatorFeeVault에 예치될 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| amount | uint256 | 예치 금액 |
| newBalance | uint256 | 예치 후 잔액 |

### DB 저장
- `v2_creator_fee_claims` 테이블: event_type=DEPOSIT, creator=NULL

**주의**: GiftVault의 Deposit도 동일 시그니처. 볼트 타입으로 구분:
- CreatorFeeVault → `CreatorDeposit`
- GiftVault → `GiftDeposit`

---

## CreatorClaim (크리에이터 수수료 청구)

크리에이터가 축적된 수수료를 인출할 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| creator | address | 크리에이터 주소 |
| amount | uint256 | 인출 금액 |

### DB 저장
- `v2_creator_fee_claims` 테이블: event_type=CLAIM, creator=주소, new_balance=NULL

**주의**: GiftVault의 Claim도 동일 시그니처. 볼트 타입으로 구분:
- CreatorFeeVault → `CreatorClaim`
- GiftVault → `GiftClaim`

---

## CreatorVaultSetup (크리에이터 볼트 바인딩)

CreatorFeeVault에 토큰-크리에이터 관계를 최초 등록할 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 (indexed) |
| creator | address | 초기 크리에이터 주소 |

### DB 저장
- `v2_creator_updates` 테이블: event_type=SETUP, old_creator=NULL, new_creator=creator

---

## CreatorUpdate (크리에이터 변경)

기등록된 토큰의 크리에이터 주소를 변경할 때 발생 (`setCreator` 호출).

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 (indexed) |
| oldCreator | address | 이전 크리에이터 (indexed) |
| newCreator | address | 새 크리에이터 (indexed) |

### DB 저장
- `v2_creator_updates` 테이블: event_type=UPDATE, old_creator, new_creator

**"토큰 X의 현재 크리에이터" 쿼리**:
```sql
SELECT new_creator FROM v2_creator_updates
WHERE token_id = $1
ORDER BY block_number DESC, log_index DESC LIMIT 1;
```

---

## GiftVaultSetup (기프트 설정)

기프트 볼트를 특정 외부 플랫폼 식별자에 바인딩할 때 발생.
이전 버전은 X(트위터) 전용(`xHandleHash`/`xHandle`)이었으나, 멀티 플랫폼을 지원하기 위해 `platform` enum + `id` 문자열로 일반화됐다.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| platform | uint8 | `GiftVault.Platform` enum 값 (컨트랙트에서 정의) |
| id | string | 플랫폼별 식별자 (예: X handle, GitHub login) |

### Platform enum 매핑

컨트랙트: `nadfun-contract-v2/src/vault/GiftVault.sol`

| on-chain 값 | Rust `GiftPlatform` | DB `platform` |
|---|---|---|
| 0 | `GitHub` | `'GITHUB'` |
| 1 | `X` | `'X'` |

`DB CHECK` 제약으로 `platform IN ('GITHUB', 'X')` 강제. 신규 플랫폼 추가 시 Rust enum + CHECK 둘 다 업데이트 필요.

### DB 저장
- `v2_gifts` 테이블: event_type=SETUP, platform, platform_id, expires_at
- `expires_at`은 SETUP 이벤트 블록에서 `GiftVault.expiryDuration()`을 조회해 계산한다. 과거 동기화가 최신 설정값을 잘못 적용하지 않는다.

---

## GiftDeposit (기프트 예치)

기프트 볼트에 토큰을 예치할 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| amount | uint256 | 예치 금액 |
| newBalance | uint256 | 예치 후 잔액 |

### DB 저장
- `v2_gifts` 테이블: event_type=DEPOSIT

---

## GiftClaim (기프트 수령)

플랫폼 식별자 인증 후 기프트를 수령할 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| receiver | address | 수령자 주소 |
| amount | uint256 | 수령 금액 |

### DB 저장
- `v2_gifts` 테이블: event_type=CLAIM, receiver

---

## GiftExpire (기프트 만료)

기프트가 만료되어 원래 예치자에게 반환될 때 발생.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 |
| amount | uint256 | 반환 금액 |

### DB 저장
- `v2_gifts` 테이블: event_type=EXPIRE

---

## GiftReceiverSet (기프트 수령자 지정)

플랫폼 검증 후 기프트의 수령자 주소를 바인딩할 때 발생 (`setReceiver` 호출).

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| token | address | 토큰 주소 (indexed) |
| receiver | address | 수령자 주소 (indexed) |

### DB 저장
- `v2_gifts` 테이블: event_type=RECEIVER_SET, receiver=주소

---

## GiftExpiryUpdate (기프트 만료 기간 변경)

GiftVault의 글로벌 만료 기간을 변경할 때 발생. 토큰 스코프가 아닌 config 이벤트.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| oldDuration | uint256 | 이전 만료 기간 (초) |
| newDuration | uint256 | 새 만료 기간 (초) |

### DB 저장
- `v2_gift_expiry_updates` 테이블 (토큰 스코프 없음)

**"현재 만료 기간" 쿼리**:
```sql
SELECT new_duration FROM v2_gift_expiry_updates
ORDER BY block_number DESC, log_index DESC LIMIT 1;
```

---

## 토큰별 볼트 집계 (VIEW)

이벤트 테이블 위에 올려진 4개의 read-only view — `v2_burn_vault_stats`, `v2_lp_vault_stats`, `v2_creator_fee_vault_stats`, `v2_gift_vault_stats`. 모두 `token_id` key, 항상 최신 값.

각 view는 누적 금액과 최신 상태를 토큰별로 노출하며, GiftVault는 Accumulating/Active/Burned 상태를 파생한다.
