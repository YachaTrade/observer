# Dividend (v2 배당 볼트)

**스트림**: `Vault` (DividendVault는 싱글톤 vault이므로 Burn/LP/CreatorFee/Gift와 함께 Vault 멀티플렉서 스트림에서 topic0+주소로 디코딩된다. 전용 Dividend EventType은 없고 v2 페이로드는 `V2VaultEvent::Dividend(V2DividendEvent)`로 래핑된다.)
**체크포인트**: `vault`
**컨트랙트**: DividendVault (싱글톤 UUPS — 토큰별 인스턴스 없음)
**ABI**: `abi/v2/DividendVault.json`
**블록 의존성**: Curve를 1블록 오프셋으로 대기 (Vault 스트림과 공유)

주소 설정: `DIVIDEND_VAULT` (선택. 미설정 시 dividend 로그만 건너뛰고 나머지 vault는 계속 처리)

DB 처리(`vault/receive.rs`): 기존 vault 배치 insert 뒤에 dividend 2-phase 블록 실행 — ① setups+merkle_roots → ② deposits/conversions/claims. merkle_root insert 실패 시 claims만 skip(CRITICAL 로그), deposits/conversions는 진행(missing-data > wrong-data).

---

## 자금 흐름 개요

CreatorFeeProcessor → `afterDeposit(sourceToken, quoteToken, amount)` → ratio(BPS) 분할.
**`afterDeposit` 1회당 `Deposit` 이벤트도 1회** 발생하며, 모든 슬라이스를 병렬 배열로 담는다:

- `dividendToken == quoteToken` 슬라이스 (`pending=false`): `dividendBalance += slice` — 즉시 적립.
- 그 외 슬라이스 (`pending=true`): `pendingSwap += slice` — 변환 대기. 이후 `Converted`가 소비.

각 `slices[i]`는 항상 source 토큰의 quote 단위로 표기된다 (pending 여부 무관). 즉시/대기 슬라이스
모두 동일 `Deposit` 이벤트의 `entry_index` 행으로 분해되어 인덱싱된다.

봇 변환(`executeConversion`/`executeBondingBuy`): `pendingSwap -= consumed`,
`dividendBalance += received` → **`Converted` 이벤트**.

운영자 `setMerkleRoot`: 전역 root 덮어쓰기 → **`SetMerkleRoot` 이벤트** (분배 기간 마커).

홀더 `claim`: merkle 검증(현재 root 기준), 자격 미달/중복/잔고 부족 항목은 skip(`paidAmounts[i]=0`)
→ **`Claim` 이벤트**. **on-chain `dividendBalance`는 claim으로 차감되지 않는다** — 누적 지표.

인덱싱 대상은 코어 5개 이벤트. 운영자 설정 3종(SetWmon/SetAdapters/SetAllowedDividendToken)은 제외.

---

## DividendSetup (배당 설정)

토큰당 1회 불변 — 재설정 시 `AlreadyConfigured` revert. 따라서 `v2_dividend_setups`가
config 조회 테이블을 겸한다.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| sourceToken | address (indexed) | 수수료 발생 source 토큰 |
| dividendTokens | address[] | 배당 토큰 목록 (병렬 배열) |
| ratios | uint16[] | 분배 비율 BPS (병렬 배열, 0 < r ≤ 10000) |
| minBalance | uint256 | claim 자격 최소 보유량 |

병렬 배열은 `entry_index`를 붙여 행으로 분해한다. 배열 길이 불일치 시 error 로그 + 해당 로그 전체 skip (fail loud).

### DB 저장
- `v2_dividend_setups` 테이블, PK `(transaction_hash, tx_index, log_index, entry_index)`
- 트리거: `v2_dividend_vault_stats`에 `(source_token, dividend_token)` 행 시드 (설정만 되고 자금 흐름이 없는 쌍도 zero-row로 노출)

---

## Deposit (배당 슬라이스 분배 — 배열 이벤트)

`afterDeposit` 1회당 1회 발생. 모든 ratio 슬라이스를 병렬 배열로 담는다. 더 이상
스칼라 이벤트가 아니며 **on-chain `dividendBalance` 스냅샷 필드는 제거되었다.**

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| sourceToken | address (indexed) | source 토큰 |
| dividendTokens | address[] | 배당 토큰 목록 (병렬 배열) |
| slices | uint256[] | 슬라이스 값 (병렬 배열, quote 단위) |
| pending | bool[] | `true` = swap 대기(`dividendToken != quote`), `false` = 즉시 적립 |

병렬 배열은 `entry_index`로 행 분해한다. `slices[i] == 0`인 항목은 **skip**하되 원본 배열 위치를
`entry_index`로 보존한다 (`enumerate` 후 filter). 길이 불일치(3-way) 시 error 로그 + 로그 전체 skip (fail loud).

### DB 저장
- `v2_dividend_deposits` 테이블: amount(슬라이스 값), `pending`, `entry_index`, quote_id, usd_value
  — PK `(transaction_hash, tx_index, log_index, entry_index)`
- 트리거 (`NEW.pending` 분기):
  - `pending=false` → `total_deposited += amount`, `total_deposited_usd += usd_value`, `dividend_balance += amount`
  - `pending=true` → `total_pending_deposited += amount`, `total_pending_deposited_usd += usd_value` (**`dividend_balance` 미변경**)

---

## Converted (봇 변환)

pendingSwap에 쌓인 quote를 배당 토큰으로 변환할 때 발생. 결과 잔액은 이벤트에 포함되지 않는다.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| sourceTokens | address[] | source 토큰 목록 (병렬 배열) |
| dividendTokens | address[] | 배당 토큰 목록 (병렬 배열) |
| consumedQuote | uint256[] | pendingSwap에서 소비된 quote (병렬 배열) |
| received | uint256[] | 적립된 배당 토큰 수량 (병렬 배열) |

병렬 배열 → `entry_index` 행 분해. 길이 불일치 시 로그 전체 skip.

### DB 저장
- `v2_dividend_conversions` 테이블: consumed_quote, received, quote_id, usd_value(= consumed_quote 기준)
- 트리거: stats `total_consumed_quote +=`, `total_converted_received +=`, `dividend_balance += received`

`received`는 USD 미산정 — 임의 ERC20이라 가짜 수치를 만들지 않는다.

---

## SetMerkleRoot (분배 기간 마커)

운영자가 전역 merkle root를 덮어쓸 때 발생. 분배 기간의 경계 역할.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| merkleRoot | bytes32 (indexed) | 새 전역 root |

### DB 저장
- `v2_dividend_merkle_roots` 테이블 + 좌표 `(block_number, tx_index, log_index)` DESC 인덱스 (최신 root 조회용)

---

## Claim (홀더 수령)

홀더가 merkle proof로 배당을 수령할 때 발생. on-chain 중복 방지 키 =
`keccak(source, holder, dividend, root)` — root(기간) 단위.

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| holder | address (indexed) | 수령자 |
| sourceTokens | address[] | source 토큰 목록 (병렬 배열) |
| dividendTokens | address[] | 배당 토큰 목록 (병렬 배열) |
| amounts | uint256[] | 지급 금액 (병렬 배열, skip 항목은 0) |

### zero 항목 미저장 (paid 전용 히스토리)
`amounts[i] = 0`인 skip 항목(자격 미달/중복 claim/잔고 부족)은 **insert하지 않는다** —
테이블에 `CHECK (amount > 0)`. `entry_index`는 원본 배열 위치를 보존하므로 on-chain
레이아웃 재구성이 가능하다. 시도(zero) 이력 추적은 deferred.

### merkle_root 귀속
claims 행은 `merkle_root` 컬럼을 가진다. insert 시점에 해당 claim 좌표
`(block_number, tx_index, log_index)` 이전(이하)의 **최신 SetMerkleRoot**를 per-row
서브쿼리로 조회해 귀속한다 — on-chain dedup 키와 동일한 기간 단위.

```sql
SELECT m.merkle_root FROM v2_dividend_merkle_roots m
WHERE (m.block_number, m.tx_index, m.log_index) <= (claim 좌표)
ORDER BY m.block_number DESC, m.tx_index DESC, m.log_index DESC LIMIT 1;
```

### DB 저장
- `v2_dividend_claims` 테이블: holder, amount(> 0), merkle_root, entry_index, usd_value
- 트리거: stats `total_claimed +=`, `claim_count += 1` (claim_count = **paid entry 수** — 트랜잭션 수도, 유니크 홀더 수도 아님)
- **claim은 stats의 `dividend_balance`를 차감하지 않는다** (체인과 동일하게 누적 유지)
- 인덱스: holder, (source_token, dividend_token), merkle_root

---

## 테이블 구조

5 history + 1 stats. 컬럼 상세는 `migrations/dividend.sql` 참조 (단일 idempotent 파일 — fresh install과 운영 업그레이드 겸용, 단일 트랜잭션 백필 포함).

| 테이블 | 소스 이벤트 | 비고 |
|--------|------------|------|
| `v2_dividend_setups` | DividendSetup (분해) | 불변 config 겸용, stats 행 시드 |
| `v2_dividend_deposits` | Deposit (분해) | 슬라이스당 행, `pending` 플래그 + `entry_index` |
| `v2_dividend_conversions` | Converted (분해) | usd_value는 consumed_quote 기준 |
| `v2_dividend_merkle_roots` | SetMerkleRoot | 좌표 DESC 인덱스 |
| `v2_dividend_claims` | Claim (분해, paid 전용) | merkle_root 귀속 |
| `v2_dividend_vault_stats` | (트리거 파생) | PK (source_token, dividend_token) |

stats의 모든 수량 컬럼은 해당 행 dividend_token 단위.
`dividend_balance` = `total_deposited + total_converted_received` 누적 산술 미러 — claim 차감 없음
(여기서 `total_deposited`는 즉시 슬라이스 = `pending=false`만 가산).

### pendingSwap 추적 (신규)
swap 대기 슬라이스(`pending=true`)는 별도 컬럼으로 추적한다:

- `total_pending_deposited` / `total_pending_deposited_usd` — 대기 슬라이스 누적 (quote 단위).
- `pending_swap_balance` — **GENERATED 컬럼** = `total_pending_deposited − total_consumed_quote`
  (변환 대기 중인 잔여 quote). Postgres가 계산하므로 INSERT 컬럼 목록에 넣지 않는다.

즉 `total_pending_deposited`(Deposit pending)와 `total_consumed_quote`(Converted)의 차로
`pending_swap_balance = pending_deposited − consumed_quote`가 도출된다.

---

## 갱신 패턴 (replay-idempotent)

history INSERT `ON CONFLICT DO NOTHING` → **AFTER INSERT 트리거**가 stats를 upsert.
insert가 실제로 성공한 행에 대해서만 트리거가 발화하고 같은 트랜잭션에서 가산되므로,
**동일 로그를 재처리해도 stats 이중 가산이 없다 (replay-idempotent)** — 기존 vault 관례와 동일.

단, **reorg rollback은 아니다** — orphan 행 제거/차감 메커니즘 없음 (observer 전체 공통 속성).

---

## receive insert 순서 (2단계)

claims의 merkle_root 귀속이 같은 배치 내 root의 선행 insert에 의존하므로, 배치 전체를
`tokio::join!`으로 병렬 insert하지 않고 2단계로 나눈다:

1. **Phase 1**: `setups` + `merkle_roots` (config + 기간 마커)
2. **Phase 2**: `deposits` / `conversions` / `claims` (병렬)

**merkle_roots insert가 실패하면 해당 배치의 claims insert를 통째로 skip**하고 CRITICAL
로그(블록 범위 포함)를 남긴다 — NULL/stale root로 저장하느니 데이터 누락이 낫다
(missing-data over wrong-data). 해당 범위는 root 문제 해결 후 재인덱싱해야 한다.
deposits/conversions는 root와 무관하므로 정상 insert.

---

## USD 산정 규칙

| 대상 | 경로 | 가격 miss 시 |
|------|------|-------------|
| Deposit.slices[i] | quote 경로 (`enrich_usd`: source의 quote → WNATIVE → Pyth), `quote_id` 기록 — 모든 슬라이스(pending 무관) | 0 + WARN |
| Converted.consumedQuote | 동일 quote 경로 (`enrich_usd`), `quote_id` 기록 | 0 + WARN |
| Claim.amount | dividend 토큰을 quote-토큰 USD 가격 캐시로 조회 | 0 + WARN |
| Converted.received | **미산정** (임의 ERC20 — 가짜 수치 방지) | — |

Claim 경로는 dividend 토큰이 quote 토큰(WNATIVE 등)인 경우만 커버한다. 비-quote 배당
토큰은 0 + WARN으로 커버리지 갭을 가시화한다 (token-graph 가격 산정은 deferred).

---

## 의도적 제외 (deferred)

- **Claim 시도(zero) 이력** — paid 전용으로 시작. 필요 시 별도 테이블.
- **운영자 설정 이벤트 3종** — SetWmon / SetAdapters / SetAllowedDividendToken.
- **비-quote 배당 토큰 token-graph 가격 산정** — 현재는 0 + WARN.
