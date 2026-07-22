# Token (토큰 전송/잔고 추적)

**EventType**: `Token`
**소스**: 인덱싱 대상 ERC-20 Transfer 이벤트와 설정된 quote 토큰 흐름
**블록 의존성**: Curve 대기

---

## 동작 원리

Token 모듈은 `token` 테이블에 등록된 토큰들의 ERC20 Transfer와 Burn 이벤트를 추적하여 포지션(매수/매도/전송) 히스토리를 기록한다.

---

## Transfer

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| from | address | 발신자 |
| to | address | 수신자 |
| value | uint256 | 전송량 |

### Stream 처리

1. **토큰 멤버십 체크**: `token_exists`로 `token` 테이블에 존재하는 토큰만 처리
2. **quote 흐름 수집**: 같은 트랜잭션의 설정된 quote 토큰 입출금을 트랜잭션 발신자 기준으로 집계
3. **EOA 판별**: Transfer 양쪽 주소가 EOA인지 확인
4. **전송 타입 분류**: token/quote 방향을 조합해 buy, sell, lp_add, lp_remove, transfer_in/out, airdrop, other로 분류
5. **USD 환산**: 이벤트 블록의 quote 가격으로 quote 입출금을 환산

### Receive 처리

- `position_history` 테이블: 각 전송을 포지션 변경 기록으로 저장
  - transfer_type: buy / sell / transfer_in / transfer_out
  - quote_in/out: quote 토큰 기준 금액
  - usd_in/out: USD 환산 금액
  - token_in/out: 토큰 수량
- `position` 테이블: DB 트리거(`update_position_on_history`)가 자동으로 누적 집계
  - 계정별 토큰별 총 quote_in/out, usd_in/out, token_in/out 누적
- EOA 간 직접 전송은 quote/USD 흐름을 0으로 기록하고, `transfer_in` 행에 발신자 주소를 남긴다.
- pair-share 잔고, 별도 LP 포지션 히스토리, LP 비용 기반은 인덱싱하지 않는다.

---

## Burn

### 이벤트 필드
Transfer와 동일 (to=0x0 주소)

### 처리
- token burn으로 기록 (별도 burn 테이블이 아닌 position_history의 일부로 처리)
