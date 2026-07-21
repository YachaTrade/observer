# Token (토큰 전송/잔고 추적)

**EventType**: `Token`
**소스**: 모든 ERC20 컨트랙트의 Transfer / Burn 이벤트
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
2. **시스템 주소 필터링**: 본딩커브, DEX 라우터, LP매니저 등 내부 시스템 주소 간 전송은 skip
3. **전송 타입 분류**:
   - `from`이 시스템 주소, `to`가 일반 주소 → **buy** (유저가 토큰 받음)
   - `from`이 일반 주소, `to`가 시스템 주소 → **sell** (유저가 토큰 보냄)
   - 둘 다 일반 주소 → **transfer_out** (from) + **transfer_in** (to)
4. **quote 금액 조회**: 캐시에서 해당 토큰의 quote 정보로 매칭되는 swap 데이터 참조

### Receive 처리

- `position_history` 테이블: 각 전송을 포지션 변경 기록으로 저장
  - transfer_type: buy / sell / transfer_in / transfer_out
  - quote_in/out: quote 토큰 기준 금액
  - usd_in/out: USD 환산 금액
  - token_in/out: 토큰 수량
- `position` 테이블: DB 트리거(`update_position_on_history`)가 자동으로 누적 집계
  - 계정별 토큰별 총 quote_in/out, usd_in/out, token_in/out 누적

### transfer_out 시 비용 기반 계산

transfer_out 이벤트에서는 발신자의 평균 매입 단가를 계산하여 `quote_in`에 기록:
```
avg_cost = position.quote_out / position.token_in
transfer_cost = avg_cost * transfer_amount
```

### transfer_in 시 비용 전파

transfer_in 이벤트에서는 발신자(`sender_address`)의 평균 매입 단가를 조회하여 `quote_out`에 기록. 이렇게 비용 기반이 전송을 통해 전파된다.

---

## Burn

### 이벤트 필드
Transfer와 동일 (to=0x0 주소)

### 처리
- token burn으로 기록 (별도 burn 테이블이 아닌 position_history의 일부로 처리)
