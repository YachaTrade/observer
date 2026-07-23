# Price (가격 수집)

**EventType**: `Price`
**소스**: Pyth Network Oracle
**블록 의존성**: 없음 (독립, 다른 모듈이 Price에 의존)

---

## 동작 원리

Price 모듈은 온체인 이벤트가 아닌 **Pyth Oracle API**에서 가격 데이터를 가져온다.

### 다중 Quote 토큰 지원

시작 시 `quote_token` DB 테이블에서 모든 quote 토큰 설정을 로드한다:
- quote_id (주소)
- pyth_feed_id (Pyth 피드 ID)
- decimals (소수점)

예: 설정된 네이티브 quote 토큰과 USDC 등 여러 기준 통화의 USD 가격을 동시에 수집.

### Stream 처리

1. **100블록 canonical bucket 구성**: 각 블록을 `block - (block % 100)` 경계로 묶는다.
2. **canonical 캐시 확인**: quote별로 bucket 경계 블록의 exact price cache를 조회한다.
3. **캐시 미스 복구**: 하나라도 없으면 bucket 경계 블록의 타임스탬프를 range 또는 RPC에서 구하고, 모든 quote feed를 Pyth에서 한 번에 batch 조회한다.
4. **canonical 캐시 저장**: 새로 조회한 가격은 bucket 경계 블록으로 인메모리 캐시에 즉시 저장한다. 따라서 중간 블록부터 시작해도 같은 bucket에서 Pyth를 다시 호출하지 않는다.
5. **블록별 이벤트 생성**: bucket의 모든 실제 처리 블록에 canonical 가격을 복제한다. 각 이벤트는 원래 `block_number`와 `block_timestamp`를 유지한다.
6. **Receiver 전달**: 기존 range batch로 전달하며 receiver가 실제 블록별 cache/DB row를 저장하고 Price checkpoint를 갱신한다.

캐시에 없는 경계 블록이 현재 처리 range 밖에 있더라도 DB에 가상 경계 row를 만들지 않는다. DB와 Price checkpoint에는 실제 처리한 블록만 반영된다.

### Pyth 요청 제한

- Pyth provider는 프로세스 로컬 sliding window에서 10초당 최대 20회 요청한다.
- 최초 Pyth batch 호출과 모든 재시도는 실제 HTTP 요청 직전에 limiter를 통과한다.
- 429 응답은 최대 3회 재시도하며 `1초 → 3초 → 7초` 순서의 bounded exponential backoff를 사용한다.
- 같은 외부 IP의 다른 프로세스 요청은 Pyth 측 quota에서 합산될 수 있다.

### Receive 처리

1. **quote별 그룹핑**: 이벤트를 quote_id로 분류
2. **인메모리 캐시 저장**: `insert_price_batch_for_quote()` — DashMap에 (block_number → price) 캐시
3. **DB 저장**: `price` 테이블에 batch INSERT (quote_id, block_number, price)

### 다운스트림 사용

다른 모듈에서 `cache_manager.get_quote_usd_price(quote_id, block_number)` 로 조회:
1. 인메모리 DashMap 확인
2. miss → Redis 확인
3. miss → PostgreSQL `price` 테이블 조회
