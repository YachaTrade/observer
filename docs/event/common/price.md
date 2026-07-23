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

1. **25블록 bucket 구성**: 동기화 범위를 25블록 단위로 나눈다.
2. **대표 타임스탬프 조회**: 각 bucket 시작 경계 블록의 타임스탬프를 역사 가격 시점으로 사용한다.
3. **캐시 확인**: bucket에서 처음 처리할 블록에 모든 quote 가격이 있으면 bucket 전체 요청을 생략하고, 하나라도 없으면 batch 조회한다.
4. **Pyth batch 호출**: 필요한 모든 quote token feed를 같은 타임스탬프의 한 HTTP 요청으로 조회한다.
5. **이벤트 생성**: 반환된 가격을 bucket 내 대상 블록의 `(quote_id, block_number, price)` 이벤트로 만든다.

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
