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

예: WMON, USDC 등 여러 기준 통화의 USD 가격을 동시에 수집.

### Stream 처리

1. **블록 범위 조회**: `from_block ~ to_block` 범위의 블록 타임스탬프 수집
2. **타임스탬프 정규화**: 짝수로 정규화하여 중복 API 요청 방지
3. **블록 그룹핑**: 같은 정규화 타임스탬프를 가진 블록을 그룹화
4. **캐시 확인**: 이미 캐시에 있는 가격은 skip
5. **Pyth API 호출**: 각 quote 토큰별로 해당 타임스탬프의 가격 조회
6. **이벤트 생성**: (quote_id, block_number, price) 튜플

### Receive 처리

1. **quote별 그룹핑**: 이벤트를 quote_id로 분류
2. **인메모리 캐시 저장**: `insert_price_batch_for_quote()` — DashMap에 (block_number → price) 캐시
3. **DB 저장**: `price` 테이블에 batch INSERT (quote_id, block_number, price)
4. **레거시 호환**: WMON 가격은 별도로 `insert_price_batch()`도 호출

### 다운스트림 사용

다른 모듈에서 `cache_manager.get_quote_usd_price(quote_id, block_number)` 로 조회:
1. 인메모리 DashMap 확인
2. miss → Redis 확인
3. miss → PostgreSQL `price` 테이블 조회
