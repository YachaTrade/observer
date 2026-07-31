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

1. **1,000블록 cycle 구성**: inclusive range의 끝을 `from_block + 999`로 제한해 한 cycle에서 정확히 1,000블록을 처리한다.
2. **블록 timestamp 조회**: 처리 범위의 각 블록 timestamp를 조회한다. 하나라도 조회하지 못하면 해당 cycle은 checkpoint를 전진시키지 않는다.
3. **timestamp bucket 구성**: cycle 시작 시 `now`를 한 번만 샘플링한다. `now` 기준 300초 이내 블록은 60초 wall-clock grid로, 더 오래된 블록은 600초 grid로 묶는다. bucket key 자체가 Pyth historical query timestamp다.
4. **PostgreSQL 완전성 확인**: 각 bucket의 모든 요청 블록 × 모든 quote에 exact `price` row가 있는지 canonical PostgreSQL 테이블에서 확인한다. 완전한 bucket만 건너뛰며, interior gap, quote 하나의 누락, DB 오류는 모두 미완료로 처리한다. 인메모리 cache만 존재하는 row는 persisted evidence로 인정하지 않는다.
5. **새 bucket 가격 조회**: 아직 성공하지 않은 bucket은 모든 quote feed를 한 번의 Pyth batch 요청으로 조회한다.
6. **cross-cycle 재사용**: 성공한 bucket timestamp와 quote 가격을 stream loop 밖에 유지한다. 다음 cycle이 같은 bucket에 새 블록을 추가하면 Pyth를 재호출하지 않고 그 bucket에서 이미 받은 가격으로 새 블록 row를 생성한다.
7. **블록별 이벤트 생성**: 성공한 bucket의 모든 실제 처리 블록 × quote에 dense row를 만든다. 각 row는 원래 `block_number`와 원래 `block_timestamp`를 유지한다.
8. **실패와 checkpoint**: fetch가 실패한 bucket은 이웃 bucket 가격으로 채우지 않는다. partial cycle 전체를 receiver에 보내지 않고 두 Price checkpoint를 모두 유지해 다음 cycle에서 gap을 다시 처리한다. 재사용 cache는 정확히 같은 timestamp bucket에만 적용하며, 이후 bucket의 성공 가격이 실패 이전 bucket 상태를 덮어쓰지 않는다.
9. **Receiver 전달**: provider 결과가 완전한 dense row batch만 acknowledged Price channel로 전달한다. 모든 quote의 PostgreSQL 저장이 성공하고 receiver가 확인한 뒤에만 receive/stream checkpoint를 갱신한다. DB 오류는 producer까지 전파되어 supervisor 재시작과 안전한 replay를 유도한다.

### Pyth 요청 제한

- Pyth provider는 프로세스 로컬 sliding window에서 10초당 최대 20회 요청한다.
- 최초 Pyth batch 호출과 모든 재시도는 실제 HTTP 요청 직전에 limiter를 통과한다.
- 429 응답은 최대 3회 재시도하며 `1초 → 3초 → 7초` 순서의 bounded exponential backoff를 사용한다.
- 같은 외부 IP의 다른 프로세스 요청은 Pyth 측 quota에서 합산될 수 있다.

### Receive 처리

1. **quote별 그룹핑**: 이벤트를 quote_id와 block 순서로 정렬해 하나의 multi-quote batch를 구성한다.
2. **원자적 DB 저장**: 전체 quote batch를 한 PostgreSQL transaction에서 `price` 테이블에 저장한다. 하나라도 실패하면 전부 rollback하고 acknowledgment를 실패시킨다.
3. **canonical row 재조회**: conflict row를 포함한 실제 PostgreSQL 값을 transaction 안에서 다시 읽는다.
4. **인메모리 캐시 저장**: commit 이후 canonical row만 `insert_price_batch_for_quote()`로 DashMap에 저장한다.

### 다운스트림 사용

다른 모듈에서 `cache_manager.get_quote_usd_price(quote_id, block_number)` 로 조회:
1. 인메모리 DashMap 확인
2. miss → Redis 확인
3. miss → PostgreSQL `price` 테이블 조회
