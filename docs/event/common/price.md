# Price (가격 수집)

**EventType**: `Price`
**소스**: Pyth Network Oracle
**블록 의존성**: 없음. Price는 독립적으로 실행되고 Curve 등 다른 스트림이
Price 체크포인트를 기다린다.

Price 모듈은 Pyth 가격을 프로세스 시간 기준으로 수집하고, 완전한 최신
snapshot을 GIWA canonical block별 `price` row로 확장한다. 샘플링 주기는 블록
수에 의존하지 않는다.

## Runtime 계약

- sampler의 첫 tick은 프로세스 시작 직후 실행된다.
- 각 source/provider 시도가 끝나면 interval deadline을 재설정한다. 다음
  source/provider 시도는 완료 시점에서 30초가 지나기 전에 시작하지 않는다.
- `MissedTickBehavior::Skip`도 유지하므로 느리거나 실패한 시도 뒤에 overdue
  tick을 즉시 catch-up하지 않는다.
- source block/timestamp 조회가 성공한 tick은 설정된 모든 Pyth feed를 하나의
  batch HTTP 요청으로 조회한다. Provider 내부 재시도는 없으며, 한 tick에서
  HTTP 전송은 최대 한 번이다.
- 모든 configured quote 가격이 포함된 응답만 active snapshot을 교체한다.
- 첫 complete snapshot 전에는 Price event를 보내지 않고 Price stream
  체크포인트도 전진시키지 않는다.
- 첫 snapshot 이후 source 또는 provider 요청이 실패하면 last-good snapshot을
  기한 없이 유지한다. 실패 로그에는 active snapshot age가 포함된다.

## Quote 설정과 snapshot

시작 시 `quote_token` DB 테이블에서 모든 quote 설정을 로드한다.

- `quote_id`: quote token 주소
- `pyth_feed_id`: Pyth feed ID
- `decimals`: 소수점 정보

Sampler는 현재 latest canonical block에서 5블록 safety offset을 적용한
`source_block`과 그 canonical timestamp를 조회한다. 그 timestamp로 모든
configured feed를 한 번에 요청하고, feed ID를 정규화해 quote 주소별 가격
map을 만든다.

Snapshot에는 다음 정보가 함께 저장된다.

- quote 주소별 가격
- `source_block`
- `source_timestamp`
- 프로세스가 snapshot을 완성한 `sampled_at`

응답에서 quote 하나라도 빠지면 partial snapshot 전체를 폐기한다. 성공한
tick만 Tokio `watch` channel을 통해 snapshot 전체를 atomic하게 교체한다.

## Canonical block 확장

Price stream은 처리할 block range를 얻을 때 active snapshot 하나를 캡처한다.
그 range를 만드는 동안 새 snapshot이 publish되어도 현재 range는 처음 캡처한
가격을 끝까지 사용하며, 다음 range부터 새 snapshot을 사용한다.

각 canonical block은 자체 `block_number`와 실제 `block_timestamp`를 유지한다.
가격 값만 같은 captured snapshot에서 복사된다. 따라서 한 range의 row들은
동일한 snapshot 가격을 공유하지만 block identity와 timestamp는 각각 다르다.

이미 `(quote_id, block_number)` exact cache hit가 있는 row는 다시 만들지 않는다.
나머지 row는 configured quote 순서로 생성해 기존 Price event channel로 한
batch를 전송한다. 전송이 성공한 뒤에만 기존 Price stream 체크포인트를
전진시킨다.

## Backfill 의미

Backfill도 처리 시점의 최신 process-time snapshot을 사용한다. 과거 block의
timestamp는 row에 보존하지만, 그 timestamp마다 historical Pyth 가격을 다시
조회하지 않는다. 이 정책은 historical reconstruction보다 bounded Pyth traffic과
Price 및 downstream stream의 forward progress를 우선한다.

## Receive와 저장

Receiver는 event를 `quote_id`별로 묶은 뒤 기존 저장 경로를 유지한다.

1. `insert_price_batch_for_quote()`로 `(block_number, price)`를 인메모리 캐시에
   저장한다.
2. 각 event의 canonical `block_timestamp`를 `created_at`으로 사용해 `price`
   테이블에 `(quote_id, block_number, price, created_at)` batch를 저장한다.
3. 처리 완료 후 Price receive 체크포인트를 갱신한다.

다운스트림은
`cache_manager.get_quote_usd_price(quote_id, block_number)`로 가격을 조회한다.
조회 순서는 인메모리 exact block, 인메모리 latest-before, 인메모리 latest,
PostgreSQL `price` 테이블의 latest-at-or-before 및 absolute latest fallback
순이다. 이 가격 조회 경로는 Redis를 사용하지 않는다.

## 실패와 관측

Sampler 성공 로그는 source block/timestamp, quote 수, 처리 시간을 기록한다.
실패 로그는 고정된 failure kind와 last-good snapshot age만 기록하며 provider
응답 body, URL, header, credential을 기록하지 않는다.

Snapshot에는 최대 허용 age가 없다. 오래된 snapshot으로 처리가 계속되는 동안
operator는 age 로그와 Price cycle의 `snapshot_age_secs`를 사용해 freshness를
감시해야 한다.

## Flashblocks 범위

이 흐름은 GIWA의 canonical block stream만 인덱싱한다. Flashblock
preconfirmation과 pending state는 source block 선택, snapshot 생성, block별
Price row 확장에 포함되지 않는다.

## 관련 설계 문서

- [Pyth 30-Second Snapshot Sampler Design](../../superpowers/specs/2026-07-23-pyth-30s-snapshot-sampler-design.md)
- [Pyth 30-Second Snapshot Sampler Implementation Plan](../../superpowers/plans/2026-07-23-pyth-30s-snapshot-sampler.md)
