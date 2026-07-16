# Multi-Quote Price 모듈 설계

## 배경

V2에서 quote token이 WMON 외에 다른 토큰(USDC 등)이 될 수 있음.
현재 price 모듈은 WMON/USD만 Pyth에서 가져오고, non-WMON quote는 USD value = 0으로 처리 중.

## 현재 상태

- `src/event/common/price/` - Pyth에서 WMON/USD 가격 fetch, DB price 테이블 저장
- `PYTH_PRICE_FEED_ID` - 단일 feed ID (WMON/USD)
- `get_quote_usd_price()` - WMON이면 price 캐시 조회, 아닌 경우 None 반환
- `NATIVE_DECIMALS` (10^18) - WMON 고정

## 구현 계획

### 1. MarketInfo.quote_id 추가
- DB market.quote_id 컬럼에서 조회 (이미 구현됨)

### 2. QUOTE_PRICES 매핑
- `QUOTE_PRICES: HashMap<quote_address, USD_price>` - quote token별 USD 가격 캐시
- WMON은 기존 price 캐시 사용
- 새 quote token은 별도 매핑

### 3. QUOTE_FEED_IDS 매핑
- `QUOTE_FEED_IDS: HashMap<quote_address, pyth_feed_id>` - quote token별 Pyth feed ID
- 초기화 시 ProtocolManager에서 등록된 quote token 목록 가져오기
- 또는 config/env에서 설정

### 4. get_quote_price(quote_id) 함수
- WMON이면 기존 native_price 반환
- 아니면 QUOTE_PRICES에서 조회
- 없으면 None

### 5. Quote token decimals
- ProtocolManager.QuoteConfig.decimals 사용
- fee_config 테이블에 decimals 추가하거나 별도 캐시
- `NATIVE_DECIMALS` 대신 `get_quote_decimals(quote_id)` 사용

### 6. 모니터 루프 확장
- 기존 price 모니터에 등록된 quote token 가격 자동 업데이트
- `fetch_price_from_pyth(client, feed_id)` 범용화

### 7. V2 pair stream 개선
- `WMON_ADDRESS` 하드코딩 제거
- market의 quote_id로 quote/base 판별

## 영향 범위

- `src/event/common/price/` - 멀티 피드 지원
- `src/event/v2/curve/receive.rs` - get_quote_price 사용
- `src/event/v2/dex/receive.rs` - get_quote_price 사용
- `src/event/v2/dex/stream.rs` - quote_id 기반 token 판별
- `src/config.rs` - QUOTE_FEED_IDS 설정

## 환경변수 (예시)

```
# Pyth feed IDs per quote token
PYTH_FEED_WMON=0x31491744e2dbf6df7fcf4ac0820d18a609b49076d45066d3568424e62f686cd1
PYTH_FEED_USDC=0xeaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a
```
