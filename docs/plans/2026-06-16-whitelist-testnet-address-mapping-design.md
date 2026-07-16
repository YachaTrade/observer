# whitelist_token 테스트넷↔mainnet 주소 매핑 (price_usd DefiLlama)

> 상태: DRAFT (후속 작업 — `feat/price-usd-block-bucket` 머지 후 별도 브랜치/PR). author=Opus.

## 문제
DefiLlama는 **monad mainnet 주소만** 가격을 안다. 테스트넷(chain 10143)의 풀/잔고는 **mock 토큰 주소**를 참조하므로, `price_usd`는 그 테스트넷 주소로 저장돼야 다운스트림 `balance_usd` 조인이 맞는다. 즉 **DefiLlama 요청 주소(mainnet) ≠ price_usd 저장 주소(테스트넷)**.

## 결정 (2026-06-16)
**`whitelist_token`에 컬럼 추가** 방식 (사용자 확정):
- `token_id` = 이 배포의 **온체인 주소** (테스트넷=mock, 메인넷=mainnet) → price_usd 저장·조인 키.
- 신규 컬럼 **`price_source_id`** = DefiLlama 쿼리용 **mainnet 주소**. 메인넷 배포에선 NULL → `token_id` fallback.
- 규칙: DefiLlama 쿼리 = `COALESCE(price_source_id, token_id)`, price_usd 저장 = `token_id`.
- env-불문, DRY. EIP-55 checksum 양쪽 유지(소문자화 금지).

## price_usd 모듈 변경
1. `enabled_whitelist_tokens()` → `(storage_id /*token_id, 테스트넷*/, query_id /*COALESCE, mainnet*/)` 쌍 반환 (`SELECT token_id, COALESCE(NULLIF(price_source_id,''), token_id) AS query_id FROM whitelist_token WHERE enabled`).
2. `coin_ref(query_id)`로 DefiLlama 호출 (요청은 mainnet 주소로).
3. 응답을 `coin_ref(query_id) → storage_id`로 되매핑 (case-insensitive, 기존 `find_fresh_price` 패턴).
4. `build_dense_rows(storage_id, …)` — price_usd는 테스트넷 주소로 저장.
- 메인넷에선 price_source_id NULL → query_id=token_id → 현행과 동일(회귀 0).

## 마이그레이션
- two-track: base(`whitelist_token` 컬럼 추가 — 0032 류 or 신규 0035) + idempotent `v2_upgrade_*.sql`(ADD COLUMN IF NOT EXISTS).
- migrations 서브모듈 PR → 머지 → observer gitlink bump.

## 주소 (시드 데이터)

### 테스트넷 mock (저장 = token_id) — source: `~/project/nads-pump/pancake-v3-testnet/deployments/`
| 토큰 | 테스트넷 주소 | dec |
|---|---|---|
| USDC | 0xe7046ecd03426cC22Cd298E4aBccB5086977E01B | 6 |
| USDT0 | 0xcd6f528fd2E6119C1ec79A7e56ae579A8a554492 | 6 |
| AUSD | 0xEB937d6A4faa621bC4Ccf1A13c641e2f9272BE62 | 6 |
| LV | 0x21E4d841e4a7E883b8921B3540dF54A5478fe1E4 | 18 |
| XAUt0 | 0x5BAA387e3AA23a489ab3b86dFc8A36336a655077 | 6 |

### mainnet (DefiLlama 쿼리 = price_source_id) — 현 whitelist_token
| 토큰 | mainnet 주소 | dec |
|---|---|---|
| USDC | 0x754704Bc059F8C67012fEd69BC8A327a5aafb603 | 6 |
| USDT | 0xe7cd86e13AC4309349F30B3435a9d337750fC82D | 6 |
| AUSD | 0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a | 6 |
| LV | 0x1001fF13bf368Aa4fa85F21043648079F00E1001 | 18 |
| XAUt0 | 0x01bFF41798a0BcF287b996046Ca68b395DbC1071 | 6 |
| WMON | 0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A | 18 |
| LVMON | 0x91b81bfbe3A747230F0529Aa28d8b2Bc898E6D56 | 18 |
| WETH | 0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242 | 18 |
| MON | 0x0000000000000000000000000000000000000000 | 18 |

### 테스트넷→mainnet 페어 (price_source_id 시드)
- USDC 0xe7046ecd… → 0x754704Bc…
- USDT0 0xcd6f528f… → USDT 0xe7cd86e1…
- AUSD 0xEB937d6A… → 0x00000000eFE3…
- LV 0x21E4d841… → 0x1001fF13…
- XAUt0 0x5BAA387e… → 0x01bFF417…

## 미해결
- **테스트넷 WMON 주소**: 풀(WMON-USDC 등)이 참조하는 테스트넷 WMON 주소 확인 필요(source-of-truth repo). MON(0x0000)은 체인 불문.
- 테스트넷 LVMON/WETH 존재 여부 — mock 목록엔 없음. 없으면 whitelist에서 enabled=false 또는 제외.
- 테스트넷 풀(Pancake V3): WMON-USDC `0x64B34A7b…`, WMON-USDT0 `0x1eB84F17…`, WMON-AUSD `0xe6ef6D40…`, LV-WMON(2500) `0x692c2a7F…`, XAUt0-USDT0(500) `0x8ca29A0a…`. (price_usd와 무관하지만 multi-anchor 후속 시 참조.)
- Pancake V3 = Uniswap V3 류(concentrated liquidity). 현 인덱서는 V2-style(reserve) 가정 — 테스트넷 V3 풀 인덱싱은 별개 큰 주제.
