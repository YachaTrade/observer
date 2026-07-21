# dex 모듈 — Capricorn CL → GIWA canonical Uniswap V3 인덱싱 전환 설계

- 날짜: 2026-07-21
- 상태: 승인 대기
- 범위: observer의 `dex` 이벤트 모듈 (curve/lp_manager/vault/token/price 무관)

## 배경

현재 observer의 dex 모듈은 Monad 시절 **Capricorn CL DEX**를 인덱싱한다:
raw 풀 이벤트(`ICapricornCLPool::Swap/Mint/Burn/SetFeeProtocol`)와 라우터
이벤트(`IDexRouter::DexRouterBuy/DexRouterSell`)를 함께 읽는다.

그러나 GIWA는 DEX 구조가 다르다 (`giwa/contracts`, `refactor/giwa-router-v2-structure`):

- 본딩커브 졸업 후 **canonical Uniswap V3 풀**로 넘어간다 (Capricorn 아님).
- 모든 매매는 **`GiwaRouter`** 하나를 통하며, 라우터가
  `Buy(buyer, token, amountIn, amountOut, bool graduated)` /
  `Sell(seller, token, amountIn, amountOut, bool graduated)` 를 낸다.
  `graduated` 플래그로 커브 단계/졸업 후를 구분한다.
- Capricorn 풀의 raw Swap 이벤트는 GIWA에서 발생하지 않는다.

따라서 현재 dex 모듈이 잡는 `ICapricornCLPool::Swap`, `IDexRouter::DexRouterBuy`
등은 GIWA에서 뜨지 않아 **dex 인덱싱이 전혀 동작하지 않는다.**

## 결정적 사실

**canonical `IUniswapV3Pool` ABI = Capricorn `ICapricornCLPool` ABI (완전 동일).**
`Swap/Mint/Burn/Collect/Initialize/SetFeeProtocol` 이벤트 시그니처가 바이트
단위로 일치한다 (Capricorn은 순수 Uniswap V3 포크). 따라서 풀 이벤트 파싱,
reserve 합성, `slot0()` RPC 호출 로직은 **그대로 재사용**한다. 이 작업은
rewrite가 아니라 **이벤트 소스 교체 리팩터**다.

## 접근 (하이브리드 유지)

현재 아키텍처는 "라우터 이벤트 = 실트레이더/금액을 담은 swap 원장" +
"풀 이벤트 = reserve/price/liquidity 합성"의 하이브리드다. GIWA에 1:1 대응된다.

| 역할 | 현재 (Capricorn) | GIWA로 교체 |
| --- | --- | --- |
| swap 원장 (실트레이더 + 금액) | `IDexRouter::DexRouterBuy/Sell(sender, token, amountIn, amountOut)` | `GiwaRouter::Buy/Sell(buyer, token, amountIn, amountOut, graduated)` — **`graduated==true`만 처리** |
| reserve/price/liquidity | `ICapricornCLPool::Swap/Mint/Burn/SetFeeProtocol` | `IUniswapV3Pool::Swap/Mint/Burn/SetFeeProtocol` (동일 ABI) |

### `graduated`는 저장 필드가 아니라 디코드 시점 필터

GIWA는 커브 매매도 `GiwaRouter.Buy/Sell(graduated=false)`로 나오지만, 커브
매매는 **이미 curve 핸들러가 `BondingCurve.Buy/Sell`로 인덱싱**한다. dex
핸들러가 GiwaRouter Buy/Sell을 전부 잡으면 커브 매매가 이중 저장된다.

해결: 로그 디코드 시 `graduated` 값을 읽어 `false`면 스킵하고 `true`만
통과시킨다. `DexRouterBuy`/`DexRouterSell` 구조체에는 **새 필드를 추가하지
않는다** — `graduated`는 "이 로그를 처리할지 버릴지"만 정한다.

```rust
let Buy { buyer, token, amount_in, amount_out, graduated } = decode(log)?;
if !graduated {
    continue; // 커브 매매 → curve 핸들러 담당, dex에서는 스킵
}
// graduated == true 만 기존 DexRouterBuy 처리 경로로
```

## 변경 항목 (dex 모듈 국한)

### 1. ABI 교체
- 추가: `abi/GiwaRouter.json` (giwa/contracts `out/GiwaRouter.sol/GiwaRouter.json`
  에서 추출 — Buy/Sell/Create/DexRouterFeeRateUpdate 이벤트 포함),
  `abi/IUniswapV3Pool.json` (Capricorn과 동일 시그니처).
- 제거: `abi/ICapricornCLPool.json`, `abi/IDexRouter.json` — 단, 다른 모듈이
  참조하지 않는지 확인 후 제거.

### 2. `src/event/dex/stream.rs`
- `sol!` 매크로 대상: `ICapricornCLPool` → `IUniswapV3Pool`,
  `IDexRouter` → `GiwaRouter` (ABI 경로 갱신).
- 필터의 이벤트 시그니처:
  `IUniswapV3Pool::{Swap,Mint,Burn,SetFeeProtocol}::SIGNATURE` +
  `GiwaRouter::{Buy,Sell}::SIGNATURE`.
- Buy/Sell 디코드에 `graduated` 필드 추출 + `!graduated` 스킵.
- `slot0()` 호출 대상: `IUniswapV3Pool::new(...)` (동일 인터페이스).
- 라우터 주소 필터: `DEX_ROUTER_ADDRESS` (아래 config에서 GiwaRouter 주소로
  재사용).

### 3. `src/event/dex/receive.rs`
- 라우터 이벤트 → swap 레코드 매핑 골격 유지. `tx_sender`(실트레이더),
  `amount_in`/`amount_out`, market_type `DEX`, fee_history, points, chart 로직
  무변경.

### 4. `src/types/dex.rs`
- ABI 타입 참조 정리(`ICapricornCLPool`→`IUniswapV3Pool`,
  `IDexRouter`→`GiwaRouter`). `DexRouterBuy`/`DexRouterSell` 구조체 필드 무변경.

### 5. `src/config.rs`
- 기존 `DEX_ROUTER` env를 **GiwaRouter 주소**로 재사용 (이름 유지, churn 최소).
  observer 관점에서 curve는 `BONDING_CURVE`를 직접 읽고 GiwaRouter는 dex
  Buy/Sell에만 쓰이므로 "dex 라우터"로 봐도 의미가 맞는다. `.env.example`의
  주석을 GiwaRouter로 갱신.

### 6. 문서
- `README.md`, `docs/event-indexing.md`: Dex 스트림 설명을 "GIWA canonical
  Uniswap V3 풀 + GiwaRouter Buy/Sell(graduated=true)"로 갱신. `docs/event/dex.md`
  동일.

## 그대로 유지

- 풀 등록 경로: Curve Graduate 처리가 이미 `pool` 테이블에 풀을 등록하고
  (`batch_insert_pools`), Create가 whitelist 캐시에 풀을 넣는다. dex 핸들러의
  `check_token_pool`/`get_pool_pair`는 무변경으로 동작한다.
- `dex_swap`/`dex_sync`/`dex_mint`/`dex_burn` 스키마·수신 로직.
- `SetFeeProtocol → set_fee_history` (canonical V3도 동일 시그니처).
- market_type 값 `DEX` (되돌린 상태 유지).

## 범위 밖 (YAGNI)

- `GiwaRouter.DexRouterFeeRateUpdate`(동적 dex 수수료율 이벤트) — 지금은 정적
  `DEX_ROUTER_FEE_RATE` env 유지. 향후 동적화 시 별도 작업.
- curve 매매 경로(`BondingCurve.Buy/Sell`) — curve 핸들러 담당, 무관.
- market_type 이름/값 변경.

## 검증

1. `cargo build`, `cargo clippy`.
2. `cargo test --lib` + dex 관련 통합 테스트(`group_*_controllers` 중 dex swap/
   pool/market 커버). 기존 결함(pool_batch_update_reserves 3, cache doc-test 2)은
   무관.
3. GiwaRouter/V3 풀 이벤트 시그니처 해시가 ABI와 일치하는지 단위 테스트로 고정.
4. `graduated=false` 로그가 dex swap을 만들지 않는(스킵) 회귀 테스트.

## 리스크

- **GiwaRouter ABI 확정 전제**: `giwa/contracts`의 GiwaRouter가 배포본과 일치해야
  한다. Buy/Sell 시그니처가 바뀌면 SIGNATURE_HASH 단위 테스트가 먼저 실패한다.
- **실주소 미확정**: GiwaRouter/V3 factory 실배포 주소는 배포 시 `.env`에
  기입 (이 작업 범위 밖).
- **이중 저장 방지**는 `graduated` 필터에 전적으로 의존한다. 회귀 테스트로 고정.
