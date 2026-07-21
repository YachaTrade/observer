# dex 모듈 GIWA canonical V3 전환 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** observer의 dex 모듈이 Capricorn CL 대신 GIWA canonical Uniswap V3 풀 + GiwaRouter Buy/Sell(graduated=true)을 인덱싱하도록 이벤트 소스를 교체한다.

**Architecture:** rewrite가 아니라 이벤트 소스 교체. Capricorn 풀 ABI = canonical V3 ABI가 완전 동일하므로 풀 파싱/reserve 합성/`slot0()` 로직은 그대로 두고 `sol!` 대상 ABI와 라우터 이벤트 디코드만 바꾼다. 변경은 `src/event/dex/stream.rs` 한 파일 + ABI 파일 + 문서로 국한된다(`ICapricornCLPool`/`IDexRouter` 참조는 stream.rs에만 존재).

**Tech Stack:** Rust 2024, alloy `sol!` 매크로, GIWA contracts ABI.

**Spec:** `docs/superpowers/specs/2026-07-21-dex-canonical-v3-indexing-design.md`

## Global Constraints

- `graduated`는 디코드 시점 필터다. `DexRouterBuy`/`DexRouterSell` 구조체(`src/types/dex.rs`)에 필드를 추가하지 않는다.
- GiwaRouter.Buy/Sell 중 **`graduated == true`만** 처리한다. `false`(커브 매매)는 curve 핸들러가 담당하므로 스킵 — 이중 저장 방지.
- 풀 이벤트 ABI(`IUniswapV3Pool`)는 Capricorn과 시그니처가 동일하다. 파싱·reserve 합성·`slot0()` 로직은 변경하지 않는다.
- `migrations/`, market_type 값(`DEX`), `dex_swap/dex_sync/dex_mint/dex_burn` 스키마·수신 로직은 건드리지 않는다.
- GiwaRouter ABI 출처: `~/project/giwa/contracts/out/GiwaRouter.sol/GiwaRouter.json`. canonical 풀 ABI: `~/project/giwa/contracts/out/IUniswapV3Pool.sol/IUniswapV3Pool.json`.

---

### Task 1: ABI 파일 교체

**Files:**
- Create: `abi/GiwaRouter.json`, `abi/IUniswapV3Pool.json`
- Delete: `abi/ICapricornCLPool.json`, `abi/IDexRouter.json`, `abi/ICapricornCLFactory.json`, `abi/ILens.json` (뒤 둘은 Rust 미참조 죽은 ABI)

**Interfaces:**
- Produces: `abi/GiwaRouter.json`(Buy/Sell/Create/DexRouterFeeRateUpdate 이벤트 포함), `abi/IUniswapV3Pool.json`(Swap/Mint/Burn/Collect/Initialize/SetFeeProtocol). Task 2가 `sol!`로 참조.

- [ ] **Step 1: GiwaRouter ABI 추출**

`giwa/contracts`의 GiwaRouter ABI에서 `abi` 배열만 뽑아 저장:

```bash
python3 -c "import json; d=json.load(open('/Users/gyu/project/giwa/contracts/out/GiwaRouter.sol/GiwaRouter.json')); json.dump(d['abi'], open('abi/GiwaRouter.json','w'), indent=2)"
```

확인: `grep -c '"name": "Buy"' abi/GiwaRouter.json` → 1 이상.

- [ ] **Step 2: canonical V3 풀 ABI 추출**

```bash
python3 -c "import json; d=json.load(open('/Users/gyu/project/giwa/contracts/out/IUniswapV3Pool.sol/IUniswapV3Pool.json')); json.dump(d['abi'], open('abi/IUniswapV3Pool.json','w'), indent=2)"
```

확인: `python3 -c "import json; e=[x['name'] for x in json.load(open('abi/IUniswapV3Pool.json')) if x.get('type')=='event']; print(sorted(e))"` 에 `Swap`, `Mint`, `Burn`, `SetFeeProtocol` 포함.

- [ ] **Step 3: 시그니처 동일성 검증 (Capricorn == V3)**

교체 전 안전 확인:

```bash
python3 -c "
import json
def evs(p):
    d=json.load(open(p)); a=d if isinstance(d,list) else d['abi']
    return {e['name']+'('+','.join(i['type'] for i in e['inputs'])+')' for e in a if e.get('type')=='event'}
cap=evs('abi/ICapricornCLPool.json'); v3=evs('abi/IUniswapV3Pool.json')
common={'Swap','Mint','Burn','SetFeeProtocol'}
for n in common:
    c=[x for x in cap if x.startswith(n+'(')]; v=[x for x in v3 if x.startswith(n+'(')]
    assert c==v, f'{n} 불일치: {c} vs {v}'
print('Swap/Mint/Burn/SetFeeProtocol 시그니처 동일 확인')
"
```

Expected: "시그니처 동일 확인" 출력. 불일치 시 STOP — 설계 전제가 깨진 것이므로 재검토.

- [ ] **Step 4: 구 ABI 삭제**

```bash
git rm abi/ICapricornCLPool.json abi/IDexRouter.json abi/ICapricornCLFactory.json abi/ILens.json
```

(`IToken.json`, `ILpManager.json`은 다른 모듈이 쓰므로 유지.)

- [ ] **Step 5: 커밋**

```bash
git add abi/GiwaRouter.json abi/IUniswapV3Pool.json
git commit -m "chore: add GiwaRouter and canonical V3 pool ABIs, drop Capricorn ABIs"
```

---

### Task 2: stream.rs 이벤트 소스 교체

**Files:**
- Modify: `src/event/dex/stream.rs` (sol! 매크로 32-43행, 필터 74-84행, match arm 210/387/484/581/614/647행, `slot0()` 대상 427/524행)

**Interfaces:**
- Consumes: Task 1의 `abi/GiwaRouter.json`, `abi/IUniswapV3Pool.json`
- Produces: dex 스트림이 `IUniswapV3Pool::{Swap,Mint,Burn,SetFeeProtocol}` + `GiwaRouter::{Buy,Sell}` 로그를 소비. `DexRouterBuy`/`DexRouterSell` 타입(`src/types/dex.rs`)은 무변경으로 계속 생성됨.

- [ ] **Step 1: 회귀 테스트 먼저 작성 (graduated 스킵 + 시그니처 존재)**

`src/event/dex/stream.rs`의 `#[cfg(test)] mod tests`에 추가:

```rust
#[test]
fn giwa_router_and_v3_pool_signatures_resolve() {
    // ABI 교체가 성공하면 이 심볼들이 컴파일된다. GiwaRouter.Buy/Sell,
    // IUniswapV3Pool.Swap/Mint/Burn/SetFeeProtocol 시그니처 해시를 고정한다.
    let _ = GiwaRouter::Buy::SIGNATURE_HASH;
    let _ = GiwaRouter::Sell::SIGNATURE_HASH;
    let _ = IUniswapV3Pool::Swap::SIGNATURE_HASH;
    let _ = IUniswapV3Pool::Mint::SIGNATURE_HASH;
    let _ = IUniswapV3Pool::Burn::SIGNATURE_HASH;
    let _ = IUniswapV3Pool::SetFeeProtocol::SIGNATURE_HASH;
}
```

- [ ] **Step 2: 테스트 실패(컴파일 에러) 확인**

Run: `RUSTC_WRAPPER= cargo test --lib --no-run 2>&1 | grep -E "cannot find|unresolved|error\[" | head`
Expected: `GiwaRouter`, `IUniswapV3Pool` 미정의로 컴파일 실패.

- [ ] **Step 3: sol! 매크로 교체**

`src/event/dex/stream.rs` 32-43행:

```rust
sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    IUniswapV3Pool,
    "abi/IUniswapV3Pool.json"
}
sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    GiwaRouter,
    "abi/GiwaRouter.json"
}
```

(위 블록은 현재 파일 32-43행과 attribute가 동일하다 — `#[allow(missing_docs)]` + `#[sol(rpc)]`를 유지하고 타입명·경로만 교체.)

- [ ] **Step 4: 필터 시그니처 교체**

74-84행 `.events(vec![...])`:

```rust
            .events(vec![
                IUniswapV3Pool::Swap::SIGNATURE,
                IUniswapV3Pool::Mint::SIGNATURE,
                IUniswapV3Pool::Burn::SIGNATURE,
                IUniswapV3Pool::SetFeeProtocol::SIGNATURE,
                GiwaRouter::Buy::SIGNATURE,
                GiwaRouter::Sell::SIGNATURE,
            ]);
```

- [ ] **Step 5: 풀 match arm 교체 (기계적)**

아래 4개 arm의 타입 접두어만 `ICapricornCLPool` → `IUniswapV3Pool`으로 교체 (본문 로직 무변경):
- 210행 `Some(&ICapricornCLPool::Swap::SIGNATURE_HASH)` + 내부 `let ICapricornCLPool::Swap { .. }`
- 387행 `Mint`
- 484행 `Burn`
- 647행 `SetFeeProtocol`

그리고 `slot0()` 호출부 427행·524행:
```rust
IUniswapV3Pool::new(log.address(), client.get_current_provider().await?);
```

- [ ] **Step 6: 라우터 Buy match arm 교체 + graduated 필터**

581-612행의 `DexRouterBuy` arm을 아래로 교체 (필드명 `sender`→`buyer`, `graduated` 추출 + 스킵):

```rust
        Some(&GiwaRouter::Buy::SIGNATURE_HASH) => {
            let address = log.address().to_string();
            if !check_dex_router(address) {
                return Err(anyhow::anyhow!("Not a DexRouter address"));
            }
            let GiwaRouter::Buy {
                buyer: event_sender,
                token,
                amountIn,
                amountOut,
                graduated,
            } = log.log_decode()?.inner.data;

            // graduated=false 는 커브 매매 — curve 핸들러(BondingCurve.Buy)가
            // 이미 인덱싱하므로 dex 에서는 스킵(이중 저장 방지).
            if !graduated {
                return Ok(vec![]);
            }

            let sender = event_sender.to_string();
            let tx_sender = match cache_manager.get_tx_sender(&transaction_hash).await {
                Ok(Some(s)) => s.to_string(),
                _ => sender.clone(),
            };

            let buy = DexRouterBuy {
                token: Arc::new(token.to_string()),
                sender: Arc::new(sender),
                amount_in: Arc::new(to_big_decimal(amountIn)),
                amount_out: Arc::new(to_big_decimal(amountOut)),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
                tx_sender: Arc::new(tx_sender),
            };

            Ok(vec![DexEvent::from(buy)])
        }
```

- [ ] **Step 7: 라우터 Sell match arm 교체 + graduated 필터**

614-646행의 `DexRouterSell` arm을 동일 패턴으로 교체:

```rust
        Some(&GiwaRouter::Sell::SIGNATURE_HASH) => {
            let address = log.address().to_string();
            if !check_dex_router(address) {
                return Err(anyhow::anyhow!("Not a DexRouter address"));
            }
            let GiwaRouter::Sell {
                seller: event_sender,
                token,
                amountIn,
                amountOut,
                graduated,
            } = log.log_decode()?.inner.data;

            if !graduated {
                return Ok(vec![]);
            }

            let sender = event_sender.to_string();
            let tx_sender = match cache_manager.get_tx_sender(&transaction_hash).await {
                Ok(Some(s)) => s.to_string(),
                _ => sender.clone(),
            };

            let sell = DexRouterSell {
                token: Arc::new(token.to_string()),
                sender: Arc::new(sender),
                amount_in: Arc::new(to_big_decimal(amountIn)),
                amount_out: Arc::new(to_big_decimal(amountOut)),
                transaction_hash: Arc::new(transaction_hash),
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
                tx_sender: Arc::new(tx_sender),
            };

            Ok(vec![DexEvent::from(sell)])
        }
```

- [ ] **Step 8: 컴파일 + 회귀 테스트 통과**

Run: `RUSTC_WRAPPER= cargo build && RUSTC_WRAPPER= cargo test --lib dex 2>&1 | grep -E "test result|signatures_resolve"`
Expected: 빌드 성공, `giwa_router_and_v3_pool_signatures_resolve` PASS.

- [ ] **Step 9: 잔존 참조 확인**

Run: `grep -rn "ICapricornCLPool\|IDexRouter\|DexRouterBuy::SIGNATURE\|DexRouterSell::SIGNATURE" src/`
Expected: `src/types/dex.rs`의 `DexRouterBuy`/`DexRouterSell` **구조체** 정의·사용만 남고(이건 유지), `ICapricornCLPool`/`IDexRouter` **ABI 타입** 참조는 0건.

- [ ] **Step 10: 커밋**

```bash
git add src/event/dex/stream.rs
git commit -m "feat: index GIWA canonical V3 pool + GiwaRouter Buy/Sell in dex stream

Swap the dex event source from Capricorn (ICapricornCLPool + IDexRouter)
to GIWA's canonical Uniswap V3 pool (identical ABI) plus GiwaRouter
Buy/Sell. Router events are filtered to graduated == true so post-
graduation dex trades are recorded while curve trades stay with the
curve handler (no double count)."
```

---

### Task 3: config + .env.example 갱신

**Files:**
- Modify: `src/config.rs` (DEX_ROUTER 주석), `.env.example`

**Interfaces:**
- Consumes: 없음. `DEX_ROUTER` env는 그대로, 의미만 GiwaRouter 주소로 재정의.

- [ ] **Step 1: config.rs 주석 갱신**

`src/config.rs`의 `DEX_ROUTER_ADDRESS` 정의 위 주석을 GiwaRouter 기준으로 수정 (env 이름·파싱 로직 무변경). 예:

```rust
    // GiwaRouter address. On GIWA every trade routes through GiwaRouter,
    // which emits Buy/Sell(graduated) — the dex handler filters graduated=true.
    pub static ref DEX_ROUTER_ADDRESS: String =
        normalize_required_env_address("DEX_ROUTER");
```

- [ ] **Step 2: .env.example 주석 갱신**

`.env.example`의 `DEX_ROUTER=` 라인 주석을 `# GiwaRouter (dex Buy/Sell 소스)`로. `DEX_FACTORY`는 canonical V3 factory 주석으로 정리.

- [ ] **Step 3: 빌드 확인 + 커밋**

Run: `RUSTC_WRAPPER= cargo build`
Expected: 성공.

```bash
git add src/config.rs .env.example
git commit -m "docs: point DEX_ROUTER env at GiwaRouter for GIWA dex indexing"
```

---

### Task 4: 문서 갱신

**Files:**
- Modify: `README.md`, `docs/event-indexing.md`, `docs/event/dex.md`

**Interfaces:**
- Consumes: 없음.

- [ ] **Step 1: 런타임 계약 표 갱신**

`README.md`와 `docs/event-indexing.md`의 Dex 행 설명을 "v1 Capricorn DEX ABI"에서 "GIWA canonical Uniswap V3 pool + GiwaRouter Buy/Sell(graduated)"로 교체. Dex 이벤트 상세 절도 canonical V3 기준으로 갱신.

- [ ] **Step 2: docs/event/dex.md 갱신**

풀 이벤트 소스(canonical V3)와 라우터 소스(GiwaRouter Buy/Sell, graduated=true 필터), 커브 매매와의 경계를 반영.

- [ ] **Step 3: 커밋**

```bash
git add README.md docs/event-indexing.md docs/event/dex.md
git commit -m "docs: describe GIWA canonical V3 dex indexing"
```

---

### Task 5: 최종 검증

**Files:** 없음 (검증만)

- [ ] **Step 1: 포맷/린트**

Run: `RUSTC_WRAPPER= cargo fmt --all -- --check && RUSTC_WRAPPER= cargo clippy 2>&1 | tail -3`
Expected: 신규 위반 없음(기존 drift는 무관).

- [ ] **Step 2: 라이브러리 + dex 통합 테스트**

Run: `RUSTC_WRAPPER= cargo test --lib && RUSTC_WRAPPER= cargo test --test group_b_controllers 2>&1 | grep "test result"`
Expected: lib PASS, group_b는 기존 결함 3건(pool_batch_update_reserves) 외 PASS.

- [ ] **Step 3: 잔재 최종 grep**

Run: `grep -rn "Capricorn\|ICapricornCLPool\|IDexRouter" src/ abi/`
Expected: 0건 (ABI 타입·파일 모두 제거됨).

- [ ] **Step 4: 상태 보고**

Run: `git log --oneline -6 && git status --short`
Expected: Task 1-4 커밋, working tree clean. 사용자에게 결과 요약 보고.
