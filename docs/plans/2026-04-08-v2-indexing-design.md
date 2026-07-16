# V2 Contract Indexing Design

## Decisions

- v1 + v2 동시 인덱싱 (같은 observer 바이너리)
- 같은 DB 테이블 사용, version 구분 컬럼 추가
- Chester/Reward Pool은 v2에서 제거
- NadFunRouter, BondingCurveRouter, DexRouter 이벤트는 인덱싱 제외
- ProtocolManager 이벤트 인덱싱 제외
- WMON(WNATIVE)은 v1/v2 공유

## Config

### V2 Contract Addresses (env vars)
```
V2_BONDING_CURVE
V2_DEX_FACTORY
V2_FEE_COLLECTOR
V2_CREATOR_FEE_PROCESSOR
V2_LP_MANAGER
V2_BURN_VAULT
V2_LP_VAULT
V2_CREATOR_FEE_VAULT
V2_GIFT_VAULT
V2_VAULT_REGISTRY
```

## Handler Structure

```
src/event/
├── v1/
│   ├── curve/          # BondingCurve (CurveCreate/Buy/Sell/Sync/Graduate)
│   ├── dex/            # CLPool Swap + Mint/Burn
│   ├── lp_manager/     # LpManagerAllocate/Collect
│   ├── distributor/    # FeeDistributor (Distributed)
│   ├── chester/        # ChesterRewardPool
│   └── reward/         # RewardPool
├── v2/
│   ├── curve/          # BondingCurve (Create/Buy/Sell/Sync/Graduate/SnipingPenalty)
│   ├── dex/            # NadFunPair (Swap/Mint/Burn/Sync)
│   ├── fee/            # FeeCollector + CreatorFeeProcessor
│   ├── lp_manager/     # LPManager (Allocate/Remove/ClaimFee)
│   └── vault/          # BurnVault, LPVault, CreatorFeeVault, GiftVault
└── token/              # v1/v2 공용 (ERC20 Transfer)
```

## V2 Event -> DB Mapping

### v2/curve (BondingCurve)

| Event | DB Table | Notes |
|-------|----------|-------|
| Create(creator, token, pair, name, symbol, tokenURI, virtualQuoteReserve, virtualTokenReserve, minTokenReserve) | tokens, markets | v1 CurveCreate 대응 |
| Buy(token, buyer, quoteIn, tokenOut) | swaps, chart, points, fee_history | v1 CurveBuy 대응 |
| Sell(token, seller, tokenIn, quoteOut) | swaps, chart, points, fee_history | v1 CurveSell 대응 |
| Sync(token, realQuoteReserve, realTokenReserve, virtualQuoteReserve, virtualTokenReserve) | markets | reserve 업데이트 |
| Graduate(token, pair) | markets | 졸업 처리 |
| SnipingPenalty(token, buyer, snipingFee, penaltyBps) | sniping_penalties (신규) | 안티스나이핑 기록 |

### v2/dex (NadFunPair)

| Event | DB Table | Notes |
|-------|----------|-------|
| Swap(sender, amount0In, amount1In, amount0Out, amount1Out, to) | swaps, chart, points | V2 AMM swap |
| Mint(sender, amount0, amount1) | mint | LP 추가 |
| Burn(sender, amount0, amount1, to) | burn | LP 제거 |
| Sync(reserve0, reserve1) | markets | reserve 업데이트 |

### v2/fee (FeeCollector + CreatorFeeProcessor)

| Event | DB Table | Notes |
|-------|----------|-------|
| ConfigSet(pair, baseToken, quoteToken, creatorFeeRate, curveProtocolFeeRate, dexProtocolFeeRate) | - | 설정 이벤트, 캐시용 |
| Collect(pair, quoteToken, amount) | fee_history | 수수료 수집 |
| Settle(pair, totalFee, protocolFee, creatorFee) | fee_distribution | v1 Distributed 대응 |
| Process(token, quoteToken, amount) | creator_fee_distributions (신규) | 크리에이터 수수료 처리 |
| Distribute(vault, amount) | creator_fee_distributions (신규) | Vault별 분배 |
| CallbackFail(vault, amount, reason) | creator_fee_distributions (신규) | 실패 기록 |
| Configure(token, vaultCount) | - | 설정 이벤트 |

### v2/lp_manager (LPManager)

| Event | DB Table | Notes |
|-------|----------|-------|
| Allocate(token, pair, caller, dexType, tokenIn, quoteIn, liquidity) | lp_allocations | v1 Allocate 대응 |
| Remove(token, to, dexType, amount0, amount1) | lp_allocations | 신규 |
| ClaimFee(token, to, dexType, amount0, amount1) | lp_collections | v1 Collect 대응 |

### v2/vault (4 Vaults)

| Event | DB Table | Notes |
|-------|----------|-------|
| BurnVault.Burn(token, quoteIn, tokenBurned) | vault_burns (신규) | 바이백 소각 |
| LPVault.Inject(token, quoteUsed, tokenUsed, lpBurned) | vault_lp_injections (신규) | LP 주입 |
| CreatorFeeVault.Deposit(token, amount, newBalance) | creator_fee_claims (신규) | 입금 |
| CreatorFeeVault.Claim(token, creator, amount) | creator_fee_claims (신규) | 출금 |
| CreatorFeeVault.SetCreator(token, creator) | - | 설정 이벤트 |
| GiftVault.Setup(token, xHandleHash, xHandle) | gifts (신규) | 기프트 설정 |
| GiftVault.Deposit(token, amount, newBalance) | gifts (신규) | 기프트 입금 |
| GiftVault.Claim(token, claimer, amount) | gifts (신규) | 기프트 클레임 |
| GiftVault.Expire(token, amount) | gifts (신규) | 만료 처리 |
| GiftVault.Burn(token, quoteIn, tokenBurned) | vault_burns (신규) | 기프트 소각 |
| GiftVault.ExpiryUpdate(oldDuration, newDuration) | - | 설정 이벤트 |

## New Tables

```sql
-- 안티스나이핑 페널티 기록
sniping_penalties (token, buyer, sniping_fee, penalty_bps, tx_hash, block_number, timestamp)

-- 크리에이터 수수료 분배 추적
creator_fee_distributions (token, quote_token, vault, amount, type, tx_hash, block_number, timestamp)

-- Vault 소각 기록 (BurnVault + GiftVault)
vault_burns (token, vault_type, quote_in, token_burned, tx_hash, block_number, timestamp)

-- LP Vault 주입 기록
vault_lp_injections (token, quote_used, token_used, lp_burned, tx_hash, block_number, timestamp)

-- CreatorFeeVault 입출금
creator_fee_claims (token, creator, amount, type, tx_hash, block_number, timestamp)

-- GiftVault 기프트
gifts (token, x_handle_hash, x_handle, claimer, amount, type, tx_hash, block_number, timestamp)
```

## Implementation Order

1. **Phase 1: Config + Structure**
   - V2 env vars 추가 (config.rs)
   - v1 핸들러를 event/v1/으로 이동
   - mod.rs import 경로 업데이트

2. **Phase 2: V2 Core Handlers**
   - v2/curve (BondingCurve Create/Buy/Sell/Sync/Graduate/SnipingPenalty)
   - v2/dex (NadFunPair Swap/Mint/Burn/Sync)

3. **Phase 3: V2 Fee + LP**
   - v2/fee (FeeCollector + CreatorFeeProcessor)
   - v2/lp_manager (Allocate/Remove/ClaimFee)

4. **Phase 4: V2 Vault**
   - v2/vault (BurnVault, LPVault, CreatorFeeVault, GiftVault)

5. **Phase 5: DB Migration**
   - 기존 테이블에 version 컬럼 추가
   - 신규 테이블 생성
