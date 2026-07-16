# GIWA Single-Version Indexing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the GIWA observer with one public Curve/Dex/LpManager event surface, using the v2 Curve implementation and v1 Dex/LPManager implementations, while removing inactive Vault and versioned stacks and writing only GIWA-specific generic market/fee values for new rows.

**Architecture:** Runtime identity is separated from contract provenance: the active v2 Curve module runs as `EventType::Curve`, while the active v1 Dex and LPManager modules run as `EventType::Dex` and `EventType::LpManager`. Configuration and new database values are unversioned, but active ABI-backed Rust paths may keep `v1`/`v2` names. Historical database rows, tables, constraints, migrations, and seeds are preserved without normalization or cleanup SQL.

**Tech Stack:** Rust 2024, Tokio, Alloy, SQLx, PostgreSQL 17, testcontainers, SQL migrations, Git submodules

## Global Constraints

- Work on `giwa` in both `/Users/gyu/project/nads-pump/observer` and its `migrations` submodule.
- The active runtime event set is exactly `Curve`, `Dex`, `LpManager`, `Token`, `Price`, and `PriceUsd`.
- `Curve` uses `event::v2::curve`; `Dex` uses `event::v1::dex`; `LpManager` uses `event::v1::lp_manager`.
- Active address variables are exactly `BONDING_CURVE`, `DEX_FACTORY`, `DEX_ROUTER`, and `LP_MANAGER`.
- Active fee variables are exactly `CREATE_FEE_AMOUNT`, `GRADUATE_FEE_AMOUNT`, `BONDING_CURVE_FEE_RATE`, and `DEX_ROUTER_FEE_RATE`.
- New GIWA tokens explicitly store `version = 'V2'` and `chain = 'GIWA'`.
- New GIWA market writes use only `CURVE` and `DEX`; new Curve trade fee writes use only `curve_buy` and `curve_sell`.
- Keep `token_id` as the sole primary key and do not add `chain` to other tables or keys.
- Do not update existing `V2_CURVE`, `V2_DEX`, `v2_curve_buy`, or `v2_curve_sell` rows.
- Do not drop, truncate, delete, or rewrite historical Vault, Dividend, Reward, Creator, Distributor, v2 DEX, v2 Fee, or v2 LPManager data, tables, migrations, or seeds.
- Remove inactive observer code instead of retaining feature flags. Keep common code required by one of the six active handlers.
- Preserve all pre-existing untracked workspace files.
- Do not fix the pre-existing benchmark dependency failures or the unrelated pool-reserve integration-test bind mismatch in this feature.

## Delivered Prerequisites

These commits are already complete and must not be reimplemented:

- migrations `a6612de` adds `0036_token_chain.sql` with `chain VARCHAR NOT NULL DEFAULT 'MON'` and no key change.
- observer `9a44ae5` tests the token-chain migration and advances the migrations gitlink.
- observer `bb1bcb2` explicitly writes `chain = 'GIWA'` in the production token CTE.
- observer `22f50ce` removes the two Vault event types that previously reached runtime startup.
- observer `ddc0b33` records the approved single-version design.

Task 1 starts from the currently preserved, uncommitted Vault source-removal work. Do not reset or discard it.

---

## File Map

### Create

- `tests/giwa_runtime_contract.rs` — source/runtime regression tests for active handler wiring and generic configuration.
- `docs/event/curve.md` — generic Curve documentation backed by the v2 ABI implementation.
- `docs/event/dex.md` — generic Dex documentation backed by the v1 ABI implementation.
- `docs/event/lp-manager.md` — generic LPManager documentation backed by the v1 ABI implementation.

### Modify

- `src/main.rs` — start exactly six handlers and map active implementation modules to generic event types.
- `src/sync/mod.rs` — expose exactly six generic `EventType` values.
- `src/sync/receive.rs` — keep only active dependencies and remove the v2 Dex wait from Token.
- `src/sync/stream.rs` — schedule only active generic checkpoints.
- `src/config.rs` — expose only generic active contract and fee configuration.
- `src/event/v2/curve/stream.rs` — consume `BONDING_CURVE_ADDRESS` while retaining the v2 ABI path.
- `src/event/v2/curve/receive.rs` — write generic market/fee categories and consume generic fee constants.
- `src/event/v1/dex/stream.rs` — consume `DEX_ROUTER_ADDRESS`.
- `src/event/v1/dex/receive.rs` — consume `DEX_ROUTER_FEE_RATE`.
- `src/event/v1/lp_manager/stream.rs` — consume `LP_MANAGER_ADDRESS`.
- `src/event/common/token/stream.rs` — build its system-address set only from active generic addresses.
- `src/types/token.rs` — own the shared LP-position event type after the inactive v2 DEX type module is removed.
- `src/event/common/token/lp_position.rs` — import the shared LP-position type from `types::token`.
- `src/event/common/token/receive.rs` — import the shared LP-position type from `types::token`.
- `src/db/postgres/controller/lp_position.rs` — import the shared LP-position type from `types::token`.
- `src/types/fee.rs` — retain only generic fee categories used by active handlers.
- `src/event/v1/mod.rs`, `src/event/v2/mod.rs`, `src/types/v1/mod.rs`, `src/types/v2/mod.rs` — export only active/shared modules.
- `src/db/postgres/controller/mod.rs`, `src/db/postgres/controller/v2/mod.rs` — remove inactive controller exports while retaining active common and sniping controllers.
- `src/db/postgres/controller/token.rs` — remove the inactive v2 DEX metadata helper and update generic comments.
- `tests/common/mod.rs` — remove helpers that import deleted inactive controllers and make the token helper bind `version = 'V2'`.
- `tests/group_b_controllers.rs` — assert GIWA token version, chain, and generic market lifecycle.
- `tests/group_c_controllers.rs` — retain generic fee/point tests and remove Reward/Distributor-only sections.
- `tests/group_d_controllers.rs` — retain Chart/Price/Account tests and remove v2 `dex_token` sections.
- `tests/v2_controllers.rs` — retain the active Curve sniping-controller tests only.
- `README.md`, `docs/event-indexing.md` — document the six generic streams and generic environment variables.

### Delete: Vault stack

- `src/event/v2/vault/`
- `src/event/v2/vault_registry/`
- `src/types/v2/vault.rs`, `src/types/v2/vault_registry.rs`, `src/types/v2/dividend.rs`
- `src/db/postgres/controller/v2/vault.rs`, `src/db/postgres/controller/v2/vault_registry.rs`, `src/db/postgres/controller/v2/dividend.rs`
- `src/utils/vault_metadata.rs`, `src/bin/backfill_vault_metadata.rs`
- Vault/VaultRegistry/DividendVault ABI, dedicated test, branch-note, event-doc, query-doc, and superseded feature-plan files already staged by the interrupted Vault task.

### Delete: inactive non-Vault observer stacks

- `src/event/v1/curve/`, `src/event/v1/reward/`, `src/event/v1/creator/`, `src/event/v1/distributor/`
- `src/event/v2/dex/`, `src/event/v2/factory/`, `src/event/v2/fee/`, `src/event/v2/lp_manager/`, `src/event/v2/usd_enrich.rs`
- `src/types/v1/creator.rs`, `src/types/v1/distributor.rs`, `src/types/v1/fee.rs`, `src/types/v1/reward.rs`
- `src/types/v2/dex.rs`, `src/types/v2/factory.rs`, `src/types/v2/fee.rs`, `src/types/v2/lp_manager.rs`
- `src/db/postgres/controller/creator.rs`, `src/db/postgres/controller/distributor.rs`, `src/db/postgres/controller/reward.rs`
- `src/db/postgres/controller/dex_swap.rs`, `src/db/postgres/controller/dex_token.rs`
- `src/db/postgres/controller/v2/fee.rs`, `src/db/postgres/controller/v2/lp.rs`
- Dedicated inactive ABIs and tests enumerated in Task 3.

### Preserve Explicitly

- `src/event/v2/curve/`, `src/types/v2/curve.rs`, `abi/v2/BondingCurve.json`
- `src/event/v1/dex/`, `src/types/v1/dex.rs`, `abi/v1/ICapricornCLPool.json`, `abi/v1/IDexRouter.json`
- `src/event/v1/lp_manager/`, `src/types/v1/lp_manager.rs`, `abi/v1/ILpManager.json`
- `src/types/v1/curve.rs` because the active v1 Dex types reuse its `Buy`, `Sell`, `MarketType`, and metadata shapes.
- `src/db/postgres/controller/v2/sniping.rs` because the active v2 Curve emits sniping penalties.
- Common Token, Price, PriceUsd, market, swap, fee-history, pool, mint/burn, and LP controllers consumed by active handlers.
- Every file under `migrations/` except the already committed `0036_token_chain.sql`; especially Vault/Dividend and v2 historical SQL/seeds.

---

### Task 1: Complete the Preserved Vault Purge

**Files:**
- Modify/Delete: every Vault path listed in the File Map and currently shown by `git status`.
- Verify: `migrations/vault.sql`, `migrations/dividend.sql`, `migrations/v2_upgrade_vault_usd.sql`, `migrations/v2_upgrade_gift_expires_at.sql`, `migrations/seeds/v2_vault_metadata.sql`.

**Interfaces:**
- Consumes: commit `22f50ce`, where Vault runtime event types are already detached.
- Produces: no compiled Vault/VaultRegistry/DividendVault observer stack; historical database artifacts remain byte-for-byte present.

- [ ] **Step 1: Confirm the preserved partial work is still present**

Run:

```bash
git status --short
git diff -- src/config.rs src/db/cache/mod.rs src/event/v2/mod.rs src/types/v2/mod.rs src/utils/mod.rs tests/v2_controllers.rs README.md
git diff --cached --name-status
```

Expected: staged Vault deletions and unstaged Vault-reference edits are present. No file under `migrations/` is deleted or modified.

- [ ] **Step 2: Finish the compile-time reference cleanup**

Keep the already prepared four-argument actor API in `src/db/cache/mod.rs`:

```rust
pub async fn resolve_actor(
    &self,
    tx_hash: &str,
    event_sender: &str,
    token: &str,
    is_buy: bool,
) -> Result<String> {
```

Remove the BurnVault/GiftVault early returns and keep the generic EOA, receipt-transfer, zero-address, and transaction-origin fallback logic. Keep all caller updates already present in the v1/v2 Curve and Dex stream files.

Make the module exports contain no Vault modules:

```rust
// src/event/v2/mod.rs
pub mod curve;
pub mod dex;
pub mod fee;
pub mod lp_manager;
pub(crate) mod usd_enrich;
```

```rust
// src/types/v2/mod.rs
pub mod curve;
pub mod dex;
pub mod fee;
pub mod lp_manager;
```

```rust
// src/db/postgres/controller/v2/mod.rs — declarations/re-exports
pub mod fee;
pub mod lp;
pub mod sniping;

pub use fee::*;
pub use lp::*;
pub use sniping::*;
```

The shared retry helper below those declarations remains unchanged.

- [ ] **Step 3: Prove Vault observer references are absent**

Run:

```bash
if rg -n 'V2Vault|V2VaultRegistry|V2_(BURN|LP|CREATOR_FEE|GIFT|DIVIDEND)_VAULT|V2_VAULT_REGISTRY|types::v2::(vault|vault_registry|dividend)|controller::v2::(vault|vault_registry|dividend)' src tests README.md; then exit 1; fi
```

Expected: no matches and exit status 0.

- [ ] **Step 4: Prove historical Vault database artifacts remain**

Run:

```bash
test -f migrations/vault.sql
test -f migrations/dividend.sql
test -f migrations/v2_upgrade_vault_usd.sql
test -f migrations/v2_upgrade_gift_expires_at.sql
test -f migrations/seeds/v2_vault_metadata.sql
git diff --exit-code -- migrations/vault.sql migrations/dividend.sql migrations/v2_upgrade_vault_usd.sql migrations/v2_upgrade_gift_expires_at.sql migrations/seeds/v2_vault_metadata.sql
```

Expected: every command exits 0.

- [ ] **Step 5: Compile and run the retained controller tests**

Run:

```bash
cargo check --lib --bin observer
cargo test --test v2_controllers -- --nocapture
git diff --check
```

Expected: all commands pass. At this stage `v2_controllers` still covers Fee, LP, and sniping; Task 3 will remove the inactive Fee/LP sections.

- [ ] **Step 6: Commit only the Vault purge**

Run:

```bash
git add README.md \
  abi/v2/BurnVault.json abi/v2/CreatorFeeVault.json abi/v2/DividendVault.json abi/v2/GiftVault.json abi/v2/LPVault.json abi/v2/VaultRegistry.json \
  src/bin/backfill_vault_metadata.rs src/config.rs src/db/cache/mod.rs src/db/postgres/controller/v2 \
  src/event/v1/curve/stream.rs src/event/v1/dex/stream.rs src/event/v2 src/types/v2 src/utils \
  tests/v2_controllers.rs tests/dividend_via_v2vault.rs tests/v2_dividend.rs tests/vault_registry_type.rs \
  branches/feat-dividend-claim-nonquote-usd.md branches/feat-v2-dividend-vault-indexing.md branches/refactor-dividend-into-v2vault.md \
  docs/event/v2/dividend.md docs/event/v2/vault.md docs/event/v2/vault_registry.md \
  docs/plans/2026-06-16-dividend-into-v2vault-stream-design.md docs/plans/2026-06-19-dividend-claim-nonquote-usd-design.md \
  docs/query/backfill_dividend_claim_usd.sql docs/query/v2-vault-stats.md \
  docs/superpowers/plans/2026-05-10-vault-usd-value.md docs/superpowers/plans/2026-06-13-dividend-vault-indexing.md \
  docs/superpowers/specs/2026-05-10-vault-usd-value-design.md docs/superpowers/specs/2026-06-12-dividend-vault-indexing-design.md
git diff --cached --name-only
git commit -m "refactor: remove v2 vault indexing stack"
```

Expected: the staged list contains only the known Vault-related paths. It does not contain `migrations/`, `.gstack/`, `graphify-out/`, `scripts/`, `next_session.md`, `docs/architecture/`, or the pre-existing untracked plan files.

---

### Task 2: Collapse Runtime Identity to Six Generic Streams

**Files:**
- Create: `tests/giwa_runtime_contract.rs`
- Modify: `src/sync/mod.rs`, `src/sync/receive.rs`, `src/sync/stream.rs`, `src/main.rs`

**Interfaces:**
- Consumes: `event::v2::curve::V2CurveEventHandler`, `event::v1::dex::DexEventHandler`, `event::v1::lp_manager::LpManagerEventHandler`, and the three common handlers.
- Produces: generic checkpoint names `curve`, `dex`, `lp_manager`, `token`, `price`, `price_usd`; no removed checkpoint can be initialized or awaited.

- [ ] **Step 1: Replace the existing EventType test with the exact failing contract**

In `src/sync/mod.rs`, use:

```rust
#[cfg(test)]
mod tests {
    use super::EventType;

    #[test]
    fn giwa_event_types_are_exactly_the_six_generic_streams() {
        let names: Vec<&str> = EventType::all().iter().map(EventType::as_str).collect();
        assert_eq!(
            names,
            vec!["curve", "dex", "lp_manager", "token", "price", "price_usd"]
        );
    }
}
```

Create `tests/giwa_runtime_contract.rs`:

```rust
#[test]
fn main_wires_the_selected_implementations_to_generic_events() {
    let main = include_str!("../src/main.rs");
    let compact = main.split_whitespace().collect::<Vec<_>>().join(" ");

    assert!(compact.contains("event_v2_curve::V2CurveEventHandler>( EventType::Curve"));
    assert!(compact.contains("event_dex::DexEventHandler>( EventType::Dex"));
    assert!(compact.contains("event_lp_manager::LpManagerEventHandler, >(EventType::LpManager"));
    assert!(!compact.contains("EventType::V2"));
    assert!(!compact.contains("EventType::Reward"));
    assert!(!compact.contains("EventType::Creator"));
    assert!(!compact.contains("EventType::Distributor"));
}
```

- [ ] **Step 2: Run both tests to verify the old surface fails**

Run:

```bash
cargo test --lib sync::tests::giwa_event_types_are_exactly_the_six_generic_streams
cargo test --test giwa_runtime_contract main_wires_the_selected_implementations_to_generic_events
```

Expected: both fail because the runtime still exposes/spawns old v1 and version-prefixed handlers.

- [ ] **Step 3: Reduce EventType to the generic six**

Replace the enum and mappings in `src/sync/mod.rs` with:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    Curve,
    Dex,
    LpManager,
    Token,
    Price,
    PriceUsd,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Curve => "curve",
            EventType::Dex => "dex",
            EventType::LpManager => "lp_manager",
            EventType::Token => "token",
            EventType::Price => "price",
            EventType::PriceUsd => "price_usd",
        }
    }

    pub fn all() -> [EventType; 6] {
        [
            EventType::Curve,
            EventType::Dex,
            EventType::LpManager,
            EventType::Token,
            EventType::Price,
            EventType::PriceUsd,
        ]
    }
}
```

- [ ] **Step 4: Simplify synchronization dependencies**

In `src/sync/receive.rs`, make the `match event_type` branches equivalent to:

```rust
match event_type {
    EventType::Curve => {
        self.wait_for_dependency(
            start,
            timeout,
            block,
            EventType::Curve,
            &[(EventType::Price, 1)],
        )
        .await;
    }
    EventType::Dex | EventType::LpManager => {
        self.wait_for_dependency(
            start,
            timeout,
            block,
            event_type,
            &[(EventType::Curve, 1)],
        )
        .await;
    }
    EventType::Token => {
        self.wait_for_dependency_strict(block, EventType::Token, &[(EventType::Curve, 1)])
            .await;
    }
    EventType::Price | EventType::PriceUsd => {}
}
```

Remove all comments and branches referring to Reward, Creator, Distributor, `V2Curve`, `V2Dex`, `V2Fee`, or `V2LpManager`.

In `src/sync/stream.rs`, initialize every event with the same `block_range` without a match:

```rust
for event_type in EventType::all() {
    self.stream_event_block
        .write()
        .await
        .insert(event_type, block_range.clone());
}
```

Keep Curve's `BLOCK_OFFSET`, Dex/LPManager's Curve-ahead wait, Price/PriceUsd's existing 1,000-block cap, and replace Token's old dual-dependency branch with:

```rust
EventType::Token => {
    loop {
        let curve_block = self.get_event_block_range(EventType::Curve).await;
        if from_block < curve_block.from_block {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }
    let curve_block = self.get_event_block_range(EventType::Curve).await;
    let end = from_block + block_batch_size;
    end.min(curve_block.from_block.saturating_sub(1))
        .min(latest_block.saturating_sub(1))
}
```

- [ ] **Step 5: Start exactly the six selected handlers**

Use these event imports in `src/main.rs`:

```rust
event::{
    common::{price as event_price, price_usd as event_price_usd, token as event_token},
    handler::run_event_handler as event_run_event_handler,
    v1::{dex as event_dex, lp_manager as event_lp_manager},
    v2::curve as event_v2_curve,
},
```

Warm the price cache from the generic Dex checkpoint:

```rust
let dex_range = STREAM_MANAGER.get_event_block_range(EventType::Dex).await;
let sentinel = (dex_range.from_block as i64).saturating_sub(1);
```

Start only:

```rust
set.spawn(event_run_event_handler::<event_v2_curve::V2CurveEventHandler>(
    EventType::Curve,
));
set.spawn(event_run_event_handler::<event_dex::DexEventHandler>(
    EventType::Dex,
));
set.spawn(event_run_event_handler::<event_lp_manager::LpManagerEventHandler>(
    EventType::LpManager,
));
set.spawn(event_run_event_handler::<event_token::TokenEventHandler>(
    EventType::Token,
));
set.spawn(event_run_event_handler::<event_price::PriceEventHandler>(
    EventType::Price,
));
set.spawn(event_run_event_handler::<event_price_usd::PriceUsdEventHandler>(
    EventType::PriceUsd,
));
```

- [ ] **Step 6: Run runtime tests and compile**

Run:

```bash
cargo test --lib sync::tests::giwa_event_types_are_exactly_the_six_generic_streams
cargo test --test giwa_runtime_contract main_wires_the_selected_implementations_to_generic_events
cargo check --lib --bin observer
```

Expected: all commands pass.

- [ ] **Step 7: Commit the runtime collapse**

Run:

```bash
git add src/main.rs src/sync/mod.rs src/sync/receive.rs src/sync/stream.rs tests/giwa_runtime_contract.rs
git commit -m "refactor: collapse GIWA runtime to generic streams"
```

---

### Task 3: Remove Inactive Non-Vault Implementations

**Files:**
- Delete/Modify: the inactive non-Vault paths listed in the File Map.

**Interfaces:**
- Consumes: Task 2's runtime, which no longer imports inactive handlers.
- Produces: only v2 Curve, v1 Dex, v1 LPManager, and common Token/Price event implementations compile; shared LP-position rows use a common type rather than the deleted v2 DEX event module.

- [ ] **Step 1: Capture the failing inactive-stack inventory**

Run:

```bash
rg -n 'pub mod (creator|distributor|reward)|pub mod (dex|factory|fee|lp_manager)|V2(Dex|Fee|LpManager|Factory)Event|RewardEvent|CreatorEvent|Distributed' src/event/v1 src/event/v2 src/types/v1 src/types/v2
```

Expected: matches for every inactive stack.

- [ ] **Step 2: Move the shared LP-position shape out of the v2 DEX type file**

Move this exact struct to `src/types/token.rs`:

```rust
#[derive(Debug, Clone)]
pub struct LpPositionHistoryEvent {
    pub account_id: Arc<String>,
    pub pool_id: Arc<String>,
    pub lp_in: Arc<BigDecimal>,
    pub lp_out: Arc<BigDecimal>,
    pub event_type: &'static str,
    pub counterparty: Option<Arc<String>>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: Arc<String>,
    pub transaction_index: u64,
    pub log_index: u64,
}
```

Replace imports in these files with `crate::types::token::LpPositionHistoryEvent`:

```rust
// src/event/common/token/lp_position.rs
use crate::types::token::LpPositionHistoryEvent;

// src/event/common/token/receive.rs
types::token::{LpPositionHistoryEvent, TokenEvent},

// src/db/postgres/controller/lp_position.rs
pub use crate::types::token::LpPositionHistoryEvent;
```

Remove `use crate::types::v2::dex::LpPositionHistoryEvent;` from `src/types/token.rs` because the type is now local.

- [ ] **Step 3: Delete inactive event/type/controller stacks and dedicated ABIs**

Run:

```bash
git rm -r src/event/v1/curve src/event/v1/reward src/event/v1/creator src/event/v1/distributor
git rm -r src/event/v2/dex src/event/v2/factory src/event/v2/fee src/event/v2/lp_manager
git rm src/event/v2/usd_enrich.rs
git rm src/types/v1/creator.rs src/types/v1/distributor.rs src/types/v1/fee.rs src/types/v1/reward.rs
git rm src/types/v2/dex.rs src/types/v2/factory.rs src/types/v2/fee.rs src/types/v2/lp_manager.rs
git rm src/db/postgres/controller/creator.rs src/db/postgres/controller/distributor.rs src/db/postgres/controller/reward.rs
git rm src/db/postgres/controller/dex_swap.rs src/db/postgres/controller/dex_token.rs
git rm src/db/postgres/controller/v2/fee.rs src/db/postgres/controller/v2/lp.rs
git rm abi/v1/IBondingCurve.json abi/v1/IBondingCurveRouter.json abi/v1/ICreatorManager.json abi/v1/ICreatorTreasury.json abi/v1/IFeeDistributor.json abi/v1/IRewardPool.json abi/v1/ITokenTreasury.json abi/v1/ITreasury.json
git rm abi/v2/CreatorFeeProcessor.json abi/v2/FeeCollector.json abi/v2/FeeTo.json abi/v2/LPManager.json abi/v2/NadFunFactory.json abi/v2/NadFunPair.json abi/v2/NadFunRouter.json abi/v2/NadSwapAdapter.json abi/v2/ProtocolManager.json abi/v2/Token.json abi/v2/TokenRegistry.json
```

Keep `abi/v2/BondingCurve.json`, the v1 Dex/LPManager ABIs, and `abi/v1/IToken.json`.

- [ ] **Step 4: Reduce module/controller exports**

Use these module surfaces:

```rust
// src/event/v1/mod.rs
pub mod dex;
pub mod lp_manager;
```

```rust
// src/event/v2/mod.rs
pub mod curve;
```

```rust
// src/types/v1/mod.rs
pub mod curve;
pub mod dex;
pub mod lp_manager;
```

```rust
// src/types/v2/mod.rs
pub mod curve;
```

Remove `creator`, `distributor`, `reward`, `dex_swap`, and `dex_token` from `src/db/postgres/controller/mod.rs`. Make the v2 controller declarations/re-exports:

```rust
pub mod sniping;
pub use sniping::*;
```

Keep the existing `retry_query` helper because `sniping.rs` uses it.

- [ ] **Step 5: Remove inactive controller-only helpers and tests**

Delete:

```bash
git rm tests/creator_controller.rs tests/dex_token_registration.rs tests/pool_fee_accrual.rs
```

In `src/db/postgres/controller/token.rs`, delete `get_basic_metadata`; active Curve metadata continues through `fetch_metadata`/`utils::metadata`.

In `tests/group_c_controllers.rs`, delete the Reward and Distributor sections and their imports, retaining Fee and Point tests. In `tests/group_d_controllers.rs`, delete the `dex_token.rs tests` section and its imports, retaining Chart, Price, and Account tests. In `tests/v2_controllers.rs`, delete Fee and LP sections and retain only sniping tests; change its module comment to:

```rust
//! Integration tests for the active v2 Curve sniping controller.
```

Remove the now-unused Reward/Creator/Distributor/DexToken/DexSwap helpers from `tests/common/mod.rs`. Do not delete raw migration helpers used by retained historical SQL tests.

- [ ] **Step 6: Prove only the selected implementation stacks remain**

Run:

```bash
test -d src/event/v2/curve
test -d src/event/v1/dex
test -d src/event/v1/lp_manager
test -f src/db/postgres/controller/v2/sniping.rs
if rg -n 'event::v1::(curve|reward|creator|distributor)|event::v2::(dex|factory|fee|lp_manager)|types::v2::(dex|factory|fee|lp_manager)|controller::(creator|reward|distributor|dex_swap|dex_token)' src tests; then exit 1; fi
```

Expected: the four `test` commands and guarded absence check pass.

- [ ] **Step 7: Compile and run retained focused tests**

Run:

```bash
cargo check --lib --bin observer
cargo test --lib
cargo test --test v2_controllers -- --nocapture
cargo test --test group_c_controllers -- --nocapture
cargo test --test group_d_controllers -- --nocapture
```

Expected: all commands pass with Docker available for integration tests.

- [ ] **Step 8: Commit inactive-source removal**

Run:

```bash
git add -u abi src tests
git add src/types/token.rs src/event/common/token/lp_position.rs src/event/common/token/receive.rs src/db/postgres/controller/lp_position.rs
git commit -m "refactor: remove inactive versioned indexers"
```

---

### Task 4: Replace Versioned Deployment Configuration with Generic Names

**Files:**
- Modify: `tests/giwa_runtime_contract.rs`, `src/config.rs`, all active stream/receiver files listed in the File Map.

**Interfaces:**
- Consumes: the selected active modules from Task 3.
- Produces: required, fail-fast generic address/fee statics; no removed deployment variable is read.

- [ ] **Step 1: Add the failing configuration source contract**

Append to `tests/giwa_runtime_contract.rs`:

```rust
#[test]
fn configuration_uses_only_generic_giwa_names() {
    let config = include_str!("../src/config.rs");

    for required in [
        "\"BONDING_CURVE\"",
        "\"DEX_FACTORY\"",
        "\"DEX_ROUTER\"",
        "\"LP_MANAGER\"",
        "\"CREATE_FEE_AMOUNT\"",
        "\"GRADUATE_FEE_AMOUNT\"",
        "\"BONDING_CURVE_FEE_RATE\"",
        "\"DEX_ROUTER_FEE_RATE\"",
    ] {
        assert!(config.contains(required), "missing {required}");
    }

    for forbidden in [
        "V1_BONDING_CURVE",
        "V1_DEX_FACTORY",
        "V1_DEX_ROUTER",
        "V1_LP_MANAGER",
        "V1_CREATE_FEE_AMOUNT",
        "V1_GRADUATE_FEE_AMOUNT",
        "V1_BONDING_CURVE_FEE_RATE",
        "V1_DEX_ROUTER_FEE_RATE",
        "V2_BONDING_CURVE",
        "V2_FEE_",
        "V2_LP_MANAGER",
        "V2_NAD_FUN_FACTORY",
    ] {
        assert!(!config.contains(forbidden), "stale {forbidden}");
    }
}
```

- [ ] **Step 2: Run the test to verify the current versioned names fail**

Run:

```bash
cargo test --test giwa_runtime_contract configuration_uses_only_generic_giwa_names
```

Expected: FAIL because `src/config.rs` still contains V1/V2 deployment names.

- [ ] **Step 3: Define only generic active configuration**

Replace contract address statics in `src/config.rs` with:

```rust
pub static ref BONDING_CURVE_ADDRESS: String =
    normalize_required_env_address("BONDING_CURVE");
pub static ref DEX_FACTORY_ADDRESS: String =
    normalize_required_env_address("DEX_FACTORY");
pub static ref DEX_ROUTER_ADDRESS: String =
    normalize_required_env_address("DEX_ROUTER");
pub static ref LP_MANAGER_ADDRESS: String =
    normalize_required_env_address("LP_MANAGER");
```

Keep `WNATIVE_ADDRESS` backed by `WMON`. Delete `normalize_optional_env_address` because every active address is required.

Replace fee statics with:

```rust
pub static ref GRADUATE_FEE_AMOUNT: BigDecimal = BigDecimal::from_str(
    &env::var("GRADUATE_FEE_AMOUNT")
        .expect("GRADUATE_FEE_AMOUNT must be set")
        .replace("_", ""),
)
.unwrap();

pub static ref CREATE_FEE_AMOUNT: BigDecimal = BigDecimal::from_str(
    &env::var("CREATE_FEE_AMOUNT")
        .expect("CREATE_FEE_AMOUNT must be set")
        .replace("_", ""),
)
.unwrap();

pub static ref BONDING_CURVE_FEE_RATE: BigDecimal = BigDecimal::from_str(
    &env::var("BONDING_CURVE_FEE_RATE")
        .expect("BONDING_CURVE_FEE_RATE must be set")
        .replace("_", ""),
)
.unwrap();

pub static ref DEX_ROUTER_FEE_RATE: BigDecimal = BigDecimal::from_str(
    &env::var("DEX_ROUTER_FEE_RATE")
        .expect("DEX_ROUTER_FEE_RATE must be set")
        .replace("_", ""),
)
.unwrap();
```

Make eager initialization exactly:

```rust
pub fn force_init_address_configs() {
    let _ = &*WNATIVE_ADDRESS;
    let _ = &*BONDING_CURVE_ADDRESS;
    let _ = &*DEX_FACTORY_ADDRESS;
    let _ = &*DEX_ROUTER_ADDRESS;
    let _ = &*LP_MANAGER_ADDRESS;
    tracing::info!(
        "[CONFIG] GIWA address configs normalized to EIP-55 checksum (WNATIVE={})",
        *WNATIVE_ADDRESS,
    );
}
```

- [ ] **Step 4: Rewire the active implementations**

Use the following imports/usages:

```rust
// src/event/v2/curve/stream.rs
config::{BLOCK_BATCH_SIZE, BONDING_CURVE_ADDRESS, WNATIVE_ADDRESS}
// Filter address:
.address(BONDING_CURVE_ADDRESS.parse::<Address>().unwrap())
```

```rust
// src/event/v2/curve/receive.rs
config::{BONDING_CURVE_FEE_RATE, CREATE_FEE_AMOUNT, GRADUATE_FEE_AMOUNT}
```

Replace all three former `V1_*` fee constant uses with these generic statics.

```rust
// src/event/v1/dex/stream.rs
config::{BLOCK_BATCH_SIZE, DEX_ROUTER_ADDRESS, WNATIVE_ADDRESS}
```

Replace both router-address comparisons with `DEX_ROUTER_ADDRESS`.

```rust
// src/event/v1/dex/receive.rs
use crate::config::DEX_ROUTER_FEE_RATE;
```

```rust
// src/event/v1/lp_manager/stream.rs
config::{BLOCK_BATCH_SIZE, LP_MANAGER_ADDRESS}
// Filter address:
.address(LP_MANAGER_ADDRESS.parse::<Address>().unwrap())
```

In `src/event/common/token/stream.rs`, import only the generic active addresses and construct:

```rust
static SYSTEM_ADDRESSES: LazyLock<HashSet<Address>> = LazyLock::new(|| {
    let mut set = HashSet::with_capacity(5);
    set.insert(Address::ZERO);
    for address in [
        &*BONDING_CURVE_ADDRESS,
        &*DEX_FACTORY_ADDRESS,
        &*DEX_ROUTER_ADDRESS,
        &*LP_MANAGER_ADDRESS,
    ] {
        set.insert(address.parse().expect("active GIWA address must be valid"));
    }
    set
});
```

- [ ] **Step 5: Run configuration and compile checks**

Run:

```bash
cargo test --test giwa_runtime_contract configuration_uses_only_generic_giwa_names
if rg -n 'V1_(BONDING_CURVE|DEX_FACTORY|DEX_ROUTER|LP_MANAGER|CREATE_FEE|GRADUATE_FEE)|V2_(BONDING_CURVE|FEE|LP_MANAGER|NAD_FUN_FACTORY)' src; then exit 1; fi
cargo check --lib --bin observer
```

Expected: all commands pass.

- [ ] **Step 6: Commit generic configuration**

Run:

```bash
git add src/config.rs src/event/v2/curve/stream.rs src/event/v2/curve/receive.rs src/event/v1/dex/stream.rs src/event/v1/dex/receive.rs src/event/v1/lp_manager/stream.rs src/event/common/token/stream.rs tests/giwa_runtime_contract.rs
git commit -m "refactor: use generic GIWA deployment config"
```

---

### Task 5: Write Generic GIWA Market and Fee Categories

**Files:**
- Modify: `src/event/v2/curve/receive.rs`, `src/types/fee.rs`, `tests/common/mod.rs`, `tests/group_b_controllers.rs`, controller comments.

**Interfaces:**
- Consumes: `TokenBatchData`, `MarketController`, `SwapController`, and `FeeController`.
- Produces: new Curve writes use `CURVE`, graduation uses `DEX`, Curve fee history uses `curve_buy`/`curve_sell`; token version remains `V2` and chain remains `GIWA`.

- [ ] **Step 1: Add failing semantic helper tests**

Add these helpers near `process_token_events` in `src/event/v2/curve/receive.rs`:

```rust
fn giwa_market_type(market_type: &MarketType) -> &'static str {
    match market_type {
        MarketType::Curve => "CURVE",
        MarketType::Dex => "DEX",
    }
}

fn giwa_curve_fee_type(is_buy: bool) -> FeeType {
    if is_buy {
        FeeType::CurveBuy
    } else {
        FeeType::CurveSell
    }
}
```

Add the unit test before changing call sites:

```rust
#[cfg(test)]
mod tests {
    use super::{giwa_curve_fee_type, giwa_market_type};
    use crate::types::{
        fee::FeeType,
        v2::curve::MarketType,
    };

    #[test]
    fn giwa_curve_uses_generic_database_categories() {
        assert_eq!(giwa_market_type(&MarketType::Curve), "CURVE");
        assert_eq!(giwa_market_type(&MarketType::Dex), "DEX");
        assert_eq!(giwa_curve_fee_type(true), FeeType::CurveBuy);
        assert_eq!(giwa_curve_fee_type(false), FeeType::CurveSell);
        assert_eq!(giwa_curve_fee_type(true).as_str(), "curve_buy");
        assert_eq!(giwa_curve_fee_type(false).as_str(), "curve_sell");
    }
}
```

In `tests/group_b_controllers.rs`, add after the existing chain assertion:

```rust
let (version,): (String,) =
    sqlx::query_as("SELECT version FROM token WHERE token_id = $1")
        .bind(TOKEN)
        .fetch_one(&db.pool)
        .await?;
assert_eq!(version, "V2");
```

- [ ] **Step 2: Run tests to expose the remaining V1 helper bind**

Run:

```bash
cargo test --lib event::v2::curve::receive::tests::giwa_curve_uses_generic_database_categories
cargo test --test group_b_controllers token_batch_insert_tokens_and_markets_happy_path -- --nocapture
```

Expected: the unit test passes because the new helpers define the target, while the integration test fails with version `V1` from `tests/common/mod.rs`.

- [ ] **Step 3: Route every active Curve write through generic categories**

In `src/event/v2/curve/receive.rs`:

```rust
// Create TokenBatchData
version: "V2".to_string(),
market_type: "CURVE".to_string(),

// Buy/Sell SwapBatchData
let market_type = giwa_market_type(&buy.market_type);
let market_type = giwa_market_type(&sell.market_type);

// Buy/Sell FeeHistoryEvent
fee_type: giwa_curve_fee_type(true),
fee_type: giwa_curve_fee_type(false),

// CurveSyncData
market_type: "CURVE".to_string(),

// Graduation
market_controller
    .batch_handle_graduates(&graduates_data, "DEX")
    .await
```

Remove the `V2CurveBuy`, `V2CurveSell`, `V2DexBuy`, and `V2DexSell` variants and `as_str` arms from `src/types/fee.rs`. Generic `Create`, `CurveBuy`, `CurveSell`, `SwapBuy`, `SwapSell`, `DexRouterBuy`, and `DexRouterSell` remain.

- [ ] **Step 4: Make the production-SQL test helper represent GIWA**

In `tests/common/mod.rs`, change:

```rust
let versions = vec!["V2"];
```

Keep the helper's market argument as `CURVE`, and keep the production CTE's already committed literal `chain = 'GIWA'`.

Update comments in `src/db/postgres/controller/token.rs` and `src/db/postgres/controller/market.rs` so examples say `CURVE`/`DEX`, while keeping SQL constraints and behavior unchanged.

- [ ] **Step 5: Prove no new-write path uses legacy categories**

Run:

```bash
if rg -n 'V2_CURVE|V2_DEX|FeeType::V2Curve|FeeType::V2Dex' src/event/v2/curve/receive.rs src/types/fee.rs tests/group_b_controllers.rs; then exit 1; fi
rg -n "version: \"V2\"|market_type: \"CURVE\"|batch_handle_graduates\(&graduates_data, \"DEX\"\)|FeeType::Curve(Buy|Sell)" src/event/v2/curve/receive.rs
git diff --exit-code -- migrations
```

Expected: the guarded search and migration diff pass; the positive search shows every required new-write category. Legacy values may still appear in database read-compatibility code and historical migrations.

- [ ] **Step 6: Run semantic and migration tests**

Run:

```bash
cargo test --lib event::v2::curve::receive::tests::giwa_curve_uses_generic_database_categories
cargo test --test group_b_controllers token_batch_insert_tokens_and_markets_happy_path -- --nocapture
cargo test --test token_chain -- --nocapture
cargo check --lib --bin observer
```

Expected: all commands pass.

- [ ] **Step 7: Commit generic database writes**

Run:

```bash
git add src/event/v2/curve/receive.rs src/types/fee.rs src/db/postgres/controller/token.rs src/db/postgres/controller/market.rs tests/common/mod.rs tests/group_b_controllers.rs
git commit -m "feat: write generic GIWA market and fee values"
```

---

### Task 6: Publish Generic Documentation and Verify Both Branches

**Files:**
- Modify/Create/Delete: README and event docs described in the File Map.
- Verify: all implementation commits and the migrations submodule.

**Interfaces:**
- Consumes: completed runtime, configuration, source-pruning, and database-write tasks.
- Produces: a documented, reviewed `giwa` observer/migrations pair ready for integration.

- [ ] **Step 1: Replace versioned public event docs**

Move the active implementation documentation into generic paths:

```bash
git mv docs/event/v2/curve.md docs/event/curve.md
git mv docs/event/v1/dex.md docs/event/dex.md
git mv docs/event/v1/lp-manager.md docs/event/lp-manager.md
```

In those documents, use public names `Curve`, `Dex`, and `LpManager`; mention v2/v1 only once in an “Implementation provenance” note. Delete inactive event docs:

```bash
git rm docs/event/v1/creator.md docs/event/v1/curve.md docs/event/v1/distributor.md docs/event/v1/reward.md
git rm docs/event/v2/dex.md docs/event/v2/fee.md docs/event/v2/lp-manager.md
```

- [ ] **Step 2: Document the exact runtime/config contract**

Replace the event summary in `README.md` and `docs/event-indexing.md` with:

```markdown
| Event | Contract implementation | Checkpoint |
| --- | --- | --- |
| Curve | v2 BondingCurve ABI | `curve` |
| Dex | v1 Capricorn DEX ABI | `dex` |
| LpManager | v1 LPManager ABI | `lp_manager` |
| Token | common ERC-20 stream | `token` |
| Price | common quote-price stream | `price` |
| PriceUsd | common token-USD stream | `price_usd` |
```

Document only these deployment variables:

```dotenv
BONDING_CURVE=0x...
DEX_FACTORY=0x...
DEX_ROUTER=0x...
LP_MANAGER=0x...
CREATE_FEE_AMOUNT=...
GRADUATE_FEE_AMOUNT=...
BONDING_CURVE_FEE_RATE=...
DEX_ROUTER_FEE_RATE=...
```

State that GIWA writes `token.version='V2'`, `token.chain='GIWA'`, market values `CURVE`/`DEX`, and Curve fee values `curve_buy`/`curve_sell`. State that existing MON/versioned database rows are intentionally unchanged.

- [ ] **Step 3: Remove current branch notes dedicated to inactive v2 streams**

Run:

```bash
git rm branches/feat-fee-to-claim-indexing.md branches/feat-v2-dex-pool-price-usd.md branches/fix-v2-critical-defects.md branches/fix-v2-dex-chart-volume.md branches/fix-v2-dex-swap-reserve.md
```

Do not delete historical SQL migrations or the pre-existing untracked design/plan files.

- [ ] **Step 4: Verify both repositories and immutable DB history**

Run:

```bash
git branch --show-current
git -C migrations branch --show-current
git submodule status migrations
git -C migrations log -1 --oneline --decorate
git diff --exit-code -- migrations/vault.sql migrations/dividend.sql migrations/v2_upgrade_vault_usd.sql migrations/v2_upgrade_gift_expires_at.sql migrations/seeds/v2_vault_metadata.sql
rg -n "ADD COLUMN IF NOT EXISTS chain VARCHAR|SET chain = 'MON'|SET DEFAULT 'MON'|SET NOT NULL" migrations/0036_token_chain.sql
```

Expected: both branches are `giwa`; the gitlink points to migrations commit `a6612de`; historical Vault SQL has no diff; all four token-chain invariants are present.

- [ ] **Step 5: Run focused passing verification**

Run:

```bash
cargo fmt --all
cargo build
cargo test --lib
cargo test --test giwa_runtime_contract
cargo test --test token_chain -- --nocapture
cargo test --test group_b_controllers token_batch_insert_tokens_and_markets_happy_path -- --nocapture
cargo test --test v2_controllers -- --nocapture
git diff --check
```

Expected: every command passes with Docker available for database-backed tests.

- [ ] **Step 6: Record known broader-suite failures without expanding scope**

Run:

```bash
cargo test --tests
cargo test --all-targets --no-run
```

Expected baseline exceptions:

- `cargo test --tests` may still report the three pre-existing pool-reserve bind-count failures (9 parameters supplied for SQL expecting 11).
- `cargo test --all-targets --no-run` still fails compiling `benches/sort_benchmark.rs` because `criterion` and `rayon` are not declared.

If any new failure appears outside those exact exceptions, fix it in the task that introduced it and rerun the focused suite. Do not add benchmark dependencies or alter unrelated pool-reserve SQL in this feature.

- [ ] **Step 7: Commit documentation and inspect final scope**

Run:

```bash
git add README.md docs/event-indexing.md docs/event branches
git commit -m "docs: describe GIWA single-version indexing"
git log --oneline --decorate 7772712..HEAD
git diff --stat 7772712..HEAD
git diff --submodule=log 7772712..HEAD
git status --short --branch
git -C migrations status --short --branch
```

Expected: implementation history is task-scoped; no cleanup migration or key change exists; the migrations worktree is clean on `giwa`; pre-existing untracked user files remain untracked and unchanged.
