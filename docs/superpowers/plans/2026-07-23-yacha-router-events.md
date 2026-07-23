# YachaRouter and LPManager Event Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode the deployed YachaRouter and LPManager event signatures while preserving the existing observer checkpoints and downstream records.

**Architecture:** Canonical ABI arrays come from `new_contract` commit `8ee03dd`. Router event names are adapted at the stream boundary to existing DEX domain records; LPManager event fields are adapted to the existing allocation/collection persistence boundary without historical contract calls or a destructive database migration.

**Tech Stack:** Rust 2024, Alloy `sol!`, Tokio, SQLx, PostgreSQL.

## Global Constraints

- Source ABIs are `/Users/gyu/project/giwa/new_contract/abis/YachaRouter.json` and `/Users/gyu/project/giwa/new_contract/abis/LPManager.json`.
- `YACHA_ROUTER=0x733132B6f0FEbd58D062f61657F1b3dbb2aDEB5A`.
- Keep `DEX_ROUTER_FEE_RATE`.
- Preserve `dex` and `lp_manager` checkpoints.
- Do not add old-name compatibility fallbacks.
- Do not run a database migration or modify persisted rows.

---

### Task 1: Lock the active contract boundary

**Files:**
- Modify: `tests/giwa_runtime_contract.rs`
- Create: `abi/YachaRouter.json`
- Create: `abi/LPManager.json`
- Delete: `abi/GiwaRouter.json`
- Delete: `abi/ILpManager.json`
- Modify: `src/config.rs`
- Modify: `src/event/common/token/stream.rs`

**Interfaces:**
- Consumes: canonical ABI event names and `YACHA_ROUTER`.
- Produces: `YACHA_ROUTER_ADDRESS`, `YachaRouter`, and `LPManager` symbols for stream tasks.

- [ ] **Step 1: Write the failing runtime-contract test**

Add assertions requiring `"YACHA_ROUTER"`, `YACHA_ROUTER_ADDRESS`,
`abi/YachaRouter.json`, `abi/LPManager.json`, `RouterBuy`, `RouterSell`,
`Allocate`, and `Collect`, while forbidding the old address key, old address
static, and old ABI paths.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test giwa_runtime_contract configuration_uses_only_generic_giwa_names`

Expected: FAIL because `YACHA_ROUTER` is absent.

- [ ] **Step 3: Install the canonical ABIs and rename config consumers**

Copy the canonical ABI arrays, remove old ABI files, define:

```rust
pub static ref YACHA_ROUTER_ADDRESS: String =
    normalize_required_env_address("YACHA_ROUTER");
```

Replace active token-system-address use of `DEX_ROUTER_ADDRESS` with
`YACHA_ROUTER_ADDRESS`.

- [ ] **Step 4: Run the focused runtime-contract test**

Run: `cargo test --test giwa_runtime_contract configuration_uses_only_generic_giwa_names`

Expected: PASS.

### Task 2: Decode deployed router and LPManager events

**Files:**
- Modify: `src/event/dex/stream.rs`
- Modify: `src/event/lp_manager/stream.rs`
- Modify: `src/types/lp_manager.rs`
- Modify: `src/db/postgres/controller/lp.rs`
- Modify: `tests/common/mod.rs`
- Modify: `tests/group_b_controllers.rs`

**Interfaces:**
- Consumes: `YachaRouter::{RouterBuy,RouterSell}` and
  `LPManager::{Allocate,Collect}`.
- Produces: unchanged `DexRouterBuy`, `DexRouterSell`,
  `LpManagerEvent::Allocate`, and `LpManagerEvent::Collect` downstream events.

- [ ] **Step 1: Write failing ABI round-trip tests**

Require the new Rust ABI symbols and round-trip their deployed fields:

```rust
let _ = YachaRouter::RouterBuy::SIGNATURE_HASH;
let _ = YachaRouter::RouterSell::SIGNATURE_HASH;
let _ = LPManager::Allocate::SIGNATURE_HASH;
let _ = LPManager::Collect::SIGNATURE_HASH;
```

- [ ] **Step 2: Run tests to verify compilation fails**

Run: `cargo test --lib event::dex::stream::tests::yacha_router_and_v3_pool_signatures_resolve`

Expected: FAIL because `YachaRouter::RouterBuy` does not yet exist.

- [ ] **Step 3: Implement minimal stream decoding**

Use `YachaRouter::RouterBuy/RouterSell` in the DEX filter and match arms. Use
`LPManager::Allocate/Collect` in the LP filter and match arms, mapping
`quoteAmount`, `tokenAmount`, and `timestamp` directly. Remove the LP
`config()` RPC call and the three obsolete split fields from `Collect`.

- [ ] **Step 4: Keep the existing schema writable without legacy event fields**

Use SQL numeric constants for required historical columns:

```sql
INSERT INTO lp_collect_history (
    token_id, quote_amount, token_amount, c_amount, ft_amount, ct_amount,
    transaction_hash, tx_index, log_index, created_at
)
VALUES ($1, $2, $3, 0, 0, 0, $4, $5, $6, $7)
```

Apply the same mapping to batch SQL and update test bind helpers.

- [ ] **Step 5: Run focused tests**

Run: `cargo test --lib event::dex::stream::tests`

Expected: PASS.

Run: `cargo test --test giwa_runtime_contract`

Expected: PASS.

### Task 3: Active configuration, docs, and full validation

**Files:**
- Modify: `.env.example`
- Modify: ignored `.env`
- Modify: ignored `.env.testnet`
- Modify: `README.md`
- Modify: `docs/event-indexing.md`
- Modify: `docs/event/dex.md`
- Modify: `docs/event/lp-manager.md`

**Interfaces:**
- Consumes: deployed address and event names from Tasks 1-2.
- Produces: operator configuration and active indexing documentation.

- [ ] **Step 1: Update tracked configuration and active docs**

Replace active GiwaRouter/DEX_ROUTER references with
YachaRouter/YACHA_ROUTER and document RouterBuy/RouterSell and
Allocate/Collect fields.

- [ ] **Step 2: Update ignored env files without displaying contents**

Mechanically replace the router key and set the supplied address in `.env` and
`.env.testnet`; report only whether both files were updated.

- [ ] **Step 3: Format and validate**

Run:

```bash
cargo fmt --all -- --check
cargo clippy -- -D warnings
cargo test --lib
cargo test --test giwa_runtime_contract
cargo test --verbose
cargo build --verbose
```

Expected: all commands pass. Database-backed tests may be skipped only when
PostgreSQL/Redis infrastructure is unavailable, and must be reported.

- [ ] **Step 4: Inspect the final diff**

Run `git diff --check`, `git status --short`, and review every changed file for
old active names, ABI drift, secret exposure, and unrelated changes.
