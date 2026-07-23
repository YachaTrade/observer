# Price Stream Catch-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Price publish promptly by parallelizing timestamp lookup safely and resuming from the last complete persisted quote block.

**Architecture:** Keep Price's existing 25-block Pyth buckets and 1,001-block range policy. Add an isolated bounded timestamp collector, a Price-only PostgreSQL resume-watermark calculation, and acknowledged persistence so a failed range cannot advance.

**Tech Stack:** Rust 2024, Tokio, SQLx, PostgreSQL, existing event channels.

## Global Constraints

- Preserve Curve → Price, Dex/LpManager/Vault → Curve, and Token → Curve ordering.
- Keep existing event names and checkpoint identities stable.
- Limit timestamp lookup to 32 concurrent operations and restore block order before processing.
- Any timestamp, Pyth-feed, channel, or persistence failure must not advance Price.
- Do not modify migrations, `.env*`, PriceUsd behavior, or contract indexing.
- Do not commit, push, open a PR, merge, or deploy during this task.

---

### Task 1: Bounded Price timestamp collection

**Files:**
- Modify: `src/event/common/price/stream.rs`
- Test: `src/event/common/price/stream.rs`

**Interfaces:**
- Produces: `collect_block_timestamps(from_block, to_block, max_concurrency, load_timestamp) -> Result<Vec<(u64, u64)>>`
- Consumes: the existing `get_block_timestamp(&RpcClient, u64)` function.

- [ ] **Step 1: Write failing tests**

Add async unit tests that use atomics and short Tokio delays to assert that
timestamp calls overlap, peak concurrency does not exceed the supplied limit,
results are block-sorted, and a single lookup error returns `Err`.

- [ ] **Step 2: Run tests and verify RED**

Run `cargo test event::common::price::stream::tests --lib`. The tests must fail
because `collect_block_timestamps` does not exist.

- [ ] **Step 3: Implement bounded collection**

Use a Tokio `JoinSet` with at most `max_concurrency` tasks in flight. Attach
block context to errors and sort the final vector by block number.

- [ ] **Step 4: Use it in the Price range**

Replace the sequential timestamp loop with the helper. Build the bucket map
only after the complete timestamp vector succeeds.

- [ ] **Step 5: Verify GREEN**

Run `cargo test event::common::price::stream::tests --lib`.

### Task 2: Complete Price restart watermark

**Files:**
- Modify: `src/sync/stream.rs`
- Test: `src/sync/stream.rs`

**Interfaces:**
- Produces: a pure helper that selects the minimum per-quote maximum only when
  all configured quotes are present.
- Consumes: configured quote addresses from `config::quote_configs()` and
  grouped PostgreSQL rows `(quote_id, max_block)`.

- [ ] **Step 1: Write failing tests**

Cover one quote, multiple quotes with different maxima, case-insensitive quote
IDs, a missing configured quote, and a stored maximum before `START_BLOCK`.

- [ ] **Step 2: Run tests and verify RED**

Run `cargo test sync::stream::tests --lib`. New tests must fail because the
resume helper does not exist.

- [ ] **Step 3: Implement the pure selector and SQL loader**

Query maximum Price block per quote, select the minimum complete maximum, and
override only `EventType::Price` with `max_block + 1`. Preserve the configured
fallback for incomplete or absent rows.

- [ ] **Step 4: Verify GREEN**

Run `cargo test sync::stream::tests --lib`.

### Task 3: Fail-closed Pyth and persistence

**Files:**
- Modify: `src/event/common/price/mod.rs`
- Modify: `src/event/common/price/receive.rs`
- Modify: `src/event/common/price/stream.rs`
- Test: `src/event/common/price/stream.rs`
- Test: `src/event/common/price/mod.rs`

**Interfaces:**
- `PriceEventChannel` becomes `AcknowledgedEventChannel<UpdatePrice>`.
- Price receiver acknowledges only after all quote rows persist.

- [ ] **Step 1: Write failing tests**

Add a channel test proving receiver failure reaches the sender, plus pure batch
completion tests for missing feeds and failed bucket fetches where practical.

- [ ] **Step 2: Run tests and verify RED**

Run the focused Price library tests and confirm the new failure propagation
assertions fail.

- [ ] **Step 3: Implement fail-closed processing**

Abort the range if a bucket timestamp, Pyth request, or configured feed is
missing. Acknowledge success only after all `batch_insert_prices` calls
succeed. Propagate channel failure and advance the Price watermark only after
acknowledgment.

- [ ] **Step 4: Verify GREEN**

Run the focused Price tests and `cargo test --lib`.

### Task 4: Integration validation and review

**Files:**
- Modify only files required by review findings.

- [ ] **Step 1: Run repository checks**

Run:

```bash
cargo fmt --all -- --check
cargo clippy -- -D warnings
cargo test --lib
cargo test --test giwa_runtime_contract
cargo test --verbose
cargo build --verbose
```

- [ ] **Step 2: Run local observer smoke test**

Use existing local PostgreSQL/Redis and a non-secret database URL override.
Confirm timestamp completion is materially faster, `price_events` sends, the
Price receiver advances, and Curve no longer waits on an uninitialized Price
receiver.

- [ ] **Step 3: Review at xhigh**

Run an independent read-only review against `origin/dev`, focusing on bounded
concurrency, range atomicity, restart watermark safety, ordering, and
persistence acknowledgment. Fix Critical and Important findings and rerun the
covering checks.
