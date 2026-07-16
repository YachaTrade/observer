# Multi-Quote Position Tracking Implementation Plan (Plan D)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend position tracking in `src/event/common/token/stream.rs` to cover all quotes in `QUOTE_CONFIGS`, not just WMON. The current tx-hash correlation model is preserved; only the input signals and per-quote lookup are generalized. After this PR, a token paired with USDC (or any other non-WMON quote) has its position flows correctly recorded in `position_history` and `position` tables under the `quote_in` / `quote_out` columns that PR #143 prepared.

**Architecture:**
1. **Preserve WMON path** — WMON contract's `Deposit(dst, wad)` / `Withdrawal(src, wad)` events continue to be the signal for native-MON flows. Semantic is clear and special; no reason to reshape.
2. **Add non-WMON Transfer filter** — for each non-WMON address in `QUOTE_CONFIGS`, add an ERC-20 `Transfer(from, to, value)` log filter. These events are parsed into a new `ParsedLog::QuoteTransfer` variant.
3. **Generalize flow aggregation** — replace `HashMap<tx_hash, (quote_in, quote_out)>` with `HashMap<tx_hash, HashMap<quote_address, (quote_in, quote_out)>>`. WMON entries come from Deposit/Withdrawal (unchanged); non-WMON entries come from QuoteTransfer where `from` or `to` equals `tx_sender`.
4. **Per-token quote_id lookup** — when building position history for a token Transfer, call `cache_manager.get_token_quote_id(token)` to find the token's quote address. If unknown, fall back to WMON (`&*WNATIVE_ADDRESS`).
5. **Per-quote USD conversion** — `create_position_history` now takes a `quote_id` instead of a pre-fetched native price. Inside, it calls `cache_manager.get_quote_usd_price(&quote_id, block_num)` for the price and `config::get_quote_decimals(&quote_id)` for the decimals. The caller no longer threads `native_price` through.
6. **transfer_type classification** — the existing match logic `(is_eoa_to_eoa_transfer, has_quote_in, has_quote_out)` stays intact. It operates on the looked-up `(quote_in, quote_out)` regardless of which quote they came from, so it's already quote-agnostic.

**Semantic invariants preserved:**
- EOA→EOA transfer detection (`is_eoa_to_eoa_transfer` = from_is_eoa && to_is_eoa && no quote flow for this tx+quote)
- Buy/Sell/LpAdd/LpRemove/TransferIn/TransferOut/Airdrop classification
- tx_sender-based flow attribution (if `tx_sender == from` or `tx_sender == to` for the token Transfer)

**Tech Stack:** Rust (edition 2024), alloy log filters, BigDecimal, sqlx, DashMap. No new dependencies.

**Branch:** `feat/v2-multi-quote-position-tracking` (branched from `v2` after PR #143 merges; if #143 is not yet merged, branch from the feat/v2-position-quote-rename tip).

**Blast radius:** medium.
- **Files modified:** 1 primary (`src/event/common/token/stream.rs`). Possibly `src/db/cache/mod.rs` if `get_token_quote_id` needs a small batch-lookup helper (to be determined during recon).
- **Files NOT touched:** V1 curve/dex/receive.rs (unchanged — they read trade events directly, not position history builder); `types/token.rs` (PositionHistoryEvent fields already renamed in PR #143); migrations (no schema change).

---

## File Structure

### New files
None.

### Modified files
- `src/event/common/token/stream.rs` — all the work lives here

### Potentially modified
- `src/db/cache/mod.rs` — only if a batch version of `get_token_quote_id` is needed for performance. Default: use the existing single-token lookup in a loop; optimize only if profiling shows it's hot.

---

## Task 1: Create feature branch

- [ ] **Step 1: Sync and branch**

```bash
cd /Users/gyu/project/nads-pump/observer
git checkout v2
git pull origin v2
git checkout -b feat/v2-multi-quote-position-tracking
```

If PR #143 is not yet merged, branch from its tip instead:

```bash
git checkout feat/v2-position-quote-rename
git checkout -b feat/v2-multi-quote-position-tracking
```

Verify `git log -1` shows the expected base.

- [ ] **Step 2: Commit the plan doc**

```bash
git add docs/superpowers/plans/2026-04-10-multi-quote-position-tracking.md
git commit -m "docs: add multi-quote position tracking plan (Plan D)"
```

---

## Task 2: Extend `ParsedLog` with QuoteTransfer variant

**Files:**
- Modify: `src/event/common/token/stream.rs`

### Step 1: Add the variant

Find the `ParsedLog` enum (around lines 74-98) and add a new variant:

```rust
/// Non-WMON quote ERC-20 Transfer. Direction resolved against tx_sender later.
QuoteTransfer {
    quote_id: Address,
    from: Address,
    to: Address,
    amount: BigDecimal,
    tx_hash: Arc<String>,
},
```

Place it after the existing `Transfer`, `Deposit`, `Withdrawal` variants.

### Step 2: Verify build

```bash
cargo build 2>&1 | tail -10
```

Expected: warnings about unused `QuoteTransfer` variant. That's fine for this intermediate state.

### Step 3: Commit (or defer to Task 3's commit)

Defer. Combine with Task 3.

---

## Task 3: Add filter + parser for non-WMON quote Transfer events

**Files:**
- Modify: `src/event/common/token/stream.rs`

This task adds the log fetching for non-WMON quote contracts and their parsing into `ParsedLog::QuoteTransfer`.

### Step 1: Locate the log fetching section

Read the relevant portion of `stream.rs`. Find:
1. The function that builds the filter for WMON Deposit/Withdrawal
2. The main block loop that fetches logs and dispatches them to parsers

Typical shape (approximate):

```rust
let wmon_filter = Filter::new()
    .address(*WMON_ADDRESS)
    .events([...])
    .from_block(from).to_block(to);

let wmon_logs = client.get_logs(&wmon_filter).await?;
for log in wmon_logs {
    let (parsed, events) = parse_wmon_log(&log);
    // ...
}
```

If the actual code uses a different pattern (e.g. a single combined filter for multiple addresses), adapt accordingly.

### Step 2: Build a list of non-WMON quote addresses

At the top of `stream_events()` (or in a static initializer), compute the set of non-WMON quote addresses once:

```rust
use crate::config::QUOTE_CONFIGS;

let non_wmon_quote_addresses: Vec<Address> = QUOTE_CONFIGS
    .iter()
    .filter(|q| q.address.to_lowercase() != WNATIVE_ADDRESS.to_lowercase())
    .filter_map(|q| q.address.parse::<Address>().ok())
    .collect();
```

If no non-WMON quotes are configured, this vector is empty and the rest of Task 3 becomes a no-op at runtime — the code still compiles.

### Step 3: Add the log filter for non-WMON quotes

Build a filter that subscribes to ERC-20 `Transfer` events on all non-WMON quote addresses in the same block range:

```rust
use alloy::sol_types::SolEvent;

if !non_wmon_quote_addresses.is_empty() {
    let quote_transfer_filter = Filter::new()
        .address(non_wmon_quote_addresses.clone())
        .event_signature(IToken::Transfer::SIGNATURE_HASH)
        .from_block(from_block)
        .to_block(to_block);

    let quote_transfer_logs = client.get_logs(&quote_transfer_filter).await?;
    for log in quote_transfer_logs {
        if let Some(parsed) = parse_quote_transfer_log(&log) {
            parsed_logs.push(parsed);
        }
    }
}
```

Note: `IToken::Transfer` is the ERC-20 Transfer event already used for token Transfer parsing — we reuse the same ABI.

If the current code uses `tokio::join!` or `JoinSet` to fetch multiple filters in parallel, add the quote transfer fetch as another branch in that join to keep the parallelism.

### Step 4: Add the `parse_quote_transfer_log` function

Place it near `parse_wmon_log`:

```rust
/// Non-WMON quote Transfer parsing. Captures amount and from/to for later
/// tx_sender-based direction inference.
fn parse_quote_transfer_log(log: &Log) -> Option<ParsedLog> {
    let tx_hash = Arc::new(log.transaction_hash?.to_string());
    let quote_id = log.address();

    let decoded = log.log_decode::<IToken::Transfer>().ok()?;
    let IToken::Transfer { from, to, value } = decoded.inner.data;

    if from == to {
        return None;
    }

    Some(ParsedLog::QuoteTransfer {
        quote_id,
        from,
        to,
        amount: to_big_decimal(value),
        tx_hash,
    })
}
```

### Step 5: Update the `tx_hash` extraction in `build_position_histories`

Find the `tx_groups` building loop (around line 566-574) and add a `QuoteTransfer` arm:

```rust
let tx_hash = match log {
    ParsedLog::Transfer { tx_hash, .. } => tx_hash.as_str(),
    ParsedLog::Deposit { tx_hash, .. } => tx_hash.as_str(),
    ParsedLog::Withdrawal { tx_hash, .. } => tx_hash.as_str(),
    ParsedLog::QuoteTransfer { tx_hash, .. } => tx_hash.as_str(),
};
```

### Step 6: Update the `wmon_tx_hashes` collection

The current code collects tx_hashes that have WMON events (to fetch tx_sender for them). Extend this to also include tx_hashes that have QuoteTransfer events. Rename the variable to `quote_tx_hashes` for clarity:

```rust
let quote_tx_hashes: HashSet<&str> = parsed_logs
    .iter()
    .filter(|log| matches!(
        log,
        ParsedLog::Deposit { .. }
            | ParsedLog::Withdrawal { .. }
            | ParsedLog::QuoteTransfer { .. }
    ))
    .map(|log| match log {
        ParsedLog::Deposit { tx_hash, .. } => tx_hash.as_str(),
        ParsedLog::Withdrawal { tx_hash, .. } => tx_hash.as_str(),
        ParsedLog::QuoteTransfer { tx_hash, .. } => tx_hash.as_str(),
        _ => unreachable!(),
    })
    .collect();

let tx_senders = fetch_tx_senders_for_hashes(&quote_tx_hashes, cache_manager.clone()).await;
```

### Step 7: Build

```bash
cargo build 2>&1 | tail -20
```

Expected: clean build (the QuoteTransfer variant is now used). There might still be an "unused variable" or similar warning if Task 4's aggregation isn't updated yet — that's fine; Task 4 handles it.

---

## Task 4: Generalize flow aggregation (`build_wmon_flows` → `build_quote_flows`)

**Files:**
- Modify: `src/event/common/token/stream.rs`

### Step 1: Rename the type alias

Find the `TxWmonFlow` type alias (around line 554) and rename it:

```rust
// Old:
/// Quote (WMON) flows: tx_sender -> (quote_in, quote_out)
type TxWmonFlow = (BigDecimal, BigDecimal);

// New:
/// Per-tx, per-quote flows keyed by quote contract address.
/// quote_address -> (quote_in, quote_out)
type TxQuoteFlows = std::collections::HashMap<Address, (BigDecimal, BigDecimal)>;
```

### Step 2: Rewrite `build_wmon_flows` as `build_quote_flows`

Find the existing `build_wmon_flows` (around lines 790-808) and replace entirely with:

```rust
/// Build per-tx, per-quote flow maps from parsed logs.
///
/// WMON flows come from Deposit/Withdrawal events, summed at the tx level
/// (the `dst`/`src` field is ignored — flows are attributed to the tx_sender later).
///
/// Non-WMON quote flows come from ERC-20 Transfer events: flows are only
/// recorded if `from` or `to` matches the tx_sender (direction follows).
fn build_quote_flows(
    parsed_logs: &[ParsedLog],
    tx_senders: &HashMap<String, Address>,
) -> HashMap<String, TxQuoteFlows> {
    let mut flows: HashMap<String, TxQuoteFlows> = HashMap::new();
    let wmon_addr = *WMON_ADDRESS;

    for log in parsed_logs {
        match log {
            ParsedLog::Deposit { amount, tx_hash } => {
                let entry = flows.entry(tx_hash.to_string()).or_default();
                let slot = entry.entry(wmon_addr).or_default();
                slot.1 += amount; // quote_out
            }
            ParsedLog::Withdrawal { amount, tx_hash } => {
                let entry = flows.entry(tx_hash.to_string()).or_default();
                let slot = entry.entry(wmon_addr).or_default();
                slot.0 += amount; // quote_in
            }
            ParsedLog::QuoteTransfer {
                quote_id,
                from,
                to,
                amount,
                tx_hash,
            } => {
                let sender = match tx_senders.get(tx_hash.as_str()) {
                    Some(s) => s,
                    None => continue,
                };

                let entry = flows.entry(tx_hash.to_string()).or_default();
                let slot = entry.entry(*quote_id).or_default();
                if from == sender {
                    slot.1 += amount; // quote_out
                }
                if to == sender {
                    slot.0 += amount; // quote_in
                }
            }
            ParsedLog::Transfer { .. } => {}
        }
    }

    flows
}
```

Notes:
- The key is now `String` (owned) not `&str` because the function signature previously borrowed the slice and we need a more flexible lifetime. Convert callers accordingly.
- WMON entries use `*WMON_ADDRESS` (the static Address) as the inner key.
- Non-WMON entries use the log's contract address (captured in `ParsedLog::QuoteTransfer::quote_id`).

### Step 3: Update the call in `build_position_histories`

Find where `build_wmon_flows(parsed_logs)` is called (around line 591) and update it:

```rust
// Old:
let wmon_flows = build_wmon_flows(parsed_logs);

// New:
let quote_flows = build_quote_flows(parsed_logs, &tx_senders);
```

Note: `build_quote_flows` now takes `tx_senders` as a second argument (needed to attribute QuoteTransfer direction). `tx_senders` is already fetched earlier in the function on the line above.

Also the variable must be renamed everywhere downstream: `wmon_flows` → `quote_flows`.

### Step 4: Update the lookup in the transfer loop

Find the lines that look up the tx's flow (around line 609):

```rust
// Old:
let tx_wmon_flow = wmon_flows.get(*tx_hash);
```

Replace with a helper that fetches the per-token flow. Since each token might have a different quote, we defer the lookup until we know which token's flow we need. For now, just rename the variable:

```rust
let tx_quote_flows = quote_flows.get(*tx_hash);
```

(Actual per-token flow selection happens in the transfer loop body in Task 5.)

### Step 5: Build

```bash
cargo build 2>&1 | tail -30
```

Expected: errors about `tx_wmon_flow` being used as a `(BigDecimal, BigDecimal)` when it's now a `HashMap<Address, (BigDecimal, BigDecimal)>`. These are exactly the sites Task 5 updates.

---

## Task 5: Per-token quote_id lookup and per-quote flow selection

**Files:**
- Modify: `src/event/common/token/stream.rs`
- Possibly: `src/db/cache/mod.rs` (only if batch optimization needed)

### Step 1: Update the transfer handling loop

Find the `for transfer in transfers` loop (around line 629). The current loop body references `tx_wmon_flow` (or now `tx_quote_flows`) to compute `(quote_in, quote_out)`. Rewrite the relevant section:

```rust
for transfer in transfers {
    if let ParsedLog::Transfer {
        token,
        from,
        to,
        amount,
        tx_hash,
        block_number,
        block_timestamp,
        tx_index,
        log_index,
    } = transfer
    {
        // Resolve the token's quote_id (default to WMON if unknown/unregistered).
        let token_str = token.to_string();
        let quote_id_str: String = cache_manager
            .get_token_quote_id(&token_str)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| WNATIVE_ADDRESS.to_lowercase());

        let quote_addr: Address = quote_id_str
            .parse()
            .unwrap_or_else(|_| *WMON_ADDRESS);

        // Look up the (quote_in, quote_out) for this specific quote in this tx.
        let this_tx_quote_flow: Option<(BigDecimal, BigDecimal)> = tx_quote_flows
            .and_then(|flows| flows.get(&quote_addr))
            .cloned();

        // EOA checks unchanged
        let from_str = from.to_string();
        let to_str = to.to_string();
        let (from_is_eoa, to_is_eoa) = tokio::join!(
            async {
                *from != *ZERO_ADDRESS
                    && cache_manager.check_is_eoa(&from_str).await.unwrap_or(false)
            },
            async {
                *to != *ZERO_ADDRESS
                    && cache_manager.check_is_eoa(&to_str).await.unwrap_or(false)
            }
        );

        let tx_sender = tx_senders.get(tx_hash.as_str());

        // EOA→EOA transfer = both are EOA AND no quote flow for this quote in this tx
        let is_eoa_to_eoa_transfer =
            from_is_eoa && to_is_eoa && this_tx_quote_flow.is_none();

        // from's side
        if from_is_eoa {
            let (quote_in, quote_out) = match tx_sender == Some(from) {
                true => this_tx_quote_flow.clone().unwrap_or_default(),
                false => (BigDecimal::from(0), BigDecimal::from(0)),
            };

            let has_quote_in = quote_in > BigDecimal::from(0);
            let has_quote_out = quote_out > BigDecimal::from(0);

            let transfer_type = match (is_eoa_to_eoa_transfer, has_quote_in, has_quote_out) {
                (true, _, _) => TransferType::TransferOut,
                (false, true, _) => TransferType::Sell,
                (false, _, true) => TransferType::LpAdd,
                _ => TransferType::Other,
            };

            positions.push(create_position_history(
                *token,
                *from,
                tx_hash,
                *block_number,
                *block_timestamp,
                *tx_index,
                *log_index,
                quote_in,
                quote_out,
                BigDecimal::from(0),
                amount.clone(),
                &quote_id_str,
                cache_manager.clone(),
                transfer_type,
                None,
            ).await);
        }

        // to's side — symmetric
        if to_is_eoa {
            let (quote_in, quote_out) = match tx_sender == Some(to) {
                true => this_tx_quote_flow.clone().unwrap_or_default(),
                false => (BigDecimal::from(0), BigDecimal::from(0)),
            };

            let has_quote_in = quote_in > BigDecimal::from(0);
            let has_quote_out = quote_out > BigDecimal::from(0);

            let transfer_type = match (is_eoa_to_eoa_transfer, has_quote_out, has_quote_in, from_is_eoa) {
                (true, _, _, _) => TransferType::TransferIn,
                (false, true, _, _) => TransferType::Buy,
                (false, _, true, _) => TransferType::LpRemove,
                (false, _, _, false) => TransferType::Airdrop,
                _ => TransferType::Other,
            };

            let sender_address = match is_eoa_to_eoa_transfer {
                true => Some(Arc::new(from_str.clone())),
                false => None,
            };

            positions.push(create_position_history(
                *token,
                *to,
                tx_hash,
                *block_number,
                *block_timestamp,
                *tx_index,
                *log_index,
                quote_in,
                quote_out,
                amount.clone(),
                BigDecimal::from(0),
                &quote_id_str,
                cache_manager.clone(),
                transfer_type,
                sender_address,
            ).await);
        }
    }
}
```

Key differences from old code:
- `quote_id_str` resolved per token via `get_token_quote_id`, fallback to `WNATIVE_ADDRESS.to_lowercase()`
- `this_tx_quote_flow` is selected from the nested map by quote address (not just tx)
- `create_position_history` is now `async` and takes `quote_id_str` + `cache_manager` instead of a pre-fetched `native_price`
- EOA→EOA detection uses the **per-quote** flow absence (not the overall tx having no WMON event)

### Step 2: Delete the `price_cache` and `get_native_price` usage

The old code had a per-block price cache pre-fetched before the loop. That's no longer needed because per-quote prices are fetched inside `create_position_history` (per quote_id) lazily.

Find and delete:
```rust
let mut price_cache: HashMap<i64, Option<Arc<BigDecimal>>> = HashMap::new();
```

And the `let native_price = match price_cache.get(&block_num) { ... }` block.

### Step 3: Build

```bash
cargo build 2>&1 | tail -40
```

Expected: errors about `create_position_history` signature mismatch — Task 6 fixes it.

---

## Task 6: Rewrite `create_position_history` for per-quote USD conversion

**Files:**
- Modify: `src/event/common/token/stream.rs`

### Step 1: Rewrite the function

Find `create_position_history` (around line 824). Replace with:

```rust
/// PositionHistory 생성 헬퍼 (quote-aware)
#[allow(clippy::too_many_arguments)]
async fn create_position_history(
    token: Address,
    account: Address,
    tx_hash: &Arc<String>,
    block_number: u64,
    block_timestamp: u64,
    tx_index: u64,
    log_index: u64,
    quote_in: BigDecimal,
    quote_out: BigDecimal,
    token_in: BigDecimal,
    token_out: BigDecimal,
    quote_id: &str,
    cache_manager: Arc<CacheManager>,
    transfer_type: TransferType,
    sender_address: Option<Arc<String>>,
) -> PositionHistoryEvent {
    // Per-quote USD price lookup (fallback chain inside get_quote_usd_price).
    let quote_price = cache_manager
        .get_quote_usd_price(quote_id, block_number as i64)
        .await;

    let quote_decimals = get_quote_decimals(quote_id);

    let (usd_in, usd_out) = match quote_price {
        Some(price) => (
            (&quote_in / quote_decimals) * &*price,
            (&quote_out / quote_decimals) * &*price,
        ),
        None => (BigDecimal::from(0), BigDecimal::from(0)),
    };

    PositionHistoryEvent {
        account_id: Arc::new(account.to_string()),
        token_id: Arc::new(token.to_string()),
        quote_in: Arc::new(quote_in),
        quote_out: Arc::new(quote_out),
        usd_in: Arc::new(usd_in),
        usd_out: Arc::new(usd_out),
        token_in: Arc::new(token_in),
        token_out: Arc::new(token_out),
        transaction_hash: tx_hash.clone(),
        block_number,
        block_timestamp,
        tx_index,
        log_index,
        transfer_type,
        sender_address,
    }
}
```

Changes:
- `#[inline]` removed (function is now async, inline doesn't apply the same way)
- `native_price: &Option<Arc<BigDecimal>>` parameter removed
- `quote_id: &str` + `cache_manager: Arc<CacheManager>` parameters added
- Body calls `cache_manager.get_quote_usd_price(quote_id, block_number as i64).await` for price
- Body calls `get_quote_decimals(quote_id)` for decimals
- USD conversion uses the per-quote price and decimals
- Returns `PositionHistoryEvent` (no change in return type)

### Step 2: Delete the now-unused `get_native_price` helper

Around line 810. Delete the entire function:

```rust
// Delete:
async fn get_native_price(cache_manager: &CacheManager, block_num: i64) -> Option<Arc<BigDecimal>> {
    ...
}
```

If anything else in the file still references `get_native_price`, the compiler will complain — search and remove/replace accordingly.

### Step 3: Build

```bash
cargo build 2>&1 | tail -30
```

Expected: clean build. If errors remain, they're likely:
- A missed `native_price` reference in the callers
- Lifetime issues from the `async fn` change (the callers must `.await` the call)
- `get_quote_decimals` not in scope — add `use crate::config::get_quote_decimals;` at the top of the file (already added by Task 4 of the previous plan)

### Step 4: Run tests

```bash
cargo test --lib 2>&1 | tail -15
```

Expected: all tests pass (there are no unit tests touching this specific file).

### Step 5: Commit Tasks 2-6 together

```bash
git add src/event/common/token/stream.rs
git commit -m "refactor: multi-quote position tracking via per-quote Transfer events"
```

If `src/db/cache/mod.rs` was touched for a batch quote_id lookup helper, include it:

```bash
git add src/event/common/token/stream.rs src/db/cache/mod.rs
git commit -m "refactor: multi-quote position tracking via per-quote Transfer events"
```

---

## Task 7: Verification + PR

- [ ] **Step 1: Grep for stale references**

```bash
grep -n "build_wmon_flows\|TxWmonFlow\|wmon_flow\|wmon_flows\|native_price\|get_native_price" src/event/common/token/stream.rs
```

Expected: zero matches.

- [ ] **Step 2: Build clean**

```bash
cargo build 2>&1 | tail -10
```

Expected: zero errors, zero new warnings.

- [ ] **Step 3: Clippy**

```bash
cargo clippy --lib 2>&1 | grep -A 2 "warning:\|error:" | grep "src/event/common/token/stream.rs" | head -10
```

Expected: no new warnings in this file beyond the pre-existing lines 60/63 ones (collapsible if statements).

- [ ] **Step 4: Full test suite**

```bash
cargo test --lib 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 5: `MODE=testnet cargo build`**

```bash
MODE=testnet cargo build 2>&1 | tail -5
```

Expected: clean build.

- [ ] **Step 6: Push branch**

```bash
git push -u origin feat/v2-multi-quote-position-tracking
```

- [ ] **Step 7: Open PR**

```bash
gh pr create --base v2 --title "refactor: multi-quote position tracking via per-quote Transfer events" --body "$(cat <<'EOF'
## Summary
Extends position tracking in `src/event/common/token/stream.rs` from WMON-only to all quotes in `QUOTE_CONFIGS`. The tx-hash correlation model is preserved: a token's position history entries still pair with quote flows from the same transaction. Non-WMON quotes (USDC, USDT, etc.) now contribute their flows via ERC-20 `Transfer(from, to, value)` events, where `from == tx_sender` means quote_out and `to == tx_sender` means quote_in.

## Architecture
1. WMON path unchanged — `Deposit`/`Withdrawal` events remain the signal for native-MON flows.
2. New per-quote filter — for each non-WMON address in `QUOTE_CONFIGS`, subscribe to `Transfer` events in the same block range.
3. New `ParsedLog::QuoteTransfer` variant captures `(quote_id, from, to, amount, tx_hash)`.
4. Flow aggregation becomes two-level: `HashMap<tx_hash, HashMap<quote_address, (quote_in, quote_out)>>`.
5. Per-token quote_id lookup via `cache_manager.get_token_quote_id(token)`; fallback to WMON when unknown.
6. Per-quote USD conversion: `create_position_history` is now async, calls `cache_manager.get_quote_usd_price(quote_id, block)` and `config::get_quote_decimals(quote_id)` inside.
7. EOA→EOA transfer detection now keyed on per-quote flow absence (not overall tx WMON absence).

## Scope
- 1 file modified: `src/event/common/token/stream.rs`
- No schema changes; no migration
- No new dependencies
- V1 curve/dex/receive.rs untouched (different code path — reads trade events directly)

## Preserved semantic invariants
- transfer_type classification (Buy/Sell/LpAdd/LpRemove/TransferIn/TransferOut/Airdrop) — match logic unchanged
- tx_sender-based attribution — unchanged
- Backward compat: if `QUOTE_CONFIGS` only has WMON, runtime behavior is identical to pre-PR

## Test plan
- [x] `cargo build` clean
- [x] `cargo test --lib` passing
- [x] `cargo clippy` no new warnings in touched file
- [x] `MODE=testnet cargo build` compiles
- [ ] Runtime smoke on mainnet: WMON position flows continue populating
- [ ] Runtime smoke on testnet with a configured non-WMON quote: verify position_history.quote_in/quote_out populate for the non-WMON quote's trades

## Rollback
Revert the PR. No schema migration to reverse.
EOF
)"
```

Return the PR URL.

---

## Self-Review Notes

- **Spec coverage:** user requested Plan D = generalize position tracking to multi-quote. Core mechanism (tx-hash correlation) is preserved; input signals and lookup keys are generalized. Covered.
- **No placeholders:** code blocks show exact before/after. Actual line numbers may drift — use the Read tool liberally during implementation.
- **Open decisions (answered):**
  - Strategy 1 (per-quote Transfer filter) ← chosen over Strategy 2 (tx_receipt lookup)
  - WMON Deposit/Withdrawal ← kept (not unified with Transfer)
  - Fallback for unknown quote_id ← WMON (via `WNATIVE_ADDRESS.to_lowercase()`)
- **Out of scope (deliberately):**
  - Per-user log filtering (requires known user set — not available statically)
  - Unifying WMON path with Transfer-based detection (keeps the semantic distinction between Deposit/Withdrawal and normal transfer)
  - Any query on the V1 curve/dex receive.rs paths (they compute USD via V1-specific trade-event fields, not via the position history builder)
- **Risks:**
  - `get_token_quote_id` is called per-token per-tx — could be a hot path. If profiling shows it's a bottleneck, add a batch lookup or tx-local cache. Not optimizing prematurely.
  - `get_quote_usd_price` is now called per position entry instead of once per block — same concern, but the unified fallback chain already caches.
  - `create_position_history` becoming `async` propagates `.await`s upward. The caller loop already runs inside an `async fn`, so no architectural issue — just syntactic.
- **Follow-ups (not this PR):**
  - Batch `get_token_quote_id` lookup if it becomes hot
  - Per-tx quote_price cache inside `build_position_histories` if redundant lookups dominate
  - Consider adding a runtime metric for "unknown quote_id fallback to WMON" rate
