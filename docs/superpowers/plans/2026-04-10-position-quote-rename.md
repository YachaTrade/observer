# Position `native` → `quote` Rename + NATIVE_DECIMALS Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two coupled refactors that land in one PR:
1. Rename `native_in` / `native_out` columns in `position_history` and `position` tables (and every Rust symbol that references them) to `quote_in` / `quote_out`, matching the quote-aware terminology introduced by Plan A + Plan B.
2. Eliminate all external uses of the hardcoded `NATIVE_DECIMALS` (10^18) constant. Replace the 22 call sites across V1 and token-event code with `config::get_quote_decimals(&WNATIVE_ADDRESS)`, making `QUOTE_CONFIGS` the single source of truth for per-quote decimals. `NATIVE_DECIMALS` becomes private to `config.rs` as the fallback value for `get_quote_decimals` when a quote_id is not registered.

**Architecture:**
1. **DB migration** renames two columns on each of `position_history` and `position` (four `ALTER TABLE ... RENAME COLUMN` statements), then recreates the `update_position_on_history` trigger function whose body references the old column names. Indexes are unaffected.
2. **`PositionHistoryEvent` struct** in `src/types/token.rs` renames the two fields.
3. **`src/event/common/token/stream.rs`** renames ~25 `native_*` occurrences. The two `NATIVE_DECIMALS` uses are additionally replaced with `get_quote_decimals(&WNATIVE_ADDRESS)`.
4. **`src/db/postgres/controller/position.rs`** renames ~15 occurrences in SQL and bind chain.
5. **`src/event/v1/curve/receive.rs`** — 7 `NATIVE_DECIMALS` uses replaced with `get_quote_decimals(&WNATIVE_ADDRESS)`. Top-level import updated.
6. **`src/event/v1/dex/receive.rs`** — 12 `NATIVE_DECIMALS` uses replaced. The file currently has ~6 inline `use crate::config::NATIVE_DECIMALS;` statements scattered in match arms; these are removed.
7. **`src/config.rs`** — `NATIVE_DECIMALS` loses its `pub` visibility and is renamed to `FALLBACK_DECIMALS`. The only remaining usage is inside `get_quote_decimals` as the fallback when a quote_id is not found in `QUOTE_CONFIGS`. External callers have been rewritten to go through `get_quote_decimals`.

**Semantic note:** no runtime behavior change. `get_quote_decimals(&WNATIVE_ADDRESS)` returns the WMON decimals from `QUOTE_CONFIGS`, which is always 18, matching the old `NATIVE_DECIMALS` constant. If a new quote with different decimals is added to `QUOTE_CONFIGS`, V1 code will still use WMON (because it explicitly passes `&WNATIVE_ADDRESS`); V1 is structurally WMON-only. The position-tracking code in `token/stream.rs` also remains WMON-only for now (it only listens to WMON Deposit/Withdrawal events); generalizing to per-quote flow tracking is out of scope.

**Tech Stack:** Rust (edition 2024), PostgreSQL, sqlx 0.8 runtime queries, BigDecimal. No new dependencies.

**Branch:** `feat/v2-position-quote-rename` (branched from `v2` after Plan B merged at `d883b7b`). Final PR merges into `v2`.

**Blast radius:** medium.
- **Migration:** local file only (`migrations/0019_rename_position_native_to_quote.sql`), not committed to the migrations submodule.
- **Rust files touched:** 6 — `src/config.rs`, `src/types/token.rs`, `src/event/common/token/stream.rs`, `src/db/postgres/controller/position.rs`, `src/event/v1/curve/receive.rs`, `src/event/v1/dex/receive.rs`.
- **Total Rust changes:** ~65 (40 for position rename, 22 for NATIVE_DECIMALS swap, ~3 for config.rs privatization).

---

## File Structure

### New files
- `migrations/0019_rename_position_native_to_quote.sql` — schema migration (local only).

### Modified files
- `src/types/token.rs` — `PositionHistoryEvent` field rename (2 lines).
- `src/event/common/token/stream.rs` — position rename (~25 occurrences) + NATIVE_DECIMALS swap (2 sites).
- `src/db/postgres/controller/position.rs` — position rename in SQL + bind chain (~15 occurrences).
- `src/event/v1/curve/receive.rs` — NATIVE_DECIMALS swap (7 sites + import update).
- `src/event/v1/dex/receive.rs` — NATIVE_DECIMALS swap (12 sites, remove ~6 inline imports).
- `src/config.rs` — rename `NATIVE_DECIMALS` → `FALLBACK_DECIMALS`, remove `pub`, update `get_quote_decimals` fallback reference.

---

## Task 1: Create feature branch

- [ ] **Step 1: Sync and branch**

```bash
cd /Users/gyu/project/nads-pump/observer
git checkout v2
git pull origin v2
git checkout -b feat/v2-position-quote-rename
```

Expected: branched from `d883b7b` (or later).

- [ ] **Step 2: Commit the updated plan doc**

```bash
git add docs/superpowers/plans/2026-04-10-position-quote-rename.md
git commit -m "docs: add position rename + NATIVE_DECIMALS removal plan"
```

---

## Task 2: Write the migration SQL (local file only)

**Files:**
- Create: `migrations/0019_rename_position_native_to_quote.sql`

Same as the original plan — 4 `ALTER TABLE ... RENAME COLUMN` statements + `CREATE OR REPLACE FUNCTION update_position_on_history()` with the renamed column references. This file is NOT committed to git (matches Plan B pattern). See the SQL block below.

Write file `migrations/0019_rename_position_native_to_quote.sql`:

```sql
-- Rename position tracking columns from `native_*` to `quote_*` to match
-- the multi-quote terminology introduced in the Plan A/B refactors.
--
-- No semantic change: these columns continue to hold WMON flows only
-- until the tracking logic itself is generalized (future work).

BEGIN;

ALTER TABLE position_history RENAME COLUMN native_in TO quote_in;
ALTER TABLE position_history RENAME COLUMN native_out TO quote_out;

ALTER TABLE position RENAME COLUMN native_in TO quote_in;
ALTER TABLE position RENAME COLUMN native_out TO quote_out;

CREATE OR REPLACE FUNCTION update_position_on_history()
RETURNS TRIGGER AS $$
DECLARE
    sender_position RECORD;
    avg_cost_quote NUMERIC;
    avg_cost_usd NUMERIC;
    transfer_cost_quote NUMERIC;
    transfer_cost_usd NUMERIC;
    current_balance NUMERIC;
BEGIN
    IF NEW.transfer_type = 'transfer_out' THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.account_id AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;
            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd := sender_position.usd_out / sender_position.token_in;
                transfer_cost_quote := avg_cost_quote * NEW.token_out;
                transfer_cost_usd := avg_cost_usd * NEW.token_out;
                NEW.quote_in := transfer_cost_quote;
                NEW.usd_in := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    IF NEW.transfer_type = 'transfer_in' AND NEW.sender_address IS NOT NULL THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.sender_address AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;
            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd := sender_position.usd_out / sender_position.token_in;
                transfer_cost_quote := avg_cost_quote * NEW.token_in;
                transfer_cost_usd := avg_cost_usd * NEW.token_in;
                NEW.quote_out := transfer_cost_quote;
                NEW.usd_out := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    INSERT INTO position (
        account_id, token_id,
        quote_in, quote_out,
        usd_in, usd_out,
        token_in, token_out,
        created_at, updated_at
    )
    VALUES (
        NEW.account_id, NEW.token_id,
        NEW.quote_in, NEW.quote_out,
        NEW.usd_in, NEW.usd_out,
        NEW.token_in, NEW.token_out,
        NEW.created_at, NEW.created_at
    )
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        quote_in = position.quote_in + EXCLUDED.quote_in,
        quote_out = position.quote_out + EXCLUDED.quote_out,
        usd_in = position.usd_in + EXCLUDED.usd_in,
        usd_out = position.usd_out + EXCLUDED.usd_out,
        token_in = position.token_in + EXCLUDED.token_in,
        token_out = position.token_out + EXCLUDED.token_out,
        updated_at = EXCLUDED.updated_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;
```

Do NOT `git add` this file.

---

## Task 3: Position rename across 3 files (single commit)

**Files:**
- Modify: `src/types/token.rs`
- Modify: `src/event/common/token/stream.rs`
- Modify: `src/db/postgres/controller/position.rs`

This task handles ONLY the `native_*` → `quote_*` rename. NATIVE_DECIMALS usage stays untouched here; Task 4 replaces those references separately for clarity of git history.

### Step 1: `src/types/token.rs` — rename struct fields

In `PositionHistoryEvent` (around lines 233-253):

```rust
    pub native_in: Arc<BigDecimal>,
    pub native_out: Arc<BigDecimal>,
```

→

```rust
    pub quote_in: Arc<BigDecimal>,
    pub quote_out: Arc<BigDecimal>,
```

### Step 2: `src/event/common/token/stream.rs` — mechanical rename

Apply these symbol renames globally in this file:

| Old | New |
|-----|-----|
| `native_in` | `quote_in` |
| `native_out` | `quote_out` |
| `has_native_in` | `has_quote_in` |
| `has_native_out` | `has_quote_out` |

Expected occurrences (~25):

1. Line ~88: `/// WMON Deposit (native_out) - tx_sender는 나중에 매칭` → `(quote_out)`
2. Line ~93: `/// WMON Withdrawal (native_in) - tx_sender는 나중에 매칭` → `(quote_in)`
3. Line ~387: `// Deposit 이벤트 (유저가 MON을 보내서 WMON을 받음 → native_out)` → `→ quote_out`
4. Line ~399: `// Withdrawal 이벤트 (유저가 WMON을 태워서 MON을 받음 → native_in)` → `→ quote_in`
5. Line ~553: `/// WMON flows: tx_sender -> (native_in, native_out)` → `/// Quote (WMON) flows: tx_sender -> (quote_in, quote_out)`
6. Line ~664: `let (native_in, native_out) = match tx_sender == Some(from) {` → `let (quote_in, quote_out) = ...`
7. Line ~669-670: `has_native_in`/`has_native_out` declarations → `has_quote_in`/`has_quote_out`
8. Line ~673: match tuple `(is_eoa_to_eoa_transfer, has_native_in, has_native_out)` → `has_quote_in, has_quote_out`
9. Lines ~689-690: struct literal fields `native_in,` `native_out,` → `quote_in,` `quote_out,`
10. Line ~701: `let (native_in, native_out) = match tx_sender == Some(to) {` → `let (quote_in, quote_out) = ...`
11. Lines ~706-707: `has_native_in`/`has_native_out` → `has_quote_in`/`has_quote_out`
12. Line ~710: match tuple `(is_eoa_to_eoa_transfer, has_native_out, has_native_in, from_is_eoa)` → `has_quote_out, has_quote_in`
13. Lines ~732-733: struct literal fields `native_in,` `native_out,` → `quote_in,` `quote_out,`
14. Line ~797: inline comment `// native_out` → `// quote_out`
15. Line ~801: inline comment `// native_in` → `// quote_in`
16. Line ~835: function parameter `native_in: BigDecimal,` → `quote_in: BigDecimal,`
17. Line ~836: function parameter `native_out: BigDecimal,` → `quote_out: BigDecimal,`
18. Line ~845: `(&native_in / &*NATIVE_DECIMALS) * &**price` → `(&quote_in / &*NATIVE_DECIMALS) * &**price` (NATIVE_DECIMALS kept for now — Task 4 replaces)
19. Line ~846: `(&native_out / &*NATIVE_DECIMALS) * &**price` → `(&quote_out / &*NATIVE_DECIMALS) * &**price`
20. Line ~854: struct field `native_in: Arc::new(native_in),` → `quote_in: Arc::new(quote_in),`
21. Line ~855: struct field `native_out: Arc::new(native_out),` → `quote_out: Arc::new(quote_out),`

### Step 3: `src/db/postgres/controller/position.rs` — SQL and bind chain rename

Update `batch_insert_position_history_chunk`:

1. INSERT column list (lines ~59-60): `native_in, native_out,` → `quote_in, quote_out,`
2. SELECT list (lines ~76-77): same
3. UNNEST comments (lines ~92-93): `-- native_ins`, `-- native_outs` → `-- quote_ins`, `-- quote_outs`
4. `AS t(...)` column list (line ~105): `native_in, native_out,` → `quote_in, quote_out,`
5. RETURNING clause (line ~107): same
6. Vec declarations (lines ~112-115): `let native_ins` → `let quote_ins`, `let native_outs` → `let quote_outs`, field access `h.native_in` → `h.quote_in`, `h.native_out` → `h.quote_out`
7. Bind chain (lines ~163-164): `.bind(&native_ins)` → `.bind(&quote_ins)`, `.bind(&native_outs)` → `.bind(&quote_outs)`
8. Destructured RETURNING tuple (lines ~186-187): `native_in,` `native_out,` → `quote_in,` `quote_out,`
9. Struct literal (lines ~203-204): `native_in: Arc::new(native_in),` → `quote_in: Arc::new(quote_in),`, same for out

### Step 4: Build

```bash
cargo build 2>&1 | tail -40
```

Expected: clean build. Stream.rs still uses `&*NATIVE_DECIMALS` — that's fine, it compiles because the rename of `native_in`/`native_out` to `quote_in`/`quote_out` doesn't affect the `NATIVE_DECIMALS` symbol.

### Step 5: Test

```bash
cargo test --lib 2>&1 | tail -15
```

Expected: all tests pass.

### Step 6: Commit

```bash
git add src/types/token.rs src/event/common/token/stream.rs src/db/postgres/controller/position.rs
git commit -m "refactor: rename position native_in/out to quote_in/out"
```

---

## Task 4: Replace NATIVE_DECIMALS with `get_quote_decimals(&WNATIVE_ADDRESS)`

**Files:**
- Modify: `src/event/common/token/stream.rs` (2 sites — the lines already touched in Task 3)
- Modify: `src/event/v1/curve/receive.rs` (7 sites + import)
- Modify: `src/event/v1/dex/receive.rs` (12 sites, ~6 inline imports)
- Modify: `src/config.rs` (privatize NATIVE_DECIMALS, rename to FALLBACK_DECIMALS)

This task has two logical parts: (A) update call sites to use `get_quote_decimals`, and (B) privatize the constant in config.rs. Both land in one commit.

### Step 1: `src/event/common/token/stream.rs` — replace 2 NATIVE_DECIMALS uses

Find the imports block at the top of the file. It currently imports NATIVE_DECIMALS from `crate::config`:

```rust
    V1_DEX_ROUTER_ADDRESS, V1_LP_MANAGER_ADDRESS, NATIVE_DECIMALS, WNATIVE_ADDRESS,
```

Remove `NATIVE_DECIMALS` from that line (WNATIVE_ADDRESS stays). Add `get_quote_decimals` as an import from `crate::config`:

```rust
    V1_DEX_ROUTER_ADDRESS, V1_LP_MANAGER_ADDRESS, WNATIVE_ADDRESS, get_quote_decimals,
```

Lines 845-846 (after Task 3's rename) currently read:

```rust
            (&quote_in / &*NATIVE_DECIMALS) * &**price,
            (&quote_out / &*NATIVE_DECIMALS) * &**price,
```

Replace with:

```rust
            (&quote_in / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price,
            (&quote_out / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price,
```

Note: `get_quote_decimals` returns `&BigDecimal` directly, so no `&*` deref is needed.

### Step 2: `src/event/v1/curve/receive.rs` — 7 sites + import update

Top of file (line 7), current import:

```rust
    config::{BONDING_CURVE_FEE_RATE, CREATE_FEE_AMOUNT, GRADUATE_FEE_AMOUNT, NATIVE_DECIMALS},
```

Replace `NATIVE_DECIMALS` with `WNATIVE_ADDRESS, get_quote_decimals`:

```rust
    config::{BONDING_CURVE_FEE_RATE, CREATE_FEE_AMOUNT, GRADUATE_FEE_AMOUNT, WNATIVE_ADDRESS, get_quote_decimals},
```

Locate each usage (~7 sites) and apply the replacement:

```rust
// Old:
(&*CREATE_FEE_AMOUNT / &*NATIVE_DECIMALS) * &**price
// New:
(&*CREATE_FEE_AMOUNT / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
```

```rust
// Old:
(&*GRADUATE_FEE_AMOUNT / &*NATIVE_DECIMALS) * &*price
// New:
(&*GRADUATE_FEE_AMOUNT / get_quote_decimals(&WNATIVE_ADDRESS)) * &*price
```

```rust
// Old:
(&*buy.amount_in / &*NATIVE_DECIMALS) * &**price
// New:
(&*buy.amount_in / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
```

```rust
// Old:
(&fee_native / &*NATIVE_DECIMALS) * &**price
// New:
(&fee_native / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
```

```rust
// Old:
(&*sell.amount_out / &*NATIVE_DECIMALS) * &**price
// New:
(&*sell.amount_out / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
```

There are 7 total replacements of the `&*NATIVE_DECIMALS` pattern in this file. Verify by grepping after the edit:

```bash
grep -n "NATIVE_DECIMALS" src/event/v1/curve/receive.rs
```

Expected: zero matches.

### Step 3: `src/event/v1/dex/receive.rs` — 12 sites + remove inline imports

This file has multiple inline `use crate::config::NATIVE_DECIMALS;` statements scattered inside match arms. Some are bundled with `DEX_ROUTER_FEE_RATE`:

```rust
use crate::config::{DEX_ROUTER_FEE_RATE, NATIVE_DECIMALS};
```

**For each inline `use crate::config::NATIVE_DECIMALS;`:** remove it entirely. Then add a single top-of-file import:

```rust
use crate::config::{WNATIVE_ADDRESS, get_quote_decimals};
```

(If there's already a top-level `use crate::config::...` block, extend it.)

**For each `use crate::config::{DEX_ROUTER_FEE_RATE, NATIVE_DECIMALS};`** line: replace with `use crate::config::DEX_ROUTER_FEE_RATE;` (just remove `NATIVE_DECIMALS` from the list).

**Then apply the 12 `NATIVE_DECIMALS` → `get_quote_decimals(&WNATIVE_ADDRESS)` replacements:**

```rust
// Typical pattern:
(&*buy.amount_in / &*NATIVE_DECIMALS) * &**price
// →
(&*buy.amount_in / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
```

```rust
// Bundled with DEX_ROUTER_FEE_RATE:
((&*buy.amount_in / &*NATIVE_DECIMALS) * &**price * &*DEX_ROUTER_FEE_RATE)
// →
((&*buy.amount_in / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price * &*DEX_ROUTER_FEE_RATE)
```

```rust
(&fee_native / &*NATIVE_DECIMALS) * &**price
// →
(&fee_native / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
```

```rust
(&*sell.amount_out / &*NATIVE_DECIMALS) * &**price
// →
(&*sell.amount_out / get_quote_decimals(&WNATIVE_ADDRESS)) * &**price
```

Verify after:

```bash
grep -n "NATIVE_DECIMALS" src/event/v1/dex/receive.rs
```

Expected: zero matches.

### Step 4: `src/config.rs` — privatize the constant

Find the `NATIVE_DECIMALS` declaration (around line 79):

```rust
lazy_static! {
    // 18 decimals for native token
    pub static ref NATIVE_DECIMALS: BigDecimal = BigDecimal::from_str("1000000000000000000").unwrap(); // 10^18
```

Replace with:

```rust
lazy_static! {
    // Fallback decimals (10^18) for quotes not registered in QUOTE_CONFIGS.
    // Only used internally by get_quote_decimals().
    static ref FALLBACK_DECIMALS: BigDecimal = BigDecimal::from_str("1000000000000000000").unwrap();
```

(Note: `pub` removed, name changed from `NATIVE_DECIMALS` to `FALLBACK_DECIMALS`.)

Find `get_quote_decimals` (around line 228):

```rust
/// Get decimals for a quote token. Returns NATIVE_DECIMALS (10^18) if not found.
pub fn get_quote_decimals(quote_id: &str) -> &BigDecimal {
    QUOTE_CONFIGS
        .iter()
        .find(|q| q.address == quote_id)
        .map(|q| &q.decimals)
        .unwrap_or(&*NATIVE_DECIMALS)
}
```

Replace with:

```rust
/// Get decimals for a quote token. Returns FALLBACK_DECIMALS (10^18) if not found.
pub fn get_quote_decimals(quote_id: &str) -> &BigDecimal {
    QUOTE_CONFIGS
        .iter()
        .find(|q| q.address == quote_id)
        .map(|q| &q.decimals)
        .unwrap_or(&*FALLBACK_DECIMALS)
}
```

### Step 5: Build

```bash
cargo build 2>&1 | tail -40
```

Expected: clean build. If any error mentions `NATIVE_DECIMALS`, you missed a call site — grep again and fix.

### Step 6: Grep for leftover `NATIVE_DECIMALS`

```bash
grep -rn "NATIVE_DECIMALS" src/
```

Expected: zero matches. Every reference should now be `FALLBACK_DECIMALS` (only inside `config.rs`) or `get_quote_decimals(&WNATIVE_ADDRESS)` (at call sites).

### Step 7: Run tests

```bash
cargo test --lib 2>&1 | tail -15
```

Expected: all tests pass.

### Step 8: Commit

```bash
git add src/event/common/token/stream.rs src/event/v1/curve/receive.rs src/event/v1/dex/receive.rs src/config.rs
git commit -m "refactor: replace NATIVE_DECIMALS with get_quote_decimals(WMON)"
```

---

## Task 5: Verification + PR

- [ ] **Step 1: Grep for leftover `native_in` / `native_out` / `NATIVE_DECIMALS` in code**

```bash
grep -rn "native_in\|native_out\|NATIVE_DECIMALS" src/
```

Expected: zero matches. Any surviving occurrence is a bug.

- [ ] **Step 2: Clippy on touched files**

```bash
cargo clippy --lib 2>&1 | tail -10
```

Expected: no new warnings in the 6 touched files (pre-existing warnings in other files are OK).

- [ ] **Step 3: `MODE=testnet` compile check**

```bash
MODE=testnet cargo build 2>&1 | tail -10
```

Expected: clean build.

- [ ] **Step 4: Full test suite**

```bash
cargo test --lib 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 5: Push branch**

```bash
git push -u origin feat/v2-position-quote-rename
```

- [ ] **Step 6: Open PR**

```bash
gh pr create --base v2 --title "refactor: position native->quote rename + NATIVE_DECIMALS removal" --body "$(cat <<'EOF'
## Summary
Two coupled refactors:

1. **Position rename** — `position_history` and `position` tables rename `native_in`/`native_out` columns to `quote_in`/`quote_out`. `PositionHistoryEvent` struct fields renamed. ~40 Rust occurrences mechanically updated across `types/token.rs`, `event/common/token/stream.rs`, `db/postgres/controller/position.rs`.

2. **NATIVE_DECIMALS removal** — All 22 external uses of the public `NATIVE_DECIMALS` constant replaced with `config::get_quote_decimals(&WNATIVE_ADDRESS)`, making `QUOTE_CONFIGS` the single source of truth for per-quote decimals. The constant is privatized and renamed to `FALLBACK_DECIMALS` inside `config.rs`, where it remains only as the fallback for `get_quote_decimals` when a quote_id is not registered.

Zero runtime behavior change: `get_quote_decimals(&WNATIVE_ADDRESS)` returns 18 (from `QUOTE_CONFIGS`), matching the old 10^18 constant.

## Schema migration (applied out-of-band)
`migrations/0019_rename_position_native_to_quote.sql` (local file, not committed — matches Plan B pattern):

```sql
ALTER TABLE position_history RENAME COLUMN native_in TO quote_in;
ALTER TABLE position_history RENAME COLUMN native_out TO quote_out;
ALTER TABLE position RENAME COLUMN native_in TO quote_in;
ALTER TABLE position RENAME COLUMN native_out TO quote_out;
CREATE OR REPLACE FUNCTION update_position_on_history() ... -- see file
```

Column renames are metadata-only in Postgres. Trigger function is swapped atomically via CREATE OR REPLACE.

## Why
Aligning with the multi-quote terminology introduced by Plan A (#141, PriceProvider trait) and Plan B (#142, unified price table). "native" is misleading in a world where the quote token can be USDC or any other address. Stopping direct `NATIVE_DECIMALS` usage removes the duplicated 10^18 constant and enforces `QUOTE_CONFIGS` as the source of truth.

## Files touched (6)
- `src/types/token.rs` — 2 struct field renames
- `src/event/common/token/stream.rs` — ~25 position renames + 2 NATIVE_DECIMALS swaps
- `src/db/postgres/controller/position.rs` — ~15 SQL/bind chain renames
- `src/event/v1/curve/receive.rs` — 7 NATIVE_DECIMALS swaps + import update
- `src/event/v1/dex/receive.rs` — 12 NATIVE_DECIMALS swaps + inline imports cleanup
- `src/config.rs` — NATIVE_DECIMALS → FALLBACK_DECIMALS (private)

## Test plan
- [x] `cargo build` clean
- [x] `cargo test --lib` — all passing
- [x] `cargo clippy --lib` — no new warnings in touched files
- [x] `MODE=testnet cargo build` compiles
- [x] Grep: zero leftover `native_in` / `native_out` / `NATIVE_DECIMALS` in `src/`
- [ ] **Before deploy:** apply migration 0019 to target DB
- [ ] **After deploy:** confirm WMON Deposit/Withdrawal events continue to populate `position_history.quote_in` / `position_history.quote_out`
- [ ] **After deploy:** confirm V1 curve/dex USD conversions continue to produce the same values (NATIVE_DECIMALS=10^18 == QUOTE_CONFIGS WMON decimals=18, no runtime difference expected)

## Rollback
- Column rename is reversible (just rename back).
- `FALLBACK_DECIMALS` can be renamed back to `NATIVE_DECIMALS` and given `pub` if we need to un-ship.
- Trigger function has `CREATE OR REPLACE`, so rollback = apply the old function body.
- **Ordering matters:** apply migration before deploying the code. Reverse-migrate before rolling back the code.
EOF
)"
```

Return the PR URL.

---

## Self-Review Notes

- **Spec coverage:** user asked for (1) rename position_history native to quote, and (2) stop using NATIVE_DECIMALS since QUOTE_CONFIGS is the source. Both covered.
- **No placeholders:** every SQL and Rust change shows the exact before/after.
- **Type consistency:** `get_quote_decimals` returns `&BigDecimal`; all new call sites match this type. The old `&*NATIVE_DECIMALS` pattern derefs the lazy_static to get `&BigDecimal`; both shapes yield the same type at the call site.
- **Out of scope (deliberately):**
  - Generalizing V1 code to use per-token quote decimals (V1 is structurally WMON-only by design)
  - Generalizing position tracking to non-WMON flows (requires listening to USDC Transfer events etc.)
  - Deleting `FALLBACK_DECIMALS` entirely and panicking on unknown quote_id (breaks V2 defensive paths)
- **Risks:**
  - Mechanical find-and-replace can miss an occurrence — the grep verification in Task 4 Step 6 and Task 5 Step 1 catches this.
  - The `use crate::config::...` bundle structure in v1/dex/receive.rs has multiple scoped imports; the refactor must add a clean top-level import AND remove all inline ones. A missed inline import leaves a dead `use` statement (warning, not error).
