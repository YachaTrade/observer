# Remove Address Lowercasing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every `.to_lowercase()` call applied to EVM addresses in the observer codebase. From now on, addresses are kept in whatever casing the source produces (alloy = EIP-55 checksum; env vars = whatever the operator sets).

**Architecture:** On-chain data flows through `alloy::primitives::Address`, whose `Display` impl emits EIP-55 checksum strings. The current code defensively lowercases at every comparison; this is wasted work and the user wants it gone. Most call sites are no-ops once we trust the source casing and can simply be deleted. The three Uniswap-style pool-ordering sites (v1 curve graduate, v2 curve graduate, `db::cache::get_pool_pair`) are an **intentional exception**: they keep `.to_lowercase()` *inside the comparison expression only* — the comparison helper, not the stored output. The returned `(token0, token1)` tuple uses the original-cased strings. This guarantees deterministic ordering across legacy lowercase rows and fresh checksum rows without parsing to `Address`. The deadlock-detection `to_lowercase()` calls in `db/postgres/controller/*.rs` are on error message strings, not addresses — those are out of scope and stay.

**Tech Stack:** Rust, alloy-primitives, sqlx, DashMap.

**Operator preconditions (REQUIRED before deploy):**
1. `WMON` env var must be in EIP-55 checksum form (matching what `alloy::primitives::Address::to_string()` would emit for that address).
2. Each entry in `QUOTE_CONFIGS` env var must use checksum form for the address part.
3. `VANITY_ADDRESS_SUFFIX` is digit-only by default (`"7777"`) — safe regardless of casing. If the operator changes it to include hex letters they must understand it'll only match EIP-55 checksum tail bytes from now on.

**Out of scope:**
- DB column casing migration (existing rows stay as-is — sibling services like `api-server` continue using their own casing rules).
- Sibling service updates (`api-server`, `alert-bot`, etc.).
- Removing `.to_lowercase()` on non-address strings (deadlock detection, error matching, mode flags).

---

## File Structure

**Files modified:**
- `src/config.rs` — drop lowercasing in `WNATIVE_ADDRESS` lazy init, `parse_quote_configs`, `get_quote_decimals`, `is_quote_token`. Update doc comments.
- `src/event/common/token/stream.rs` — drop lowercasing in `NON_WMON_QUOTE_ADDRESSES` filter and the `quote_id_arc` resolution block.
- `src/event/v1/curve/stream.rs` — drop lowercasing in vanity-suffix check; switch pool ordering to `Address` byte compare.
- `src/event/v2/curve/stream.rs` — same as v1 curve.
- `src/event/v2/curve/receive.rs` — drop `.map(|s| s.to_lowercase())` on the cached quote_id.
- `src/event/v2/dex/receive.rs` — same as curve receive.
- `src/db/cache/mod.rs` — pool-ordering site in `get_pool_pair`: switch to `Address` byte compare.

**Files NOT modified (intentionally):**
- `src/db/postgres/controller/*.rs` — deadlock detection only, no address lowercasing.
- `src/utils/metadata.rs:66`, `src/event/common/price/provider/mod.rs:34` — error/mode flag strings, not addresses.
- DB schema / migration files — see "Out of scope".

---

## Task 1: Drop lowercasing in `config.rs`

**Files:**
- Modify: `src/config.rs:18-22, 207-226, 228-255`

- [ ] **Step 1: Remove `.to_lowercase()` from `WNATIVE_ADDRESS` and update its doc comment**

Replace lines 18-22:

```rust
    // WMON address. Must be set in EIP-55 checksum form (matching the
    // EIP-55 representation that `alloy::primitives::Address::to_string()`
    // emits) so it lex-equals every alloy-derived address downstream.
    pub static ref WNATIVE_ADDRESS: String = env::var("WMON")
        .expect("WMON must be set");
```

- [ ] **Step 2: Remove `.to_lowercase()` from `parse_quote_configs`**

In `parse_quote_configs` (around line 220), change:

```rust
            QuoteConfig {
                address: parts[0].to_lowercase(),
```

to:

```rust
            QuoteConfig {
                address: parts[0].to_string(),
```

- [ ] **Step 3: Update `get_quote_decimals` and `is_quote_token` to compare without lowercasing**

Replace the body of `get_quote_decimals` (lines 237-249) with:

```rust
pub fn get_quote_decimals(quote_id: &str) -> &BigDecimal {
    QUOTE_CONFIGS
        .iter()
        .find(|q| q.address == quote_id)
        .map(|q| &q.decimals)
        .unwrap_or_else(|| {
            panic!(
                "get_quote_decimals: quote_id '{}' not found in QUOTE_CONFIGS",
                quote_id
            )
        })
}
```

Replace the body of `is_quote_token` (lines 252-255) with:

```rust
pub fn is_quote_token(address: &str) -> bool {
    QUOTE_CONFIGS.iter().any(|q| q.address == address)
}
```

Also update the `get_quote_decimals` doc comment (lines 230-236) — remove the "case-insensitive" line and replace with:

```rust
/// Get decimals for a quote token registered in `QUOTE_CONFIGS`.
///
/// Lookup uses exact string equality. Callers and `QUOTE_CONFIGS` env var
/// must agree on casing — both should be EIP-55 checksum (alloy default).
///
/// **Panics** if `quote_id` is not present in `QUOTE_CONFIGS`. Any failure
/// here indicates a bug in the upstream quote_id resolution (e.g. a token
/// with a non-configured quote leaked into trade processing) or a casing
/// mismatch between env config and chain data.
```

And update `is_quote_token`'s doc comment to say `/// Check if an address is a known quote token. Exact-match (no normalization).`.

- [ ] **Step 4: Run `cargo check`**

Run: `cargo check -p observer 2>&1 | tail -30`
Expected: clean compile (or errors only from downstream files we'll fix in later tasks).

- [ ] **Step 5: Commit**

```bash
git add src/config.rs
git commit -m "refactor: stop lowercasing addresses in config.rs

WMON, QUOTE_CONFIGS addresses, and quote lookup helpers now keep
whatever casing the env var provides. Operator must set them in
EIP-55 checksum form so they lex-equal alloy-emitted addresses."
```

---

## Task 2: Drop lowercasing in `event/common/token/stream.rs`

**Files:**
- Modify: `src/event/common/token/stream.rs:49-56, 722-748`

- [ ] **Step 1: Drop lowercasing in `NON_WMON_QUOTE_ADDRESSES` filter**

Replace lines 49-56:

```rust
/// Non-WMON quote addresses parsed once at startup.
static NON_WMON_QUOTE_ADDRESSES: LazyLock<Vec<Address>> = LazyLock::new(|| {
    QUOTE_CONFIGS
        .iter()
        .filter(|q| q.address != *WNATIVE_ADDRESS)
        .filter_map(|q| q.address.parse::<Address>().ok())
        .collect()
});
```

- [ ] **Step 2: Drop lowercasing in the `quote_id_arc` resolution block**

Replace lines 726-748 (the `match token_quote_cache.get(token) { ... }` arm). The new version:

```rust
                let quote_id_arc: Arc<String> = match token_quote_cache.get(token) {
                    Some(q) => Arc::clone(q),
                    None => {
                        let token_str = token.to_string();
                        let resolved: String = match cache_manager
                            .get_token_quote_id(&token_str)
                            .await
                        {
                            Ok(Some(q)) => q,
                            Ok(None) => WNATIVE_ADDRESS.clone(),
                            Err(e) => {
                                warn!(
                                    "[TOKEN] get_token_quote_id failed for {}: {} - falling back to WMON",
                                    token_str, e
                                );
                                WNATIVE_ADDRESS.clone()
                            }
                        };
                        let arc = Arc::new(resolved);
                        token_quote_cache.insert(*token, Arc::clone(&arc));
                        arc
                    }
                };
```

(Note: the `Ok(Some(q)) => q` branch returns whatever casing the cache held. Legacy lowercase rows stay lowercase; new alloy-derived rows are checksum. The downstream `quote_id_arc.parse::<Address>()` call on line ~750 still works because hex parsing is case-insensitive.)

- [ ] **Step 3: Run `cargo check`**

Run: `cargo check -p observer 2>&1 | tail -30`
Expected: clean compile.

- [ ] **Step 4: Commit**

```bash
git add src/event/common/token/stream.rs
git commit -m "refactor: stop lowercasing addresses in token stream

NON_WMON_QUOTE_ADDRESSES filter and per-token quote_id resolution
now use the source casing as-is. Address parsing downstream is
casing-insensitive so this is a pure removal of wasted work."
```

---

## Task 3: Drop lowercasing in `event/v1/curve/stream.rs` (vanity suffix only)

**Files:**
- Modify: `src/event/v1/curve/stream.rs:213-223`

**Note:** Pool ordering site at lines 398-402 is **intentionally left unchanged**. It already uses `.to_lowercase()` *only inside the comparison expression* and returns the original-cased strings — that's the desired pattern.

- [ ] **Step 1: Drop vanity suffix lowercasing**

Replace lines 213-223:

```rust
            // Check if token address ends with VANITY_ADDRESS_SUFFIX.
            // Both sides use the source casing as-is — default suffix is
            // digit-only ("7777"), so casing is irrelevant. If operator
            // changes the suffix to include hex letters they must use the
            // EIP-55 case for the trailing bytes.
            let token_str = token.to_string();
            if !token_str.ends_with(&*crate::config::VANITY_ADDRESS_SUFFIX) {
                return Err(anyhow::anyhow!(
                    "Token address does not end with required suffix: {}",
                    &*crate::config::VANITY_ADDRESS_SUFFIX
                ));
            }
```

- [ ] **Step 2: Run `cargo check`**

Run: `cargo check -p observer 2>&1 | tail -30`
Expected: clean compile.

- [ ] **Step 3: Commit**

```bash
git add src/event/v1/curve/stream.rs
git commit -m "refactor: drop vanity suffix lowercasing in v1 curve stream

Default suffix is digit-only so casing is a noop; alloy-emitted
token addresses use the source casing directly. Pool ordering at
the graduate site keeps its inline lowercase compare on purpose."
```

---

## Task 4: Drop lowercasing in `event/v2/curve/stream.rs` (vanity suffix only)

**Files:**
- Modify: `src/event/v2/curve/stream.rs:213-225`

**Note:** Pool ordering site at lines 383-387 is **intentionally left unchanged** — same rationale as Task 3.

- [ ] **Step 1: Drop vanity suffix lowercasing**

Replace lines 213-225 with:

```rust
            let token_str = token.to_string();
            if !token_str.ends_with(&*crate::config::VANITY_ADDRESS_SUFFIX) {
                return Err(anyhow::anyhow!(
                    "Token address does not end with required suffix: {}",
                    &*crate::config::VANITY_ADDRESS_SUFFIX
                ));
            }
```

- [ ] **Step 2: Run `cargo check`**

Run: `cargo check -p observer 2>&1 | tail -30`
Expected: clean compile.

- [ ] **Step 3: Commit**

```bash
git add src/event/v2/curve/stream.rs
git commit -m "refactor: drop vanity suffix lowercasing in v2 curve stream

Mirror of v1 change. Pool ordering at the graduate site keeps its
inline lowercase compare on purpose."
```

---

## Task 5: Drop `.to_lowercase()` in `event/v2/curve/receive.rs`

**Files:**
- Modify: `src/event/v2/curve/receive.rs:147-154`

- [ ] **Step 1: Remove the `.map(|s| s.to_lowercase())`**

Replace lines 147-154:

```rust
    // Resolve quote token for USD price conversion. Use cached value as-is;
    // legacy rows may be lowercase, new ones from alloy are EIP-55 checksum.
    // Downstream `get_quote_decimals` does exact-match against QUOTE_CONFIGS,
    // which the operator now configures in checksum form.
    let quote_id_str = cache_manager
        .get_token_quote_id(&token)
        .await
        .unwrap_or(None)
        .unwrap_or_else(|| (*crate::config::WNATIVE_ADDRESS).clone());
    let quote_decimals = crate::config::get_quote_decimals(&quote_id_str);
```

- [ ] **Step 2: Run `cargo check`**

Run: `cargo check -p observer 2>&1 | tail -30`
Expected: clean compile.

- [ ] **Step 3: Commit**

```bash
git add src/event/v2/curve/receive.rs
git commit -m "refactor: stop lowercasing quote_id in v2 curve receive"
```

---

## Task 6: Drop `.to_lowercase()` in `event/v2/dex/receive.rs`

**Files:**
- Modify: `src/event/v2/dex/receive.rs:118-125`

- [ ] **Step 1: Remove the `.map(|s| s.to_lowercase())`**

Replace lines 118-125 with the same pattern as Task 5:

```rust
    // Get quote token for USD price conversion. Use cached value as-is.
    let quote_id_str = cache_manager
        .get_token_quote_id(&token)
        .await
        .unwrap_or(None)
        .unwrap_or_else(|| (*crate::config::WNATIVE_ADDRESS).clone());
    let quote_decimals = crate::config::get_quote_decimals(&quote_id_str);
```

- [ ] **Step 2: Run `cargo check`**

Run: `cargo check -p observer 2>&1 | tail -30`
Expected: clean compile.

- [ ] **Step 3: Commit**

```bash
git add src/event/v2/dex/receive.rs
git commit -m "refactor: stop lowercasing quote_id in v2 dex receive"
```

---

## Task 7: `db/cache/mod.rs` pool ordering — NO CHANGE (intentional)

**Files:** none modified.

The pool ordering site at `src/db/cache/mod.rs:563` already uses `.to_lowercase()` *only inside the comparison expression* — the returned `(token0, token1)` is the original-cased strings from `WNATIVE_ADDRESS` and the DB row. This is the desired pattern (lowercase only when comparing) so it stays as-is.

- [ ] **Step 1: Verify the file is untouched and confirm the comment captures intent**

Run: `rg -n "to_lowercase" src/db/cache/mod.rs`

Expected: line 563 still present:

```
563:                        if wnative_address.to_lowercase() < token_id.to_lowercase() {
```

If a comment is missing explaining *why* this site keeps the lowercase compare, add a one-line comment above line 562 (no behavioral change):

```rust
                    // Uniswap-style ordering. Lowercase only inside the
                    // comparison so legacy lowercase rows and fresh checksum
                    // rows produce the same (token0, token1); the returned
                    // values keep their original casing.
```

- [ ] **Step 2: Commit (only if a comment was added)**

```bash
git add src/db/cache/mod.rs
git commit -m "docs: clarify why get_pool_pair keeps inline lowercase compare"
```

(Skip the commit if no comment was needed.)

---

## Task 8: Verification — full grep + build + tests

**Files:** none modified.

- [ ] **Step 1: Verify only the intentional pool-ordering compares remain**

Run: `rg "to_lowercase\(\)" src/ | rg -v "deadlock|timeout|testnet|\.to_string\(\)\.to_lowercase\(\)\.contains"`

Expected output (only these three pool-ordering compares should remain):

```
src/event/v1/curve/stream.rs:398:            let (token0, token1) = if WNATIVE_ADDRESS.to_lowercase() < token.to_lowercase() {
src/event/v2/curve/stream.rs:383:            let (token0, token1) = if WNATIVE_ADDRESS.to_lowercase() < token.to_lowercase() {
src/db/cache/mod.rs:563:                        if wnative_address.to_lowercase() < token_id.to_lowercase() {
```

Anything else is a regression and must be removed.

- [ ] **Step 2: Full build**

Run: `cargo build -p observer 2>&1 | tail -30`
Expected: clean build.

- [ ] **Step 3: Run tests**

Run: `cargo test -p observer 2>&1 | tail -50`
Expected: all tests pass. If a test asserts on lowercase address strings, fix the test to assert on whatever casing the source naturally produces — do NOT re-introduce `.to_lowercase()`.

- [ ] **Step 4: Verify clippy**

Run: `cargo clippy -p observer -- -D warnings 2>&1 | tail -30`
Expected: no warnings (or only pre-existing ones unrelated to this change).

- [ ] **Step 5: Final commit if anything was touched in step 3**

```bash
git add -u
git commit -m "test: align tests with non-lowercased address casing"
```

(Skip if no test fixes were needed.)

---

## Self-Review Notes

**Spec coverage:** Every `.to_lowercase()` call site enumerated in the original investigation is addressed:
- `src/config.rs:22,220,238,253` → Task 1 (removed)
- `src/event/common/token/stream.rs:53,734,735,741` → Task 2 (removed)
- `src/event/v1/curve/stream.rs:216-217` (vanity) → Task 3 (removed); `:398` (pool ordering) → kept on purpose
- `src/event/v2/curve/stream.rs:218-219` (vanity) → Task 4 (removed); `:383` (pool ordering) → kept on purpose
- `src/event/v2/curve/receive.rs:152` → Task 5 (removed)
- `src/event/v2/dex/receive.rs:123` → Task 6 (removed)
- `src/db/cache/mod.rs:563` (pool ordering) → Task 7 (kept on purpose, optional comment add)
- Final grep verification → Task 8

**Risks the engineer must understand:**
1. Pool ordering sites are unchanged — they still lowercase *only inside the comparison expression* and return the original-cased strings. This works for both legacy lowercase and fresh checksum operands. Do not "clean up" these three sites — they are intentional.
2. `quote_id` resolution: legacy Redis/DB cached values may still be lowercase. After this change, observer will store new values in whatever casing the source produces (alloy = checksum), so for a transitional period the cache has a mix. `get_quote_decimals` does exact-match against env config — if the env config quote address is checksum but a legacy lowercase value flows in from cache, it'll panic. **Operator action item**: clear Redis cache for `PREFIX_TOKEN_QUOTE` keys before deploy, OR keep env config in lowercase for now and switch to checksum after the cache rolls over.
3. The vanity suffix default `"7777"` is digit-only so casing-irrelevant. Confirm `.env` doesn't set it to a hex-letter value before deploy.
