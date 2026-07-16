# PriceProvider Trait Abstraction — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract Pyth-specific fetch/rate-limit/retry logic behind a `PriceProvider` trait so the streaming loop is decoupled from any single oracle, enabling mock-based unit tests and future provider swaps (Chainlink, Redstone, etc.).

**Architecture:** Create a `provider` submodule under `src/event/common/price/` exposing a `PriceProvider` trait with a single async method `fetch(feed_id, timestamp)`. Implement two concrete providers — `PythProvider` (HTTP + rate limiter + retry/backoff) and `MockProvider` (programmable test/testnet stub). The streaming loop in `stream.rs` takes `Arc<dyn PriceProvider>` via a constructor selector (`build_provider()`) that reads `MODE` from env, keeping the existing testnet hack out of the hot path. All rate-limit constants and the `RateLimiter` helper move into `pyth.rs` because they are Pyth-specific.

**Tech Stack:** Rust (edition 2024), `reqwest`, `tokio`, `bigdecimal`, `async-trait` (new dep — required for `dyn PriceProvider` object safety), `tracing`.

**Branch:** `feat/v2-price-provider-trait` (branched from `v2`). Final PR merges into `v2`.

---

## File Structure

### New files
- `src/event/common/price/provider/mod.rs` — module root: declares `PriceProvider` trait, re-exports providers, exposes `build_provider()` factory.
- `src/event/common/price/provider/pyth.rs` — `PythProvider` struct, `RateLimiter` (moved from `stream.rs`), `fetch_price_with_retry` body, Pyth-specific constants (URL, rate limits, retry).
- `src/event/common/price/provider/mock.rs` — `MockProvider` with a configurable fixed price plus unit tests.

### Modified files
- `src/event/common/price/mod.rs` — add `pub mod provider;`.
- `src/event/common/price/stream.rs` — delete `RateLimiter`, delete `fetch_price_with_retry`, delete `MODE == "testnet"` hack, delete `REQUEST_TIMEOUT_SECS`/`MAX_REQUESTS_PER_10_SECONDS`/`RATE_LIMIT_WINDOW` constants. Replace with `Arc<dyn PriceProvider>` obtained from `provider::build_provider()`. The two fetch sites (WMON + non-WMON quotes) both call `provider.fetch(feed_id, ts)`.
- `Cargo.toml` — add `async-trait = "0.1"`.

### Responsibility boundaries
- **Trait (`provider/mod.rs`)** — zero logic; only the contract and factory.
- **PythProvider (`provider/pyth.rs`)** — owns HTTP client, rate limiter, retry loop, Pyth response parsing. No knowledge of streaming, blocks, caching.
- **MockProvider (`provider/mock.rs`)** — returns a configured fixed price or a scripted sequence; used for testnet runtime AND unit tests.
- **stream.rs** — orchestration: block range, cache checks, provider.fetch, event emission. No HTTP code.

---

## Task 1: Create feature branch

- [ ] **Step 1: Create and check out branch from `v2`**

```bash
cd /Users/gyu/project/nads-pump/observer
git checkout v2
git pull origin v2
git checkout -b feat/v2-price-provider-trait
```

Expected: `Switched to a new branch 'feat/v2-price-provider-trait'`

- [ ] **Step 2: Verify clean status**

```bash
git status
```

Expected: only the untracked plan file under `docs/superpowers/plans/` (plus possibly pre-existing untracked files). No `M` on tracked files under `src/`.

---

## Task 2: Add `async-trait` dependency

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add dep**

Open `Cargo.toml` and under `[dependencies]` add the line below. Put it near the other small utility crates (e.g., near `lazy_static` or `anyhow`).

```toml
async-trait = "0.1"
```

- [ ] **Step 2: Build to confirm resolution**

```bash
cargo build
```

Expected: successful build (no warnings about `async-trait` unused yet — cargo won't warn on unused deps in a lib/bin).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add async-trait for PriceProvider trait objects"
```

---

## Task 3: Create `provider` module skeleton with empty trait

**Files:**
- Create: `src/event/common/price/provider/mod.rs`
- Modify: `src/event/common/price/mod.rs`

- [ ] **Step 1: Create `provider/mod.rs` with trait declaration**

Write file `src/event/common/price/provider/mod.rs`:

```rust
//! Price oracle provider abstraction.
//!
//! The streaming loop talks to providers exclusively through the
//! [`PriceProvider`] trait so oracles (Pyth, mocks, future providers)
//! can be swapped without touching orchestration code.

pub mod mock;
pub mod pyth;

use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use std::sync::Arc;

/// A price oracle capable of returning a spot price for a given feed id
/// at (or near) the supplied unix timestamp (seconds).
#[async_trait]
pub trait PriceProvider: Send + Sync {
    /// Fetch price for `feed_id` at `timestamp`.
    ///
    /// Returns `Ok(None)` when the oracle has no data for that point in time.
    /// Returns `Err(_)` for transport, parsing, or rate-limit failures that
    /// the provider could not recover from internally.
    async fn fetch(&self, feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>>;
}

/// Build the provider selected by runtime env (`MODE`).
///
/// - `MODE=testnet` → [`mock::MockProvider`] with a fixed 0.03 price,
///   preserving the legacy testnet hardcoded value.
/// - otherwise      → [`pyth::PythProvider`] backed by the Pyth Hermes API.
pub fn build_provider() -> Result<Arc<dyn PriceProvider>> {
    let mode = std::env::var("MODE").unwrap_or_else(|_| "mainnet".to_string());
    if mode.to_lowercase() == "testnet" {
        tracing::info!("[PRICE] Using MockProvider (MODE=testnet)");
        Ok(Arc::new(mock::MockProvider::fixed_str("0.03")))
    } else {
        tracing::info!("[PRICE] Using PythProvider (MODE={})", mode);
        Ok(Arc::new(pyth::PythProvider::new()?))
    }
}
```

- [ ] **Step 2: Register the submodule in the parent**

Open `src/event/common/price/mod.rs` and add `pub mod provider;` right below `pub mod stream;`. Final top of file should read:

```rust
pub mod receive;
pub mod stream;
pub mod provider;
```

- [ ] **Step 3: Confirm the crate still builds (will fail — submodules do not exist)**

```bash
cargo build 2>&1 | tail -20
```

Expected: build FAILS with `file not found for module pyth` and `file not found for module mock`. This is expected — the stub files land in Tasks 4 and 5.

(Do not commit yet — the crate is broken between tasks and commits are made once per working milestone.)

---

## Task 4: Stub `MockProvider` to make the crate compile

**Files:**
- Create: `src/event/common/price/provider/mock.rs`

- [ ] **Step 1: Write minimal `MockProvider`**

Write file `src/event/common/price/provider/mock.rs`:

```rust
//! In-memory price provider used for testnet runtime and unit tests.

use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;

use super::PriceProvider;

/// Always returns a single fixed price regardless of feed/timestamp.
#[derive(Debug, Clone)]
pub struct MockProvider {
    price: BigDecimal,
}

impl MockProvider {
    pub fn fixed(price: BigDecimal) -> Self {
        Self { price }
    }

    pub fn fixed_str(price: &str) -> Self {
        Self {
            price: BigDecimal::from_str(price)
                .expect("MockProvider::fixed_str received invalid decimal"),
        }
    }
}

#[async_trait]
impl PriceProvider for MockProvider {
    async fn fetch(&self, _feed_id: &str, _timestamp: u64) -> Result<Option<BigDecimal>> {
        Ok(Some(self.price.clone()))
    }
}
```

- [ ] **Step 2: Build (still fails on missing pyth.rs)**

```bash
cargo build 2>&1 | tail -10
```

Expected: fails with `file not found for module pyth`. Proceed to Task 5.

---

## Task 5: Implement `PythProvider`

**Files:**
- Create: `src/event/common/price/provider/pyth.rs`

- [ ] **Step 1: Write `pyth.rs` with `RateLimiter` + `PythProvider`**

This file is a near-verbatim move of the existing helpers from `stream.rs` (lines 20-68 and 287-387), minus the testnet hack and reshaped as a struct owning its state.

Write file `src/event/common/price/provider/pyth.rs`:

```rust
//! Pyth Hermes HTTP price provider.
//!
//! Owns the HTTP client, the Pyth rate limiter, and the retry/backoff
//! loop. All constants below are Pyth-specific and intentionally local
//! to this module.

use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use reqwest::Client;
use tokio::{sync::Mutex, time::Instant};
use tracing::{error, info, warn};

use super::PriceProvider;
use crate::{config::PYTH_API_URL, types::price::PriceFeedResponse};

const REQUEST_TIMEOUT_SECS: u64 = 30;
/// Pyth Hermes: 30 requests per 10 seconds.
const MAX_REQUESTS_PER_10_SECONDS: usize = 30;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;

/// Sliding-window rate limiter for the Pyth Hermes API.
struct RateLimiter {
    request_times: Mutex<Vec<Instant>>,
    max_requests: usize,
    window: Duration,
}

impl RateLimiter {
    fn new(max_requests: usize, window: Duration) -> Self {
        Self {
            request_times: Mutex::new(Vec::new()),
            max_requests,
            window,
        }
    }

    async fn wait_if_needed(&self) {
        let mut times = self.request_times.lock().await;
        let now = Instant::now();
        times.retain(|&t| now.duration_since(t) < self.window);

        if times.len() >= self.max_requests
            && let Some(&oldest) = times.first()
        {
            let wait_time = self.window - now.duration_since(oldest);
            if wait_time > Duration::ZERO {
                tokio::time::sleep(wait_time + Duration::from_millis(100)).await;
            }
        }

        times.push(now);
    }
}

pub struct PythProvider {
    http: Client,
    rate_limiter: RateLimiter,
}

impl PythProvider {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for PythProvider")?;
        Ok(Self {
            http,
            rate_limiter: RateLimiter::new(MAX_REQUESTS_PER_10_SECONDS, RATE_LIMIT_WINDOW),
        })
    }
}

#[async_trait]
impl PriceProvider for PythProvider {
    async fn fetch(&self, feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>> {
        self.rate_limiter.wait_if_needed().await;

        let mut retry_count: u32 = 0;
        let mut backoff = Duration::from_secs(1);

        loop {
            let url = format!(
                "{}/{}?ids%5B%5D={}&encoding=hex&parsed=true&ignore_invalid_price_ids=false",
                *PYTH_API_URL, timestamp, feed_id
            );

            let response = self
                .http
                .get(&url)
                .header("Accept", "application/json")
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let feed: PriceFeedResponse = resp
                            .json()
                            .await
                            .context("Failed to parse Pyth API response")?;

                        let Some(parsed) = feed.parsed.first() else {
                            return Ok(None);
                        };

                        let price_bigint = parsed
                            .price
                            .price
                            .parse::<i128>()
                            .context("Failed to parse price string")?;
                        let mut price = BigDecimal::from(price_bigint);
                        let expo = parsed.price.expo;
                        if expo < 0 {
                            let divisor = BigDecimal::from(10i64.pow((-expo) as u32));
                            price = price / divisor;
                        } else {
                            let multiplier = BigDecimal::from(10i64.pow(expo as u32));
                            price *= multiplier;
                        }

                        info!(
                            "Fetched Pyth price: feed={} ts={} price={}",
                            feed_id, timestamp, price
                        );
                        return Ok(Some(price));
                    } else if resp.status() == 429 {
                        error!("Pyth rate limit hit (429), backoff={:?}", backoff);
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            return Err(anyhow::anyhow!("Max retries exceeded for rate limit"));
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = backoff * 2 + Duration::from_millis(1000);
                        if backoff > Duration::from_secs(60) {
                            backoff = Duration::from_secs(60);
                        }
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth API returned status: {}",
                            resp.status()
                        ));
                    }
                }
                Err(e) => {
                    if retry_count < MAX_RETRIES {
                        warn!("Pyth request failed, retrying: {}", e);
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        return Err(anyhow::anyhow!("Pyth request failed after retries: {}", e));
                    }
                }
            }
        }
    }
}

// Needed so `BigDecimal::from_str` import above is not flagged when the
// compiler cannot see usages in this particular layout.
#[allow(dead_code)]
fn _assert_from_str() -> BigDecimal {
    BigDecimal::from_str("0").unwrap()
}
```

Note: the trailing `_assert_from_str` is defensive — the `FromStr` import is not actually needed by the logic above (we use `parse::<i128>()` and `BigDecimal::from`), so drop the `FromStr` import if your compiler complains of unused import. Prefer removing the import over the dummy function.

- [ ] **Step 2: Build — this time it should succeed (stream.rs still has old code)**

```bash
cargo build 2>&1 | tail -20
```

Expected: success OR an "unused" warning for `provider` module because `stream.rs` hasn't switched over yet. Warnings are OK; hard errors are not.

If there's a `FromStr` unused-import warning in `pyth.rs`, remove `use std::str::FromStr;` and the `_assert_from_str` helper.

---

## Task 6: Write unit test for `MockProvider`

**Files:**
- Modify: `src/event/common/price/provider/mock.rs` (append test module)

- [ ] **Step 1: Append test module at the bottom of `mock.rs`**

Add to `src/event/common/price/provider/mock.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_fixed_price_for_any_feed_and_timestamp() {
        let provider = MockProvider::fixed_str("0.03");
        let price = provider
            .fetch("0xdeadbeef", 1_700_000_000)
            .await
            .expect("fetch must not error");
        assert_eq!(price, Some(BigDecimal::from_str("0.03").unwrap()));

        // Different feed / timestamp — still the same fixed value.
        let price2 = provider
            .fetch("0xcafebabe", 1_800_000_000)
            .await
            .expect("fetch must not error");
        assert_eq!(price2, Some(BigDecimal::from_str("0.03").unwrap()));
    }

    #[tokio::test]
    async fn fixed_accepts_arbitrary_bigdecimal() {
        let provider = MockProvider::fixed(BigDecimal::from_str("12345.6789").unwrap());
        let price = provider.fetch("any", 0).await.unwrap();
        assert_eq!(price, Some(BigDecimal::from_str("12345.6789").unwrap()));
    }
}
```

- [ ] **Step 2: Run just the new tests**

```bash
cargo test --lib event::common::price::provider::mock::tests 2>&1 | tail -30
```

Expected: 2 passed.

- [ ] **Step 3: Commit the provider module (pre-wiring milestone)**

```bash
git add Cargo.toml Cargo.lock src/event/common/price/mod.rs src/event/common/price/provider/
git commit -m "feat: introduce PriceProvider trait with Pyth and mock impls"
```

---

## Task 7: Wire `stream.rs` to use `Arc<dyn PriceProvider>`

**Files:**
- Modify: `src/event/common/price/stream.rs`

This task deletes substantial code and replaces fetch sites. Read the current stream.rs first so you know what is being removed:

```bash
sed -n '1,100p' src/event/common/price/stream.rs
```

- [ ] **Step 1: Replace imports and delete `RateLimiter`, constants, and `fetch_price_with_retry`**

In `src/event/common/price/stream.rs`:

**Remove** these items entirely:
- The `RateLimiter` struct and its `impl` block (current lines ~20-55).
- Constants `REQUEST_TIMEOUT_SECS`, `MAX_REQUESTS_PER_10_SECONDS`, `RATE_LIMIT_WINDOW` (~lines 65-68).
- The entire `async fn fetch_price_with_retry(...)` function at the bottom of the file (~lines 287-387).

**Change the top imports** to this exact block:

```rust
use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use reqwest as _; // no longer directly used; keep dev deps happy — DELETE if build is clean without it
use tokio::time::Instant;
use tracing::{error, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, QUOTE_CONFIGS, WNATIVE_ADDRESS},
    db::cache::CacheManager,
    event::{
        common::price::{PriceEventChannel, provider},
        get_block_timestamp,
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::price::UpdatePrice,
};

use super::receive::receive_events;
```

Delete the `reqwest as _;` stub line after Step 4 compile passes if the compiler does not require it. It is only a fallback if the `Client` import lingered somewhere.

- [ ] **Step 2: Replace `http_client` and `rate_limiter` initialization with `provider`**

Find the block that currently constructs `http_client`, `rate_limiter`, and `cache_manager` (around lines 82-97). Replace with:

```rust
    let client = RpcClient::instance()?;
    let cache_manager = CacheManager::instance()?;
    let price_provider = provider::build_provider()?;

    // Find WMON feed_id from QUOTE_CONFIGS
    let wmon_feed_id = QUOTE_CONFIGS
        .iter()
        .find(|q| q.address == *WNATIVE_ADDRESS)
        .map(|q| q.pyth_feed_id.clone())
        .expect("WMON must be in QUOTE_CONFIGS");
```

(Note `.clone()` on `pyth_feed_id` — we now own a `String` so we can pass `&str` to provider without holding a reference into `QUOTE_CONFIGS`.)

- [ ] **Step 3: Replace the WMON fetch site**

Find the WMON fetch block (`let price_result = fetch_price_with_retry(...)` around line 168). Replace with:

```rust
            // Fetch price via provider (rate-limit/retry live inside the provider)
            let price_result = price_provider
                .fetch(&wmon_feed_id, *normalized_timestamp)
                .await;
```

Also **delete** the line `rate_limiter.wait_if_needed().await;` directly above it — the provider handles its own rate limiting internally.

- [ ] **Step 4: Replace the non-WMON quote fetch site**

Find the non-WMON loop (around line 224). Replace the entire `fetch_price_with_retry(...)` call with:

```rust
                match price_provider
                    .fetch(&quote_config.pyth_feed_id, *normalized_timestamp)
                    .await
                {
```

Also **delete** the `rate_limiter.wait_if_needed().await;` line directly above.

- [ ] **Step 5: Build and fix any leftover import/type warnings**

```bash
cargo build 2>&1 | tail -30
```

Expected: clean build or at most minor warnings. If `reqwest`, `Mutex`, or `Duration` imports are flagged unused — remove them. If `Instant` is still needed for `time::Instant::now()` (it is, for timing `time`/`elapsed_ms`), keep it.

- [ ] **Step 6: Run the full test suite**

```bash
cargo test --lib 2>&1 | tail -30
```

Expected: all tests pass, including the two new `MockProvider` tests.

- [ ] **Step 7: Commit**

```bash
git add src/event/common/price/stream.rs
git commit -m "refactor: wire price stream through PriceProvider trait"
```

---

## Task 8: Remove dead testnet branch and verify end-to-end

**Files:**
- Verify only — no code changes expected unless leftover `MODE == "testnet"` references remain.

- [ ] **Step 1: Confirm the old testnet hack is gone from `stream.rs`**

```bash
grep -n "testnet" src/event/common/price/stream.rs
```

Expected: no matches. The testnet branch now lives in `provider::build_provider()`.

- [ ] **Step 2: Confirm rate-limiter/Pyth constants are only in `pyth.rs`**

```bash
grep -rn "MAX_REQUESTS_PER_10_SECONDS\|REQUEST_TIMEOUT_SECS\|RATE_LIMIT_WINDOW\|RateLimiter" src/
```

Expected: matches only inside `src/event/common/price/provider/pyth.rs`.

- [ ] **Step 3: Confirm `fetch_price_with_retry` is only defined inside `pyth.rs`**

```bash
grep -rn "fetch_price_with_retry\|fn fetch(" src/event/common/price/
```

Expected: the only `fetch(` is the trait method in `provider/mod.rs` and its impls in `pyth.rs`/`mock.rs`. No top-level `fn fetch_price_with_retry`.

- [ ] **Step 4: Run clippy**

```bash
cargo clippy --all-targets -- -D warnings 2>&1 | tail -40
```

Expected: no new warnings in the files we touched. Pre-existing warnings unrelated to this refactor may remain — do not fix them in this PR.

- [ ] **Step 5: Run full test suite with race detection**

```bash
cargo test --lib 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 6: Run `MODE=testnet` smoke build (compile-only)**

```bash
MODE=testnet cargo build 2>&1 | tail -10
```

Expected: successful build. (Runtime verification happens in QA after PR merge; here we only prove the testnet code path compiles.)

---

## Task 9: Open the pull request

- [ ] **Step 1: Push branch**

```bash
git push -u origin feat/v2-price-provider-trait
```

- [ ] **Step 2: Create PR into `v2`**

```bash
gh pr create --base v2 --title "refactor: PriceProvider trait abstraction" --body "$(cat <<'EOF'
## Summary
- Extract Pyth HTTP/rate-limit/retry logic behind a new `PriceProvider` trait
- Add `PythProvider` (production) and `MockProvider` (testnet + unit tests) implementations
- Stream loop now depends only on `Arc<dyn PriceProvider>`; `MODE=testnet` selection moves into `provider::build_provider()`
- Remove `RateLimiter` and Pyth constants from `stream.rs`

## Why
Step 1 of the multi-quote-price architectural cleanup. Decoupling from Pyth enables
unit tests without HTTP, future oracle swaps, and removes the hardcoded
`MODE == "testnet"` branch from the streaming hot path.

## Test plan
- [ ] `cargo build`
- [ ] `cargo test --lib` (includes new `MockProvider` tests)
- [ ] `cargo clippy --all-targets -- -D warnings` on touched files
- [ ] `MODE=testnet cargo build` compiles
- [ ] Runtime smoke test on testnet env after merge (mocked price flows through stream → receive → cache/DB)
- [ ] Runtime smoke test on mainnet shadow after merge (Pyth HTTP path unchanged)
EOF
)"
```

Expected: PR URL printed. Return this URL to the user.

---

## Self-Review Notes

- **Spec coverage:** All four motivations from the architecture discussion are addressed — (1) trait abstraction ✅, (2) testability via MockProvider ✅, (3) removal of `MODE == testnet` hack from hot path ✅, (4) rate-limiter encapsulation inside provider ✅.
- **No placeholders:** All code blocks are complete. All `grep` expected outputs are specified. Task 7's edits reference exact line ranges and provide full replacement code.
- **Type consistency:** `PriceProvider::fetch(feed_id: &str, timestamp: u64) -> Result<Option<BigDecimal>>` is the single signature used in the trait, both impls, and both call sites in `stream.rs`. `Arc<dyn PriceProvider>` is how the stream holds it. `build_provider() -> Result<Arc<dyn PriceProvider>>` matches.
- **Out of scope for this plan:** WMON unification (plan B) and batch DB insert (plan C) are deliberately NOT touched here — this PR stays small and reversible.
