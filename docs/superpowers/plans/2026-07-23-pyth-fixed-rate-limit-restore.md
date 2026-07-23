# Pyth Fixed Rate-Limit Restore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the existing Pyth provider flow with a fixed process-local limit of 5 requests per rolling 10-second window.

**Architecture:** Keep the `PriceProvider` seam and the Price stream unchanged. Replace environment-derived limiter and cooldown settings inside the Pyth adapter with fixed production settings, retain the strict sliding-window implementation, and restore bounded exponential 429 backoff while preserving test-only injected settings for deterministic tests.

**Tech Stack:** Rust 2024, Tokio paused time, Reqwest, Axum test server, Cargo

## Global Constraints

- Keep `PRICE_MODE` provider selection unchanged.
- Keep the 1,001-block Price cycle, 25-block buckets, channel send, receiver persistence, and checkpoints unchanged.
- Production Pyth calls use 5 requests per rolling 10-second window and 3 retries.
- HTTP 429 backoff starts at 1 second, grows as `1, 3, 7, 15, ...`, and is capped at 60 seconds.
- Do not add a sampler, forward-fill path, schema change, or migration.
- Do not inspect or modify `.env*` files.
- Do not commit, push, open a PR, merge, or deploy.

---

### Task 1: Fix the Pyth adapter limits and retry behavior

**Files:**
- Modify: `src/event/common/price/provider/pyth.rs:7-415`
- Test: `src/event/common/price/provider/pyth.rs:417-680`

**Interfaces:**
- Consumes: `PriceProvider::fetch` and `PriceProvider::fetch_batch` from `src/event/common/price/provider/mod.rs`
- Produces: unchanged `PythProvider::new() -> anyhow::Result<PythProvider>`
- Produces internally: `PythRateLimitConfig::fixed() -> PythRateLimitConfig`
- Produces internally: `next_backoff(current: Duration, maximum: Duration) -> Duration`

- [ ] **Step 1: Replace the environment-configuration tests with failing fixed-policy tests**

Add focused tests before production changes:

```rust
#[test]
fn production_rate_limit_is_five_requests_per_ten_seconds() {
    let config = PythRateLimitConfig::fixed();

    assert_eq!(config.max_requests, 5);
    assert_eq!(config.window, Duration::from_secs(10));
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.initial_backoff, Duration::from_secs(1));
    assert_eq!(config.max_backoff, Duration::from_secs(60));
}

#[test]
fn exponential_backoff_matches_existing_sequence_and_cap() {
    let maximum = Duration::from_secs(60);
    let mut delay = Duration::from_secs(1);
    let mut observed = Vec::new();

    for _ in 0..7 {
        observed.push(delay.as_secs());
        delay = next_backoff(delay, maximum);
    }

    assert_eq!(observed, vec![1, 3, 7, 15, 31, 60, 60]);
}

#[tokio::test(start_paused = true)]
async fn limiter_admits_five_requests_and_delays_the_sixth() {
    let limiter = Arc::new(RateLimiter::new(5, Duration::from_secs(10)));
    for _ in 0..5 {
        limiter.wait_if_needed().await;
    }

    let sixth = {
        let limiter = Arc::clone(&limiter);
        tokio::spawn(async move { limiter.wait_if_needed().await })
    };
    tokio::task::yield_now().await;
    assert!(!sixth.is_finished());

    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert!(!sixth.is_finished());

    tokio::time::advance(Duration::from_secs(1)).await;
    sixth.await.unwrap();
}
```

Delete tests whose only contract is parsing `PYTH_MAX_REQUESTS`,
`PYTH_RATE_LIMIT_WINDOW_SECS`, `PYTH_429_COOLDOWN_SECS`, or
`PYTH_MAX_RETRIES`. Retain the local Axum server and bounded-retry coverage.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
SQLX_OFFLINE=true cargo test event::common::price::provider::pyth::tests --lib -- --nocapture
```

Expected: compilation fails because `PythRateLimitConfig::fixed`,
`initial_backoff`, `max_backoff`, and `next_backoff` do not exist yet.

- [ ] **Step 3: Implement the fixed production policy**

Replace environment parsing and `Retry-After` cooldown handling with:

```rust
const REQUEST_TIMEOUT_SECS: u64 = 30;
const MAX_REQUESTS_PER_WINDOW: usize = 5;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;
const INITIAL_429_BACKOFF: Duration = Duration::from_secs(1);
const MAX_429_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PythRateLimitConfig {
    max_requests: usize,
    window: Duration,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl PythRateLimitConfig {
    const fn fixed() -> Self {
        Self {
            max_requests: MAX_REQUESTS_PER_WINDOW,
            window: RATE_LIMIT_WINDOW,
            max_retries: MAX_RETRIES,
            initial_backoff: INITIAL_429_BACKOFF,
            max_backoff: MAX_429_BACKOFF,
        }
    }
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current
        .saturating_mul(2)
        .saturating_add(Duration::from_secs(1))
        .min(maximum)
}
```

Keep `PythProvider::with_config` private for deterministic tests, store
`initial_backoff` and `max_backoff` on the provider, and make
`PythProvider::new()` call:

```rust
Self::with_config((*PYTH_API_URL).clone(), PythRateLimitConfig::fixed())
```

In both `fetch` and `fetch_batch`, initialize:

```rust
let mut backoff = self.initial_backoff;
```

For each retryable 429, log and sleep using the current backoff, then update it:

```rust
let delay = backoff;
retry_count += 1;
warn!(
    "[PYTH] 429 ts={} attempt={}/{}; retrying in {}ms",
    timestamp,
    attempt,
    total_attempts(self.max_retries),
    delay.as_millis()
);
tokio::time::sleep(delay).await;
backoff = next_backoff(backoff, self.max_backoff);
```

Continue calling `rate_limiter.wait_if_needed()` immediately before every HTTP
attempt so initial calls and retries share the same five-request window.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
SQLX_OFFLINE=true cargo test event::common::price::provider::pyth::tests --lib -- --nocapture
```

Expected: all Pyth provider tests pass with no environment parsing tests left.

- [ ] **Step 5: Run formatting and focused static checks**

Run:

```bash
cargo fmt --all -- --check
SQLX_OFFLINE=true cargo clippy --lib -- -D warnings
```

Expected: both commands exit 0.

### Task 2: Align active Price documentation and validate runtime contracts

**Files:**
- Modify: `docs/event/common/price.md:29-37`
- Verify: `src/event/common/price/stream.rs`
- Verify: `src/event/common/price/receive.rs`
- Verify: `src/sync/receive.rs`
- Verify: `tests/giwa_runtime_contract.rs`

**Interfaces:**
- Consumes: fixed Pyth adapter policy from Task 1
- Produces: active operator documentation stating 5 requests per 10 seconds and bounded exponential 429 retry

- [ ] **Step 1: Update active Price documentation**

Replace the environment-driven request-limit section with:

```markdown
### Pyth 요청 제한

- Pyth provider는 프로세스 로컬 sliding window에서 10초당 최대 5회 요청한다.
- 최초 호출과 모든 재시도는 실제 HTTP 전송 직전에 같은 limiter를 통과한다.
- 429 응답은 최대 3회 재시도하며 `1초 → 3초 → 7초` 순서의 bounded exponential backoff를 사용한다.
- 같은 외부 IP의 다른 프로세스 요청은 Pyth 측 quota에서 합산될 수 있다.
```

- [ ] **Step 2: Verify removed runtime configuration names are absent from active source and documentation**

Run:

```bash
rg -n 'PYTH_MAX_REQUESTS|PYTH_RATE_LIMIT_WINDOW_SECS|PYTH_429_COOLDOWN_SECS|PYTH_MAX_RETRIES' src docs/event README.md
```

Expected: no matches.

- [ ] **Step 3: Run required project validation**

Run:

```bash
cargo fmt --all -- --check
SQLX_OFFLINE=true cargo clippy -- -D warnings
SQLX_OFFLINE=true cargo test --lib
SQLX_OFFLINE=true cargo test --test giwa_runtime_contract
SQLX_OFFLINE=true cargo test --verbose
SQLX_OFFLINE=true cargo build --verbose
```

Expected: every command exits 0. Database-, Redis-, RPC-, and long-running
replay validation remain skipped because this change has no live infrastructure
available locally.

- [ ] **Step 4: Inspect the final diff and working tree**

Run:

```bash
git diff --check
git diff -- src/event/common/price/provider/pyth.rs docs/event/common/price.md docs/superpowers/specs/2026-07-23-pyth-fixed-rate-limit-restore-design.md docs/superpowers/plans/2026-07-23-pyth-fixed-rate-limit-restore.md
git status --short
```

Expected: only the Pyth provider, active Price documentation, and the approved
spec/plan are changed by this task; pre-existing untracked files remain
untouched.
