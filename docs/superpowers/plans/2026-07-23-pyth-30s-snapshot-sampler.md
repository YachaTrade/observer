# Pyth 30-Second Snapshot Sampler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy 25-block Pyth bucket with one complete price snapshot sampled every 30 seconds and forward-filled into every processed GIWA block.

**Architecture:** A dedicated Tokio sampler owns all Pyth calls and publishes complete quote snapshots through a `watch` channel. The sampler resets its interval after every completed source/provider attempt, the Price stream captures one snapshot per block range and expands it into the existing per-quote, per-block `UpdatePrice` rows, and the Pyth provider performs only one HTTP attempt per sampler attempt.

**Tech Stack:** Rust 2024, Tokio `watch`/paused time, async-trait, Reqwest, BigDecimal, existing Price event channel/cache/PostgreSQL persistence.

## Global Constraints

- Start execution from `origin/dev`; never target or merge this work into `main`.
- Use a fixed `PRICE_SAMPLE_INTERVAL` of exactly 30 seconds; do not add an environment variable.
- Each sampler attempt performs at most one Pyth HTTP request, including 429 and transport failures.
- Reset the interval after every completed attempt so the next source/provider attempt
  cannot start for 30 seconds; retain `tokio::time::MissedTickBehavior::Skip` and never
  replay overdue ticks as a burst.
- Sample the canonical source block at `latest_block.saturating_sub(5)`.
- Publish a snapshot only when every configured quote feed is present.
- Before the first complete snapshot, do not emit an empty Price batch or advance the Price stream checkpoint.
- After the first complete snapshot, provider failures retain the previous snapshot without stopping Price block processing.
- Backfill uses the latest process-time snapshot; do not query historical Pyth buckets.
- Preserve the `price` stream/receive checkpoint names and `(quote_id, block_number)` persistence.
- Do not change migrations, `.env*`, `price_usd`, or downstream dependency ordering.
- Do not commit, push, or open/merge a PR unless the user explicitly authorizes that external action. Commit steps below are execution checkpoints and must be skipped without that authorization.

---

## File Map

- Create `src/event/common/price/sampler.rs`: snapshot type, one-shot sampling, fixed interval loop, complete-response validation.
- Modify `src/event/common/price/mod.rs`: expose the sampler module.
- Modify `src/event/common/price/provider/pyth.rs`: remove sliding-window/retry state and make each trait call one HTTP attempt.
- Modify `src/event/common/price/stream.rs`: remove block bucketing/provider calls, spawn the sampler, and expand one captured snapshot into block rows.
- Modify `docs/event/common/price.md`: document 30-second process-time sampling and forward-fill semantics.
- Modify `tests/giwa_runtime_contract.rs`: lock down sampler wiring, stable checkpoints, and removal of the active 25-block bucket.
- Keep `src/event/common/price/receive.rs`, `src/db/postgres/controller/price.rs`, and migrations unchanged.

---

### Task 1: Make Pyth provider calls single-attempt

**Files:**
- Modify: `src/event/common/price/provider/pyth.rs`

**Interfaces:**
- Consumes: `PriceProvider::{fetch, fetch_batch}` from `src/event/common/price/provider/mod.rs`.
- Produces: the same trait behavior and response parsing, but exactly one HTTP send per method invocation.

- [ ] **Step 1: Replace retry-policy tests with a failing single-attempt 429 test**

Retain the loopback Axum server helper and add:

```rust
#[tokio::test]
async fn batch_429_is_returned_after_one_http_attempt() {
    let (base_url, request_count) =
        spawn_pyth_server(vec![StatusCode::TOO_MANY_REQUESTS]).await;
    let provider = PythProvider::with_base_url(base_url).unwrap();

    let error = provider
        .fetch_batch(&["feed"], 123)
        .await
        .expect_err("429 must be returned without an internal retry");

    assert!(error.to_string().contains("429"));
    assert_eq!(request_count.load(Ordering::SeqCst), 1);
}
```

Change `spawn_pyth_server` to accept only `statuses: Vec<StatusCode>`; remove its
`Retry-After` header argument because the provider no longer consumes it.

- [ ] **Step 2: Run the focused provider test and verify RED**

Run:

```bash
SQLX_OFFLINE=true cargo test \
  event::common::price::provider::pyth::tests::batch_429_is_returned_after_one_http_attempt \
  --lib -- --nocapture
```

Expected: FAIL to compile because `PythProvider::with_base_url` does not exist, or fail
with more than one observed request under the old retry loop.

- [ ] **Step 3: Remove limiter and retry state**

Reduce the provider to:

```rust
const REQUEST_TIMEOUT_SECS: u64 = 30;

pub struct PythProvider {
    http: Client,
    base_url: String,
}

impl PythProvider {
    pub fn new() -> Result<Self> {
        Self::with_base_url((*PYTH_API_URL).clone())
    }

    fn with_base_url(base_url: String) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for PythProvider")?;
        Ok(Self { http, base_url })
    }
}
```

Delete `PythRateLimitConfig`, `RateLimiter`, backoff constants/helpers, retry counters,
and retry sleeps. In both `fetch` and `fetch_batch`:

1. build the URL once;
2. call `send().await.context(...)` once;
3. return an error immediately for every non-success status;
4. preserve current JSON parsing, exponent conversion, and normalized feed map output.

Use status-specific errors:

```rust
if !response.status().is_success() {
    anyhow::bail!(
        "Pyth batch API returned status: {}",
        response.status()
    );
}
```

Do not log or include response headers/bodies in errors.

- [ ] **Step 4: Run all Pyth provider tests and verify GREEN**

Run:

```bash
SQLX_OFFLINE=true cargo test event::common::price::provider::pyth::tests --lib -- --nocapture
```

Expected: all Pyth tests PASS and the persistent-429 server observes exactly one request.

- [ ] **Step 5: Review the provider diff**

Run:

```bash
git diff --check -- src/event/common/price/provider/pyth.rs
rg -n "RateLimiter|MAX_RETRIES|next_backoff|retry_count|sleep\\(" \
  src/event/common/price/provider/pyth.rs
```

Expected: `git diff --check` succeeds and `rg` returns no retry/limiter matches.

- [ ] **Step 6: Commit only if explicitly authorized**

```bash
git add src/event/common/price/provider/pyth.rs
git commit -m "refactor: make Pyth requests single-attempt"
```

Expected: one focused provider commit. Without explicit commit authorization, leave the
validated diff uncommitted and continue.

---

### Task 2: Add the 30-second snapshot sampler

**Files:**
- Create: `src/event/common/price/sampler.rs`
- Modify: `src/event/common/price/mod.rs`

**Interfaces:**
- Consumes:
  - `Arc<dyn PriceProvider>`
  - `Vec<QuoteConfig>`
  - async source closure returning `Result<(u64, u64)>` as `(source_block, source_timestamp)`
- Produces:
  - `pub const PRICE_SAMPLE_INTERVAL: Duration`
  - `pub struct PriceSnapshot`
  - `pub async fn sample_once(...) -> Result<PriceSnapshot>`
  - `pub async fn run_sampler(...)`
  - `watch::Receiver<Option<Arc<PriceSnapshot>>>` consumed by Task 3

- [ ] **Step 1: Register the empty sampler module**

In `src/event/common/price/mod.rs` add:

```rust
pub mod sampler;
```

Create `src/event/common/price/sampler.rs` with imports and test module only so the
first test can define the required interface.

- [ ] **Step 2: Write failing complete-snapshot tests**

Add a recording `PriceProvider` test double and these tests:

```rust
#[tokio::test]
async fn sample_once_maps_every_feed_to_its_quote_address() {
    let provider = RecordingProvider::with_prices([
        ("feed-a", BigDecimal::from(10)),
        ("feed-b", BigDecimal::from(20)),
    ]);
    let quotes = vec![quote("0xaaa", "feed-a"), quote("0xbbb", "feed-b")];

    let snapshot = sample_once(&provider, &quotes, 100, 1_000)
        .await
        .unwrap();

    assert_eq!(snapshot.source_block, 100);
    assert_eq!(snapshot.source_timestamp, 1_000);
    assert_eq!(snapshot.prices_by_quote["0xaaa"], BigDecimal::from(10));
    assert_eq!(snapshot.prices_by_quote["0xbbb"], BigDecimal::from(20));
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn partial_provider_response_is_rejected() {
    let provider =
        RecordingProvider::with_prices([("feed-a", BigDecimal::from(10))]);
    let quotes = vec![quote("0xaaa", "feed-a"), quote("0xbbb", "feed-b")];

    let error = sample_once(&provider, &quotes, 100, 1_000)
        .await
        .expect_err("a partial response must not publish a snapshot");

    assert!(error.to_string().contains("0xbbb"));
    assert_eq!(provider.calls(), 1);
}
```

The helper constructs `QuoteConfig` with `decimals: BigDecimal::from(1)`. The recording
provider must normalize output keys with `normalize_feed_id`.

- [ ] **Step 3: Run the sampler tests and verify RED**

Run:

```bash
SQLX_OFFLINE=true cargo test event::common::price::sampler::tests --lib -- --nocapture
```

Expected: FAIL because `PriceSnapshot` and `sample_once` are not defined.

- [ ] **Step 4: Implement `PriceSnapshot` and `sample_once`**

Implement:

```rust
pub const PRICE_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
pub const PRICE_HEAD_OFFSET: u64 = 5;

#[derive(Debug, Clone)]
pub struct PriceSnapshot {
    pub prices_by_quote: HashMap<String, BigDecimal>,
    pub source_block: u64,
    pub source_timestamp: u64,
    pub sampled_at: Instant,
}

pub async fn sample_once(
    provider: &dyn PriceProvider,
    quotes: &[QuoteConfig],
    source_block: u64,
    source_timestamp: u64,
) -> Result<PriceSnapshot> {
    let feed_ids: Vec<&str> =
        quotes.iter().map(|quote| quote.pyth_feed_id.as_str()).collect();
    let fetched = provider.fetch_batch(&feed_ids, source_timestamp).await?;
    let mut prices_by_quote = HashMap::with_capacity(quotes.len());

    for quote in quotes {
        let key = normalize_feed_id(&quote.pyth_feed_id);
        let price = fetched.get(&key).with_context(|| {
            format!(
                "Pyth response missing quote {} feed {}",
                quote.address, quote.pyth_feed_id
            )
        })?;
        prices_by_quote.insert(quote.address.clone(), price.clone());
    }

    Ok(PriceSnapshot {
        prices_by_quote,
        source_block,
        source_timestamp,
        sampled_at: Instant::now(),
    })
}
```

- [ ] **Step 5: Write failing paused-time cadence tests**

Define the loop with this exact callable boundary:

```rust
pub async fn run_sampler<S, Fut>(
    provider: Arc<dyn PriceProvider>,
    quotes: Vec<QuoteConfig>,
    snapshot_tx: watch::Sender<Option<Arc<PriceSnapshot>>>,
    mut source: S,
) where
    S: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(u64, u64)>> + Send,
```

Then add:

```rust
#[tokio::test(start_paused = true)]
async fn sampler_calls_immediately_then_once_at_thirty_seconds() {
    let provider = Arc::new(RecordingProvider::complete());
    let (tx, rx) = watch::channel(None);
    let handle = tokio::spawn(run_sampler(
        provider.clone(),
        vec![quote("0xaaa", "feed-a")],
        tx,
        || async { Ok((100, 1_000)) },
    ));

    tokio::task::yield_now().await;
    assert_eq!(provider.calls(), 1);
    assert!(rx.borrow().is_some());

    tokio::time::advance(Duration::from_secs(29)).await;
    tokio::task::yield_now().await;
    assert_eq!(provider.calls(), 1);

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(provider.calls(), 2);

    handle.abort();
}
```

Add a second test whose provider returns one success, one error, and one success. Assert
that the snapshot after the error retains the first price and the 60-second tick
atomically publishes the third response.

Add a slow-attempt test that blocks the first source call, advances paused time by 95
seconds, and releases it. Assert source/provider calls remain at one immediately after
release and for the next 29 seconds, then become two after one more second. On source
and provider failure paths, also assert that 29 seconds pass without a new attempt and
the next attempt begins at the 30-second boundary.

- [ ] **Step 6: Run cadence tests and verify RED**

Run:

```bash
SQLX_OFFLINE=true cargo test \
  event::common::price::sampler::tests::sampler_calls_immediately_then_once_at_thirty_seconds \
  --lib -- --nocapture
```

Expected: FAIL because `run_sampler` is not implemented.

- [ ] **Step 7: Implement the sampler loop**

Use:

```rust
pub async fn run_sampler<S, Fut>(
    provider: Arc<dyn PriceProvider>,
    quotes: Vec<QuoteConfig>,
    snapshot_tx: watch::Sender<Option<Arc<PriceSnapshot>>>,
    mut source: S,
) where
    S: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(u64, u64)>> + Send,
{
    let mut interval = tokio::time::interval(PRICE_SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        let started = Instant::now();
        let sample = match source().await {
            Ok((block, timestamp)) => sample_once(
                provider.as_ref(),
                &quotes,
                block,
                timestamp,
            )
            .await
            .map_err(|_| "provider_or_incomplete_snapshot"),
            Err(_) => Err("source"),
        };

        match sample {
            Ok(snapshot) => {
                info!(
                    "[PRICE-SAMPLER] success block={} ts={} quotes={} elapsed={}ms",
                    snapshot.source_block,
                    snapshot.source_timestamp,
                    snapshot.prices_by_quote.len(),
                    started.elapsed().as_millis()
                );
                drop(snapshot_tx.send_replace(Some(Arc::new(snapshot))));
            }
            Err(failure_kind) => {
                let age = snapshot_tx
                    .borrow()
                    .as_ref()
                    .map(|snapshot| snapshot.sampled_at.elapsed().as_secs());
                warn!(
                    "[PRICE-SAMPLER] failed failure_kind={} active_snapshot_age_secs={:?}",
                    failure_kind, age
                );
            }
        }

        interval.reset();
    }
}
```

The failure log must contain only a fixed failure kind and active snapshot age. It must
not display arbitrary provider/RPC error text.

- [ ] **Step 8: Run all sampler tests and verify GREEN**

Run:

```bash
SQLX_OFFLINE=true cargo test event::common::price::sampler::tests --lib -- --nocapture
```

Expected: all snapshot completeness, cadence, and failure-retention tests PASS.

- [ ] **Step 9: Commit only if explicitly authorized**

```bash
git add src/event/common/price/mod.rs src/event/common/price/sampler.rs
git commit -m "feat: add 30-second price sampler"
```

Without explicit commit authorization, leave the validated diff uncommitted.

---

### Task 3: Forward-fill snapshots into Price block ranges

**Files:**
- Modify: `src/event/common/price/stream.rs`

**Interfaces:**
- Consumes:
  - `sampler::{run_sampler, PriceSnapshot, PRICE_HEAD_OFFSET}`
  - `watch::Receiver<Option<Arc<PriceSnapshot>>>`
  - existing `PriceEventChannel`, `CacheManager`, `UpdatePrice`
- Produces:
  - `fn build_events(...) -> Vec<UpdatePrice>`
  - one snapshot-expanded event batch per Price block range

- [ ] **Step 1: Write failing pure expansion tests**

Extract this interface in `stream.rs`:

```rust
fn build_events(
    snapshot: &PriceSnapshot,
    quotes: &[QuoteConfig],
    blocks: &[(u64, u64)],
    exact_cache_hits: &HashSet<(String, u64)>,
) -> Vec<UpdatePrice>
```

Add tests:

```rust
#[test]
fn one_snapshot_is_copied_to_every_block_with_original_timestamps() {
    let snapshot = snapshot([("0xaaa", 2_500)]);
    let blocks = vec![(100, 1_000), (101, 1_001), (102, 1_002)];

    let events = build_events(
        &snapshot,
        &[quote("0xaaa", "feed-a")],
        &blocks,
        &HashSet::new(),
    );

    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|event| event.price == BigDecimal::from(2_500)));
    assert_eq!(
        events
            .iter()
            .map(|event| (event.block_number, event.block_timestamp))
            .collect::<Vec<_>>(),
        blocks
    );
}

#[test]
fn exact_cached_quote_block_rows_are_not_emitted_again() {
    let snapshot = snapshot([("0xaaa", 2_500)]);
    let hits = HashSet::from([("0xaaa".to_string(), 101)]);

    let events = build_events(
        &snapshot,
        &[quote("0xaaa", "feed-a")],
        &[(100, 1_000), (101, 1_001)],
        &hits,
    );

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].block_number, 100);
}
```

- [ ] **Step 2: Run the stream tests and verify RED**

Run:

```bash
SQLX_OFFLINE=true cargo test event::common::price::stream::tests --lib -- --nocapture
```

Expected: FAIL because `build_events` does not exist.

- [ ] **Step 3: Implement deterministic event expansion**

Implement `build_events` by iterating `blocks` outermost and `quotes` in configured
order. For every non-hit pair, require `snapshot.prices_by_quote[quote.address]` and
construct:

```rust
UpdatePrice {
    quote_id: quote.address.clone(),
    block_number,
    price: price.clone(),
    block_timestamp,
}
```

Do not iterate the snapshot `HashMap` directly; configured quote order keeps event
ordering deterministic.

- [ ] **Step 4: Rewire `stream_events` to start the sampler**

After obtaining `client`, `cache_manager`, and `price_provider`, create:

```rust
let (snapshot_tx, snapshot_rx) =
    tokio::sync::watch::channel::<Option<Arc<PriceSnapshot>>>(None);
let sampler_quotes = quote_configs().clone();
let sampler_provider = Arc::clone(&price_provider);

tokio::spawn(run_sampler(
    sampler_provider,
    sampler_quotes,
    snapshot_tx,
    move || async move {
        let latest_block = client.get_cached_latest_block();
        let source_block = latest_block.saturating_sub(PRICE_HEAD_OFFSET);
        let source_timestamp = get_block_timestamp(client, source_block).await?;
        Ok((source_block, source_timestamp))
    },
));
```

The global `RpcClient` reference is `'static`, so the source closure can be spawned.

- [ ] **Step 5: Replace the 25-block bucket loop**

Delete:

- `timestamp_to_blocks`;
- `BUCKET_BLOCK_INTERVAL`;
- all per-bucket cache checks;
- all direct `price_provider.fetch_batch` calls;
- bucket fetch counters.

For each block range:

1. capture `let snapshot = snapshot_rx.borrow().clone();`;
2. when it is `None`, log `[PRICE] waiting for initial snapshot`, sleep the existing
   polling interval, and `continue` before sending or advancing the checkpoint;
3. collect `(block_number, block_timestamp)` in ascending order;
4. collect exact cache hits by calling `get_price_for_quote` for each quote/block;
5. call `build_events`;
6. send the existing event batch;
7. advance the existing stream checkpoint only after the send succeeds.

The cycle log becomes:

```rust
info!(
    "[PRICE] cycle blocks={} rows={} exact_cache_hits={} snapshot_block={} snapshot_age_secs={}",
    blocks.len(),
    events_count,
    exact_cache_hits.len(),
    snapshot.source_block,
    snapshot.sampled_at.elapsed().as_secs()
);
```

Keep `POLL_INTERVAL` at 10 seconds and keep the existing 1,000-block Price range cap
and five-block head lag.

- [ ] **Step 6: Add a source contract for no-snapshot checkpoint behavior**

Add a unit-testable helper:

```rust
fn current_snapshot(
    receiver: &watch::Receiver<Option<Arc<PriceSnapshot>>>,
) -> Option<Arc<PriceSnapshot>> {
    receiver.borrow().clone()
}
```

Test that a fresh channel returns `None`, and after `send_replace(Some(...))` it returns
that snapshot. The runtime-contract test in Task 4 will ensure the `None` branch occurs
before `channel.send` and `set_event_block_processed_block`.

- [ ] **Step 7: Run focused stream and sampler tests**

Run:

```bash
SQLX_OFFLINE=true cargo test event::common::price --lib -- --nocapture
```

Expected: provider, sampler, and stream tests all PASS.

- [ ] **Step 8: Commit only if explicitly authorized**

```bash
git add src/event/common/price/stream.rs
git commit -m "feat: forward-fill price snapshots by block"
```

Without explicit commit authorization, leave the validated diff uncommitted.

---

### Task 4: Lock runtime contracts and update documentation

**Files:**
- Modify: `tests/giwa_runtime_contract.rs`
- Modify: `docs/event/common/price.md`
- Include: `docs/superpowers/specs/2026-07-23-pyth-30s-snapshot-sampler-design.md`
- Include: `docs/superpowers/plans/2026-07-23-pyth-30s-snapshot-sampler.md`

**Interfaces:**
- Consumes: active source text and the stable runtime contract suite.
- Produces: regression protection for 30-second sampling and operator-facing behavior.

- [ ] **Step 1: Add failing runtime-contract assertions**

Add:

```rust
fn normalized_production_source(source: &str) -> String {
    source
        .split("#[cfg(test)]")
        .next()
        .expect("production source precedes the test module")
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

#[test]
fn price_stream_uses_process_time_sampler_instead_of_block_buckets() {
    let sampler =
        normalized_production_source(include_str!("../src/event/common/price/sampler.rs"));
    let stream = normalized_production_source(include_str!("../src/event/common/price/stream.rs"));

    assert!(sampler.contains(
        "pubconstPRICE_SAMPLE_INTERVAL:Duration=Duration::from_secs(30);"
    ));
    assert!(sampler.contains(
        "interval.set_missed_tick_behavior(MissedTickBehavior::Skip);"
    ));
    assert!(sampler.contains("interval.reset();"));
    assert!(stream.contains("tokio::spawn(run_sampler("));
    assert!(!stream.contains("BUCKET_BLOCK_INTERVAL"));
    assert!(!stream.contains("block_number%"));
}

#[test]
fn price_stream_waits_for_an_initial_snapshot_before_advancing() {
    let stream = normalized_production_source(include_str!("../src/event/common/price/stream.rs"));
    let wait_branch = concat!(
        "letSome(snapshot)=current_snapshot(&snapshot_rx)else{",
        "warn!(\"[PRICE]waitingforinitialsnapshot\");",
        "tokio::time::sleep(POLL_INTERVAL).await;",
        "continue;",
        "};"
    );
    let wait = stream
        .find(wait_branch)
        .expect("missing exact initial-snapshot wait branch");
    let send = stream
        .find("channel.send(events,to_block,latest_block).await")
        .expect("missing stable Price event-channel send");
    let checkpoint = stream
        .find("STREAM_MANAGER.set_event_block_processed_block(event_type,to_block).await;")
        .expect("missing stable Price checkpoint update");

    assert!(wait < send);
    assert!(send < checkpoint);
}
```

- [ ] **Step 2: Run runtime-contract tests and verify RED if wiring is incomplete**

Run:

```bash
SQLX_OFFLINE=true cargo test --test giwa_runtime_contract
```

Expected before complete wiring: one or both new tests FAIL. After Task 3 is complete:
15 existing tests plus the new tests PASS.

- [ ] **Step 3: Rewrite the active Price documentation**

Update `docs/event/common/price.md` to state:

- Pyth sampling is process-time based, not block-count based;
- the first call is immediate and every completed attempt is followed by a full
  30-second quiet period;
- missed and overdue ticks are skipped without immediate catch-up;
- each sampler attempt has one batch HTTP attempt and no provider retry;
- only complete quote snapshots replace the active value;
- every block retains its own block number/timestamp but shares the captured snapshot
  price;
- backfill uses the latest process-time snapshot;
- a failed tick retains the last snapshot indefinitely and logs its age;
- no initial snapshot means the Price checkpoint waits;
- GIWA Flashblocks are outside this canonical block stream.

Remove active references to 25-block bucketing and `5 requests / 10 seconds`.

- [ ] **Step 4: Search for active stale policy references**

Run:

```bash
rg -n \
  "BUCKET_BLOCK_INTERVAL|25.?block|5 requests|5회|MAX_RETRIES|RateLimiter" \
  src/event/common/price docs/event/common/price.md tests/giwa_runtime_contract.rs
```

Expected: matches occur only in negative runtime-contract assertions and historical
test names where removal would reduce clarity; no active implementation/documentation
claims the old policy.

- [ ] **Step 5: Commit only if explicitly authorized**

```bash
git add \
  tests/giwa_runtime_contract.rs \
  docs/event/common/price.md \
  docs/superpowers/specs/2026-07-23-pyth-30s-snapshot-sampler-design.md \
  docs/superpowers/plans/2026-07-23-pyth-30s-snapshot-sampler.md
git commit -m "docs: describe 30-second price snapshots"
```

Without explicit commit authorization, leave the validated diff uncommitted.

---

### Task 5: Final validation and review

**Files:**
- Review every file listed in Tasks 1-4.

**Interfaces:**
- Consumes: complete working tree implementation.
- Produces: validation evidence and a review-ready diff.

- [ ] **Step 1: Run formatting**

```bash
cargo fmt --all -- --check
```

Expected: PASS with no output.

- [ ] **Step 2: Run focused Price tests**

```bash
SQLX_OFFLINE=true cargo test event::common::price --lib -- --nocapture
```

Expected: all Price provider/sampler/stream tests PASS.

- [ ] **Step 3: Run Clippy**

```bash
SQLX_OFFLINE=true cargo clippy -- -D warnings
```

Expected: PASS with no warnings.

- [ ] **Step 4: Run library and runtime-contract tests**

```bash
SQLX_OFFLINE=true cargo test --lib
SQLX_OFFLINE=true cargo test --test giwa_runtime_contract
```

Expected: all tests PASS.

- [ ] **Step 5: Run build and the full available suite**

```bash
SQLX_OFFLINE=true cargo build --verbose
SQLX_OFFLINE=true cargo test --verbose
```

Expected: build and pure tests PASS. If PostgreSQL/Redis/Docker-backed tests cannot run,
record the exact infrastructure limitation rather than reporting them as code failures.

- [ ] **Step 6: Inspect final scope**

```bash
git diff --check
git status --short
git diff --stat
git diff -- \
  src/event/common/price \
  tests/giwa_runtime_contract.rs \
  docs/event/common/price.md \
  docs/superpowers/specs/2026-07-23-pyth-30s-snapshot-sampler-design.md \
  docs/superpowers/plans/2026-07-23-pyth-30s-snapshot-sampler.md
```

Expected: only the approved Price sampler/provider/stream/tests/docs are in scope;
no migration, `.env*`, `price_usd`, or unrelated user files are included.

- [ ] **Step 7: Run required code review**

Use the repository-required code reviewer to check:

- no second HTTP attempt can occur inside one sampler attempt;
- no checkpoint advances before an initial complete snapshot;
- provider/RPC failure retains the last complete snapshot;
- event ordering remains deterministic;
- no unbounded task or channel growth;
- no secrets or full provider payloads are logged;
- tests cover startup, cadence, partial response, failure retention, and forward-fill.

Address Critical/Important findings and rerun the affected validations.

- [ ] **Step 8: Publish only with explicit authorization**

If the user explicitly requests publishing, create a feature branch from `origin/dev`,
stage only the approved files, push it, and open a PR whose base is exactly `dev`.
Do not target `main`, and do not merge without a separate explicit merge request.
