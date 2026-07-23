# Price 100-Block Buckets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reuse one canonical Pyth price for every absolute 100-block interval while continuing to emit and persist one Price event per indexed block.

**Architecture:** Keep the existing Price stream, channel, receiver, database schema, and checkpoint flow. Add pure bucket/group/expansion helpers inside the Price stream module, then change the stream loop to resolve each quote from the exact canonical-block cache before making one Pyth batch fetch for an uncached bucket.

**Tech Stack:** Rust 2024, Tokio, BigDecimal, Alloy RPC, Pyth Hermes, DashMap-backed `CacheManager`, SQLx/PostgreSQL.

## Global Constraints

- The canonical block is exactly `block - (block % 100)`.
- Blocks `N00..=N99` share the price sampled for `N00`.
- Continue emitting and persisting one `(quote_id, block_number, price)` row per indexed block.
- A cached canonical bucket must not make a Pyth request.
- A mid-bucket cache miss must fetch using the canonical boundary block timestamp.
- Newly fetched canonical prices must be inserted into the in-memory cache at the boundary block before the next stream cycle.
- Do not create a synthetic database row for a canonical boundary outside the processed block range.
- Preserve bounded 32-way timestamp collection, range-level channel sends, the 1,000-block Price range cap, 10-second live polling, and the Pyth 20-request-per-10-second limiter.
- Preserve cached quote events when another quote cannot be resolved from Pyth.
- Do not add a migration or modify downstream Curve, Dex, Token, Vault, API, or WebSocket lookup contracts.
- Do not commit, push, open a PR, or merge until the user explicitly requests it.

---

### Task 1: Canonical Bucketing And Per-Block Expansion

**Files:**
- Modify: `src/event/common/price/stream.rs:1-235`
- Test: `src/event/common/price/stream.rs` inline `#[cfg(test)]` module

**Interfaces:**
- Consumes: `Vec<(u64, u64)>` containing `(block_number, block_timestamp)`.
- Produces: `canonical_bucket_block(block_number: u64) -> u64`.
- Produces: `group_block_timestamps_by_bucket(blocks: Vec<(u64, u64)>) -> BTreeMap<u64, Vec<(u64, u64)>>`.
- Produces: `canonical_timestamp_from_range(bucket_block: u64, blocks: &[(u64, u64)]) -> Option<u64>`.
- Produces: `expand_bucket_events(blocks: &[(u64, u64)], quote_prices: &BTreeMap<String, BigDecimal>) -> Vec<UpdatePrice>`.

- [ ] **Step 1: Write failing canonical-boundary and grouping tests**

Add `BTreeMap` and `BigDecimal` test imports and these tests before adding the helpers:

```rust
#[test]
fn canonical_price_bucket_floors_to_the_absolute_hundred_block() {
    assert_eq!(canonical_bucket_block(800), 800);
    assert_eq!(canonical_bucket_block(801), 800);
    assert_eq!(canonical_bucket_block(899), 800);
    assert_eq!(canonical_bucket_block(900), 900);
}

#[test]
fn groups_blocks_across_hundred_block_boundaries() {
    let buckets = group_block_timestamps_by_bucket(vec![
        (899, 1_899),
        (900, 1_900),
        (901, 1_901),
    ]);

    assert_eq!(buckets.get(&800), Some(&vec![(899, 1_899)]));
    assert_eq!(
        buckets.get(&900),
        Some(&vec![(900, 1_900), (901, 1_901)])
    );
}

#[test]
fn mid_bucket_range_requires_canonical_boundary_timestamp_fallback() {
    assert_eq!(
        canonical_timestamp_from_range(800, &[(855, 1_855), (856, 1_856)]),
        None
    );
    assert_eq!(
        canonical_timestamp_from_range(800, &[(800, 1_800), (801, 1_801)]),
        Some(1_800)
    );
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test event::common::price::stream::tests::canonical_price_bucket_floors_to_the_absolute_hundred_block --lib
cargo test event::common::price::stream::tests::groups_blocks_across_hundred_block_boundaries --lib
cargo test event::common::price::stream::tests::mid_bucket_range_requires_canonical_boundary_timestamp_fallback --lib
```

Expected: compilation fails because the canonical bucket/group/timestamp helpers do not exist.

- [ ] **Step 3: Add the minimal canonical-bucket helpers**

Change the collection import and add:

```rust
use std::{collections::BTreeMap, future::Future, time::Duration};

const PRICE_BUCKET_BLOCK_INTERVAL: u64 = 100;

fn canonical_bucket_block(block_number: u64) -> u64 {
    block_number - (block_number % PRICE_BUCKET_BLOCK_INTERVAL)
}

fn group_block_timestamps_by_bucket(
    blocks: Vec<(u64, u64)>,
) -> BTreeMap<u64, Vec<(u64, u64)>> {
    let mut buckets = BTreeMap::new();
    for (block_number, block_timestamp) in blocks {
        buckets
            .entry(canonical_bucket_block(block_number))
            .or_insert_with(Vec::new)
            .push((block_number, block_timestamp));
    }
    buckets
}

fn canonical_timestamp_from_range(
    bucket_block: u64,
    blocks: &[(u64, u64)],
) -> Option<u64> {
    blocks
        .iter()
        .find_map(|(block, timestamp)| (*block == bucket_block).then_some(*timestamp))
}
```

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```bash
cargo test event::common::price::stream::tests::canonical_price_bucket_floors_to_the_absolute_hundred_block --lib
cargo test event::common::price::stream::tests::groups_blocks_across_hundred_block_boundaries --lib
cargo test event::common::price::stream::tests::mid_bucket_range_requires_canonical_boundary_timestamp_fallback --lib
```

Expected: all three tests pass.

- [ ] **Step 5: Write the failing per-block expansion test**

```rust
#[test]
fn expands_one_canonical_price_to_every_block_in_the_bucket() {
    let blocks = vec![(801, 1_801), (802, 1_802), (899, 1_899)];
    let quote_prices = BTreeMap::from([(
        "quote-a".to_string(),
        BigDecimal::from(3_500),
    )]);

    let events = expand_bucket_events(&blocks, &quote_prices);

    assert_eq!(events.len(), 3);
    assert_eq!(
        events
            .iter()
            .map(|event| event.block_number)
            .collect::<Vec<_>>(),
        vec![801, 802, 899]
    );
    assert!(
        events
            .iter()
            .all(|event| event.price == BigDecimal::from(3_500))
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.block_timestamp)
            .collect::<Vec<_>>(),
        vec![1_801, 1_802, 1_899]
    );
}
```

- [ ] **Step 6: Run the expansion test and verify RED**

Run:

```bash
cargo test event::common::price::stream::tests::expands_one_canonical_price_to_every_block_in_the_bucket --lib
```

Expected: compilation fails because `expand_bucket_events` does not exist.

- [ ] **Step 7: Implement minimal event expansion**

```rust
fn expand_bucket_events(
    blocks: &[(u64, u64)],
    quote_prices: &BTreeMap<String, BigDecimal>,
) -> Vec<UpdatePrice> {
    let mut events = Vec::with_capacity(blocks.len() * quote_prices.len());
    for (quote_id, price) in quote_prices {
        for (block_number, block_timestamp) in blocks {
            events.push(UpdatePrice {
                quote_id: quote_id.clone(),
                block_number: *block_number,
                price: price.clone(),
                block_timestamp: *block_timestamp,
            });
        }
    }
    events
}
```

- [ ] **Step 8: Run all Price stream helper tests**

Run:

```bash
cargo test event::common::price::stream::tests --lib
```

Expected: all canonical bucket, expansion, and existing timestamp-concurrency tests pass.

---

### Task 2: Cache-First Canonical Price Resolution

**Files:**
- Modify: `src/event/common/price/stream.rs:228-365`
- Test: `src/event/common/price/stream.rs` inline `#[cfg(test)]` module

**Interfaces:**
- Consumes: `QuoteConfig`, exact canonical-block cache results, and normalized Pyth batch results.
- Produces: `all_quotes_resolved(quotes: &[QuoteConfig], prices: &BTreeMap<String, BigDecimal>) -> bool`.
- Produces: `merge_missing_quote_prices(quotes: &[QuoteConfig], prices: &mut BTreeMap<String, BigDecimal>, fetched: &HashMap<String, BigDecimal>) -> Vec<(String, BigDecimal)>`.
- Uses Task 1's `group_block_timestamps_by_bucket` and `expand_bucket_events`.

- [ ] **Step 1: Write failing cache-decision and merge tests**

Add these imports and tests:

```rust
use std::collections::{BTreeMap, HashMap};

use crate::config::QuoteConfig;

fn quote(address: &str, feed: &str) -> QuoteConfig {
    QuoteConfig {
        address: address.to_string(),
        pyth_feed_id: feed.to_string(),
        decimals: BigDecimal::from(18),
    }
}

#[test]
fn fully_cached_bucket_does_not_need_provider_fetch() {
    let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
    let prices = BTreeMap::from([
        ("quote-a".to_string(), BigDecimal::from(10)),
        ("quote-b".to_string(), BigDecimal::from(20)),
    ]);

    assert!(all_quotes_resolved(&quotes, &prices));
}

#[test]
fn missing_canonical_quote_requires_provider_fetch() {
    let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
    let prices =
        BTreeMap::from([("quote-a".to_string(), BigDecimal::from(10))]);

    assert!(!all_quotes_resolved(&quotes, &prices));
}

#[test]
fn provider_results_fill_only_missing_quotes() {
    let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
    let mut prices =
        BTreeMap::from([("quote-a".to_string(), BigDecimal::from(10))]);
    let fetched = HashMap::from([
        ("feed-a".to_string(), BigDecimal::from(999)),
        ("feed-b".to_string(), BigDecimal::from(20)),
    ]);

    let newly_resolved = merge_missing_quote_prices(&quotes, &mut prices, &fetched);

    assert_eq!(prices["quote-a"], BigDecimal::from(10));
    assert_eq!(prices["quote-b"], BigDecimal::from(20));
    assert_eq!(
        newly_resolved,
        vec![("quote-b".to_string(), BigDecimal::from(20))]
    );
}

#[test]
fn unresolved_provider_quote_does_not_discard_cached_quote_events() {
    let quotes = vec![quote("quote-a", "feed-a"), quote("quote-b", "feed-b")];
    let mut prices =
        BTreeMap::from([("quote-a".to_string(), BigDecimal::from(10))]);

    let newly_resolved =
        merge_missing_quote_prices(&quotes, &mut prices, &HashMap::new());
    let events = expand_bucket_events(&[(855, 1_855)], &prices);

    assert!(newly_resolved.is_empty());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].quote_id, "quote-a");
    assert_eq!(events[0].block_number, 855);
    assert_eq!(events[0].price, BigDecimal::from(10));
}
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
cargo test event::common::price::stream::tests::fully_cached_bucket_does_not_need_provider_fetch --lib
cargo test event::common::price::stream::tests::missing_canonical_quote_requires_provider_fetch --lib
cargo test event::common::price::stream::tests::provider_results_fill_only_missing_quotes --lib
cargo test event::common::price::stream::tests::unresolved_provider_quote_does_not_discard_cached_quote_events --lib
```

Expected: compilation fails because the decision and merge helpers do not exist.

- [ ] **Step 3: Implement the minimal resolution helpers**

Import `HashMap`, `QuoteConfig`, and add:

```rust
fn all_quotes_resolved(
    quotes: &[QuoteConfig],
    prices: &BTreeMap<String, BigDecimal>,
) -> bool {
    quotes
        .iter()
        .all(|quote| prices.contains_key(&quote.address))
}

fn merge_missing_quote_prices(
    quotes: &[QuoteConfig],
    prices: &mut BTreeMap<String, BigDecimal>,
    fetched: &HashMap<String, BigDecimal>,
) -> Vec<(String, BigDecimal)> {
    let mut newly_resolved = Vec::new();
    for quote in quotes {
        if prices.contains_key(&quote.address) {
            continue;
        }
        let feed_id = provider::normalize_feed_id(&quote.pyth_feed_id);
        if let Some(price) = fetched.get(&feed_id) {
            prices.insert(quote.address.clone(), price.clone());
            newly_resolved.push((quote.address.clone(), price.clone()));
        }
    }
    newly_resolved
}
```

- [ ] **Step 4: Run the resolution tests and verify GREEN**

Run:

```bash
cargo test event::common::price::stream::tests::fully_cached_bucket_does_not_need_provider_fetch --lib
cargo test event::common::price::stream::tests::missing_canonical_quote_requires_provider_fetch --lib
cargo test event::common::price::stream::tests::provider_results_fill_only_missing_quotes --lib
cargo test event::common::price::stream::tests::unresolved_provider_quote_does_not_discard_cached_quote_events --lib
```

Expected: all four tests pass.

- [ ] **Step 5: Replace the 25-block fetch loop with canonical cache-first resolution**

After timestamp collection:

```rust
let bucket_to_blocks = group_block_timestamps_by_bucket(block_timestamps);
```

Replace the existing bucket loop with the following shape:

```rust
for (bucket_block, block_data) in &bucket_to_blocks {
    let mut resolved_prices = BTreeMap::new();
    for quote in quote_configs() {
        if let Some(price) = cache_manager
            .get_price_for_quote(&quote.address, *bucket_block as i64)
            .await
        {
            resolved_prices.insert(quote.address.clone(), price.as_ref().clone());
        }
    }

    if all_quotes_resolved(quote_configs(), &resolved_prices) {
        fetch_skipped_cached += 1;
    } else {
        let bucket_timestamp = match canonical_timestamp_from_range(*bucket_block, block_data) {
            Some(timestamp) => Some(timestamp),
            None => match get_block_timestamp(client, *bucket_block).await {
                Ok(timestamp) => Some(timestamp),
                Err(error) => {
                    error!(
                        "[PRICE] Failed to load canonical bucket timestamp for block {}: {}",
                        bucket_block, error
                    );
                    fetch_failed += 1;
                    None
                }
            },
        };

        if let Some(bucket_timestamp) = bucket_timestamp {
            let feed_ids: Vec<&str> = quote_configs()
                .iter()
                .map(|quote| quote.pyth_feed_id.as_str())
                .collect();
            fetch_attempted += 1;
            match price_provider.fetch_batch(&feed_ids, bucket_timestamp).await {
                Ok(fetched) => {
                    fetch_succeeded += 1;
                    let newly_resolved = merge_missing_quote_prices(
                        quote_configs(),
                        &mut resolved_prices,
                        &fetched,
                    );
                    for (quote_id, price) in newly_resolved {
                        cache_manager
                            .insert_price_for_quote(
                                &quote_id,
                                *bucket_block as i64,
                                price,
                            )
                            .await;
                    }
                }
                Err(error) => {
                    fetch_failed += 1;
                    error!(
                        "[PRICE] Batch fetch failed at canonical block {} timestamp {}: {}",
                        bucket_block, bucket_timestamp, error
                    );
                }
            }
        }
    }

    for quote in quote_configs() {
        if !resolved_prices.contains_key(&quote.address) {
            warn!(
                "[PRICE] Canonical bucket {} has no price for quote {} (feed_id={})",
                bucket_block, quote.address, quote.pyth_feed_id
            );
        }
    }
    events.extend(expand_bucket_events(block_data, &resolved_prices));
}
```

Update the cycle log to use `bucket_to_blocks.len()`. Remove the old 25-block constant, first-block cache check, sorted hash keys, and inline event-construction loop.

- [ ] **Step 6: Run all Price tests**

Run:

```bash
cargo test event::common::price:: --lib
```

Expected: all Price bucket, timestamp, channel, provider, limiter, and retry tests pass.

- [ ] **Step 7: Run library and runtime-contract regression tests**

Run:

```bash
cargo test --lib
cargo test --test giwa_runtime_contract
```

Expected: 0 failures; Curve still depends on Price and Price remains independently streamed.

---

### Task 3: Documentation, Full Validation, And Review

**Files:**
- Modify: `docs/event/common/price.md:20-35`
- Verify: `docs/superpowers/specs/2026-07-23-price-100-block-buckets-design.md`
- Verify: `docs/superpowers/plans/2026-07-23-price-100-block-buckets.md`

**Interfaces:**
- Documents the Task 1 and Task 2 behavior without introducing configuration or schema changes.
- Produces no runtime interface.

- [ ] **Step 1: Update active Price documentation**

Replace the Stream processing section with:

```markdown
1. **100블록 canonical bucket 구성**: 각 블록을 `block - (block % 100)` 경계로 묶는다.
2. **canonical 캐시 확인**: quote별로 bucket 경계 블록의 exact price cache를 조회한다.
3. **캐시 미스 복구**: 하나라도 없으면 bucket 경계 블록 타임스탬프로 모든 quote feed를 Pyth batch 조회한다.
4. **블록별 이벤트 생성**: 같은 bucket의 모든 블록에 canonical 가격을 복제하되 원래 block number와 timestamp를 유지한다.
5. **Receiver 전달**: 기존 range batch로 전달하며 receiver는 블록별 cache/DB row를 저장하고 Price checkpoint를 갱신한다.
```

Add one sentence stating that provider-level retries still count against the 20-request-per-10-second limiter.

- [ ] **Step 2: Run formatting and static checks**

Run:

```bash
cargo fmt --all -- --check
cargo clippy -- -D warnings
git diff --check
```

Expected: all commands exit 0 with no warnings.

- [ ] **Step 3: Run focused and full test commands**

Run:

```bash
cargo test event::common::price:: --lib
cargo test --lib
cargo test --test giwa_runtime_contract
cargo test --verbose
cargo build --verbose
```

Expected: all compilation and pure/runtime-contract tests pass. If PostgreSQL, Redis, Docker/Testcontainers, or RPC-backed tests cannot run because infrastructure is unavailable, record the exact skipped or failed command without changing production data.

- [ ] **Step 4: Inspect the final diff**

Run:

```bash
git status -sb
git diff --stat
git diff -- src/event/common/price/stream.rs docs/event/common/price.md docs/superpowers/specs/2026-07-23-price-100-block-buckets-design.md docs/superpowers/plans/2026-07-23-price-100-block-buckets.md
```

Expected: only the Price stream, Price documentation, design, and implementation plan are changed.

- [ ] **Step 5: Request final code review**

Review the final diff against `origin/dev`, focusing on:

- canonical boundary correctness and off-by-one errors
- cached-bucket behavior making zero provider calls
- mid-bucket restart/cache-miss recovery
- preserving cached quote events on partial provider failure
- Price checkpoint and Curve dependency progression
- Pyth rate-limit regressions

Expected: no unresolved Critical or Important findings.

- [ ] **Step 6: Stop before external publication**

Report the touched files and exact validation results. Do not commit, push, create a PR, or merge until the user explicitly asks.
