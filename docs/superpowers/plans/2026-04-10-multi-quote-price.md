# Multi-Quote Price Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support USD price conversion for non-WMON quote tokens (e.g. USDC) by fetching multiple Pyth price feeds and using dynamic decimals per quote token.

**Architecture:** Add `QUOTE_CONFIGS` env-based config mapping quote token addresses to Pyth feed IDs and decimals. Extend CacheManager with per-quote price cache. Modify price stream to fetch multiple feeds. Replace hardcoded `NATIVE_DECIMALS` and `WNATIVE_ADDRESS` checks with dynamic quote-aware lookups.

**Tech Stack:** Rust, Pyth Network API, BigDecimal, DashMap, PostgreSQL

---

### Task 1: Add QuoteConfig to config.rs

**Files:**
- Modify: `src/config.rs:1-61`

- [ ] **Step 1: Add QuoteConfig struct and QUOTE_CONFIGS lazy_static**

After the existing `lazy_static!` block ending at line 61, and replacing the single `PYTH_PRICE_FEED_ID`, add:

```rust
// In the first lazy_static! block (lines 5-61), REPLACE lines 56-57:
//   pub static ref PYTH_PRICE_FEED_ID: String =
//       env::var("PYTH_PRICE_FEED_ID").expect("PYTH_PRICE_FEED_ID must be set");
// WITH:
    pub static ref QUOTE_CONFIGS: Vec<QuoteConfig> = parse_quote_configs();
```

Add the struct and parser after the last `lazy_static!` block (after line 198):

```rust
#[derive(Debug, Clone)]
pub struct QuoteConfig {
    pub address: String,
    pub pyth_feed_id: String,
    pub decimals: BigDecimal,
}

/// Parse QUOTE_CONFIGS env var.
/// Format: "address:pyth_feed_id:decimal_places,address2:feed_id2:decimal_places2"
/// Example: "0xWMON:0xfeed1:18,0xUSDC:0xfeed2:6"
fn parse_quote_configs() -> Vec<QuoteConfig> {
    let raw = env::var("QUOTE_CONFIGS").expect("QUOTE_CONFIGS must be set");
    raw.split(',')
        .map(|entry| {
            let parts: Vec<&str> = entry.trim().split(':').collect();
            if parts.len() != 3 {
                panic!("Invalid QUOTE_CONFIGS entry: '{}'. Expected format: address:feed_id:decimals", entry);
            }
            let decimal_places: u32 = parts[2].parse().expect("decimals must be a number");
            QuoteConfig {
                address: parts[0].to_string(),
                pyth_feed_id: parts[1].to_string(),
                decimals: BigDecimal::from_str(&format!("1{}", "0".repeat(decimal_places as usize))).unwrap(),
            }
        })
        .collect()
}

/// Get decimals for a quote token. Returns NATIVE_DECIMALS (10^18) if not found.
pub fn get_quote_decimals(quote_id: &str) -> &BigDecimal {
    QUOTE_CONFIGS
        .iter()
        .find(|q| q.address == quote_id)
        .map(|q| &q.decimals)
        .unwrap_or(&*NATIVE_DECIMALS)
}

/// Check if an address is a known quote token.
pub fn is_quote_token(address: &str) -> bool {
    QUOTE_CONFIGS.iter().any(|q| q.address == address)
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check 2>&1 | head -30`
Expected: Errors about removed `PYTH_PRICE_FEED_ID` in `stream.rs` (will fix in Task 4)

- [ ] **Step 3: Update .env to use new format**

Replace `PYTH_PRICE_FEED_ID=0x31491...` with:
```
QUOTE_CONFIGS=0xWMON_ADDR:0x31491744e2dbf6df7fcf4ac0820d18a609b49076d45066d3568424e62f686cd1:18
```

(Add more quote tokens as needed, comma-separated)

- [ ] **Step 4: Commit**

```bash
git add src/config.rs
git commit -m "feat: add QUOTE_CONFIGS for multi-quote token support"
```

---

### Task 2: Extend CacheManager with quote price cache

**Files:**
- Modify: `src/db/cache/mod.rs:24-76` (struct + new)
- Modify: `src/db/cache/mod.rs:613-750` (price methods section)

- [ ] **Step 1: Add quote_price_cache field to CacheManager struct**

Add to the struct at line 24:

```rust
pub struct CacheManager {
    redis: Arc<RedisDatabase>,
    postgres: Arc<PostgresDatabase>,
    price_cache: Arc<DashMap<i64, Arc<BigDecimal>>>,
    price_insertion_order: Arc<RwLock<VecDeque<i64>>>,
    /// Per-quote-token price cache: quote_address -> (block_number -> price)
    quote_price_cache: Arc<DashMap<String, DashMap<i64, Arc<BigDecimal>>>>,
}
```

Update `new()` at line 62:

```rust
pub async fn new() -> Result<Self> {
    let redis = RedisDatabase::instance()?;
    let postgres = PostgresDatabase::instance()?;
    let price_cache = Arc::new(DashMap::with_capacity(1000));
    let price_insertion_order = Arc::new(RwLock::new(VecDeque::with_capacity(1000)));
    let quote_price_cache = Arc::new(DashMap::new());

    let manager = Self {
        redis,
        postgres,
        price_cache,
        price_insertion_order,
        quote_price_cache,
    };

    Ok(manager)
}
```

- [ ] **Step 2: Add quote price methods**

Add after `remove_prices_before_or_equal` (after line 771):

```rust
    // ---- Multi-Quote Price Cache ----

    /// Insert a quote token price into cache
    pub async fn insert_quote_price(&self, quote_id: &str, block_number: i64, price: BigDecimal) {
        let inner = self.quote_price_cache
            .entry(quote_id.to_string())
            .or_insert_with(|| DashMap::with_capacity(500));
        inner.insert(block_number, Arc::new(price));
    }

    /// Batch insert quote token prices
    pub async fn insert_quote_price_batch(&self, quote_id: &str, prices: &[(i64, BigDecimal)]) {
        if prices.is_empty() {
            return;
        }
        let inner = self.quote_price_cache
            .entry(quote_id.to_string())
            .or_insert_with(|| DashMap::with_capacity(500));
        for (block_number, price) in prices {
            inner.insert(*block_number, Arc::new(price.clone()));
        }
        debug!(
            "Quote price batch cached: quote={}, count={}, cache_size={}",
            quote_id, prices.len(), inner.len()
        );
    }

    /// Get quote token price for a specific block
    pub async fn get_quote_price(&self, quote_id: &str, block_number: i64) -> Option<Arc<BigDecimal>> {
        // If it's WMON/native, use the existing price_cache
        if quote_id == *crate::config::WNATIVE_ADDRESS {
            return self.get_price(block_number).await;
        }
        self.quote_price_cache
            .get(quote_id)
            .and_then(|inner| inner.get(&block_number).map(|e| Arc::clone(e.value())))
    }

    /// Get latest quote price before a given block
    pub async fn get_latest_quote_price_before(&self, quote_id: &str, block_number: i64) -> Option<Arc<BigDecimal>> {
        if quote_id == *crate::config::WNATIVE_ADDRESS {
            return self.get_latest_price_before(block_number).await;
        }
        self.quote_price_cache
            .get(quote_id)
            .and_then(|inner| {
                inner.iter()
                    .filter(|entry| *entry.key() <= block_number)
                    .max_by_key(|entry| *entry.key())
                    .map(|entry| Arc::clone(entry.value()))
            })
    }

    /// Get most recent quote price (any block)
    pub async fn get_latest_quote_price(&self, quote_id: &str) -> Option<Arc<BigDecimal>> {
        if quote_id == *crate::config::WNATIVE_ADDRESS {
            return self.get_latest_price().await;
        }
        self.quote_price_cache
            .get(quote_id)
            .and_then(|inner| {
                inner.iter()
                    .max_by_key(|entry| *entry.key())
                    .map(|entry| Arc::clone(entry.value()))
            })
    }

    /// Remove old quote prices (cleanup)
    pub async fn remove_quote_prices_before_or_equal(&self, quote_id: &str, block_number: i64) {
        if let Some(inner) = self.quote_price_cache.get(quote_id) {
            inner.retain(|k, _| *k > block_number);
        }
    }
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check 2>&1 | head -30`
Expected: May still have errors from Task 1's PYTH_PRICE_FEED_ID removal (fixed in Task 4)

- [ ] **Step 4: Commit**

```bash
git add src/db/cache/mod.rs
git commit -m "feat: add multi-quote price cache to CacheManager"
```

---

### Task 3: Add quote_price DB table migration

**Files:**
- Create: `migrations/0017_quote_price.sql`

- [ ] **Step 1: Create migration**

```sql
-- Multi-quote token price storage
-- Stores USD prices for non-WMON quote tokens (WMON prices stay in the existing `price` table)
CREATE TABLE IF NOT EXISTS quote_price (
    quote_id VARCHAR(42) NOT NULL,
    block_number BIGINT NOT NULL,
    price NUMERIC(30,18) NOT NULL,
    block_timestamp BIGINT NOT NULL,
    PRIMARY KEY (quote_id, block_number)
);

CREATE INDEX IF NOT EXISTS idx_quote_price_quote_block
    ON quote_price (quote_id, block_number DESC);
```

- [ ] **Step 2: Verify migration numbering**

Run: `ls migrations/*.sql | tail -5`
Expected: 0016 is the last, so 0017 is correct. If not, adjust numbering.

- [ ] **Step 3: Commit**

```bash
git add migrations/0017_quote_price.sql
git commit -m "feat: add quote_price table for multi-quote USD pricing"
```

---

### Task 4: Extend price stream for multi-feed fetching

**Files:**
- Modify: `src/event/common/price/stream.rs:1-320`

- [ ] **Step 1: Update imports**

Replace line 11:
```rust
// OLD:
    config::{BLOCK_BATCH_SIZE, PYTH_API_URL, PYTH_PRICE_FEED_ID},
// NEW:
    config::{BLOCK_BATCH_SIZE, PYTH_API_URL, QUOTE_CONFIGS, WNATIVE_ADDRESS},
```

- [ ] **Step 2: Refactor fetch_price_with_retry to accept feed_id parameter**

Change the function signature at line 221:

```rust
async fn fetch_price_with_retry(
    http_client: &Client,
    timestamp: u64,
    max_retries: u32,
    feed_id: &str,
) -> Result<Option<BigDecimal>> {
```

And update the URL construction at line 238-240:

```rust
// OLD:
        let url = format!(
            "{}/{}?ids%5B%5D={}&encoding=hex&parsed=true&ignore_invalid_price_ids=false",
            *PYTH_API_URL, timestamp, *PYTH_PRICE_FEED_ID
        );
// NEW:
        let url = format!(
            "{}/{}?ids%5B%5D={}&encoding=hex&parsed=true&ignore_invalid_price_ids=false",
            *PYTH_API_URL, timestamp, feed_id
        );
```

- [ ] **Step 3: Add quote price fetching in stream_events**

In `stream_events()`, after the existing WMON price fetch loop (after line 195, before events_count), add:

```rust
        // Fetch prices for non-WMON quote tokens
        for quote_config in QUOTE_CONFIGS.iter() {
            if quote_config.address == *WNATIVE_ADDRESS {
                continue; // WMON already handled above
            }

            for (normalized_timestamp, block_data) in &timestamp_to_blocks {
                // Check cache first
                let first_block = block_data.first().map(|(block, _)| *block as i64);
                let cached = if let Some(block_num) = first_block {
                    cache_manager.get_quote_price(&quote_config.address, block_num).await
                } else {
                    None
                };

                if let Some(price) = cached {
                    // Already cached, skip fetch
                    for (block_number, _) in block_data {
                        cache_manager.insert_quote_price(
                            &quote_config.address,
                            *block_number as i64,
                            (*price).clone(),
                        ).await;
                    }
                    continue;
                }

                rate_limiter.wait_if_needed().await;

                match fetch_price_with_retry(
                    &http_client,
                    *normalized_timestamp,
                    3,
                    &quote_config.pyth_feed_id,
                ).await {
                    Ok(Some(price_data)) => {
                        for (block_number, _) in block_data {
                            cache_manager.insert_quote_price(
                                &quote_config.address,
                                *block_number as i64,
                                price_data.clone(),
                            ).await;
                        }
                    }
                    Ok(None) => {
                        warn!(
                            "No price data for quote {} at timestamp {}",
                            quote_config.address, normalized_timestamp
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to fetch price for quote {} at timestamp {}: {}",
                            quote_config.address, normalized_timestamp, e
                        );
                    }
                }
            }
        }
```

Also update the existing `fetch_price_with_retry` call at line 161 to find the WMON feed_id:

```rust
// Find WMON feed_id from QUOTE_CONFIGS
let wmon_feed_id = QUOTE_CONFIGS
    .iter()
    .find(|q| q.address == *WNATIVE_ADDRESS)
    .map(|q| q.pyth_feed_id.as_str())
    .expect("WMON must be in QUOTE_CONFIGS");

// Then in the fetch call:
let price_result = fetch_price_with_retry(
    &http_client,
    normalized_timestamp,
    3,
    wmon_feed_id,
)
.await;
```

Move the `wmon_feed_id` lookup before the loop (before line 99) so it's computed once.

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1 | head -30`
Expected: PASS (or errors in receive files, fixed in Task 5)

- [ ] **Step 5: Commit**

```bash
git add src/event/common/price/stream.rs
git commit -m "feat: multi-feed Pyth price fetching for quote tokens"
```

---

### Task 5: Refactor get_quote_usd_price to support all quote tokens

**Files:**
- Modify: `src/event/v2/curve/receive.rs:646-664`
- Modify: `src/event/v2/dex/receive.rs:436-454`

- [ ] **Step 1: Update get_quote_usd_price in curve/receive.rs**

Replace the function at lines 646-664:

```rust
async fn get_quote_usd_price(
    cache_manager: &Arc<CacheManager>,
    block_num: i64,
    quote_id: &str,
) -> Option<Arc<BigDecimal>> {
    // Try exact block -> latest before -> latest any -> DB fallback
    if let Some(price) = cache_manager.get_quote_price(quote_id, block_num).await {
        return Some(price);
    }
    if let Some(price) = cache_manager.get_latest_quote_price_before(quote_id, block_num).await {
        return Some(price);
    }
    if let Some(price) = cache_manager.get_latest_quote_price(quote_id).await {
        return Some(price);
    }
    // DB fallback only for WMON (existing price table)
    if quote_id == *crate::config::WNATIVE_ADDRESS {
        return cache_manager.get_price_from_db(block_num).await.map(Arc::new);
    }
    None
}
```

- [ ] **Step 2: Update all call sites in curve/receive.rs process_token_events**

Replace the `is_native_quote` logic (lines 148-150):

```rust
// OLD:
    let quote_id = cache_manager.get_token_quote_id(&token).await.unwrap_or(None);
    let is_native_quote = quote_id.as_deref() == Some(&*crate::config::WNATIVE_ADDRESS)
        || quote_id.is_none();
// NEW:
    let quote_id_str = cache_manager
        .get_token_quote_id(&token)
        .await
        .unwrap_or(None)
        .unwrap_or_else(|| (*crate::config::WNATIVE_ADDRESS).clone());
    let quote_decimals = crate::config::get_quote_decimals(&quote_id_str);
```

Update all `get_quote_usd_price` calls from `is_native_quote` to `&quote_id_str`:

```rust
// OLD: get_quote_usd_price(&cache_manager, block_num, is_native_quote).await
// NEW: get_quote_usd_price(&cache_manager, block_num, &quote_id_str).await
```

Replace all `&*NATIVE_DECIMALS` in USD calculations with `quote_decimals`:

```rust
// OLD: (&*CREATE_FEE_AMOUNT / &*NATIVE_DECIMALS) * &**price
// NEW: (&*CREATE_FEE_AMOUNT / quote_decimals) * &**price

// OLD: (&*buy.amount_in / &*NATIVE_DECIMALS) * &**price
// NEW: (&*buy.amount_in / quote_decimals) * &**price

// OLD: (&*sell.amount_out / &*NATIVE_DECIMALS) * &**price
// NEW: (&*sell.amount_out / quote_decimals) * &**price

// OLD: (&fee_native / &*NATIVE_DECIMALS) * &**price
// NEW: (&fee_native / quote_decimals) * &**price
```

- [ ] **Step 3: Apply same changes to dex/receive.rs**

Replace `get_quote_usd_price` at lines 436-454 with identical implementation from Step 1.

Replace `is_native_quote` logic at lines 119-121:

```rust
// OLD:
    let quote_id = cache_manager.get_token_quote_id(&token).await.unwrap_or(None);
    let is_native_quote = quote_id.as_deref() == Some(&*crate::config::WNATIVE_ADDRESS)
        || quote_id.is_none();
// NEW:
    let quote_id_str = cache_manager
        .get_token_quote_id(&token)
        .await
        .unwrap_or(None)
        .unwrap_or_else(|| (*crate::config::WNATIVE_ADDRESS).clone());
    let quote_decimals = crate::config::get_quote_decimals(&quote_id_str);
```

Replace all call sites and `NATIVE_DECIMALS` references as in Step 2.

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1 | head -30`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/event/v2/curve/receive.rs src/event/v2/dex/receive.rs
git commit -m "feat: quote-aware USD pricing with dynamic decimals"
```

---

### Task 6: Quote-aware token detection in DEX stream

**Files:**
- Modify: `src/event/v2/dex/stream.rs:161-379`

- [ ] **Step 1: Add import**

Add at top of file:
```rust
use crate::config::is_quote_token;
```

- [ ] **Step 2: Replace WNATIVE_ADDRESS checks with is_quote_token**

In `parse_log` function, replace all occurrences of:
```rust
let token0_is_mon = token0 == *WNATIVE_ADDRESS;
```
with:
```rust
let token0_is_quote = is_quote_token(&token0);
```

There are 4 occurrences at lines 214, 288, 330, 359. Update each one, and rename downstream usages of `token0_is_mon` to `token0_is_quote`.

For **Swap** (line 214-235):
```rust
let token0_is_quote = is_quote_token(&token0);

let (token, amount_in, amount_out, is_buy) = if token0_is_quote {
    if !amount0In.is_zero() {
        // Quote (token0) in, token (token1) out => Buy
        (token1.clone(), to_big_decimal(amount0In), to_big_decimal(amount1Out), true)
    } else {
        // Token (token1) in, quote (token0) out => Sell
        (token1.clone(), to_big_decimal(amount1In), to_big_decimal(amount0Out), false)
    }
} else {
    if !amount1In.is_zero() {
        // Quote (token1) in, token (token0) out => Buy
        (token0.clone(), to_big_decimal(amount1In), to_big_decimal(amount0Out), true)
    } else {
        // Token (token0) in, quote (token1) out => Sell
        (token0.clone(), to_big_decimal(amount0In), to_big_decimal(amount1Out), false)
    }
};
```

For **Sync** (line 288-289):
```rust
let token0_is_quote = is_quote_token(&token0);
let token = if token0_is_quote { &token1 } else { &token0 };

let (native_reserve, token_reserve) = if token0_is_quote {
    (to_big_decimal(reserve0), to_big_decimal(reserve1))
} else {
    (to_big_decimal(reserve1), to_big_decimal(reserve0))
};
```

For **Mint** (line 330-331):
```rust
let token0_is_quote = is_quote_token(&token0);
let token = if token0_is_quote { &token1 } else { &token0 };
```

For **Burn** (line 359-360):
```rust
let token0_is_quote = is_quote_token(&token0);
let token = if token0_is_quote { &token1 } else { &token0 };
```

- [ ] **Step 3: Remove unused WNATIVE_ADDRESS import if no longer needed**

Check if `WNATIVE_ADDRESS` is still used in this file. If the Swap event was the only usage, remove it from the import.

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1 | head -30`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/event/v2/dex/stream.rs
git commit -m "refactor: quote-aware token detection in V2 DEX stream"
```

---

### Task 7: Verify end-to-end and cleanup

**Files:**
- Verify: All modified files compile and work together

- [ ] **Step 1: Full compilation check**

Run: `cargo build 2>&1 | tail -20`
Expected: Successful build

- [ ] **Step 2: Search for remaining PYTH_PRICE_FEED_ID references**

Run: `grep -r "PYTH_PRICE_FEED_ID" src/`
Expected: No results. If any remain, update them.

- [ ] **Step 3: Search for remaining hardcoded NATIVE_DECIMALS in V2 USD calculations**

Run: `grep -rn "NATIVE_DECIMALS" src/event/v2/`
Expected: No results in V2 code. V1 code can keep using NATIVE_DECIMALS since V1 is always WMON.

- [ ] **Step 4: Search for remaining WNATIVE_ADDRESS in V2 DEX stream**

Run: `grep -n "WNATIVE_ADDRESS" src/event/v2/dex/stream.rs`
Expected: No results (all replaced with is_quote_token).

- [ ] **Step 5: Update .env.example if it exists**

Document the new `QUOTE_CONFIGS` format and remove `PYTH_PRICE_FEED_ID`.

- [ ] **Step 6: Commit final cleanup**

```bash
git add -A
git commit -m "chore: cleanup PYTH_PRICE_FEED_ID references, verify multi-quote build"
```
