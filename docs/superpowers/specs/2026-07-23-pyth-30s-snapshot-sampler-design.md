# Pyth 30-Second Snapshot Sampler Design

## Context

The active `price` stream groups blocks by `block_number % 25` and calls Pyth once for
each 25-block bucket. That policy came from Monad's roughly 400 ms block time, where
25 blocks represented about 10 seconds. GIWA is an OP Stack chain with a one-second
canonical block time, so the old block-count bucket no longer represents the intended
sampling interval.

The desired GIWA behavior is:

- start the next source/provider sampling attempt no earlier than 30 seconds after
  the previous attempt completes;
- keep the latest complete quote-price snapshot in memory;
- write that same snapshot for every canonical block processed until a newer snapshot
  is available;
- keep the existing per-block `price` rows and stream checkpoints;
- allow Price and its dependent receivers to continue when a later Pyth call fails.

Historical backfill accuracy is intentionally traded for bounded Pyth traffic and
forward progress. Backfilled blocks receive the latest snapshot available when the
range is processed, not the Pyth price at each block's historical timestamp.

## Goals

1. Enforce a full 30-second quiet period after every sampling attempt and at most one
   Pyth batch request per attempt.
2. Remove the legacy 25-block price bucket.
3. Forward-fill one complete snapshot across every processed block.
4. Preserve the `price` stream and receive checkpoint names.
5. Preserve the `price` table schema and `(quote_id, block_number)` rows.
6. Keep processing with the last complete snapshot during provider failures.
7. Avoid catch-up request bursts after a slow or delayed request.

## Non-Goals

- Historical Pyth reconstruction during backfill.
- Changes to `price_usd`.
- Database migrations or new tables.
- Changing Curve/Dex/LpManager/Token dependency ordering.
- Adding a configurable sampling interval.
- Supporting multiple independent sampler processes behind a distributed limiter.

## Considered Approaches

### A. Dedicated 30-second sampler

A background sampler owns Pyth calls and publishes the latest complete snapshot to the
price stream. The stream only expands snapshots into per-block rows.

This is the selected approach. It isolates external API timing from block processing,
keeps the stream moving with a stale-but-known price, and makes the request cadence
directly testable.

### B. Stream-local time-to-live

The price stream could keep `last_fetch_at` and refresh inline when 30 seconds pass.
This is smaller, but provider latency would continue to block range processing and the
sampling policy would remain tangled with backfill cycles.

### C. Thirty-second block-timestamp buckets

Blocks could be grouped by `block_timestamp / 30` and each historical bucket queried
from Pyth. This produces replayable historical prices, but catch-up would issue one
request per historical 30-second window and would not satisfy the strict real-time
request cadence.

## Architecture

### Snapshot model

Introduce an internal snapshot type owned by the price module:

```rust
struct PriceSnapshot {
    prices_by_quote: HashMap<String, BigDecimal>,
    source_block: u64,
    source_timestamp: u64,
}
```

`prices_by_quote` is keyed by configured quote address, not raw feed ID, so the stream
does not repeat provider-specific normalization while expanding rows.

Only a complete response containing every configured quote becomes the active
snapshot. A partial response is treated as a failed sample and cannot replace the
last complete snapshot.

### Sampler

The price handler starts one sampler task and one receiver task. The sampler:

1. ticks immediately on startup;
2. reads the current head, applies the Price stream's existing five-block safety
   offset, and resolves that safe canonical block's timestamp;
3. sends one `fetch_batch` request for all configured Pyth feed IDs;
4. validates that all quote feeds are present;
5. publishes a new complete snapshot through a Tokio `watch` channel;
6. resets the interval deadline after the attempt completes, whether it succeeds or
   fails, and waits 30 seconds before the next attempt.

The interval uses `MissedTickBehavior::Skip` and resets after every completed attempt.
A slow request therefore cannot leave an overdue tick that executes immediately, and
missed ticks never execute back-to-back.

The sampler source block is `latest_block.saturating_sub(5)`, matching the existing
Price stream head offset. It does not sample Flashblock or pending state.

The Pyth HTTP client keeps its request timeout, parsing, and feed normalization. Its
sliding-window limiter and internal retry loop are removed because each sampler tick
must perform at most one HTTP request. A 429 or transport failure is returned to the
sampler without an immediate retry.

`PRICE_MODE`/`MODE` provider selection remains unchanged. The mock provider therefore
uses the same sampling and forward-fill path in testnet mode.

### Stream expansion

The price stream keeps its existing block-range policy and ten-second idle polling
cadence. For each non-empty range it:

1. reads one snapshot from the `watch` channel;
2. resolves each block's canonical timestamp;
3. skips an exact `(quote_id, block_number)` cache hit;
4. creates `UpdatePrice` rows for every missing quote/block using the captured
   snapshot;
5. sends one event batch and advances the existing Price stream checkpoint.

One block range uses one captured snapshot, even if the sampler publishes a newer
snapshot while that range is being expanded. The next range sees the newer snapshot.
This keeps each event batch internally consistent.

The receiver continues to cache and persist one row per quote and block. The original
block timestamp remains the row's `created_at`; only the price value is forward-filled.

### Startup and failure behavior

- Before the first complete snapshot exists, the price stream does not send an empty
  batch and does not advance its stream checkpoint. It waits for the sampler.
- After at least one complete snapshot exists, a failed Pyth tick leaves the snapshot
  unchanged and block processing continues with that last good value.
- Source, provider, and incomplete-snapshot failures all reset the interval deadline,
  so failure does not trigger an immediate retry.
- There is no maximum snapshot age that stops processing. Logs must include snapshot
  age so stale data remains observable.
- A later successful tick atomically replaces the entire quote snapshot.

This behavior prioritizes receive progress over price freshness, matching the explicit
operational requirement.

## Data Flow

```text
latest canonical block timestamp
              |
              v
     30-second Pyth sampler
       | success      | failure
       v              v
 complete snapshot   keep previous snapshot
       |
       v
 Price block-range stream
       |
       +-- same snapshot copied to every block in the range
       |
       v
 Price event channel
       |
       v
 memory cache + PostgreSQL price rows
       |
       v
 Curve and downstream receivers
```

## Observability

Sampler logs must report:

- sample success or a fixed failure kind (`source` or
  `provider_or_incomplete_snapshot`);
- source block and source timestamp;
- quote count;
- request duration;
- active snapshot age after a failure.

The price stream cycle log must replace bucket counters with:

- range size;
- rows emitted;
- exact-cache hits;
- snapshot source block;
- snapshot age.

Sampler and provider logs must not include arbitrary error display text, credentials,
headers, or full error payloads.

## Testing

### Sampler tests

- the first tick calls the provider immediately;
- advancing paused time by 29 seconds produces no second call;
- the 30-second boundary produces exactly one additional call;
- after a 95-second first attempt completes, no second attempt starts immediately or
  during the next 29 seconds, and exactly one starts at 30 seconds;
- a partial quote response does not replace the active snapshot;
- source and provider failures retain the last complete snapshot and keep the same
  30-second post-completion quiet period;
- a 429 causes one HTTP request for that tick and no internal retry.

### Stream tests

- multiple blocks receive the same snapshot price and retain their own block
  timestamps;
- a newer snapshot is used by the next block range;
- exact cached quote/block rows are not emitted again;
- no initial snapshot means no event batch and no Price checkpoint advancement;
- backfill ranges use the current snapshot without historical Pyth calls.

### Regression validation

- Price stream and receive checkpoint names remain `price`;
- Curve still waits for Price under the existing dependency policy;
- `price` rows remain idempotent through the composite primary key;
- no migration is added;
- no 25-block price bucket remains in active Price code or documentation;
- formatter, Clippy, library tests, runtime-contract tests, and the full available
  suite pass.

## Operational Consequences

On GIWA's normal one-second canonical block cadence, one snapshot will usually cover
about 30 blocks. The count may vary when block delivery or processing is delayed.
Flashblock preconfirmations are not indexed by this flow and do not affect the
sampling policy.

Every observer process owns its own sampler. Running multiple observer instances
behind one public IP can therefore produce one Pyth request per process every
30 seconds. Coordinating multiple active instances is outside this change.
