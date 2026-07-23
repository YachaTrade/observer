# Price 100-Block Bucket Design

## Goal

Assign one canonical Pyth price to every 100-block interval so Price can advance cached, non-boundary blocks without waiting for another provider request.

For any block `b`, the canonical bucket block is:

```text
bucket_block = b - (b % 100)
```

Examples:

- block `31_451_800` uses the `31_451_800` price
- blocks `31_451_801..=31_451_899` reuse the `31_451_800` price
- block `31_451_900` begins a new bucket and uses the `31_451_900` price

## Scope

Change only the Price stream's bucket construction and canonical-price lookup. Preserve:

- one `UpdatePrice` row per quote and indexed block
- existing `price` table schema and uniqueness
- the Price receiver's per-block cache, database, and checkpoint behavior
- bounded parallel block-timestamp collection
- the non-acknowledged Price channel
- the 1,000-block Price stream range cap
- the 10-second live polling cadence
- the Pyth provider's 20-request-per-10-second limiter

No migration or downstream Curve, Dex, Token, Vault, API, or WebSocket change is required.

## Data Flow

1. Collect timestamps for the selected Price block range with the existing concurrency bound.
2. Group every block by its absolute 100-block canonical boundary.
3. Process canonical buckets in ascending order.
4. For every configured quote, perform an exact cache lookup at `bucket_block`.
5. If all quote prices exist at the canonical block, create per-block `UpdatePrice` events immediately from those cached prices without calling Pyth.
6. If one or more quote prices are missing:
   - resolve the canonical block timestamp from the collected range or RPC;
   - call Pyth once with all configured feed IDs;
   - use cached canonical prices for cache hits and Pyth results for missing quotes.
7. Store each newly fetched canonical price directly in the in-memory cache at `bucket_block`. This prevents a mid-bucket start such as block `855` from fetching the `800` price again at block `856`.
8. For each quote with a resolved canonical price, create an event for every actual block in that bucket. Each event keeps its own block number and block timestamp but shares the canonical price.
9. Send the completed stream-range batch to the existing receiver. The receiver caches and persists every per-block event, then advances the Price receive checkpoint.

The existing range-level channel send remains unchanged. “Immediate” means a cached bucket performs no Pyth HTTP request; it does not introduce a channel send per individual block.

## Cache Miss And Failure Behavior

- A process that starts in the middle of a bucket first checks the exact canonical block price.
- If that canonical price is absent, it fetches Pyth using the canonical boundary block's timestamp, even when the boundary block is outside the current stream range.
- A recovered canonical price is inserted into memory immediately; it is not persisted as a synthetic boundary row when the boundary is outside the processed range.
- A missing Pyth feed skips only that quote within the bucket.
- A failed canonical timestamp lookup or Pyth request leaves only the unresolved quotes absent. Quotes already resolved from the canonical cache still produce events for that bucket.
- If no quote can be resolved for a bucket, that bucket produces no events while successful buckets are preserved.
- The Price stream still sends the range batch and advances according to the existing progress-first behavior.
- Downstream price lookup retains its latest-before/latest/database fallback, so a skipped quote or bucket does not introduce a new lookup contract.

## Storage Semantics

The canonical price is expanded into one row per indexed block:

```text
(quote_id, 31_451_800, canonical_price)
(quote_id, 31_451_801, canonical_price)
...
(quote_id, 31_451_899, canonical_price)
```

This keeps all existing exact-block consumers and database queries compatible. Only the sampling frequency changes.

If processing begins at block `855`, the stream may cache the `800` canonical price in memory, but PostgreSQL receives rows beginning at the actual processed block `855`. No out-of-range synthetic database row is added.

## Testing

Focused unit tests must cover:

- canonical boundaries: `800 -> 800`, `801 -> 800`, `899 -> 800`, `900 -> 900`
- grouping a range that crosses a 100-block boundary
- expanding one canonical price into every block event in its bucket
- an exact canonical cache hit avoiding provider fetch
- a mid-bucket cache miss selecting the canonical boundary timestamp
- a newly fetched mid-bucket canonical price being identified for immediate memory caching
- cached quotes remaining usable when another quote requires Pyth
- missing feeds and failed buckets preserving successful bucket events

Existing Price channel, timestamp concurrency, Pyth limiter/retry, stream dependency, and GIWA runtime-contract tests must remain green.

## Success Criteria

- The stream invokes one Pyth batch fetch per uncached 100-block bucket, regardless of quote count; provider-level retries remain governed by the existing limiter.
- Non-boundary blocks reuse the exact canonical bucket price.
- Every processed block still receives its own Price event and database row.
- Price receive checkpoints continue advancing without waiting for a new Pyth request inside a cached bucket.
- Curve no longer waits on per-block or 25-block price sampling during normal cached-bucket progression.
