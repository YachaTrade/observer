# Price Stream Catch-up Design

## Problem

The Price stream processes an inclusive 1,001-block range. It currently awaits
`get_block_timestamp` once per block, so a cold Redis cache turns every lookup
into a sequential RPC request. The stream does not publish a channel batch
until the full timestamp scan and every Pyth bucket fetch finish. On the
observed GIWA node, the first 101 timestamp lookups took about 37 seconds, so
`price_events` remained at `Sent:0` and Curve timed out waiting for Price.

An explicit nonzero `START_BLOCK` also resets Price to that configured block on
every restart, even when complete Price rows already exist in PostgreSQL.

## Considered Approaches

1. Reduce the Price range size. This makes the first batch appear sooner, but
   reduces catch-up throughput and still repeats old work after every restart.
2. Parallelize timestamp lookup only. This removes the immediate RPC
   serialization, but a restart still replays the whole configured history.
3. Use bounded parallel timestamp lookup and resume Price from its complete
   PostgreSQL watermark. This fixes both the per-range bottleneck and repeated
   catch-up. This is the selected approach.

## Design

### Bounded timestamp collection

Price collects `(block_number, timestamp)` pairs with at most 32 timestamp RPC
lookups in flight. The collected results are sorted by block number before
bucket construction, preserving deterministic block ordering.

The operation is atomic at range level: if any timestamp lookup or worker task
fails, Price discards the range and does not publish it or advance its in-memory
watermark.

### Price-specific restart watermark

At startup, the stream manager reads `MAX(block_number)` per configured
`quote_id` from `price`. It resumes Price at one block after the minimum of
those maxima only when every configured quote has a stored maximum at or after
`START_BLOCK`. Taking the minimum prevents a quote that is farther ahead from
causing another quote's missing rows to be skipped.

If any configured quote has no qualifying Price row, Price keeps the existing
`START_BLOCK` fallback. Other event streams keep their current initialization
and checkpoint names unchanged.

### Failure and persistence behavior

A Price batch is complete only when every required timestamp and every
configured Pyth feed for every required bucket succeeds. Incomplete batches
are discarded without advancing Price.

Price uses an acknowledged event channel. The stream advances its watermark
only after the receiver persists every quote batch successfully. A persistence
failure is returned to the stream, so unpersisted blocks are retried rather
than skipped.

## Validation

- Unit tests prove timestamp work overlaps, never exceeds 32 in-flight calls,
  returns block-sorted output, and rejects the whole range on one error.
- Unit tests prove the restart block uses the minimum complete quote watermark
  and falls back when a configured quote is missing.
- Channel tests prove Price propagates receiver persistence failure.
- Existing stream-policy, runtime-contract, library, Clippy, formatting, and
  build checks must remain green.
- A local observer run should show a nonzero `price_events` send and Price
  receiver progress without waiting on a sequential 1,001-RPC timestamp scan.

## Scope

No migration, `.env` file, PriceUsd cadence, contract decoder, or external
deployment change is included.
