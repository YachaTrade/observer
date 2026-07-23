# Pyth Fixed Rate-Limit Restore Design

## Goal

Restore the Pyth provider's pre-`0c60871` execution model while using a fixed
process-local limit of 5 requests per rolling 10-second window.

This change removes the development override that reduced the provider to one
request per 60 seconds. The existing Price stream must resume completing its
1,001-block cycles without introducing a mock provider or a new forward-fill
sampler.

## Scope

- Keep `PRICE_MODE` provider selection unchanged.
- Keep the existing Price stream behavior unchanged:
  - at most 1,001 blocks per cycle;
  - one Pyth batch request per 25-block bucket;
  - one `PriceEventBatch` sent after the cycle finishes.
- Replace environment-driven Pyth rate-limit configuration with fixed provider
  constants:
  - 5 requests;
  - 10-second rolling window;
  - 3 retries.
- Restore the prior bounded exponential 429 backoff sequence, starting at one
  second and capped at 60 seconds.
- Preserve batching of all configured quote feeds into one request per bucket.

## Data Flow

The Price stream continues to collect block timestamps, group blocks into
25-block buckets, request all quote feeds for each bucket, create one
`UpdatePrice` row per quote and block, and send the completed cycle to the Price
receiver. The receiver continues to cache and persist those rows before
advancing its processed-block marker.

No new background task, shared price snapshot, mock behavior, or checkpoint
semantics are introduced.

## Failure Behavior

- The provider waits when the local rolling window already contains 5
  requests.
- HTTP 429 responses retry up to three times with bounded exponential backoff.
- Exhausted retries return an error to the existing Price stream, which logs the
  failed bucket and continues the cycle.
- Non-429 HTTP failures and transport failures retain the existing bounded retry
  behavior.

## Verification

- A focused rate-limiter test proves that the first 5 requests are admitted
  within a window and the 6th waits until the window expires.
- Existing retry tests continue to prove bounded retry counts.
- Price stream and runtime-contract tests confirm that Price batching,
  checkpoints, and Curve's Price dependency remain unchanged.
- Formatting, Clippy with warnings denied, library tests, the runtime-contract
  integration test, and the full test suite are run before completion.

## Non-Goals

- Removing Curve's Price dependency.
- Changing the 25-block bucket size or 1,001-block Price cycle.
- Adding a forward-fill sampler.
- Changing database schema or migrations.
- Changing `PRICE_MODE` or mock-provider behavior.
