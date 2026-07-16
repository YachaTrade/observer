-- 0028_quote_token_is_native.sql
--
-- Add an `is_native` flag to `quote_token` so the indexer can treat WETH and
-- 1:1 native-pegged wrappers as the same "native" token
-- when propagating chain-implied prices into `token_price_cache`.
--
-- Currently the cache propagation logic hardcodes a single `WNATIVE_ADDRESS`
-- env var: only pools where one side IS that address seed prices, so any pool
-- paired with LVMON (also MON-pegged, also seeded as a quote_token) stays
-- orphan and `dex_swap.value` / `pool.value` end up at 0. With this column
-- the indexer reads "every quote_token where is_native = true" at startup
-- and treats all of them as native-equivalent.
--
-- DEFAULT TRUE: current quote_token only holds the native-pegged WETH row,
-- so backfilling existing data to TRUE is the desired state and avoids a
-- separate UPDATE. Future non-native quotes (USDC, USDT, ...) must INSERT with
-- an explicit `is_native = FALSE`.
--
-- Idempotent: ALTER ... IF NOT EXISTS. Safe to re-run.

ALTER TABLE quote_token
    ADD COLUMN IF NOT EXISTS is_native BOOLEAN NOT NULL DEFAULT TRUE;
