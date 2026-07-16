-- v2_upgrade_backfill_dex_reserve.sql
--
-- One-off, idempotent backfill of reserve_quote / reserve_token for V2_DEX rows
-- that were indexed before the find_closest_reserve fix (observer #228).
--
-- Before the fix, process_token_events in src/event/v2/dex/receive.rs hardcoded
-- reserve_quote/reserve_token = 0 for V2_DEX SwapBuy/SwapSell/Mint/Burn. This
-- script reconstructs the snapshot the indexer should have written: for each
-- affected row, the closest preceding dex_sync (same tx_hash + tx_index, with a
-- smaller log_index) on the same pool, mapped from (reserve0/reserve1) into
-- (reserve_quote/reserve_token) via the pool's quote side.
--
-- Direction mapping:
--   dex_sync.reserve0 is the reserve of pool.token0, reserve1 of pool.token1.
--   The quote side is whichever of (token0, token1) equals market.quote_id.
--   Address casing is mixed in historical rows, so token0 vs quote_id is
--   compared case-insensitively (LOWER on both sides).
--
-- Idempotent: only rows still at 0/NULL are touched, and only when a matching
-- preceding dex_sync exists. Re-running is a no-op for already-filled rows.
-- Orphan rows (no preceding dex_sync in the same tx) are left at 0 — the same
-- observable signal the live indexer now produces.
--
-- SAFETY: run the dry-run SELECTs (see PR / handoff notes) first to confirm the
-- affected row counts before applying. Wrap in a transaction if applying live.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. swap (V2_DEX): token_id -> market -> pool -> closest preceding dex_sync
--    swap has no pool_id column, so the pool is resolved via the V2_DEX market.
-- ---------------------------------------------------------------------------
WITH swap_fill AS (
    SELECT s.account_id,
           s.token_id,
           s.transaction_hash,
           s.tx_index,
           s.log_index,
           CASE WHEN LOWER(p.token0) = LOWER(m.quote_id) THEN ds.reserve0 ELSE ds.reserve1 END AS rq,
           CASE WHEN LOWER(p.token0) = LOWER(m.quote_id) THEN ds.reserve1 ELSE ds.reserve0 END AS rt
    FROM swap s
    JOIN market m ON m.token_id = s.token_id AND m.market_type = 'V2_DEX'
    JOIN pool p ON p.pool_id = m.pool_id
    JOIN LATERAL (
        SELECT ds.reserve0, ds.reserve1
        FROM dex_sync ds
        WHERE ds.pool_id = m.pool_id
          AND ds.transaction_hash = s.transaction_hash
          AND ds.tx_index = s.tx_index
          AND ds.log_index < s.log_index
        ORDER BY ds.log_index DESC
        LIMIT 1
    ) ds ON TRUE
    WHERE s.market_type = 'V2_DEX'
      AND (s.reserve_quote IS NULL OR s.reserve_quote = 0)
      AND (s.reserve_token IS NULL OR s.reserve_token = 0)
)
UPDATE swap s
SET reserve_quote = f.rq,
    reserve_token = f.rt
FROM swap_fill f
WHERE s.account_id = f.account_id
  AND s.token_id = f.token_id
  AND s.transaction_hash = f.transaction_hash
  AND s.tx_index = f.tx_index
  AND s.log_index = f.log_index;

-- ---------------------------------------------------------------------------
-- 2. mint (V2_DEX): market_id IS the pool_id, joined directly to dex_sync.
--    market join only resolves quote_id (direction); dex_sync scoped by pool.
-- ---------------------------------------------------------------------------
WITH mint_fill AS (
    SELECT mi.token_id,
           mi.transaction_hash,
           mi.tx_index,
           mi.log_index,
           CASE WHEN LOWER(p.token0) = LOWER(m.quote_id) THEN ds.reserve0 ELSE ds.reserve1 END AS rq,
           CASE WHEN LOWER(p.token0) = LOWER(m.quote_id) THEN ds.reserve1 ELSE ds.reserve0 END AS rt
    FROM mint mi
    JOIN pool p ON p.pool_id = mi.market_id
    -- m.pool_id = mi.market_id ties the V2_DEX market to THIS row's pool, so a
    -- token traded on multiple pools never backfills a wrong-pool mint row.
    JOIN market m ON m.token_id = mi.token_id AND m.market_type = 'V2_DEX' AND m.pool_id = mi.market_id
    JOIN LATERAL (
        SELECT ds.reserve0, ds.reserve1
        FROM dex_sync ds
        WHERE ds.pool_id = mi.market_id
          AND ds.transaction_hash = mi.transaction_hash
          AND ds.tx_index = mi.tx_index
          AND ds.log_index < mi.log_index
        ORDER BY ds.log_index DESC
        LIMIT 1
    ) ds ON TRUE
    WHERE mi.reserve_quote = 0
      AND mi.reserve_token = 0
)
UPDATE mint mi
SET reserve_quote = f.rq,
    reserve_token = f.rt
FROM mint_fill f
WHERE mi.token_id = f.token_id
  AND mi.transaction_hash = f.transaction_hash
  AND mi.tx_index = f.tx_index
  AND mi.log_index = f.log_index;

-- ---------------------------------------------------------------------------
-- 3. burn (V2_DEX): same shape as mint.
-- ---------------------------------------------------------------------------
WITH burn_fill AS (
    SELECT bu.token_id,
           bu.transaction_hash,
           bu.tx_index,
           bu.log_index,
           CASE WHEN LOWER(p.token0) = LOWER(m.quote_id) THEN ds.reserve0 ELSE ds.reserve1 END AS rq,
           CASE WHEN LOWER(p.token0) = LOWER(m.quote_id) THEN ds.reserve1 ELSE ds.reserve0 END AS rt
    FROM burn bu
    JOIN pool p ON p.pool_id = bu.market_id
    -- See mint CTE: scope the V2_DEX market to this row's pool.
    JOIN market m ON m.token_id = bu.token_id AND m.market_type = 'V2_DEX' AND m.pool_id = bu.market_id
    JOIN LATERAL (
        SELECT ds.reserve0, ds.reserve1
        FROM dex_sync ds
        WHERE ds.pool_id = bu.market_id
          AND ds.transaction_hash = bu.transaction_hash
          AND ds.tx_index = bu.tx_index
          AND ds.log_index < bu.log_index
        ORDER BY ds.log_index DESC
        LIMIT 1
    ) ds ON TRUE
    WHERE bu.reserve_quote = 0
      AND bu.reserve_token = 0
)
UPDATE burn bu
SET reserve_quote = f.rq,
    reserve_token = f.rt
FROM burn_fill f
WHERE bu.token_id = f.token_id
  AND bu.transaction_hash = f.transaction_hash
  AND bu.tx_index = f.tx_index
  AND bu.log_index = f.log_index;

COMMIT;
