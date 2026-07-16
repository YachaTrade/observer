-- ======================================================================
-- v2_upgrade_vault_usd.sql — prod upgrade path for vault USD value tracking
-- ----------------------------------------------------------------------
-- Apply this once to existing prod DBs that already have vault.sql tables
-- but were created BEFORE the USD-value columns landed (2026-05-10).
--
-- Fresh DBs do NOT need this file: vault.sql / 0015_v2_events.sql now
-- include the USD columns directly in their CREATE TABLE statements.
--
-- Idempotent — safe to re-run.
--
-- ─────────────────────────────────────────────────────────────────────
-- IMPORTANT (fixed 2026-05-12)
-- ─────────────────────────────────────────────────────────────────────
-- Previous header claimed "triggers already updated by latest vault.sql
-- apply — no action". That was wrong: operators ran THIS file standalone
-- without re-applying vault.sql, leaving the live DB with the *old*
-- trigger bodies that didn't accumulate the new *_usd columns. Result:
-- *_usd columns stayed 0 even though stats.quote_spent / quote_injected
-- (non-usd) kept incrementing correctly.
--
-- This file now CREATE OR REPLACE-s the vault trigger functions directly
-- so a single apply fully fixes prod, and removes the
-- `WHERE *_usd = 0` guards in the backfill so manually-patched stale
-- values get corrected on re-run.
--
-- Running order:
--   1. ALTER ADD COLUMN — add `quote_id` (where missing) and `usd_value`
--      to event tables; add `*_usd` cumulative columns to stats tables.
--   2. CREATE OR REPLACE FUNCTION — refresh vault trigger functions so
--      live INSERTs start accumulating *_usd.
--   3. Backfill — populate historical event USD via market JOIN + price
--      latest-before lookup, then recompute stats *_usd from event SUMs.
-- ======================================================================

-- Stop psql on the first error (including user Ctrl-C). Without this, psql
-- logs the error and continues to the next statement in the file.
\set ON_ERROR_STOP on

BEGIN;

-- ----------------------------------------------------------------------
-- 1. Event-table column additions
-- ----------------------------------------------------------------------

ALTER TABLE v2_vault_burns
    ADD COLUMN IF NOT EXISTS quote_id VARCHAR(42);
ALTER TABLE v2_vault_burns
    ADD COLUMN IF NOT EXISTS usd_value NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE v2_vault_lp_injections
    ADD COLUMN IF NOT EXISTS quote_id VARCHAR(42);
ALTER TABLE v2_vault_lp_injections
    ADD COLUMN IF NOT EXISTS usd_value NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE v2_creator_fee_claims
    ADD COLUMN IF NOT EXISTS quote_id VARCHAR(42);
ALTER TABLE v2_creator_fee_claims
    ADD COLUMN IF NOT EXISTS usd_value NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE v2_gifts
    ADD COLUMN IF NOT EXISTS quote_id VARCHAR(42);
ALTER TABLE v2_gifts
    ADD COLUMN IF NOT EXISTS usd_value NUMERIC NOT NULL DEFAULT 0;

-- v2_creator_fee_distribution already has quote_id (declared in 0015_v2_events.sql)
ALTER TABLE v2_creator_fee_distribution
    ADD COLUMN IF NOT EXISTS usd_value NUMERIC NOT NULL DEFAULT 0;

-- ----------------------------------------------------------------------
-- 2. Stats-table cumulative USD column additions
-- ----------------------------------------------------------------------

ALTER TABLE v2_burn_vault_stats
    ADD COLUMN IF NOT EXISTS quote_spent_usd NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE v2_lp_vault_stats
    ADD COLUMN IF NOT EXISTS quote_injected_usd NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE v2_creator_fee_vault_stats
    ADD COLUMN IF NOT EXISTS total_deposited_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE v2_creator_fee_vault_stats
    ADD COLUMN IF NOT EXISTS total_claimed_usd NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS total_deposited_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS total_claimed_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS total_expired_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS buyback_quote_spent_usd NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE v2_creator_fee_distribution_stats
    ADD COLUMN IF NOT EXISTS distributed_quote_usd NUMERIC NOT NULL DEFAULT 0;

-- ----------------------------------------------------------------------
-- 3. Trigger functions — refresh so live INSERTs accumulate *_usd.
--    Bodies must stay in lockstep with vault.sql.
-- ----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_vault_burn_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.vault_type = 'BURN' THEN
        INSERT INTO v2_burn_vault_stats
            (token_id, quote_spent, quote_spent_usd, tokens_burned,
             burn_count, last_block, updated_at)
        VALUES
            (NEW.token_id, NEW.quote_in, NEW.usd_value, NEW.token_burned, 1,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            quote_spent     = v2_burn_vault_stats.quote_spent     + EXCLUDED.quote_spent,
            quote_spent_usd = v2_burn_vault_stats.quote_spent_usd + EXCLUDED.quote_spent_usd,
            tokens_burned   = v2_burn_vault_stats.tokens_burned   + EXCLUDED.tokens_burned,
            burn_count      = v2_burn_vault_stats.burn_count      + 1,
            last_block      = GREATEST(v2_burn_vault_stats.last_block, EXCLUDED.last_block),
            updated_at      = GREATEST(v2_burn_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.vault_type = 'GIFT' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, buyback_quote_spent, buyback_quote_spent_usd, buyback_tokens,
             last_block, updated_at)
        VALUES
            (NEW.token_id, NEW.quote_in, NEW.usd_value, NEW.token_burned,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            buyback_quote_spent     = v2_gift_vault_stats.buyback_quote_spent     + EXCLUDED.buyback_quote_spent,
            buyback_quote_spent_usd = v2_gift_vault_stats.buyback_quote_spent_usd + EXCLUDED.buyback_quote_spent_usd,
            buyback_tokens          = v2_gift_vault_stats.buyback_tokens          + EXCLUDED.buyback_tokens,
            last_block              = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at              = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_vault_lp_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO v2_lp_vault_stats
        (token_id, quote_injected, quote_injected_usd, token_injected, lp_burned,
         inject_count, last_block, updated_at)
    VALUES
        (NEW.token_id, NEW.quote_used, NEW.usd_value, NEW.token_used, NEW.lp_burned, 1,
         NEW.block_number, NEW.created_at)
    ON CONFLICT (token_id) DO UPDATE SET
        quote_injected     = v2_lp_vault_stats.quote_injected     + EXCLUDED.quote_injected,
        quote_injected_usd = v2_lp_vault_stats.quote_injected_usd + EXCLUDED.quote_injected_usd,
        token_injected     = v2_lp_vault_stats.token_injected     + EXCLUDED.token_injected,
        lp_burned          = v2_lp_vault_stats.lp_burned          + EXCLUDED.lp_burned,
        inject_count       = v2_lp_vault_stats.inject_count       + 1,
        last_block         = GREATEST(v2_lp_vault_stats.last_block, EXCLUDED.last_block),
        updated_at         = GREATEST(v2_lp_vault_stats.updated_at, EXCLUDED.updated_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_creator_fee_vault_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_type = 'DEPOSIT' THEN
        INSERT INTO v2_creator_fee_vault_stats
            (token_id, current_balance, total_deposited, total_deposited_usd,
             deposit_count, last_block, updated_at)
        VALUES
            (NEW.token_id, COALESCE(NEW.new_balance, 0), NEW.amount, NEW.usd_value, 1,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance     = COALESCE(EXCLUDED.current_balance, v2_creator_fee_vault_stats.current_balance),
            total_deposited     = v2_creator_fee_vault_stats.total_deposited     + EXCLUDED.total_deposited,
            total_deposited_usd = v2_creator_fee_vault_stats.total_deposited_usd + EXCLUDED.total_deposited_usd,
            deposit_count       = v2_creator_fee_vault_stats.deposit_count       + 1,
            last_block          = GREATEST(v2_creator_fee_vault_stats.last_block, EXCLUDED.last_block),
            updated_at          = GREATEST(v2_creator_fee_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'CLAIM' THEN
        INSERT INTO v2_creator_fee_vault_stats
            (token_id, current_balance, total_claimed, total_claimed_usd,
             claim_count, last_block, updated_at)
        VALUES
            (NEW.token_id, 0, NEW.amount, NEW.usd_value, 1,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance   = 0,
            total_claimed     = v2_creator_fee_vault_stats.total_claimed     + EXCLUDED.total_claimed,
            total_claimed_usd = v2_creator_fee_vault_stats.total_claimed_usd + EXCLUDED.total_claimed_usd,
            claim_count       = v2_creator_fee_vault_stats.claim_count       + 1,
            last_block        = GREATEST(v2_creator_fee_vault_stats.last_block, EXCLUDED.last_block),
            updated_at        = GREATEST(v2_creator_fee_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_gift_vault_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_type = 'SETUP' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, current_state, platform, platform_id,
             last_block, updated_at)
        VALUES
            (NEW.token_id, 'Accumulating', NEW.platform, NEW.platform_id,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            platform    = COALESCE(EXCLUDED.platform, v2_gift_vault_stats.platform),
            platform_id = COALESCE(EXCLUDED.platform_id, v2_gift_vault_stats.platform_id),
            last_block  = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at  = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'DEPOSIT' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, current_balance, total_deposited, total_deposited_usd,
             last_block, updated_at)
        VALUES
            (NEW.token_id, COALESCE(NEW.new_balance, 0), NEW.amount, NEW.usd_value,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance     = COALESCE(EXCLUDED.current_balance, v2_gift_vault_stats.current_balance),
            total_deposited     = v2_gift_vault_stats.total_deposited     + EXCLUDED.total_deposited,
            total_deposited_usd = v2_gift_vault_stats.total_deposited_usd + EXCLUDED.total_deposited_usd,
            last_block          = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at          = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'CLAIM' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, current_balance, total_claimed, total_claimed_usd,
             last_block, updated_at)
        VALUES
            (NEW.token_id, 0, NEW.amount, NEW.usd_value,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance   = 0,
            total_claimed     = v2_gift_vault_stats.total_claimed     + EXCLUDED.total_claimed,
            total_claimed_usd = v2_gift_vault_stats.total_claimed_usd + EXCLUDED.total_claimed_usd,
            last_block        = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at        = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'EXPIRE' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, current_state, current_balance, total_expired, total_expired_usd,
             last_block, updated_at)
        VALUES
            (NEW.token_id, 'Burned', 0, NEW.amount, NEW.usd_value,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_state     = 'Burned',
            current_balance   = 0,
            total_expired     = v2_gift_vault_stats.total_expired     + EXCLUDED.total_expired,
            total_expired_usd = v2_gift_vault_stats.total_expired_usd + EXCLUDED.total_expired_usd,
            last_block        = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at        = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'RECEIVER_SET' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, current_state, receiver, last_block, updated_at)
        VALUES
            (NEW.token_id, 'Active', NEW.receiver,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_state = CASE v2_gift_vault_stats.current_state
                WHEN 'Burned' THEN 'Burned'
                ELSE 'Active'
            END,
            receiver      = COALESCE(EXCLUDED.receiver, v2_gift_vault_stats.receiver),
            last_block    = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at    = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_creator_fee_distribution_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_type <> 'DISTRIBUTE'
       OR NEW.token IS NULL
       OR NEW.vault IS NULL THEN
        RETURN NEW;
    END IF;

    INSERT INTO v2_creator_fee_distribution_stats
        (token_id, vault_id, quote_id,
         distributed_quote, distributed_quote_usd,
         distribute_count, last_block, updated_at)
    VALUES
        (NEW.token, NEW.vault, NEW.quote_id,
         NEW.amount, NEW.usd_value,
         1, NEW.block_number, NEW.created_at)
    ON CONFLICT (token_id, vault_id) DO UPDATE SET
        distributed_quote     = v2_creator_fee_distribution_stats.distributed_quote
                              + EXCLUDED.distributed_quote,
        distributed_quote_usd = v2_creator_fee_distribution_stats.distributed_quote_usd
                              + EXCLUDED.distributed_quote_usd,
        distribute_count      = v2_creator_fee_distribution_stats.distribute_count + 1,
        last_block            = GREATEST(v2_creator_fee_distribution_stats.last_block,
                                         EXCLUDED.last_block),
        updated_at            = GREATEST(v2_creator_fee_distribution_stats.updated_at,
                                         EXCLUDED.updated_at);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------
-- 4. Backfill — populate historical USD on pre-existing rows.
--
--    Step A: per-event-row usd_value via market JOIN + latest-before
--    price lookup. Guarded by `WHERE usd_value = 0` so rows that already
--    have a real usd_value (written by the live trigger after the code
--    upgrade) are not overwritten.
--
--    Step B: cumulative *_usd in stats tables — UNCONDITIONAL recompute
--    from event SUMs. The previous `WHERE *_usd = 0` guard was a
--    footgun: once a row had a stale partial value (set by an early
--    trigger run or a manual UPDATE) the guard would skip it forever,
--    and the *_usd would never catch up to the full event SUM.
-- ----------------------------------------------------------------------

UPDATE v2_vault_burns vb
   SET quote_id  = m.quote_id,
       usd_value = COALESCE(
           (vb.quote_in / POWER(10, qt.decimals)::numeric) * (
               SELECT price FROM price
                WHERE quote_id = m.quote_id
                  AND block_number <= vb.block_number
                ORDER BY block_number DESC
                LIMIT 1
           ),
           0
       )
  FROM market m
  JOIN quote_token qt ON qt.quote_id = m.quote_id
 WHERE vb.token_id = m.token_id
   AND vb.usd_value = 0;

UPDATE v2_vault_lp_injections vli
   SET quote_id  = m.quote_id,
       usd_value = COALESCE(
           (vli.quote_used / POWER(10, qt.decimals)::numeric) * (
               SELECT price FROM price
                WHERE quote_id = m.quote_id
                  AND block_number <= vli.block_number
                ORDER BY block_number DESC
                LIMIT 1
           ),
           0
       )
  FROM market m
  JOIN quote_token qt ON qt.quote_id = m.quote_id
 WHERE vli.token_id = m.token_id
   AND vli.usd_value = 0;

UPDATE v2_creator_fee_claims cfc
   SET quote_id  = m.quote_id,
       usd_value = COALESCE(
           (cfc.amount / POWER(10, qt.decimals)::numeric) * (
               SELECT price FROM price
                WHERE quote_id = m.quote_id
                  AND block_number <= cfc.block_number
                ORDER BY block_number DESC
                LIMIT 1
           ),
           0
       )
  FROM market m
  JOIN quote_token qt ON qt.quote_id = m.quote_id
 WHERE cfc.token_id = m.token_id
   AND cfc.usd_value = 0;

UPDATE v2_gifts g
   SET quote_id  = m.quote_id,
       usd_value = COALESCE(
           (g.amount / POWER(10, qt.decimals)::numeric) * (
               SELECT price FROM price
                WHERE quote_id = m.quote_id
                  AND block_number <= g.block_number
                ORDER BY block_number DESC
                LIMIT 1
           ),
           0
       )
  FROM market m
  JOIN quote_token qt ON qt.quote_id = m.quote_id
 WHERE g.token_id = m.token_id
   AND g.amount IS NOT NULL
   AND g.usd_value = 0;

UPDATE v2_creator_fee_distribution cfd
   SET usd_value = COALESCE(
           (cfd.amount / POWER(10, qt.decimals)::numeric) * (
               SELECT price FROM price
                WHERE quote_id = cfd.quote_id
                  AND block_number <= cfd.block_number
                ORDER BY block_number DESC
                LIMIT 1
           ),
           0
       )
  FROM quote_token qt
 WHERE cfd.quote_id = qt.quote_id
   AND cfd.event_type = 'DISTRIBUTE'
   AND cfd.usd_value = 0;

-- Stats *_usd — unconditional recompute. Event tables are the source of truth.
WITH s AS (
    SELECT token_id, SUM(usd_value) AS quote_spent_usd
      FROM v2_vault_burns
     WHERE vault_type = 'BURN'
     GROUP BY token_id
)
UPDATE v2_burn_vault_stats v
   SET quote_spent_usd = s.quote_spent_usd
  FROM s
 WHERE v.token_id = s.token_id;

WITH s AS (
    SELECT token_id, SUM(usd_value) AS quote_injected_usd
      FROM v2_vault_lp_injections
     GROUP BY token_id
)
UPDATE v2_lp_vault_stats v
   SET quote_injected_usd = s.quote_injected_usd
  FROM s
 WHERE v.token_id = s.token_id;

WITH s AS (
    SELECT token_id,
           SUM(usd_value) FILTER (WHERE event_type = 'DEPOSIT') AS dep_usd,
           SUM(usd_value) FILTER (WHERE event_type = 'CLAIM')   AS clm_usd
      FROM v2_creator_fee_claims
     GROUP BY token_id
)
UPDATE v2_creator_fee_vault_stats v
   SET total_deposited_usd = COALESCE(s.dep_usd, 0),
       total_claimed_usd   = COALESCE(s.clm_usd, 0)
  FROM s
 WHERE v.token_id = s.token_id;

WITH s AS (
    SELECT token_id,
           SUM(usd_value) FILTER (WHERE event_type = 'DEPOSIT') AS dep_usd,
           SUM(usd_value) FILTER (WHERE event_type = 'CLAIM')   AS clm_usd,
           SUM(usd_value) FILTER (WHERE event_type = 'EXPIRE')  AS exp_usd
      FROM v2_gifts
     WHERE amount IS NOT NULL
     GROUP BY token_id
),
b AS (
    SELECT token_id, SUM(usd_value) AS bb_usd
      FROM v2_vault_burns
     WHERE vault_type = 'GIFT'
     GROUP BY token_id
)
UPDATE v2_gift_vault_stats v
   SET total_deposited_usd     = COALESCE(s.dep_usd, 0),
       total_claimed_usd       = COALESCE(s.clm_usd, 0),
       total_expired_usd       = COALESCE(s.exp_usd, 0),
       buyback_quote_spent_usd = COALESCE(b.bb_usd, 0)
  FROM s
  LEFT JOIN b USING (token_id)
 WHERE v.token_id = s.token_id;

-- buyback-only rows (no v2_gifts events for the token but GIFT-burns exist)
WITH b AS (
    SELECT token_id, SUM(usd_value) AS bb_usd
      FROM v2_vault_burns
     WHERE vault_type = 'GIFT'
     GROUP BY token_id
)
UPDATE v2_gift_vault_stats v
   SET buyback_quote_spent_usd = b.bb_usd
  FROM b
 WHERE v.token_id = b.token_id
   AND v.token_id NOT IN (SELECT token_id FROM v2_gifts WHERE amount IS NOT NULL);

WITH s AS (
    SELECT token AS token_id, vault AS vault_id,
           SUM(usd_value) AS distributed_quote_usd
      FROM v2_creator_fee_distribution
     WHERE event_type = 'DISTRIBUTE'
       AND token IS NOT NULL
       AND vault IS NOT NULL
     GROUP BY token, vault
)
UPDATE v2_creator_fee_distribution_stats v
   SET distributed_quote_usd = s.distributed_quote_usd
  FROM s
 WHERE v.token_id = s.token_id
   AND v.vault_id = s.vault_id;

COMMIT;
