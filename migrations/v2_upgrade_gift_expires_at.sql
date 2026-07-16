-- ======================================================================
-- v2_upgrade_gift_expires_at.sql
-- ----------------------------------------------------------------------
-- Adds two timestamp columns to the gift-vault stats so consumers can
-- surface gift lifecycle timing:
--
--   * `expires_at` (v2_gifts + v2_gift_vault_stats) — when an unbound
--     gift will expire (= SETUP block_timestamp + GIFT_EXPIRY_DURATION).
--     RECEIVER_SET clears it to 0 (gift bound, no longer expires).
--
--   * `receiver_set_at` (v2_gift_vault_stats only) — the block_timestamp
--     of the RECEIVER_SET event that bound the gift. 0 while still
--     'Accumulating'. v2_gifts already records this per-row via its own
--     `created_at`, so no new column is added there.
--
-- Idempotent — safe to re-run. ALTERs use ADD COLUMN IF NOT EXISTS;
-- trigger uses CREATE OR REPLACE; backfill UPDATEs are unconditional
-- against event-table-derived values.
--
-- Apply this once to existing prod DBs that have v2_gifts /
-- v2_gift_vault_stats from an earlier vault.sql but lack expires_at.
-- Fresh DBs get the column from vault.sql directly.
--
-- Backfill duration assumption: 864000 seconds (10 days). This matches
-- the current testnet contract GIFT_EXPIRY_DURATION. If the on-chain
-- duration ever changes via a GiftExpiryUpdate event, that change is
-- captured in v2_gift_expiry_updates and only affects gifts SETUP'd
-- AFTER the change — the backfill below covers the historical default.
--
-- ─────────────────────────────────────────────────────────────────────
-- Time semantics — what the backfill anchors expires_at to
-- ─────────────────────────────────────────────────────────────────────
-- The backfill uses `v2_gifts.created_at` of the SETUP row, which the
-- indexer populates from the event's `block_timestamp` (see
-- src/event/v2/vault/receive.rs — `created_at: e.block_timestamp as i64`).
-- It does NOT use `NOW()` / wall-clock time.
--
-- Concretely:
--     expires_at = SETUP_block_timestamp + 864000
--
-- This matches what the live trigger writes for new SETUP events
-- (Rust computes `block_timestamp + GIFT_EXPIRY_DURATION` and passes it
-- as NEW.expires_at). Backfilled rows and new rows therefore share the
-- same semantic: "10 days after the SETUP transaction landed on chain".
--
-- Consequence: the backfill is **time-invariant**. Re-running this file
-- a week later produces identical expires_at values. The wall-clock time
-- when the migration runs is irrelevant.
-- ======================================================================

-- Stop psql on the first error (including user Ctrl-C). Without this, psql
-- logs the error and continues to the next statement in the file.
\set ON_ERROR_STOP on

BEGIN;

-- ----------------------------------------------------------------------
-- 0. Rename legacy `expired_at` → `expires_at`
--    An earlier revision of this file used the past-tense name. Existing
--    DBs (testnet, anything that applied the prior file) carry the old
--    column. Rename it idempotently before the ADD COLUMN guards so the
--    final shape converges to `expires_at` regardless of starting state.
-- ----------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gifts' AND column_name = 'expired_at'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gifts' AND column_name = 'expires_at'
    ) THEN
        ALTER TABLE v2_gifts RENAME COLUMN expired_at TO expires_at;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gift_vault_stats' AND column_name = 'expired_at'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gift_vault_stats' AND column_name = 'expires_at'
    ) THEN
        ALTER TABLE v2_gift_vault_stats RENAME COLUMN expired_at TO expires_at;
    END IF;
END $$;

-- ----------------------------------------------------------------------
-- 1. Column additions (no-ops if step 0 already produced the column)
-- ----------------------------------------------------------------------
ALTER TABLE v2_gifts
    ADD COLUMN IF NOT EXISTS expires_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS expires_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS receiver_set_at BIGINT NOT NULL DEFAULT 0;

-- ----------------------------------------------------------------------
-- 2. Trigger refresh — must stay in lockstep with vault.sql
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_gift_vault_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_type = 'SETUP' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, current_state, platform, platform_id, expires_at,
             last_block, updated_at)
        VALUES
            (NEW.token_id, 'Accumulating', NEW.platform, NEW.platform_id, NEW.expires_at,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            platform    = COALESCE(EXCLUDED.platform, v2_gift_vault_stats.platform),
            platform_id = COALESCE(EXCLUDED.platform_id, v2_gift_vault_stats.platform_id),
            expires_at  = EXCLUDED.expires_at,
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
            (token_id, current_state, receiver, expires_at, receiver_set_at,
             last_block, updated_at)
        VALUES
            (NEW.token_id, 'Active', NEW.receiver, 0, NEW.created_at,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_state   = CASE v2_gift_vault_stats.current_state
                WHEN 'Burned' THEN 'Burned'
                ELSE 'Active'
            END,
            receiver        = COALESCE(EXCLUDED.receiver, v2_gift_vault_stats.receiver),
            expires_at      = 0,
            receiver_set_at = EXCLUDED.receiver_set_at,
            last_block      = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at      = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------
-- 3. Backfill historical v2_gifts SETUP rows with expires_at
--
--    `created_at` here is the SETUP event's block_timestamp (set by the
--    indexer at insert time), NOT wall-clock NOW(). So expires_at is
--    anchored to the on-chain SETUP timing and is the same regardless
--    of when this migration runs.
-- ----------------------------------------------------------------------
UPDATE v2_gifts
   SET expires_at = created_at + 864000   -- SETUP block_timestamp + 10 days
 WHERE event_type = 'SETUP'
   AND expires_at = 0;

-- ----------------------------------------------------------------------
-- 4. Backfill v2_gift_vault_stats.expires_at
--
--    Logic:
--      - If a RECEIVER_SET event has landed for this token → 0
--        (gift bound, no longer expires — matches the live trigger which
--        sets expires_at = 0 on RECEIVER_SET)
--      - Else if a SETUP event exists → that SETUP's block_timestamp + 864000
--      - Else (buyback-only token, no SETUP yet) → leave at 0
--
--    Same time semantic as #3: anchored to the SETUP event's
--    block_timestamp via v2_gifts.created_at, never to NOW().
-- ----------------------------------------------------------------------
WITH last_setup AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        created_at + 864000 AS expires_at   -- SETUP block_timestamp + 10 days
    FROM v2_gifts
    WHERE event_type = 'SETUP'
    ORDER BY token_id, block_number DESC, tx_index DESC, log_index DESC
),
has_receiver AS (
    SELECT DISTINCT token_id
    FROM v2_gifts
    WHERE event_type = 'RECEIVER_SET'
)
UPDATE v2_gift_vault_stats v
   SET expires_at = CASE
       WHEN hr.token_id IS NOT NULL THEN 0
       WHEN ls.expires_at IS NOT NULL THEN ls.expires_at
       ELSE 0
   END
  FROM v2_gift_vault_stats v2
  LEFT JOIN last_setup   ls ON ls.token_id = v2.token_id
  LEFT JOIN has_receiver hr ON hr.token_id = v2.token_id
 WHERE v.token_id = v2.token_id;

-- ----------------------------------------------------------------------
-- 5. Backfill v2_gift_vault_stats.receiver_set_at
--
--    Source of truth: v2_gifts rows with event_type='RECEIVER_SET'.
--    A token can in principle have multiple RECEIVER_SET rows (legacy
--    rebind history) — we take the latest by block ordering, which
--    matches the live trigger's "last write wins" behavior.
--
--    Tokens with no RECEIVER_SET event keep the default 0.
-- ----------------------------------------------------------------------
WITH last_receiver_set AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        created_at AS receiver_set_at  -- RECEIVER_SET event block_timestamp
    FROM v2_gifts
    WHERE event_type = 'RECEIVER_SET'
    ORDER BY token_id, block_number DESC, tx_index DESC, log_index DESC
)
UPDATE v2_gift_vault_stats v
   SET receiver_set_at = lr.receiver_set_at
  FROM last_receiver_set lr
 WHERE v.token_id = lr.token_id;

COMMIT;
