-- ======================================================================
-- v2_upgrade_lp_position.sql
-- ----------------------------------------------------------------------
-- Idempotent twin of migrations/0021_lp_position.sql. Apply this once to
-- existing prod DBs that predate the position-pattern LP tracking design
-- (or that ran an older USD-less rev of 0021). Fresh DBs get these
-- objects from 0021_lp_position.sql directly.
--
-- ⚠️ The fill_lp_cost_basis() and apply_lp_position() function bodies
-- below MUST stay byte-identical to the ones in 0021_lp_position.sql.
-- CI/reviewers should diff the two before merging using the awk slice
-- from each function header down through its LANGUAGE plpgsql terminator
-- -- both diffs must print nothing.
-- ======================================================================

-- Stop psql on the first error (including user Ctrl-C). Without this, psql
-- logs the error and continues to the next statement in the file.
\set ON_ERROR_STOP on

BEGIN;

-- ----------------------------------------------------------------------
-- 0. lp_event_type ENUM (idempotent — re-runs must not fail)
-- ----------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'lp_event_type') THEN
        CREATE TYPE lp_event_type AS ENUM ('mint', 'burn', 'transfer_in', 'transfer_out');
    END IF;
END $$;

-- ----------------------------------------------------------------------
-- 1. lp_position_history (per-event log; trigger source)
--    Schema includes cost-basis USD columns. ALTERs after CREATE handle
--    DBs that already ran an older USD-less revision of this file.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lp_position_history (
    account_id       VARCHAR(42) NOT NULL,
    pool_id          VARCHAR(42) NOT NULL,

    lp_in            NUMERIC NOT NULL DEFAULT 0,
    lp_out           NUMERIC NOT NULL DEFAULT 0,
    token0_in        NUMERIC NOT NULL DEFAULT 0,
    token0_out       NUMERIC NOT NULL DEFAULT 0,
    token1_in        NUMERIC NOT NULL DEFAULT 0,
    token1_out       NUMERIC NOT NULL DEFAULT 0,

    lp_in_usd        NUMERIC NOT NULL DEFAULT 0,
    lp_out_usd       NUMERIC NOT NULL DEFAULT 0,
    token0_in_usd    NUMERIC NOT NULL DEFAULT 0,
    token0_out_usd   NUMERIC NOT NULL DEFAULT 0,
    token1_in_usd    NUMERIC NOT NULL DEFAULT 0,
    token1_out_usd   NUMERIC NOT NULL DEFAULT 0,

    event_type       lp_event_type NOT NULL,
    counterparty     VARCHAR(42),

    transaction_hash VARCHAR(66) NOT NULL,
    block_number     BIGINT NOT NULL,
    tx_index         INT NOT NULL,
    log_index        INT NOT NULL,
    created_at       BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,

    PRIMARY KEY (account_id, pool_id, transaction_hash, tx_index, log_index)
);

ALTER TABLE lp_position_history
    ADD COLUMN IF NOT EXISTS lp_in_usd      NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS lp_out_usd     NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token0_in_usd  NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token0_out_usd NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token1_in_usd  NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token1_out_usd NUMERIC NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_lp_position_history_account ON lp_position_history(account_id);
CREATE INDEX IF NOT EXISTS idx_lp_position_history_pool    ON lp_position_history(pool_id);
CREATE INDEX IF NOT EXISTS idx_lp_position_history_tx      ON lp_position_history(transaction_hash);
CREATE INDEX IF NOT EXISTS idx_lp_position_history_block   ON lp_position_history(block_number, tx_index, log_index);
CREATE INDEX IF NOT EXISTS idx_lp_position_history_event   ON lp_position_history(event_type);

-- ----------------------------------------------------------------------
-- 2. lp_position (accumulated balances + cost basis, tokens AND USD)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lp_position (
    account_id     VARCHAR(42) NOT NULL,
    pool_id        VARCHAR(42) NOT NULL,
    lp_in          NUMERIC NOT NULL DEFAULT 0,
    lp_out         NUMERIC NOT NULL DEFAULT 0,
    -- Running open balance for this (account, pool) epoch. Maintained
    -- automatically by PostgreSQL; no trigger logic touches it. Consumers
    -- can `SELECT balance` directly instead of computing lp_in - lp_out and
    -- can index/sort by it.
    balance        NUMERIC GENERATED ALWAYS AS (lp_in - lp_out) STORED,
    token0_in      NUMERIC NOT NULL DEFAULT 0,
    token0_out     NUMERIC NOT NULL DEFAULT 0,
    token1_in      NUMERIC NOT NULL DEFAULT 0,
    token1_out     NUMERIC NOT NULL DEFAULT 0,
    lp_in_usd      NUMERIC NOT NULL DEFAULT 0,
    lp_out_usd     NUMERIC NOT NULL DEFAULT 0,
    token0_in_usd  NUMERIC NOT NULL DEFAULT 0,
    token0_out_usd NUMERIC NOT NULL DEFAULT 0,
    token1_in_usd  NUMERIC NOT NULL DEFAULT 0,
    token1_out_usd NUMERIC NOT NULL DEFAULT 0,
    created_at     BIGINT NOT NULL,
    updated_at     BIGINT NOT NULL,
    PRIMARY KEY (account_id, pool_id)
);

ALTER TABLE lp_position
    ADD COLUMN IF NOT EXISTS lp_in_usd            NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS lp_out_usd           NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token0_in_usd        NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token0_out_usd       NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token1_in_usd        NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS token1_out_usd       NUMERIC NOT NULL DEFAULT 0,
    -- Epoch boundary: see 0021_lp_position.sql header for rationale.
    ADD COLUMN IF NOT EXISTS epoch_start_block     BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS epoch_start_tx_index  INT    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS epoch_start_log_index INT    NOT NULL DEFAULT 0,
    -- Generated stored open-balance column. Auto-recomputed by PG on every
    -- UPDATE of lp_in/lp_out via apply_lp_position(); no trigger maintenance.
    ADD COLUMN IF NOT EXISTS balance               NUMERIC GENERATED ALWAYS AS (lp_in - lp_out) STORED;

CREATE INDEX IF NOT EXISTS idx_lp_position_account ON lp_position(account_id);
CREATE INDEX IF NOT EXISTS idx_lp_position_pool    ON lp_position(pool_id);

-- ----------------------------------------------------------------------
-- 3. pool.total_supply (tracked LP token supply per pair)
-- ----------------------------------------------------------------------
ALTER TABLE pool ADD COLUMN IF NOT EXISTS total_supply NUMERIC(78,0) NOT NULL DEFAULT 0;

-- ----------------------------------------------------------------------
-- 4. Trigger functions (MUST stay byte-identical to 0021_lp_position.sql)
-- ----------------------------------------------------------------------
-- BEFORE INSERT: balance bookkeeping only. Cost basis lives in the
-- lp_position_cost_basis view defined below.
--
-- Responsibilities preserved from the previous revision:
--   * burn rows: re-attribute account_id to dex_burn.to_address (the Transfer
--     log's from-field is the pair contract, not the user).
--   * transfer rows where counterparty=pool: drop (Pair.burn() emits a
--     pool↔user Transfer leg that should be folded into the matching burn).
--
-- Removed responsibilities (now derived in lp_position_cost_basis view):
--   * Filling token0_in/token1_in/USD on mint (share-weighted in the view).
--   * Filling token0_out/token1_out/USD on burn (single-recipient in the view).
--   * Pro-rating transfer cost basis from the sender's running lp_position
--     (out of scope; defer until a consumer needs running per-holder cost
--     basis after transfers).
CREATE OR REPLACE FUNCTION fill_lp_cost_basis()
RETURNS TRIGGER AS $$
DECLARE
    burn_row RECORD;
BEGIN
    IF NEW.event_type = 'burn' THEN
        SELECT * INTO burn_row
          FROM dex_burn
         WHERE pool_id = NEW.pool_id
           AND transaction_hash = NEW.transaction_hash
           AND log_index > NEW.log_index
         ORDER BY log_index ASC LIMIT 1;
        IF FOUND THEN
            NEW.account_id := burn_row.to_address;
        ELSE
            RAISE WARNING 'LP burn without matching dex_burn: pool=% tx=% (attributed to %)',
                NEW.pool_id, NEW.transaction_hash, NEW.account_id;
        END IF;

    ELSIF NEW.event_type = 'transfer_out' THEN
        -- Drop the user→pair leg of burn(); the burn row that follows in the
        -- same tx (re-attributed above) carries the user's lp_out.
        IF NEW.counterparty = NEW.pool_id THEN
            RETURN NULL;
        END IF;

    ELSIF NEW.event_type = 'transfer_in' THEN
        -- Drop the pair-receives-LP phantom row (first leg of burn).
        IF NEW.account_id = NEW.pool_id THEN
            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- AFTER INSERT: aggregate lp balance + pool.total_supply only. Token/USD
-- accumulation removed — those columns stay at 0 on lp_position and consumers
-- read cost basis from the lp_position_cost_basis view.
-- Does NOT fire when ON CONFLICT DO NOTHING skips.
CREATE OR REPLACE FUNCTION apply_lp_position()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_type = 'mint' THEN
        UPDATE pool SET total_supply = total_supply + NEW.lp_in WHERE pool_id = NEW.pool_id;
    ELSIF NEW.event_type = 'burn' THEN
        UPDATE pool SET total_supply = total_supply - NEW.lp_out WHERE pool_id = NEW.pool_id;
    END IF;

    INSERT INTO lp_position (account_id, pool_id, lp_in, lp_out, created_at, updated_at,
                             epoch_start_block, epoch_start_tx_index, epoch_start_log_index)
    VALUES (NEW.account_id, NEW.pool_id, NEW.lp_in, NEW.lp_out, NEW.created_at, NEW.created_at,
            NEW.block_number, NEW.tx_index, NEW.log_index)
    ON CONFLICT (account_id, pool_id) DO UPDATE SET
        lp_in      = lp_position.lp_in  + EXCLUDED.lp_in,
        lp_out     = lp_position.lp_out + EXCLUDED.lp_out,
        updated_at = EXCLUDED.updated_at;
    -- epoch_start_* are intentionally NOT in the SET list — they're set
    -- only on fresh INSERT (= start of a new epoch) and preserved across
    -- subsequent UPSERTs until the row is DELETEd by full-exit below.

    DELETE FROM lp_position
     WHERE account_id = NEW.account_id
       AND pool_id    = NEW.pool_id
       AND lp_in      = lp_out;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- AFTER STATEMENT: materialize the lp_position_cost_basis view definition into
-- both lp_position_history (per-row token/USD cols) and lp_position (aggregate
-- token/USD cols) for the (pool_id, transaction_hash) tuples touched in this
-- INSERT batch. Fires once per INSERT statement (sees the whole batch via the
-- NEW transition table), so share-weighted attribution is correct regardless of
-- batch row order.
--
-- Ordering invariant: V2 DEX stream completes before Token stream, so dex_mint
-- and dex_burn rows are already in the DB when this fires. If a row's matching
-- dex_mint/dex_burn is missing (= invariant broken), RAISE WARNING and leave
-- token cols at 0 — never silently mis-attribute.
CREATE OR REPLACE FUNCTION refresh_lp_position_cost_basis()
RETURNS TRIGGER AS $$
DECLARE
    feeto CONSTANT TEXT := '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a';
BEGIN
    -- (1) MINT side: re-fill lp_position_history.token cols for every mint row
    -- in (pool_id, transaction_hash) tuples touched by this batch.
    -- Uses the same share-weighted math as the lp_position_cost_basis view.
    WITH affected_mint_txs AS (
        SELECT DISTINCT pool_id, transaction_hash
          FROM new_rows
         WHERE event_type = 'mint'
    ),
    mint_with_dm AS (
        SELECT
            ph.account_id, ph.pool_id, ph.transaction_hash, ph.tx_index, ph.log_index,
            ph.lp_in,
            dm.amount0    AS dm_amount0,
            dm.amount1    AS dm_amount1,
            dm.value      AS dm_value,
            dm.token0_usd AS dm_token0_usd,
            dm.token1_usd AS dm_token1_usd,
            dm.log_index  AS dm_log_index
          FROM lp_position_history ph
          JOIN affected_mint_txs a
            ON a.pool_id = ph.pool_id AND a.transaction_hash = ph.transaction_hash
          JOIN LATERAL (
              SELECT *
                FROM dex_mint
               WHERE pool_id = ph.pool_id
                 AND transaction_hash = ph.transaction_hash
                 AND log_index > ph.log_index
               ORDER BY log_index ASC LIMIT 1
          ) dm ON true
         WHERE ph.event_type = 'mint'
    ),
    mint_truncs AS (
        SELECT
            ph.account_id, ph.pool_id, ph.transaction_hash, ph.tx_index, ph.log_index,
            ph.lp_in, ph.dm_amount0, ph.dm_amount1, ph.dm_value,
            ph.dm_token0_usd, ph.dm_token1_usd, ph.dm_log_index, r.real_lp,
            CASE WHEN LOWER(ph.account_id) = feeto THEN 0
                 ELSE TRUNC(ph.lp_in * ph.dm_amount0 / NULLIF(r.real_lp, 0))
            END AS t0_trunc,
            CASE WHEN LOWER(ph.account_id) = feeto THEN 0
                 ELSE TRUNC(ph.lp_in * ph.dm_amount1 / NULLIF(r.real_lp, 0))
            END AS t1_trunc,
            ROW_NUMBER() OVER (
                PARTITION BY ph.pool_id, ph.transaction_hash, ph.dm_log_index
                ORDER BY
                    CASE WHEN LOWER(ph.account_id) = feeto THEN 1 ELSE 0 END,
                    ph.lp_in DESC,
                    ph.log_index ASC
            ) AS anchor_rn
          FROM mint_with_dm ph
          JOIN LATERAL (
              SELECT COALESCE(SUM(sib.lp_in), 0) AS real_lp
                FROM mint_with_dm sib
               WHERE sib.pool_id = ph.pool_id
                 AND sib.transaction_hash = ph.transaction_hash
                 AND sib.dm_log_index = ph.dm_log_index
                 AND LOWER(sib.account_id) <> feeto
          ) r ON true
    ),
    mint_costs AS (
        SELECT
            mt.account_id, mt.pool_id, mt.transaction_hash, mt.tx_index, mt.log_index,
            -- Residual sum partition MUST match the anchor partition (per
            -- dex_mint, not per tx). A router-aggregated tx with multiple
            -- dex_mints carries independent residuals.
            CASE WHEN LOWER(mt.account_id) = feeto THEN 0
                 WHEN mt.anchor_rn = 1
                     THEN mt.t0_trunc + (mt.dm_amount0 - SUM(mt.t0_trunc) OVER (
                            PARTITION BY mt.pool_id, mt.transaction_hash, mt.dm_log_index))
                 ELSE mt.t0_trunc
            END AS token0_in,
            CASE WHEN LOWER(mt.account_id) = feeto THEN 0
                 WHEN mt.anchor_rn = 1
                     THEN mt.t1_trunc + (mt.dm_amount1 - SUM(mt.t1_trunc) OVER (
                            PARTITION BY mt.pool_id, mt.transaction_hash, mt.dm_log_index))
                 ELSE mt.t1_trunc
            END AS token1_in,
            CASE WHEN LOWER(mt.account_id) = feeto THEN 0
                 ELSE ROUND(mt.lp_in * COALESCE(mt.dm_token0_usd, 0) / NULLIF(mt.real_lp, 0), 10)
            END AS token0_in_usd,
            CASE WHEN LOWER(mt.account_id) = feeto THEN 0
                 ELSE ROUND(mt.lp_in * COALESCE(mt.dm_token1_usd, 0) / NULLIF(mt.real_lp, 0), 10)
            END AS token1_in_usd,
            CASE WHEN LOWER(mt.account_id) = feeto THEN 0
                 ELSE ROUND(mt.lp_in * COALESCE(mt.dm_value, 0) / NULLIF(mt.real_lp, 0), 10)
            END AS lp_in_usd
          FROM mint_truncs mt
    )
    UPDATE lp_position_history h
       SET token0_in     = c.token0_in,
           token1_in     = c.token1_in,
           token0_in_usd = c.token0_in_usd,
           token1_in_usd = c.token1_in_usd,
           lp_in_usd     = c.lp_in_usd
      FROM mint_costs c
     WHERE h.account_id       = c.account_id
       AND h.pool_id          = c.pool_id
       AND h.transaction_hash = c.transaction_hash
       AND h.tx_index         = c.tx_index
       AND h.log_index        = c.log_index;

    -- (2) BURN side: re-fill lp_position_history.token cols for burn rows
    -- in (pool_id, transaction_hash) tuples touched by this batch.
    WITH affected_burn_txs AS (
        SELECT DISTINCT pool_id, transaction_hash
          FROM new_rows
         WHERE event_type = 'burn'
    ),
    burn_costs AS (
        SELECT
            ph.account_id, ph.pool_id, ph.transaction_hash, ph.tx_index, ph.log_index,
            db.amount0    AS token0_out,
            db.amount1    AS token1_out,
            ROUND(COALESCE(db.token0_usd, 0), 10) AS token0_out_usd,
            ROUND(COALESCE(db.token1_usd, 0), 10) AS token1_out_usd,
            ROUND(COALESCE(db.value,      0), 10) AS lp_out_usd
          FROM lp_position_history ph
          JOIN affected_burn_txs a
            ON a.pool_id = ph.pool_id AND a.transaction_hash = ph.transaction_hash
          JOIN LATERAL (
              SELECT *
                FROM dex_burn
               WHERE pool_id = ph.pool_id
                 AND transaction_hash = ph.transaction_hash
                 AND log_index > ph.log_index
               ORDER BY log_index ASC LIMIT 1
          ) db ON true
         WHERE ph.event_type = 'burn'
    )
    UPDATE lp_position_history h
       SET token0_out     = c.token0_out,
           token1_out     = c.token1_out,
           token0_out_usd = c.token0_out_usd,
           token1_out_usd = c.token1_out_usd,
           lp_out_usd     = c.lp_out_usd
      FROM burn_costs c
     WHERE h.account_id       = c.account_id
       AND h.pool_id          = c.pool_id
       AND h.transaction_hash = c.transaction_hash
       AND h.tx_index         = c.tx_index
       AND h.log_index        = c.log_index;

    -- (3) WARNING for rows that landed without a matching dex_mint or dex_burn
    -- (= ordering invariant broken: V2 DEX should have finished first).
    -- Aggregate the offending (pool, tx) pairs into the message (capped to 5
    -- for bounded log line length) so operators can correlate immediately.
    DECLARE
        missing_pairs TEXT;
    BEGIN
        SELECT string_agg(
                   format('pool=%s tx=%s', n.pool_id, n.transaction_hash),
                   ', '
                   ORDER BY n.pool_id, n.transaction_hash
               )
          INTO missing_pairs
          FROM (
              SELECT DISTINCT n.pool_id, n.transaction_hash
                FROM new_rows n
               WHERE n.event_type = 'mint'
                 AND NOT EXISTS (SELECT 1 FROM dex_mint dm
                                  WHERE dm.pool_id = n.pool_id
                                    AND dm.transaction_hash = n.transaction_hash
                                    AND dm.log_index > n.log_index)
               LIMIT 5
          ) n;
        IF missing_pairs IS NOT NULL THEN
            RAISE WARNING 'LP mint without matching dex_mint — ordering invariant broken; offending pairs (first 5): %', missing_pairs;
        END IF;

        SELECT string_agg(
                   format('pool=%s tx=%s', n.pool_id, n.transaction_hash),
                   ', '
                   ORDER BY n.pool_id, n.transaction_hash
               )
          INTO missing_pairs
          FROM (
              SELECT DISTINCT n.pool_id, n.transaction_hash
                FROM new_rows n
               WHERE n.event_type = 'burn'
                 AND NOT EXISTS (SELECT 1 FROM dex_burn db
                                  WHERE db.pool_id = n.pool_id
                                    AND db.transaction_hash = n.transaction_hash
                                    AND db.log_index > n.log_index)
               LIMIT 5
          ) n;
        IF missing_pairs IS NOT NULL THEN
            RAISE WARNING 'LP burn without matching dex_burn — ordering invariant broken; offending pairs (first 5): %', missing_pairs;
        END IF;
    END;

    -- (4) Aggregate rebuild: for each (account_id, pool_id) touched by this
    -- batch, recompute lp_position.token* / *_usd absolutely from history.
    -- lp_in / lp_out are NOT touched here — they're maintained by the existing
    -- apply_lp_position() per-row UPSERT. This trigger only owns cost basis.
    WITH affected_pairs AS (
        SELECT DISTINCT account_id, pool_id FROM new_rows
        UNION
        -- The UNION is load-bearing for SHARE-WEIGHTING RECOMPUTATION across
        -- statements: when a later batch inserts a new mint row into a tx
        -- that already had stored mint rows, the prior rows' share denominator
        -- changes (= they need re-attribution). Their (account_id, pool_id)
        -- pairs are NOT in `new_rows` for this batch, so we pull them in via
        -- the stored history for any affected (pool, tx). Burn re-attribution
        -- (BEFORE-trigger rewriting account_id from pool → to_address) is
        -- already reflected in `new_rows` per PG semantics — the NEW
        -- transition table holds post-BEFORE-trigger values.
        SELECT DISTINCT h.account_id, h.pool_id
          FROM lp_position_history h
          JOIN (SELECT DISTINCT pool_id, transaction_hash FROM new_rows) t
            ON t.pool_id = h.pool_id AND t.transaction_hash = h.transaction_hash
    ),
    aggregates AS (
        SELECT h.account_id, h.pool_id,
               SUM(h.token0_in)      AS token0_in,
               SUM(h.token0_out)     AS token0_out,
               SUM(h.token1_in)      AS token1_in,
               SUM(h.token1_out)     AS token1_out,
               SUM(h.token0_in_usd)  AS token0_in_usd,
               SUM(h.token0_out_usd) AS token0_out_usd,
               SUM(h.token1_in_usd)  AS token1_in_usd,
               SUM(h.token1_out_usd) AS token1_out_usd,
               SUM(h.lp_in_usd)      AS lp_in_usd,
               SUM(h.lp_out_usd)     AS lp_out_usd
          FROM lp_position_history h
          JOIN affected_pairs ap
            ON ap.account_id = h.account_id AND ap.pool_id = h.pool_id
          JOIN lp_position lp
            ON lp.account_id = h.account_id AND lp.pool_id = h.pool_id
         -- Restrict SUM to the CURRENT open epoch only. Re-entry after a
         -- full exit creates a new lp_position row with fresh epoch_start_*
         -- coordinates; history rows from prior (closed) epochs must NOT
         -- contribute to the current row's cost basis.
         WHERE (h.block_number, h.tx_index, h.log_index)
             >= (lp.epoch_start_block, lp.epoch_start_tx_index, lp.epoch_start_log_index)
         GROUP BY h.account_id, h.pool_id
    )
    UPDATE lp_position lp
       SET token0_in      = a.token0_in,
           token0_out     = a.token0_out,
           token1_in      = a.token1_in,
           token1_out     = a.token1_out,
           token0_in_usd  = a.token0_in_usd,
           token0_out_usd = a.token0_out_usd,
           token1_in_usd  = a.token1_in_usd,
           token1_out_usd = a.token1_out_usd,
           lp_in_usd      = a.lp_in_usd,
           lp_out_usd     = a.lp_out_usd
      FROM aggregates a
     WHERE lp.account_id = a.account_id
       AND lp.pool_id    = a.pool_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------
-- 5. Triggers
-- ----------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_lp_position_on_history ON lp_position_history;  -- old single trigger
DROP TRIGGER IF EXISTS trg_fill_lp_cost_basis     ON lp_position_history;
DROP TRIGGER IF EXISTS trg_apply_lp_position      ON lp_position_history;
DROP TRIGGER IF EXISTS trg_refresh_lp_position_cost_basis ON lp_position_history;

CREATE TRIGGER trg_fill_lp_cost_basis
    BEFORE INSERT ON lp_position_history
    FOR EACH ROW EXECUTE FUNCTION fill_lp_cost_basis();

CREATE TRIGGER trg_apply_lp_position
    AFTER INSERT ON lp_position_history
    FOR EACH ROW EXECUTE FUNCTION apply_lp_position();

CREATE TRIGGER trg_refresh_lp_position_cost_basis
    AFTER INSERT ON lp_position_history
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_lp_position_cost_basis();

-- ----------------------------------------------------------------------
-- 6. View: lp_position_cost_basis (MUST stay byte-identical to
--    0021_lp_position.sql — diff with awk slicing in CI).
-- ----------------------------------------------------------------------
-- Per-row derived cost basis for mint and burn events. Replaces the
-- trigger-filled token/USD columns on lp_position_history.
--
-- Mint cost basis:
--   * Each lp_position_history mint row is matched to the dex_mint with the
--     smallest log_index greater than the row's own log_index (mirrors the
--     prior trigger's matching rule; supports router-aggregated multi-mint
--     in one tx).
--   * If account is feeTo (_mintFee() carve-out from k growth, NOT a deposit)
--     → all token/USD columns = 0.
--   * Else → share-weighted across non-feeTo siblings of the same dex_mint:
--     per-row token_in = dex_mint.amount * (row.lp_in / Σ non-feeTo lp_in
--     matched to the same dex_mint). Conservation invariant: Σ rows = full
--     dex_mint.amount.
--
-- Burn cost basis:
--   * Full attribution to the single dex_burn row (matched by smallest
--     log_index > row.log_index, same rule as mint).
--
-- feeTo = factory(0x59c51c66...).feeTo() on testnet. Hardcoded SQL constant
-- for v1; move to a protocol_config table if the factory ever rotates feeTo.
-- ----------------------------------------------------------------------
DROP VIEW IF EXISTS lp_position_cost_basis;
CREATE VIEW lp_position_cost_basis AS
WITH mint_with_dm AS (
    SELECT
        ph.account_id,
        ph.pool_id,
        ph.transaction_hash,
        ph.tx_index,
        ph.log_index,
        ph.event_type,
        ph.lp_in,
        dm.amount0    AS dm_amount0,
        dm.amount1    AS dm_amount1,
        dm.value      AS dm_value,
        dm.token0_usd AS dm_token0_usd,
        dm.token1_usd AS dm_token1_usd,
        dm.log_index  AS dm_log_index
      FROM lp_position_history ph
      JOIN LATERAL (
          SELECT *
            FROM dex_mint
           WHERE pool_id = ph.pool_id
             AND transaction_hash = ph.transaction_hash
             AND log_index > ph.log_index
           ORDER BY log_index ASC LIMIT 1
      ) dm ON true
     WHERE ph.event_type = 'mint'
),
mint_truncs AS (
    SELECT
        ph.account_id,
        ph.pool_id,
        ph.transaction_hash,
        ph.tx_index,
        ph.log_index,
        ph.event_type,
        ph.lp_in,
        ph.dm_amount0,
        ph.dm_amount1,
        ph.dm_value,
        ph.dm_token0_usd,
        ph.dm_token1_usd,
        ph.dm_log_index,
        r.real_lp,
        -- TRUNC of share-weighted amount: each non-feeTo recipient's wei-integer floor.
        -- feeTo rows zeroed (consistent with existing carve-out semantics).
        CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE TRUNC(ph.lp_in * ph.dm_amount0 / NULLIF(r.real_lp, 0))
        END AS t0_trunc,
        CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE TRUNC(ph.lp_in * ph.dm_amount1 / NULLIF(r.real_lp, 0))
        END AS t1_trunc,
        -- Anchor row selection: the LARGEST non-feeTo recipient in this
        -- (pool, tx, dex_mint) group. Tie-break by log_index ASC. feeTo
        -- rows pushed to the end (rn never == 1 for them, so they never
        -- receive the residual).
        ROW_NUMBER() OVER (
            PARTITION BY ph.pool_id, ph.transaction_hash, ph.dm_log_index
            ORDER BY
                CASE WHEN LOWER(ph.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 1 ELSE 0 END,
                ph.lp_in DESC,
                ph.log_index ASC
        ) AS anchor_rn
      FROM mint_with_dm ph
      JOIN LATERAL (
          SELECT COALESCE(SUM(sib.lp_in), 0) AS real_lp
            FROM mint_with_dm sib
           WHERE sib.pool_id = ph.pool_id
             AND sib.transaction_hash = ph.transaction_hash
             AND sib.dm_log_index = ph.dm_log_index
             AND LOWER(sib.account_id) <> '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a'
      ) r ON true
),
mint_costs AS (
    SELECT
        mt.account_id,
        mt.pool_id,
        mt.transaction_hash,
        mt.tx_index,
        mt.log_index,
        mt.event_type,
        -- Anchor-residual: the largest non-feeTo recipient (anchor_rn=1)
        -- absorbs the leftover wei so Σ over recipients = full amount.
        -- feeTo rows stay at 0 (already zeroed in t0_trunc/t1_trunc).
        -- Residual sum window MUST match the anchor partition: one residual per
        -- dex_mint event, not per tx. A router-aggregated tx can hold multiple
        -- dex_mints, each with its own dm_amount0 and its own anchor — they
        -- must NOT share a residual pool.
        CASE WHEN LOWER(mt.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             WHEN mt.anchor_rn = 1
                 THEN mt.t0_trunc + (mt.dm_amount0 - SUM(mt.t0_trunc) OVER (
                        PARTITION BY mt.pool_id, mt.transaction_hash, mt.dm_log_index))
             ELSE mt.t0_trunc
        END AS token0_in,
        CASE WHEN LOWER(mt.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             WHEN mt.anchor_rn = 1
                 THEN mt.t1_trunc + (mt.dm_amount1 - SUM(mt.t1_trunc) OVER (
                        PARTITION BY mt.pool_id, mt.transaction_hash, mt.dm_log_index))
             ELSE mt.t1_trunc
        END AS token1_in,
        -- USD columns keep ROUND(..., 10): naturally fractional, no need for
        -- wei-integer conservation (USD is cents/sub-cents, not raw wei).
        CASE WHEN LOWER(mt.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ROUND(mt.lp_in * COALESCE(mt.dm_token0_usd, 0) / NULLIF(mt.real_lp, 0), 10)
        END AS token0_in_usd,
        CASE WHEN LOWER(mt.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ROUND(mt.lp_in * COALESCE(mt.dm_token1_usd, 0) / NULLIF(mt.real_lp, 0), 10)
        END AS token1_in_usd,
        CASE WHEN LOWER(mt.account_id) = '0x715103eeeac12fb84f5d3b35c3268dd767fa8b8a' THEN 0
             ELSE ROUND(mt.lp_in * COALESCE(mt.dm_value, 0) / NULLIF(mt.real_lp, 0), 10)
        END AS lp_in_usd,
        0::NUMERIC AS token0_out,
        0::NUMERIC AS token1_out,
        0::NUMERIC AS token0_out_usd,
        0::NUMERIC AS token1_out_usd,
        0::NUMERIC AS lp_out_usd
      FROM mint_truncs mt
),
burn_costs AS (
    SELECT
        ph.account_id,
        ph.pool_id,
        ph.transaction_hash,
        ph.tx_index,
        ph.log_index,
        ph.event_type,
        0::NUMERIC AS token0_in,
        0::NUMERIC AS token1_in,
        0::NUMERIC AS token0_in_usd,
        0::NUMERIC AS token1_in_usd,
        0::NUMERIC AS lp_in_usd,
        db.amount0 AS token0_out,
        db.amount1 AS token1_out,
        ROUND(COALESCE(db.token0_usd, 0), 10) AS token0_out_usd,
        ROUND(COALESCE(db.token1_usd, 0), 10) AS token1_out_usd,
        ROUND(COALESCE(db.value,      0), 10) AS lp_out_usd
      FROM lp_position_history ph
      JOIN LATERAL (
          SELECT *
            FROM dex_burn
           WHERE pool_id = ph.pool_id
             AND transaction_hash = ph.transaction_hash
             AND log_index > ph.log_index
           ORDER BY log_index ASC LIMIT 1
      ) db ON true
     WHERE ph.event_type = 'burn'
)
SELECT * FROM mint_costs
UNION ALL
SELECT * FROM burn_costs;

-- ----------------------------------------------------------------------
-- One-time backfill: rebuild token/USD columns on lp_position_history
-- (per-row, via the same share-weighted view math) and on lp_position
-- (aggregate, via SUM-from-history). Idempotent — re-running on
-- already-correct data is a no-op.
--
-- Supersedes PR #216's zero-reset backfill (which assumed cost basis
-- would be derived at read time). Now the trigger materializes view
-- output into the columns and the backfill rebuilds them absolutely.
-- ----------------------------------------------------------------------

-- Defensive zero-reset before the selective view-based UPDATEs below.
-- Older revisions of this file (or older trigger logic) may have left stale
-- values on non-mint or non-burn rows (e.g. token0_out on a 'mint' row from
-- a pre-PR-#216 trigger). The selective backfill that follows only touches
-- mint-side cols on mint rows and burn-side cols on burn rows; without this
-- reset, the aggregate SUM-from-history would re-materialize the stale
-- values into lp_position. No-op on fresh DBs and on already-correct rows.
UPDATE lp_position_history SET
    token0_in      = 0, token0_out      = 0,
    token1_in      = 0, token1_out      = 0,
    token0_in_usd  = 0, token0_out_usd  = 0,
    token1_in_usd  = 0, token1_out_usd  = 0,
    lp_in_usd      = 0, lp_out_usd      = 0;

-- Backfill lp_position_history.token cols for ALL mint rows using the view
-- definition (which already encodes share-weighted feeTo-zero math).
UPDATE lp_position_history h
   SET token0_in     = v.token0_in,
       token1_in     = v.token1_in,
       token0_in_usd = v.token0_in_usd,
       token1_in_usd = v.token1_in_usd,
       lp_in_usd     = v.lp_in_usd
  FROM lp_position_cost_basis v
 WHERE h.event_type       = 'mint'
   AND h.account_id       = v.account_id
   AND h.pool_id          = v.pool_id
   AND h.transaction_hash = v.transaction_hash
   AND h.tx_index         = v.tx_index
   AND h.log_index        = v.log_index;

UPDATE lp_position_history h
   SET token0_out     = v.token0_out,
       token1_out     = v.token1_out,
       token0_out_usd = v.token0_out_usd,
       token1_out_usd = v.token1_out_usd,
       lp_out_usd     = v.lp_out_usd
  FROM lp_position_cost_basis v
 WHERE h.event_type       = 'burn'
   AND h.account_id       = v.account_id
   AND h.pool_id          = v.pool_id
   AND h.transaction_hash = v.transaction_hash
   AND h.tx_index         = v.tx_index
   AND h.log_index        = v.log_index;

-- Backfill epoch_start_* on every existing lp_position row to the start of
-- its CURRENT OPEN epoch. Without this, existing rows would have
-- epoch_start_* = 0 (column default) and the aggregate below would sum
-- across closed past epochs. The current-open-epoch start is the most
-- recent history row where running balance transitioned from 0 to >0.
WITH running_after AS (
    SELECT account_id, pool_id, block_number, tx_index, log_index,
           SUM(lp_in - lp_out) OVER (
               PARTITION BY account_id, pool_id
               ORDER BY block_number, tx_index, log_index
           ) AS bal_after
      FROM lp_position_history
),
running_pair AS (
    SELECT account_id, pool_id, block_number, tx_index, log_index, bal_after,
           COALESCE(LAG(bal_after) OVER (
               PARTITION BY account_id, pool_id
               ORDER BY block_number, tx_index, log_index
           ), 0) AS bal_before
      FROM running_after
),
epoch_starts AS (
    SELECT DISTINCT ON (account_id, pool_id)
           account_id, pool_id, block_number, tx_index, log_index
      FROM running_pair
     WHERE bal_before = 0 AND bal_after > 0
     ORDER BY account_id, pool_id, block_number DESC, tx_index DESC, log_index DESC
)
UPDATE lp_position lp
   SET epoch_start_block     = es.block_number,
       epoch_start_tx_index  = es.tx_index,
       epoch_start_log_index = es.log_index
  FROM epoch_starts es
 WHERE lp.account_id = es.account_id
   AND lp.pool_id    = es.pool_id;

-- Backfill lp_position aggregate from history (now epoch-bounded).
UPDATE lp_position lp
   SET token0_in      = agg.token0_in,
       token0_out     = agg.token0_out,
       token1_in      = agg.token1_in,
       token1_out     = agg.token1_out,
       token0_in_usd  = agg.token0_in_usd,
       token0_out_usd = agg.token0_out_usd,
       token1_in_usd  = agg.token1_in_usd,
       token1_out_usd = agg.token1_out_usd,
       lp_in_usd      = agg.lp_in_usd,
       lp_out_usd     = agg.lp_out_usd
  FROM (
      SELECT h.account_id, h.pool_id,
             SUM(h.token0_in)      AS token0_in,
             SUM(h.token0_out)     AS token0_out,
             SUM(h.token1_in)      AS token1_in,
             SUM(h.token1_out)     AS token1_out,
             SUM(h.token0_in_usd)  AS token0_in_usd,
             SUM(h.token0_out_usd) AS token0_out_usd,
             SUM(h.token1_in_usd)  AS token1_in_usd,
             SUM(h.token1_out_usd) AS token1_out_usd,
             SUM(h.lp_in_usd)      AS lp_in_usd,
             SUM(h.lp_out_usd)     AS lp_out_usd
        FROM lp_position_history h
        JOIN lp_position lp2
          ON lp2.account_id = h.account_id AND lp2.pool_id = h.pool_id
       WHERE (h.block_number, h.tx_index, h.log_index)
           >= (lp2.epoch_start_block, lp2.epoch_start_tx_index, lp2.epoch_start_log_index)
       GROUP BY h.account_id, h.pool_id
  ) agg
 WHERE lp.account_id = agg.account_id
   AND lp.pool_id    = agg.pool_id;

COMMIT;
