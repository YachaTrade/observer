-- ============================================================================
-- DividendVault (nadfun-contract-v2) indexing schema
--
-- Contract: singleton UUPS DividendVault (one address, all source tokens).
-- Indexed events (5):
--   DividendSetup(sourceToken idx, dividendTokens[], ratios[], minBalance)
--   Deposit(sourceToken idx, dividendTokens[], slices[], pending[])
--   Converted(sourceTokens[], dividendTokens[], consumedQuote[], received[])
--   SetMerkleRoot(merkleRoot idx)
--   Claim(holder idx, sourceTokens[], dividendTokens[], amounts[])
--
-- Contract facts the schema relies on (verified in DividendVault.sol):
--   * setup() is one-time immutable per sourceToken (reverts AlreadyConfigured,
--     DividendVault.sol:93) -> v2_dividend_setups doubles as config lookup.
--   * Deposit fires ONCE per afterDeposit with ALL ratio slices (parallel
--     arrays). For each entry i: slices[i] is the quote-denominated slice and
--     pending[i] = (dividendToken != quoteToken). pending=false -> credited
--     immediately to dividendBalance; pending=true -> accrued to pendingSwap
--     and later consumed by Converted. There is NO balance snapshot field.
--   * Converted does NOT carry the resulting balance -> stats use arithmetic.
--   * claim() does NOT decrement on-chain dividendBalance -> dividend_balance
--     in stats is CUMULATIVE (deposited + converted received), mirroring chain.
--   * Claim amounts[i] == 0 means the item was skipped on-chain (ineligible /
--     already claimed / insufficient vault balance). Zero entries are NOT
--     inserted: this table is PAID claim history, not attempt history.
--
-- Pattern: history INSERT (ON CONFLICT DO NOTHING) -> AFTER INSERT trigger
--          upserts v2_dividend_vault_stats in the same transaction. Trigger
--          fires only on rows actually inserted: insert success -> update.
--          This is REPLAY-idempotent (re-processing the same logs is safe).
--          It is NOT reorg-rollback: orphaned rows are not removed (observer
--          has no rollback machinery anywhere; same property as all tables).
--
-- Insert ordering requirement (controller): within a receive batch,
--   v2_dividend_merkle_roots MUST be inserted BEFORE v2_dividend_claims so the
--   claims' merkle_root insert-time lookup sees roots from the same batch.
--
-- LOCAL-DEV reset note: earlier commits on this branch created the v2_dividend_*
--   tables with the OLD deposit shape (dividend_balance column, 3-col PK). Because
--   the tables use CREATE TABLE IF NOT EXISTS, a dev who ran an earlier branch
--   commit must DROP the v2_dividend_* tables locally before re-running this file
--   — otherwise deposit inserts fail on the missing pending / entry_index columns.
--   Prod has no dividend tables yet, so prod is unaffected (no ALTER needed here).
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1) History: DividendSetup (exploded: one row per dividend token entry)
--    setup() is once-per-source immutable -> also serves as config lookup.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_dividend_setups (
    source_token     VARCHAR(42) NOT NULL,
    dividend_token   VARCHAR(42) NOT NULL,
    ratio            INT NOT NULL,            -- BPS (uint16, sums to 10000 per source)
    min_balance      NUMERIC NOT NULL,        -- min sourceToken holding to claim
    entry_index      INT NOT NULL,            -- position in dividendTokens[]
    transaction_hash VARCHAR NOT NULL,
    block_number     BIGINT NOT NULL,
    created_at       BIGINT NOT NULL,         -- block timestamp
    log_index        INT NOT NULL,
    tx_index         INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index, entry_index)
);
-- contract enforces ZeroRatio / BPS total
ALTER TABLE v2_dividend_setups
    DROP CONSTRAINT IF EXISTS chk_v2_dividend_setups_ratio;
ALTER TABLE v2_dividend_setups
    ADD CONSTRAINT chk_v2_dividend_setups_ratio
    CHECK (ratio > 0 AND ratio <= 10000);
CREATE INDEX IF NOT EXISTS idx_v2_dividend_setups_source
    ON v2_dividend_setups (source_token);

-- ----------------------------------------------------------------------------
-- 2) History: Deposit (exploded: one row per ratio slice)
--    Emitted ONCE per afterDeposit with ALL slices. amount is the per-slice
--    quote-denominated value; pending distinguishes immediate credit
--    (pending=false, dividend_token == quote) from swap-pending accrual
--    (pending=true, dividend_token != quote, later consumed by Converted).
--    No on-chain balance snapshot exists in the new event shape.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_dividend_deposits (
    source_token     VARCHAR(42) NOT NULL,
    dividend_token   VARCHAR(42) NOT NULL,    -- target dividend token for this slice
    amount           NUMERIC NOT NULL,        -- per-slice value (quote units)
    pending          BOOLEAN NOT NULL,        -- true = swap-pending; false = immediate credit
    entry_index      INT NOT NULL,            -- position in dividendTokens[]/slices[]
    transaction_hash VARCHAR NOT NULL,
    block_number     BIGINT NOT NULL,
    created_at       BIGINT NOT NULL,
    log_index        INT NOT NULL,
    tx_index         INT NOT NULL,
    quote_id         VARCHAR(42),             -- quote token used for USD pricing
    usd_value        NUMERIC NOT NULL DEFAULT 0,  -- USD of amount (quote-priced)
    PRIMARY KEY (transaction_hash, tx_index, log_index, entry_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_dividend_deposits_pair
    ON v2_dividend_deposits (source_token, dividend_token);

-- ----------------------------------------------------------------------------
-- 3) History: Converted (exploded: one row per conversion order)
--    USD semantics: usd_value prices consumed_quote (quote units are reliably
--    priceable via quote->WMON->Pyth). received is raw dividendToken units;
--    its USD value is intentionally NOT stored (arbitrary ERC20, often
--    unpriceable — avoid silently-bogus numbers).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_dividend_conversions (
    source_token     VARCHAR(42) NOT NULL,
    dividend_token   VARCHAR(42) NOT NULL,
    consumed_quote   NUMERIC NOT NULL,        -- quote consumed from pendingSwap
    received         NUMERIC NOT NULL,        -- dividendToken credited (balance delta)
    entry_index      INT NOT NULL,            -- order index within the batch event
    transaction_hash VARCHAR NOT NULL,
    block_number     BIGINT NOT NULL,
    created_at       BIGINT NOT NULL,
    log_index        INT NOT NULL,
    tx_index         INT NOT NULL,
    quote_id         VARCHAR(42),             -- quote token used for USD pricing
    usd_value        NUMERIC NOT NULL DEFAULT 0,  -- USD of consumed_quote
    PRIMARY KEY (transaction_hash, tx_index, log_index, entry_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_dividend_conversions_pair
    ON v2_dividend_conversions (source_token, dividend_token);

-- ----------------------------------------------------------------------------
-- 4) History: SetMerkleRoot (distribution period markers)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_dividend_merkle_roots (
    merkle_root      VARCHAR(66) NOT NULL,    -- 0x + 64 hex
    transaction_hash VARCHAR NOT NULL,
    block_number     BIGINT NOT NULL,
    created_at       BIGINT NOT NULL,
    log_index        INT NOT NULL,
    tx_index         INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
-- Latest-root lookup (claims enrichment + "current root" queries).
CREATE INDEX IF NOT EXISTS idx_v2_dividend_merkle_roots_coords
    ON v2_dividend_merkle_roots (block_number DESC, tx_index DESC, log_index DESC);

-- ----------------------------------------------------------------------------
-- 5) History: Claim — PAID entries only (exploded; zero/skipped NOT inserted)
--    merkle_root = period the claim was paid under, resolved at insert time as
--    the latest SetMerkleRoot at or before the claim's (block, tx, log) coords.
--    NULL only if no root event was ever indexed before the claim (shouldn't
--    happen: claim() reverts when merkleRoot is unset).
--    usd_value prices amount via the dividend token's cached USD price;
--    0 when the token is not WMON-reachable (pricing misses are logged).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_dividend_claims (
    holder           VARCHAR(42) NOT NULL,
    source_token     VARCHAR(42) NOT NULL,
    dividend_token   VARCHAR(42) NOT NULL,
    amount           NUMERIC NOT NULL,        -- paid amount (dividendToken units)
    merkle_root      VARCHAR(66),             -- distribution period (resolved at insert)
    entry_index      INT NOT NULL,            -- position in the claim arrays
    transaction_hash VARCHAR NOT NULL,
    block_number     BIGINT NOT NULL,
    created_at       BIGINT NOT NULL,
    log_index        INT NOT NULL,
    tx_index         INT NOT NULL,
    usd_value        NUMERIC NOT NULL DEFAULT 0,  -- USD of amount (0 if unpriceable)
    PRIMARY KEY (transaction_hash, tx_index, log_index, entry_index)
);
-- paid-only table; zero entries rejected
ALTER TABLE v2_dividend_claims
    DROP CONSTRAINT IF EXISTS chk_v2_dividend_claims_amount;
ALTER TABLE v2_dividend_claims
    ADD CONSTRAINT chk_v2_dividend_claims_amount
    CHECK (amount > 0);
CREATE INDEX IF NOT EXISTS idx_v2_dividend_claims_holder
    ON v2_dividend_claims (holder);
CREATE INDEX IF NOT EXISTS idx_v2_dividend_claims_pair
    ON v2_dividend_claims (source_token, dividend_token);
CREATE INDEX IF NOT EXISTS idx_v2_dividend_claims_root
    ON v2_dividend_claims (merkle_root);

-- ----------------------------------------------------------------------------
-- 6) Aggregate: per (source_token, dividend_token) pair
--    All NUMERIC columns are denominated in that row's dividend_token units
--    (deposits qualify: their dividend_token IS the quote token).
--    claim_count counts PAID ENTRIES, not claim transactions or unique holders.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_dividend_vault_stats (
    source_token             VARCHAR(42) NOT NULL,
    dividend_token           VARCHAR(42) NOT NULL,
    total_deposited          NUMERIC NOT NULL DEFAULT 0,  -- immediate slices (quote units)
    total_deposited_usd      NUMERIC NOT NULL DEFAULT 0,
    total_pending_deposited  NUMERIC NOT NULL DEFAULT 0,  -- swap-pending slices (quote units)
    total_pending_deposited_usd NUMERIC NOT NULL DEFAULT 0,
    total_consumed_quote     NUMERIC NOT NULL DEFAULT 0,  -- quote spent in conversions
    total_converted_received NUMERIC NOT NULL DEFAULT 0,  -- dividendToken from conversions
    -- quote awaiting conversion = pending deposited − consumed by Converted.
    -- A transient OR persisted NEGATIVE value is an ordering / replay-gap signal
    -- (a Converted row landing before its matching pending Deposit — within a
    -- batch the deposit/conversion inserts run concurrently, or across batches),
    -- NOT silent corruption. On-chain the contract never lets pendingSwap go
    -- negative, so a persisted negative means that (source_token, dividend_token)
    -- range must be re-indexed. Readers treating this as a displayable balance
    -- should GREATEST(pending_swap_balance, 0).
    pending_swap_balance     NUMERIC GENERATED ALWAYS AS
                                 (total_pending_deposited - total_consumed_quote) STORED,
    dividend_balance         NUMERIC NOT NULL DEFAULT 0,  -- cumulative mirror:
                                                          -- total_deposited + total_converted_received
    total_claimed            NUMERIC NOT NULL DEFAULT 0,  -- dividendToken paid to holders
    total_claimed_usd        NUMERIC NOT NULL DEFAULT 0,
    claim_count              INT NOT NULL DEFAULT 0,      -- paid entries
    last_block               BIGINT NOT NULL DEFAULT 0,
    updated_at               BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (source_token, dividend_token)
);

-- ----------------------------------------------------------------------------
-- Triggers: history insert success -> stats update (same transaction)
-- ----------------------------------------------------------------------------

-- Setup: seed the stats row so every configured pair exists with zeros
-- (setup is once-per-source immutable; DO NOTHING is reorg-replay-safe).
CREATE OR REPLACE FUNCTION update_dividend_stats_on_setup()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO v2_dividend_vault_stats (source_token, dividend_token, last_block, updated_at)
    VALUES (NEW.source_token, NEW.dividend_token, NEW.block_number, NEW.created_at)
    ON CONFLICT (source_token, dividend_token) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dividend_stats_on_setup ON v2_dividend_setups;
CREATE TRIGGER trg_dividend_stats_on_setup
AFTER INSERT ON v2_dividend_setups
FOR EACH ROW EXECUTE FUNCTION update_dividend_stats_on_setup();

-- Deposit: branch on pending.
--   pending=false -> immediate credit: total_deposited / _usd / dividend_balance.
--   pending=true  -> swap-pending accrual: total_pending_deposited / _usd only
--                    (dividend_balance is NOT touched; conversion credits it later).
CREATE OR REPLACE FUNCTION update_dividend_stats_on_deposit()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.pending THEN
        INSERT INTO v2_dividend_vault_stats
            (source_token, dividend_token, total_pending_deposited,
             total_pending_deposited_usd, last_block, updated_at)
        VALUES
            (NEW.source_token, NEW.dividend_token, NEW.amount, NEW.usd_value,
             NEW.block_number, NEW.created_at)
        ON CONFLICT (source_token, dividend_token) DO UPDATE SET
            total_pending_deposited     = v2_dividend_vault_stats.total_pending_deposited     + EXCLUDED.total_pending_deposited,
            total_pending_deposited_usd = v2_dividend_vault_stats.total_pending_deposited_usd + EXCLUDED.total_pending_deposited_usd,
            last_block                  = GREATEST(v2_dividend_vault_stats.last_block, EXCLUDED.last_block),
            updated_at                  = GREATEST(v2_dividend_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSE
        INSERT INTO v2_dividend_vault_stats
            (source_token, dividend_token, total_deposited, total_deposited_usd,
             dividend_balance, last_block, updated_at)
        VALUES
            (NEW.source_token, NEW.dividend_token, NEW.amount, NEW.usd_value,
             NEW.amount, NEW.block_number, NEW.created_at)
        ON CONFLICT (source_token, dividend_token) DO UPDATE SET
            total_deposited     = v2_dividend_vault_stats.total_deposited     + EXCLUDED.total_deposited,
            total_deposited_usd = v2_dividend_vault_stats.total_deposited_usd + EXCLUDED.total_deposited_usd,
            dividend_balance    = v2_dividend_vault_stats.dividend_balance    + EXCLUDED.dividend_balance,
            last_block          = GREATEST(v2_dividend_vault_stats.last_block, EXCLUDED.last_block),
            updated_at          = GREATEST(v2_dividend_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dividend_stats_on_deposit ON v2_dividend_deposits;
CREATE TRIGGER trg_dividend_stats_on_deposit
AFTER INSERT ON v2_dividend_deposits
FOR EACH ROW EXECUTE FUNCTION update_dividend_stats_on_deposit();

-- Conversion: pendingSwap -> dividendBalance.
CREATE OR REPLACE FUNCTION update_dividend_stats_on_conversion()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO v2_dividend_vault_stats
        (source_token, dividend_token, total_consumed_quote, total_converted_received,
         dividend_balance, last_block, updated_at)
    VALUES
        (NEW.source_token, NEW.dividend_token, NEW.consumed_quote, NEW.received,
         NEW.received, NEW.block_number, NEW.created_at)
    ON CONFLICT (source_token, dividend_token) DO UPDATE SET
        total_consumed_quote     = v2_dividend_vault_stats.total_consumed_quote     + EXCLUDED.total_consumed_quote,
        total_converted_received = v2_dividend_vault_stats.total_converted_received + EXCLUDED.total_converted_received,
        dividend_balance         = v2_dividend_vault_stats.dividend_balance         + EXCLUDED.dividend_balance,
        last_block               = GREATEST(v2_dividend_vault_stats.last_block, EXCLUDED.last_block),
        updated_at               = GREATEST(v2_dividend_vault_stats.updated_at, EXCLUDED.updated_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dividend_stats_on_conversion ON v2_dividend_conversions;
CREATE TRIGGER trg_dividend_stats_on_conversion
AFTER INSERT ON v2_dividend_conversions
FOR EACH ROW EXECUTE FUNCTION update_dividend_stats_on_conversion();

-- Claim: paid out to holder (does NOT reduce dividend_balance — chain doesn't).
CREATE OR REPLACE FUNCTION update_dividend_stats_on_claim()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO v2_dividend_vault_stats
        (source_token, dividend_token, total_claimed, total_claimed_usd,
         claim_count, last_block, updated_at)
    VALUES
        (NEW.source_token, NEW.dividend_token, NEW.amount, NEW.usd_value,
         1, NEW.block_number, NEW.created_at)
    ON CONFLICT (source_token, dividend_token) DO UPDATE SET
        total_claimed     = v2_dividend_vault_stats.total_claimed     + EXCLUDED.total_claimed,
        total_claimed_usd = v2_dividend_vault_stats.total_claimed_usd + EXCLUDED.total_claimed_usd,
        claim_count       = v2_dividend_vault_stats.claim_count       + 1,
        last_block        = GREATEST(v2_dividend_vault_stats.last_block, EXCLUDED.last_block),
        updated_at        = GREATEST(v2_dividend_vault_stats.updated_at, EXCLUDED.updated_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dividend_stats_on_claim ON v2_dividend_claims;
CREATE TRIGGER trg_dividend_stats_on_claim
AFTER INSERT ON v2_dividend_claims
FOR EACH ROW EXECUTE FUNCTION update_dividend_stats_on_claim();

-- ----------------------------------------------------------------------------
-- Backfill: rebuild stats from history in ONE statement set, same transaction.
-- Safe to run on fresh installs (empty history -> no-op). On live systems run
-- inside this migration transaction only — single full-aggregate rebuild, no
-- cross-statement additive accumulation (partial runs cannot corrupt totals).
-- ----------------------------------------------------------------------------
TRUNCATE v2_dividend_vault_stats;

-- pending_swap_balance is a GENERATED column — Postgres computes it; it is
-- intentionally EXCLUDED from the INSERT column list.
INSERT INTO v2_dividend_vault_stats
    (source_token, dividend_token,
     total_deposited, total_deposited_usd,
     total_pending_deposited, total_pending_deposited_usd,
     total_consumed_quote, total_converted_received,
     dividend_balance,
     total_claimed, total_claimed_usd, claim_count,
     last_block, updated_at)
SELECT
    pair.source_token,
    pair.dividend_token,
    COALESCE(d.total_deposited, 0),
    COALESCE(d.total_deposited_usd, 0),
    COALESCE(d.total_pending_deposited, 0),
    COALESCE(d.total_pending_deposited_usd, 0),
    COALESCE(c.total_consumed_quote, 0),
    COALESCE(c.total_converted_received, 0),
    COALESCE(d.total_deposited, 0) + COALESCE(c.total_converted_received, 0),
    COALESCE(cl.total_claimed, 0),
    COALESCE(cl.total_claimed_usd, 0),
    COALESCE(cl.claim_count, 0),
    GREATEST(COALESCE(s.last_block, 0), COALESCE(d.last_block, 0),
             COALESCE(c.last_block, 0), COALESCE(cl.last_block, 0)),
    GREATEST(COALESCE(s.updated_at, 0), COALESCE(d.updated_at, 0),
             COALESCE(c.updated_at, 0), COALESCE(cl.updated_at, 0))
FROM (
    SELECT source_token, dividend_token FROM v2_dividend_setups
    UNION
    SELECT source_token, dividend_token FROM v2_dividend_deposits
    UNION
    SELECT source_token, dividend_token FROM v2_dividend_conversions
    UNION
    SELECT source_token, dividend_token FROM v2_dividend_claims
) AS pair
LEFT JOIN (
    SELECT source_token, dividend_token,
           MAX(block_number) AS last_block, MAX(created_at) AS updated_at
    FROM v2_dividend_setups GROUP BY 1, 2
) s USING (source_token, dividend_token)
LEFT JOIN (
    -- Split deposits by pending: immediate (pending=false) feeds total_deposited
    -- and the dividend_balance sum; pending=true feeds total_pending_deposited.
    SELECT source_token, dividend_token,
           SUM(amount) FILTER (WHERE NOT pending) AS total_deposited,
           SUM(usd_value) FILTER (WHERE NOT pending) AS total_deposited_usd,
           SUM(amount) FILTER (WHERE pending) AS total_pending_deposited,
           SUM(usd_value) FILTER (WHERE pending) AS total_pending_deposited_usd,
           MAX(block_number) AS last_block, MAX(created_at) AS updated_at
    FROM v2_dividend_deposits GROUP BY 1, 2
) d USING (source_token, dividend_token)
LEFT JOIN (
    SELECT source_token, dividend_token,
           SUM(consumed_quote) AS total_consumed_quote, SUM(received) AS total_converted_received,
           MAX(block_number) AS last_block, MAX(created_at) AS updated_at
    FROM v2_dividend_conversions GROUP BY 1, 2
) c USING (source_token, dividend_token)
LEFT JOIN (
    SELECT source_token, dividend_token,
           SUM(amount) AS total_claimed, SUM(usd_value) AS total_claimed_usd,
           COUNT(*) AS claim_count,
           MAX(block_number) AS last_block, MAX(created_at) AS updated_at
    FROM v2_dividend_claims GROUP BY 1, 2
) cl USING (source_token, dividend_token);


-- ============================================================================
-- Scheduler distribution/accrual schema (merged from former 0034).
-- history INSERT -> trigger -> aggregate; leaf amount = cumulative accrued.
-- ============================================================================

CREATE TABLE IF NOT EXISTS dividend_accrual_history (
    source_token     VARCHAR(42) NOT NULL,
    dividend_token   VARCHAR(42) NOT NULL,
    holder           VARCHAR(42) NOT NULL,
    accrued          NUMERIC(78,0) NOT NULL CHECK (accrued >= 0),
    snapshot_balance NUMERIC(78,0) NOT NULL CHECK (snapshot_balance >= 0),
    balance_to       NUMERIC(78,0) NOT NULL CHECK (balance_to >= 0),
    snapshot_block   BIGINT  NOT NULL,
    created_at       BIGINT  NOT NULL,
    PRIMARY KEY (source_token, dividend_token, holder, balance_to)
);
CREATE INDEX IF NOT EXISTS idx_dividend_accrual_history_pair ON dividend_accrual_history (source_token, dividend_token);

CREATE TABLE IF NOT EXISTS dividend_accrual (
    source_token   VARCHAR(42) NOT NULL,
    holder         VARCHAR(42) NOT NULL,
    dividend_token VARCHAR(42) NOT NULL,
    accrued        NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (accrued >= 0),
    updated_at     BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (source_token, holder, dividend_token)
);
CREATE INDEX IF NOT EXISTS idx_dividend_accrual_holder ON dividend_accrual (holder);

CREATE TABLE IF NOT EXISTS dividend_pair_state (
    source_token           VARCHAR(42) NOT NULL,
    dividend_token         VARCHAR(42) NOT NULL,
    last_allocated_balance NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (last_allocated_balance >= 0),
    last_snapshot_block    BIGINT  NOT NULL DEFAULT 0,
    updated_at             BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (source_token, dividend_token)
);

CREATE OR REPLACE FUNCTION update_dividend_accrual_on_history()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO dividend_accrual (source_token, holder, dividend_token, accrued, updated_at)
    VALUES (NEW.source_token, NEW.holder, NEW.dividend_token, NEW.accrued, NEW.created_at)
    ON CONFLICT (source_token, holder, dividend_token) DO UPDATE SET
        accrued    = dividend_accrual.accrued + EXCLUDED.accrued,
        updated_at = GREATEST(dividend_accrual.updated_at, EXCLUDED.updated_at);

    INSERT INTO dividend_pair_state (source_token, dividend_token, last_allocated_balance, last_snapshot_block, updated_at)
    VALUES (NEW.source_token, NEW.dividend_token, NEW.balance_to, NEW.snapshot_block, NEW.created_at)
    ON CONFLICT (source_token, dividend_token) DO UPDATE SET
        last_allocated_balance = GREATEST(dividend_pair_state.last_allocated_balance, EXCLUDED.last_allocated_balance),
        last_snapshot_block    = GREATEST(dividend_pair_state.last_snapshot_block, EXCLUDED.last_snapshot_block),
        updated_at             = GREATEST(dividend_pair_state.updated_at, EXCLUDED.updated_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dividend_accrual_on_history ON dividend_accrual_history;
CREATE TRIGGER trg_dividend_accrual_on_history
AFTER INSERT ON dividend_accrual_history
FOR EACH ROW EXECUTE FUNCTION update_dividend_accrual_on_history();

CREATE TABLE IF NOT EXISTS dividend_merkle_root (
    merkle_root      VARCHAR(66) PRIMARY KEY,
    transaction_hash VARCHAR NOT NULL,
    leaf_count       INT NOT NULL,
    created_at       BIGINT NOT NULL
);

-- Latest-only claim snapshot (one row per (source, holder, dividend); rebuilt each run via upsert).
-- status set at build time from v2_dividend_claims (CLAIMED = claimed >= amount). Cumulative model:
-- a new root grows amount, flipping status back to AWAITING. No per-root history kept.
CREATE TABLE IF NOT EXISTS dividend_distribution (
    source_token   VARCHAR(42) NOT NULL,
    holder         VARCHAR(42) NOT NULL,
    dividend_token VARCHAR(42) NOT NULL,
    merkle_root    VARCHAR(66) NOT NULL,                          -- current published root
    amount         NUMERIC(78,0) NOT NULL CHECK (amount >= 0),    -- leaf = cumulative accrued
    proof          TEXT[]  NOT NULL,
    status         VARCHAR NOT NULL CHECK (status IN ('AWAITING', 'CLAIMED')),
    created_at     BIGINT  NOT NULL,
    PRIMARY KEY (source_token, holder, dividend_token)
);
CREATE INDEX IF NOT EXISTS idx_dividend_distribution_holder ON dividend_distribution (holder);

-- BACKFILL (rebuild aggregates from history). pair_state must come from ONE row per pair
-- (the max-balance_to row) so its (balance, snapshot_block) stay paired (Codex L10).
TRUNCATE dividend_accrual;
INSERT INTO dividend_accrual (source_token, holder, dividend_token, accrued, updated_at)
SELECT source_token, holder, dividend_token, SUM(accrued), MAX(created_at)
FROM dividend_accrual_history GROUP BY source_token, holder, dividend_token;

TRUNCATE dividend_pair_state;
INSERT INTO dividend_pair_state (source_token, dividend_token, last_allocated_balance, last_snapshot_block, updated_at)
SELECT DISTINCT ON (source_token, dividend_token)
       source_token, dividend_token, balance_to, snapshot_block, created_at
FROM dividend_accrual_history
ORDER BY source_token, dividend_token, balance_to DESC, snapshot_block DESC, created_at DESC;

COMMIT;
