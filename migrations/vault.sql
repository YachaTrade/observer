-- ======================================================================
-- V2 Vault — single source of truth for all vault-related schema.
-- ----------------------------------------------------------------------
-- Covers BurnVault, LPVault, CreatorFeeVault, GiftVault and the
-- VaultRegistry that catalogues them. Idempotent: works as the fresh
-- install schema and as the prod upgrade path.
--
-- Contents
--
--   1. Event log tables
--      - v2_vault_burns
--      - v2_vault_lp_injections
--      - v2_creator_fee_claims
--      - v2_gifts                    (with legacy x_handle/claimer migration)
--      - v2_creator_updates
--      - v2_gift_expiry_updates
--      - v2_vault_registry
--      - v2_vault_metadata
--   2. Pre-aggregated stat tables (per token)
--      - v2_burn_vault_stats
--      - v2_lp_vault_stats
--      - v2_creator_fee_vault_stats  (with current_balance)
--      - v2_gift_vault_stats         (with current_state, current_balance)
--   3. AFTER INSERT triggers that maintain the stat tables
--   4. Initial backfill from existing event rows
--
-- v2_creator_fee_distribution lives in 0015_v2_events.sql (it's a
-- CreatorFeeProcessor event, not a vault event).
-- ======================================================================

BEGIN;

-- ======================================================================
-- 1. Event log tables
-- ======================================================================

-- 1.1 v2_vault_burns — BurnVault.Burn / GiftVault.Burn
CREATE TABLE IF NOT EXISTS v2_vault_burns (
    vault_type VARCHAR NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    quote_in NUMERIC NOT NULL,       -- UNIT: quote raw (wei) — BurnVault.Burn.quoteIn
    token_burned NUMERIC NOT NULL,   -- UNIT: token raw (wei) — BurnVault.Burn.tokenBurned
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    quote_id VARCHAR(42),
    usd_value NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — quote_in / 10^quote_decimals * quote_price
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_vault_burns_token
    ON v2_vault_burns (token_id);

-- 1.2 v2_vault_lp_injections — LPVault.AddLiquidity
CREATE TABLE IF NOT EXISTS v2_vault_lp_injections (
    token_id VARCHAR(42) NOT NULL,
    quote_used NUMERIC NOT NULL,     -- UNIT: quote raw (wei) — LPVault.AddLiquidity.quoteUsed
    token_used NUMERIC NOT NULL,     -- UNIT: token raw (wei) — LPVault.AddLiquidity.tokenUsed
    lp_burned NUMERIC NOT NULL,      -- UNIT: token raw (wei) — LP-token amount, AddLiquidity.lpBurned
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    quote_id VARCHAR(42),
    usd_value NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — quote_used / 10^quote_decimals * quote_price
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_vault_lp_inject_token
    ON v2_vault_lp_injections (token_id);

-- 1.3 v2_creator_fee_claims — CreatorFeeVault.Deposit / Claim
CREATE TABLE IF NOT EXISTS v2_creator_fee_claims (
    event_type VARCHAR NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    creator VARCHAR(42),
    amount NUMERIC NOT NULL,         -- UNIT: quote raw (wei) — CreatorFeeVault Deposit/Claim.amount
    new_balance NUMERIC,             -- UNIT: quote raw (wei) — CreatorFeeVault.Deposit.newBalance (NULL on CLAIM)
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    quote_id VARCHAR(42),
    usd_value NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — amount / 10^quote_decimals * quote_price
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_creator_fee_claims_token
    ON v2_creator_fee_claims (token_id);
ALTER TABLE v2_creator_fee_claims
    DROP CONSTRAINT IF EXISTS v2_creator_fee_claims_event_type_check;
ALTER TABLE v2_creator_fee_claims
    ADD CONSTRAINT v2_creator_fee_claims_event_type_check
    CHECK (event_type IN ('DEPOSIT', 'CLAIM'));

-- 1.4 v2_gifts — GiftVault.Setup / Deposit / Claim / Expire / ReceiverSet
--   GiftVault ABI generalized X-only Setup into (platform uint8, id string)
--   and renamed Claim.claimer to Claim.receiver. The ALTER block below is
--   idempotent: fresh DBs get the new schema, DBs that already created
--   the old schema get migrated via RENAME.
CREATE TABLE IF NOT EXISTS v2_gifts (
    event_type VARCHAR NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    platform VARCHAR,
    platform_id VARCHAR,
    receiver VARCHAR(42),
    amount NUMERIC,                  -- UNIT: quote raw (wei) — GiftVault Deposit/Claim/Expire.amount (NULL on SETUP/RECEIVER_SET)
    new_balance NUMERIC,             -- UNIT: quote raw (wei) — GiftVault.Deposit.newBalance (NULL on non-DEPOSIT)
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    quote_id VARCHAR(42),
    usd_value NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — amount / 10^quote_decimals * quote_price
    -- Gift expiry epoch (unix seconds). Meaningful on SETUP rows
    -- (= block_timestamp + GIFT_EXPIRY_DURATION) and on RECEIVER_SET
    -- rows (= 0, expiry cleared). Other event types record 0 as a
    -- placeholder; consumers read v2_gift_vault_stats.expires_at for
    -- the live value.
    expires_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
ALTER TABLE v2_gifts
    ADD COLUMN IF NOT EXISTS expires_at BIGINT NOT NULL DEFAULT 0;

DO $$
DECLARE
    platform_data_type TEXT;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gifts' AND column_name = 'claimer'
    ) THEN
        ALTER TABLE v2_gifts RENAME COLUMN claimer TO receiver;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gifts' AND column_name = 'x_handle'
    ) THEN
        ALTER TABLE v2_gifts RENAME COLUMN x_handle TO platform_id;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gifts' AND column_name = 'x_handle_hash'
    ) THEN
        ALTER TABLE v2_gifts DROP COLUMN x_handle_hash;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'v2_gifts' AND column_name = 'platform'
    ) THEN
        ALTER TABLE v2_gifts ADD COLUMN platform VARCHAR;
    END IF;

    -- An earlier rev briefly used SMALLINT. Convert to VARCHAR.
    SELECT data_type INTO platform_data_type
      FROM information_schema.columns
      WHERE table_name = 'v2_gifts' AND column_name = 'platform';
    IF platform_data_type = 'smallint' THEN
        ALTER TABLE v2_gifts ALTER COLUMN platform TYPE VARCHAR
            USING CASE platform
                WHEN 0 THEN 'GITHUB'
                WHEN 1 THEN 'X'
                ELSE NULL
            END;
    END IF;

    -- Backfill legacy SETUP rows. Old contract was X-only, so any SETUP
    -- row with a platform_id but no platform is an X handle carried over
    -- from the old x_handle column.
    UPDATE v2_gifts
       SET platform = 'X'
     WHERE event_type = 'SETUP'
       AND platform IS NULL
       AND platform_id IS NOT NULL;
END $$;

ALTER TABLE v2_gifts DROP CONSTRAINT IF EXISTS v2_gifts_platform_check;
ALTER TABLE v2_gifts ADD CONSTRAINT v2_gifts_platform_check
    CHECK (platform IS NULL OR platform IN ('GITHUB', 'X'));

ALTER TABLE v2_gifts DROP CONSTRAINT IF EXISTS v2_gifts_event_type_check;
ALTER TABLE v2_gifts ADD CONSTRAINT v2_gifts_event_type_check
    CHECK (event_type IN ('SETUP', 'DEPOSIT', 'CLAIM', 'EXPIRE', 'RECEIVER_SET'));

DROP INDEX IF EXISTS idx_v2_gifts_x_handle;
CREATE INDEX IF NOT EXISTS idx_v2_gifts_token
    ON v2_gifts (token_id);
CREATE INDEX IF NOT EXISTS idx_v2_gifts_setup
    ON v2_gifts (platform, platform_id) WHERE event_type = 'SETUP';

-- 1.5 v2_creator_updates — CreatorFeeVault.VaultSetup / CreatorUpdate
--   event_type='SETUP'  -> initial creator bind (old_creator NULL)
--   event_type='UPDATE' -> subsequent creator change
CREATE TABLE IF NOT EXISTS v2_creator_updates (
    event_type VARCHAR NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    old_creator VARCHAR(42),
    new_creator VARCHAR(42) NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
ALTER TABLE v2_creator_updates
    DROP CONSTRAINT IF EXISTS v2_creator_updates_event_type_check;
ALTER TABLE v2_creator_updates
    ADD CONSTRAINT v2_creator_updates_event_type_check
    CHECK (event_type IN ('SETUP', 'UPDATE'));
CREATE INDEX IF NOT EXISTS idx_v2_creator_updates_token
    ON v2_creator_updates (token_id);
CREATE INDEX IF NOT EXISTS idx_v2_creator_updates_new_creator
    ON v2_creator_updates (new_creator);

-- 1.6 v2_gift_expiry_updates — GiftVault.ExpiryUpdate (governance config)
CREATE TABLE IF NOT EXISTS v2_gift_expiry_updates (
    old_duration NUMERIC NOT NULL,   -- UNIT: seconds — GiftVault.ExpiryUpdate.oldDuration (gift expiry window)
    new_duration NUMERIC NOT NULL,   -- UNIT: seconds — GiftVault.ExpiryUpdate.newDuration (gift expiry window)
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);

-- 1.7 v2_vault_registry — VaultRegistry.Register (append-only log)
CREATE TABLE IF NOT EXISTS v2_vault_registry (
    vault_id VARCHAR(42) NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_vault_registry_vault
    ON v2_vault_registry (vault_id);

-- 1.8 v2_vault_metadata — denormalized per-vault catalog. One row per
--     registered vault. Upserted on Register (name / creator / vault_type
--     + fetched metadata JSON), `active` toggled by Deactivate.
--     metadata / metadata_uri may be NULL if off-chain fetch failed.
CREATE TABLE IF NOT EXISTS v2_vault_metadata (
    vault_id VARCHAR(42) PRIMARY KEY,
    name VARCHAR NOT NULL,
    creator VARCHAR(42) NOT NULL,
    vault_type VARCHAR NOT NULL
        CONSTRAINT v2_vault_metadata_type_check
        CHECK (vault_type IN ('CUSTOM','BURN','LP','CREATOR_FEE','GIFT','DIVIDEND')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata_uri VARCHAR,
    metadata JSONB,
    metadata_fetched_at BIGINT,
    registered_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_v2_vault_metadata_type
    ON v2_vault_metadata (vault_type);
CREATE INDEX IF NOT EXISTS idx_v2_vault_metadata_active
    ON v2_vault_metadata (active) WHERE active;

-- 1.9 v2_creator_fee_allocation — per-token vault distribution percentages
--   from CreatorFeeProcessor.Setup(token, vaults[(vault, bps)]).
--   PK = (token_id, vault_id). Re-Setup of the same token UPSERTs
--   each row's bps. Drives UI fee-distribution % displays.
CREATE TABLE IF NOT EXISTS v2_creator_fee_allocation (
    token_id VARCHAR(42) NOT NULL,
    vault_id VARCHAR(42) NOT NULL,
    bps INT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (token_id, vault_id)
);
ALTER TABLE v2_creator_fee_allocation
    DROP CONSTRAINT IF EXISTS v2_creator_fee_allocation_bps_check;
ALTER TABLE v2_creator_fee_allocation
    ADD CONSTRAINT v2_creator_fee_allocation_bps_check
    CHECK (bps >= 0 AND bps <= 10000);
CREATE INDEX IF NOT EXISTS idx_v2_creator_fee_allocation_vault
    ON v2_creator_fee_allocation (vault_id);

-- ======================================================================
-- 2. Pre-aggregated stat tables (one row per token)
-- ----------------------------------------------------------------------
-- Each vault aggregates from its own event tables — no cross-vault JOIN,
-- no VaultRegistry dependency. v2_creator_fee_distribution stays a pure
-- event log and is NOT an aggregation source (BurnVault / LPVault never
-- emit a "deposit" event of their own).
--
-- current_balance on creator_fee / gift mirrors on-chain `_balances` /
-- `gift.balance` exactly:
--   DEPOSIT → set to Deposit.newBalance (from the event row)
--   CLAIM   → 0
--   EXPIRE  → 0     (gift only — sweep + buyback zeros gift.balance)
-- ======================================================================

-- Drop legacy view names if this DB once ran the VIEW-based revision.
-- Guarded so we don't error when the name is already a TABLE (the
-- current shape) — `DROP VIEW IF EXISTS` errors on the wrong relkind.
DO $$
DECLARE
    v TEXT;
BEGIN
    FOREACH v IN ARRAY ARRAY[
        'v2_burn_vault_stats',
        'v2_lp_vault_stats',
        'v2_creator_fee_vault_stats',
        'v2_gift_vault_stats'
    ] LOOP
        IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = v) THEN
            EXECUTE format('DROP VIEW %I', v);
        END IF;
    END LOOP;
END $$;

-- 2.1 BurnVault — buyback+burn totals. No "received" metric (BurnVault
--     never emits a Deposit event).
CREATE TABLE IF NOT EXISTS v2_burn_vault_stats (
    token_id VARCHAR(42) PRIMARY KEY,
    quote_spent NUMERIC NOT NULL DEFAULT 0,      -- UNIT: quote raw (wei) — SUM(v2_vault_burns.quote_in)
    quote_spent_usd NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — SUM(v2_vault_burns.usd_value)
    tokens_burned NUMERIC NOT NULL DEFAULT 0,    -- UNIT: token raw (wei) — SUM(v2_vault_burns.token_burned)
    burn_count INT NOT NULL DEFAULT 0,
    last_block BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0
);

-- 2.2 LPVault — LP injection totals.
CREATE TABLE IF NOT EXISTS v2_lp_vault_stats (
    token_id VARCHAR(42) PRIMARY KEY,
    quote_injected NUMERIC NOT NULL DEFAULT 0,      -- UNIT: quote raw (wei) — SUM(v2_vault_lp_injections.quote_used)
    quote_injected_usd NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — SUM(v2_vault_lp_injections.usd_value)
    token_injected NUMERIC NOT NULL DEFAULT 0,      -- UNIT: token raw (wei) — SUM(v2_vault_lp_injections.token_used)
    lp_burned NUMERIC NOT NULL DEFAULT 0,           -- UNIT: token raw (wei) — LP-token, SUM(v2_vault_lp_injections.lp_burned)
    inject_count INT NOT NULL DEFAULT 0,
    last_block BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0
);

-- 2.3 CreatorFeeVault — deposit / claim totals + live balance mirror.
CREATE TABLE IF NOT EXISTS v2_creator_fee_vault_stats (
    token_id VARCHAR(42) PRIMARY KEY,
    current_balance NUMERIC NOT NULL DEFAULT 0,      -- UNIT: quote raw (wei) — mirrors Deposit.newBalance, 0 after CLAIM
    total_deposited NUMERIC NOT NULL DEFAULT 0,      -- UNIT: quote raw (wei) — SUM(DEPOSIT amount)
    total_deposited_usd NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — SUM(DEPOSIT usd_value)
    total_claimed NUMERIC NOT NULL DEFAULT 0,        -- UNIT: quote raw (wei) — SUM(CLAIM amount)
    total_claimed_usd NUMERIC NOT NULL DEFAULT 0,    -- UNIT: USD (human) — SUM(CLAIM usd_value)
    deposit_count INT NOT NULL DEFAULT 0,
    claim_count INT NOT NULL DEFAULT 0,
    last_block BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0
);

-- 2.4 GiftVault — full lifecycle + current_state + live balance mirror.
--   platform / platform_id captured from the SETUP event (X handle,
--   GitHub login, ...). receiver captured from the RECEIVER_SET event
--   so consumers (gift-bot, UI) can read current bind state without
--   joining v2_gifts.
CREATE TABLE IF NOT EXISTS v2_gift_vault_stats (
    token_id VARCHAR(42) PRIMARY KEY,
    current_state VARCHAR NOT NULL DEFAULT 'Accumulating',
    current_balance NUMERIC NOT NULL DEFAULT 0,          -- UNIT: quote raw (wei) — mirrors Deposit.newBalance, 0 after CLAIM/EXPIRE
    platform VARCHAR,
    platform_id VARCHAR,
    receiver VARCHAR(42),
    total_deposited NUMERIC NOT NULL DEFAULT 0,          -- UNIT: quote raw (wei) — SUM(DEPOSIT amount)
    total_deposited_usd NUMERIC NOT NULL DEFAULT 0,      -- UNIT: USD (human) — SUM(DEPOSIT usd_value)
    total_claimed NUMERIC NOT NULL DEFAULT 0,            -- UNIT: quote raw (wei) — SUM(CLAIM amount)
    total_claimed_usd NUMERIC NOT NULL DEFAULT 0,        -- UNIT: USD (human) — SUM(CLAIM usd_value)
    total_expired NUMERIC NOT NULL DEFAULT 0,            -- UNIT: quote raw (wei) — SUM(EXPIRE amount)
    total_expired_usd NUMERIC NOT NULL DEFAULT 0,        -- UNIT: USD (human) — SUM(EXPIRE usd_value)
    buyback_quote_spent NUMERIC NOT NULL DEFAULT 0,      -- UNIT: quote raw (wei) — SUM(v2_vault_burns.quote_in WHERE vault_type='GIFT')
    buyback_quote_spent_usd NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human) — SUM(GIFT-burn usd_value)
    buyback_tokens NUMERIC NOT NULL DEFAULT 0,           -- UNIT: token raw (wei) — SUM(v2_vault_burns.token_burned WHERE vault_type='GIFT')
    -- Current expiry epoch for the gift. Set from the SETUP event's
    -- expires_at (= setup block_timestamp + GIFT_EXPIRY_DURATION) and
    -- cleared to 0 when a RECEIVER_SET event lands (gift is claimed by
    -- a bound receiver, no longer expires).
    expires_at BIGINT NOT NULL DEFAULT 0,
    -- block_timestamp of the RECEIVER_SET event that bound this gift
    -- to its receiver. 0 while the gift is still 'Accumulating'.
    receiver_set_at BIGINT NOT NULL DEFAULT 0,
    last_block BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0
);

-- 2.5 CreatorFeeDistribution — per-(token, vault) fee distribution totals.
--     Sourced from CreatorFeeProcessor.Distribute events
--     (event_type = 'DISTRIBUTE') in v2_creator_fee_distribution. Unlike
--     vault-side stats, this captures the *outgoing* fee a token routed to
--     each vault. CALLBACKFAIL rows are ignored (failed distribution
--     attempts; on-chain side handles refunds separately).
CREATE TABLE IF NOT EXISTS v2_creator_fee_distribution_stats (
    token_id              VARCHAR(42) NOT NULL,
    vault_id              VARCHAR(42) NOT NULL,
    quote_id              VARCHAR(42) NOT NULL,
    distributed_quote     NUMERIC     NOT NULL DEFAULT 0,  -- UNIT: quote raw (wei) — SUM(v2_creator_fee_distribution.amount WHERE event_type='DISTRIBUTE')
    distributed_quote_usd NUMERIC     NOT NULL DEFAULT 0,  -- UNIT: USD (human) — SUM(DISTRIBUTE usd_value)
    distribute_count      INT         NOT NULL DEFAULT 0,
    last_block            BIGINT      NOT NULL DEFAULT 0,
    updated_at            BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (token_id, vault_id)
);

-- Drop older GENERATED columns (pending_balance / pending_or_claimable),
-- then ensure current_balance is present. Idempotent for re-runs.
ALTER TABLE v2_creator_fee_vault_stats
    DROP COLUMN IF EXISTS pending_balance;
ALTER TABLE v2_gift_vault_stats
    DROP COLUMN IF EXISTS pending_or_claimable;
ALTER TABLE v2_creator_fee_vault_stats
    ADD COLUMN IF NOT EXISTS current_balance NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS current_balance NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS platform VARCHAR;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS platform_id VARCHAR;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS receiver VARCHAR(42);
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS expires_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE v2_gift_vault_stats
    ADD COLUMN IF NOT EXISTS receiver_set_at BIGINT NOT NULL DEFAULT 0;

ALTER TABLE v2_gift_vault_stats
    DROP CONSTRAINT IF EXISTS v2_gift_vault_stats_platform_check;
ALTER TABLE v2_gift_vault_stats
    ADD CONSTRAINT v2_gift_vault_stats_platform_check
    CHECK (platform IS NULL OR platform IN ('GITHUB', 'X'));

ALTER TABLE v2_gift_vault_stats
    DROP CONSTRAINT IF EXISTS v2_gift_vault_stats_state_check;
ALTER TABLE v2_gift_vault_stats
    ADD CONSTRAINT v2_gift_vault_stats_state_check
    CHECK (current_state IN ('Accumulating','Active','Burned'));

-- Supporting indexes on event tables for the backfill + ad-hoc queries.
CREATE INDEX IF NOT EXISTS idx_v2_vault_burns_type_token
    ON v2_vault_burns (vault_type, token_id);
CREATE INDEX IF NOT EXISTS idx_v2_creator_fee_claims_event_token
    ON v2_creator_fee_claims (event_type, token_id);
CREATE INDEX IF NOT EXISTS idx_v2_gifts_event_token
    ON v2_gifts (event_type, token_id);
CREATE INDEX IF NOT EXISTS idx_v2_creator_fee_dist_vault_event
    ON v2_creator_fee_distribution (vault, event_type);

-- Profile gift-fee endpoint (`GET /profile/gift-fee/{account_id}`) filters by
-- receiver and orders by claimable balance (current_balance DESC). Partial
-- index on (receiver, current_balance DESC) WHERE receiver IS NOT NULL covers
-- both WHERE and ORDER BY in a single index-only scan, and excludes the
-- "Accumulating" rows that have NULL receiver to keep the index small.
CREATE INDEX IF NOT EXISTS idx_v2_gift_vault_stats_receiver_balance
    ON v2_gift_vault_stats (receiver, current_balance DESC)
    WHERE receiver IS NOT NULL;

-- ======================================================================
-- 3. Trigger functions + triggers
-- ----------------------------------------------------------------------
-- Idempotency: every v2_* event table uses
--   INSERT ... ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
-- Postgres skips AFTER INSERT triggers when no row is actually inserted,
-- so reorg / restart replays cannot double-count aggregates.
-- ======================================================================

-- 3.1 v2_vault_burns → burn_stats (vault_type='BURN')
--                   → gift_stats (vault_type='GIFT', buyback columns)
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

DROP TRIGGER IF EXISTS trg_update_vault_burn_stats ON v2_vault_burns;
CREATE TRIGGER trg_update_vault_burn_stats
AFTER INSERT ON v2_vault_burns
FOR EACH ROW EXECUTE FUNCTION update_vault_burn_stats();

-- 3.2 v2_vault_lp_injections → lp_stats
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

DROP TRIGGER IF EXISTS trg_update_vault_lp_stats ON v2_vault_lp_injections;
CREATE TRIGGER trg_update_vault_lp_stats
AFTER INSERT ON v2_vault_lp_injections
FOR EACH ROW EXECUTE FUNCTION update_vault_lp_stats();

-- 3.3 v2_creator_fee_claims → creator_fee_stats
--   DEPOSIT: current_balance = NEW.new_balance, total_deposited += amount
--   CLAIM:   current_balance = 0,              total_claimed   += amount
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

DROP TRIGGER IF EXISTS trg_update_creator_fee_vault_stats ON v2_creator_fee_claims;
CREATE TRIGGER trg_update_creator_fee_vault_stats
AFTER INSERT ON v2_creator_fee_claims
FOR EACH ROW EXECUTE FUNCTION update_creator_fee_vault_stats();

-- 3.4 v2_gifts → gift_stats
--   SETUP:        init row ('Accumulating')
--   DEPOSIT:      current_balance = NEW.new_balance, total_deposited += amount
--   CLAIM:        current_balance = 0,              total_claimed   += amount
--   EXPIRE:       current_balance = 0, total_expired += amount, state = 'Burned'
--   RECEIVER_SET: state = 'Active' (stays Burned if already Burned)
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
                WHEN 'Burned' THEN 'Burned'  -- terminal
                ELSE 'Active'
            END,
            receiver        = COALESCE(EXCLUDED.receiver, v2_gift_vault_stats.receiver),
            -- receiver bound → gift no longer expires
            expires_at      = 0,
            receiver_set_at = EXCLUDED.receiver_set_at,
            last_block      = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at      = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_gift_vault_stats ON v2_gifts;
CREATE TRIGGER trg_update_gift_vault_stats
AFTER INSERT ON v2_gifts
FOR EACH ROW EXECUTE FUNCTION update_gift_vault_stats();

-- 3.5 v2_creator_updates → token.creator
--   CreatorFeeVault.VaultSetup (initial bind) and CreatorUpdate
--   (subsequent change) are the on-chain source of truth for a token's
--   creator after V2 graduation. Mirror new_creator into the canonical
--   `token` row so consumers can keep reading token.creator without
--   joining v2_creator_updates.
CREATE OR REPLACE FUNCTION sync_token_creator_from_v2_updates()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE token
       SET creator = NEW.new_creator
     WHERE token_id = NEW.token_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_token_creator_from_v2_updates
    ON v2_creator_updates;
CREATE TRIGGER trg_sync_token_creator_from_v2_updates
AFTER INSERT ON v2_creator_updates
FOR EACH ROW EXECUTE FUNCTION sync_token_creator_from_v2_updates();

-- 3.6 v2_creator_fee_distribution → distribution_stats
--     Aggregates 'DISTRIBUTE' rows into per-(token, vault) totals. Other
--     event_type values (e.g. 'CALLBACKFAIL') are skipped — failed
--     callbacks aren't successful fee transfers.
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

DROP TRIGGER IF EXISTS trg_update_creator_fee_distribution_stats
    ON v2_creator_fee_distribution;
CREATE TRIGGER trg_update_creator_fee_distribution_stats
AFTER INSERT ON v2_creator_fee_distribution
FOR EACH ROW EXECUTE FUNCTION update_creator_fee_distribution_stats();

-- ======================================================================
-- 4. One-time backfill from existing event data.
--    ON CONFLICT DO NOTHING makes this safe to re-run: rows the trigger
--    already maintains stay untouched.
-- ======================================================================

-- 4.1 BurnVault
INSERT INTO v2_burn_vault_stats
    (token_id, quote_spent, tokens_burned, burn_count, last_block, updated_at)
SELECT token_id,
       SUM(quote_in),
       SUM(token_burned),
       COUNT(*),
       MAX(block_number),
       MAX(created_at)
FROM v2_vault_burns
WHERE vault_type = 'BURN'
GROUP BY token_id
ON CONFLICT (token_id) DO NOTHING;

-- 4.2 LPVault
INSERT INTO v2_lp_vault_stats
    (token_id, quote_injected, token_injected, lp_burned, inject_count,
     last_block, updated_at)
SELECT token_id,
       SUM(quote_used),
       SUM(token_used),
       SUM(lp_burned),
       COUNT(*),
       MAX(block_number),
       MAX(created_at)
FROM v2_vault_lp_injections
GROUP BY token_id
ON CONFLICT (token_id) DO NOTHING;

-- 4.3 CreatorFeeVault — current_balance from latest event per token.
WITH latest AS (
    SELECT DISTINCT ON (token_id) token_id, event_type, new_balance
    FROM v2_creator_fee_claims
    ORDER BY token_id, block_number DESC, log_index DESC
),
totals AS (
    SELECT token_id,
           COALESCE(SUM(amount) FILTER (WHERE event_type = 'DEPOSIT'), 0) AS total_deposited,
           COALESCE(SUM(amount) FILTER (WHERE event_type = 'CLAIM'),   0) AS total_claimed,
           COUNT(*) FILTER (WHERE event_type = 'DEPOSIT') AS deposit_count,
           COUNT(*) FILTER (WHERE event_type = 'CLAIM')   AS claim_count,
           MAX(block_number) AS last_block,
           MAX(created_at)   AS updated_at
    FROM v2_creator_fee_claims
    GROUP BY token_id
)
INSERT INTO v2_creator_fee_vault_stats
    (token_id, current_balance, total_deposited, total_claimed,
     deposit_count, claim_count, last_block, updated_at)
SELECT t.token_id,
       CASE l.event_type
           WHEN 'DEPOSIT' THEN COALESCE(l.new_balance, 0)
           ELSE 0
       END,
       t.total_deposited,
       t.total_claimed,
       t.deposit_count,
       t.claim_count,
       t.last_block,
       t.updated_at
FROM totals t
LEFT JOIN latest l USING (token_id)
ON CONFLICT (token_id) DO NOTHING;

-- 4.4 GiftVault — current_state from event presence; current_balance
--                 from latest balance-affecting event.
WITH latest_bal AS (
    SELECT DISTINCT ON (token_id) token_id, event_type, new_balance
    FROM v2_gifts
    WHERE event_type IN ('DEPOSIT','CLAIM','EXPIRE')
    ORDER BY token_id, block_number DESC, log_index DESC
),
latest_setup AS (
    SELECT DISTINCT ON (token_id) token_id, platform, platform_id
    FROM v2_gifts
    WHERE event_type = 'SETUP'
    ORDER BY token_id, block_number DESC, log_index DESC
),
latest_receiver AS (
    SELECT DISTINCT ON (token_id) token_id, receiver
    FROM v2_gifts
    WHERE event_type = 'RECEIVER_SET'
    ORDER BY token_id, block_number DESC, log_index DESC
),
totals AS (
    SELECT token_id,
           CASE
               WHEN bool_or(event_type = 'EXPIRE') THEN 'Burned'
               WHEN bool_or(event_type = 'RECEIVER_SET') THEN 'Active'
               ELSE 'Accumulating'
           END AS current_state,
           COALESCE(SUM(amount) FILTER (WHERE event_type = 'DEPOSIT'), 0) AS total_deposited,
           COALESCE(SUM(amount) FILTER (WHERE event_type = 'CLAIM'),   0) AS total_claimed,
           COALESCE(SUM(amount) FILTER (WHERE event_type = 'EXPIRE'),  0) AS total_expired,
           MAX(block_number) AS last_block,
           MAX(created_at)   AS updated_at
    FROM v2_gifts
    GROUP BY token_id
),
burns AS (
    SELECT token_id,
           SUM(quote_in)     AS buyback_quote_spent,
           SUM(token_burned) AS buyback_tokens,
           MAX(block_number) AS last_block,
           MAX(created_at)   AS updated_at
    FROM v2_vault_burns
    WHERE vault_type = 'GIFT'
    GROUP BY token_id
)
INSERT INTO v2_gift_vault_stats
    (token_id, current_state, current_balance,
     platform, platform_id, receiver,
     total_deposited, total_claimed, total_expired,
     buyback_quote_spent, buyback_tokens,
     last_block, updated_at)
SELECT t.token_id,
       t.current_state,
       CASE l.event_type
           WHEN 'DEPOSIT' THEN COALESCE(l.new_balance, 0)
           ELSE 0
       END,
       s.platform,
       s.platform_id,
       r.receiver,
       t.total_deposited,
       t.total_claimed,
       t.total_expired,
       COALESCE(b.buyback_quote_spent, 0),
       COALESCE(b.buyback_tokens, 0),
       GREATEST(t.last_block, COALESCE(b.last_block, 0)),
       GREATEST(t.updated_at, COALESCE(b.updated_at, 0))
FROM totals t
LEFT JOIN latest_bal l USING (token_id)
LEFT JOIN latest_setup s USING (token_id)
LEFT JOIN latest_receiver r USING (token_id)
LEFT JOIN burns b USING (token_id)
ON CONFLICT (token_id) DO NOTHING;

-- Pick up Burn-only rows (tokens that went Burned via afterDeposit
-- without a prior v2_gifts row for them).
INSERT INTO v2_gift_vault_stats
    (token_id, current_state, buyback_quote_spent, buyback_tokens,
     last_block, updated_at)
SELECT token_id, 'Burned', SUM(quote_in), SUM(token_burned),
       MAX(block_number), MAX(created_at)
FROM v2_vault_burns
WHERE vault_type = 'GIFT'
GROUP BY token_id
ON CONFLICT (token_id) DO NOTHING;

-- 4.5 token.creator backfill from latest v2_creator_updates row.
--     Existing rows that pre-date the trg_sync_token_creator_from_v2_updates
--     trigger don't auto-propagate. Pull latest new_creator per token here.
WITH latest_creator AS (
    SELECT DISTINCT ON (token_id) token_id, new_creator
    FROM v2_creator_updates
    ORDER BY token_id, block_number DESC, log_index DESC
)
UPDATE token t
   SET creator = lc.new_creator
  FROM latest_creator lc
 WHERE t.token_id = lc.token_id
   AND t.creator IS DISTINCT FROM lc.new_creator;

-- 4.6 CreatorFeeDistribution — per-(token, vault) totals from event log.
INSERT INTO v2_creator_fee_distribution_stats
    (token_id, vault_id, quote_id,
     distributed_quote, distribute_count, last_block, updated_at)
SELECT
    token,
    vault,
    MIN(quote_id),
    SUM(amount),
    COUNT(*),
    MAX(block_number),
    MAX(created_at)
FROM v2_creator_fee_distribution
WHERE event_type = 'DISTRIBUTE'
  AND token IS NOT NULL
  AND vault IS NOT NULL
GROUP BY token, vault
ON CONFLICT (token_id, vault_id) DO NOTHING;

-- ======================================================================
-- 5. USD value backfill (added 2026-05-10)
-- ----------------------------------------------------------------------
-- Step A: populate quote_id + usd_value on event rows that pre-date the
-- stream-side enrichment. Idempotent via `WHERE usd_value = 0` guard.
-- Live trigger inserts will land with the correct usd_value already set,
-- so the WHERE clause skips them harmlessly.
-- ======================================================================

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

-- v2_gifts: only DEPOSIT/CLAIM/EXPIRE rows have a real amount.
-- SETUP/RECEIVER_SET have amount NULL; leave their usd_value at default 0.
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

-- v2_creator_fee_distribution: quote_id already on row, no market JOIN.
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

-- ----------------------------------------------------------------------
-- Step B: backfill cumulative USD into stats tables. Sum-aggregates
-- per token from the now-populated event rows. Guarded by `WHERE = 0`
-- so re-runs and live trigger inserts don't double-count.
-- ----------------------------------------------------------------------

WITH s AS (
    SELECT token_id, SUM(usd_value) AS quote_spent_usd
      FROM v2_vault_burns
     WHERE vault_type = 'BURN'
     GROUP BY token_id
)
UPDATE v2_burn_vault_stats v
   SET quote_spent_usd = s.quote_spent_usd
  FROM s
 WHERE v.token_id = s.token_id
   AND v.quote_spent_usd = 0;

WITH s AS (
    SELECT token_id, SUM(usd_value) AS quote_injected_usd
      FROM v2_vault_lp_injections
     GROUP BY token_id
)
UPDATE v2_lp_vault_stats v
   SET quote_injected_usd = s.quote_injected_usd
  FROM s
 WHERE v.token_id = s.token_id
   AND v.quote_injected_usd = 0;

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
 WHERE v.token_id = s.token_id
   AND (v.total_deposited_usd = 0 AND v.total_claimed_usd = 0);

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
 WHERE v.token_id = s.token_id
   AND v.total_deposited_usd = 0
   AND v.total_claimed_usd = 0
   AND v.total_expired_usd = 0
   AND v.buyback_quote_spent_usd = 0;

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
   AND v.vault_id = s.vault_id
   AND v.distributed_quote_usd = 0;

COMMIT;
