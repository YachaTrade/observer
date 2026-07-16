-- ======================================================================
-- V2 Upgrade: New tables only
-- ----------------------------------------------------------------------
-- Run this AFTER `v2_upgrade_alter.sql`. All CREATE TABLE statements
-- here are for tables that did NOT exist in the 0001-0013 production
-- schema. All use IF NOT EXISTS so re-running is safe.
--
-- Tables created:
--   v2_sniping_history          -- BondingCurve.SnipingFeeCollected
--   v2_lp_allocate_history      -- LPManager.LPAllocated
--   v2_fee_collect_history      -- FeeCollector.Collect
--   v2_fee_settle_history       -- FeeCollector.Settle
--   v2_creator_fee_distribution -- CreatorFeeProcessor.Distribute/CallbackFail
--   v2_vault_burns              -- BurnVault.Burn / GiftVault.Burn
--   v2_vault_lp_injections      -- LPVault.Inject
--   v2_creator_fee_claims       -- CreatorFeeVault.Deposit/Claim
--   v2_gifts                    -- GiftVault.Setup/Deposit/Claim/Expire
--   dex_token                   -- external tokens via PairCreated
--   pool                        -- DEX pairs (graduated + pure DEX)
--   fee_config                  -- per-pair fee rates from FeeCollector.Setup
--   v2_fee_to_claim_history     -- FeeTo.Claimed
--
-- The testnet WMON default for quote_id fields below MUST be updated
-- to mainnet WMON (0x3bd359c1119da7da1d913d1c4d2b7c461115433a) before
-- deploying to mainnet.
-- ======================================================================

BEGIN;

-- ----------------------------------------------------------------------
-- 1. V2 Sniping Penalties
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_sniping_history (
    token_id VARCHAR(42) NOT NULL,
    buyer VARCHAR(42) NOT NULL,
    sniping_fee NUMERIC NOT NULL,
    penalty_bps NUMERIC NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_sniping_history_token
    ON v2_sniping_history (token_id);


-- ----------------------------------------------------------------------
-- 2. V2 LP Allocate History
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_lp_allocate_history (
    token_id VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    caller VARCHAR(42) NOT NULL,
    dex_type SMALLINT NOT NULL,
    token_in NUMERIC NOT NULL,
    quote_in NUMERIC NOT NULL,
    liquidity NUMERIC NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_lp_allocate_token
    ON v2_lp_allocate_history (token_id);


-- ----------------------------------------------------------------------
-- 3. V2 Fee Collect History (FeeCollector.Collect)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_fee_collect_history (
    token VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    quote_id VARCHAR(42) NOT NULL
        DEFAULT '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A',
    amount NUMERIC NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_fee_collect_token
    ON v2_fee_collect_history (token);
CREATE INDEX IF NOT EXISTS idx_v2_fee_collect_pair
    ON v2_fee_collect_history (pair);


-- ----------------------------------------------------------------------
-- 4. V2 Fee Settle History (FeeCollector.Settle)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_fee_settle_history (
    token VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    quote_id VARCHAR(42) NOT NULL
        DEFAULT '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A',
    total_fee NUMERIC NOT NULL,
    creator_fee NUMERIC NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_fee_settle_token
    ON v2_fee_settle_history (token);
CREATE INDEX IF NOT EXISTS idx_v2_fee_settle_pair
    ON v2_fee_settle_history (pair);


-- ----------------------------------------------------------------------
-- 5. V2 Creator Fee Distribution
--    (CreatorFeeProcessor.Distribute / CallbackFail)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_creator_fee_distribution (
    event_type VARCHAR NOT NULL,
    token VARCHAR(42),
    quote_id VARCHAR(42) NOT NULL
        DEFAULT '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A',
    vault VARCHAR(42),
    amount NUMERIC NOT NULL,
    reason BYTEA,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_creator_fee_dist_token
    ON v2_creator_fee_distribution (token);



-- All vault-related schema (event logs, registry, metadata, aggregates,
-- triggers, backfill) lives in migrations/vault.sql.


-- ----------------------------------------------------------------------
-- 10. DEX Token (external tokens discovered via PairCreated)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dex_token (
    token_id VARCHAR(42) PRIMARY KEY,
    name VARCHAR NOT NULL DEFAULT '',
    symbol VARCHAR NOT NULL DEFAULT '',
    decimals INT NOT NULL DEFAULT 18,
    image_uri VARCHAR NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL
);


-- ----------------------------------------------------------------------
-- 11. Pool (DEX pairs - both graduated launchpad and pure DEX)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pool (
    pool_id VARCHAR(42) PRIMARY KEY,
    token0 VARCHAR(42) NOT NULL,
    token1 VARCHAR(42) NOT NULL,
    reserve0 NUMERIC NOT NULL DEFAULT 0,
    reserve1 NUMERIC NOT NULL DEFAULT 0,
    price NUMERIC NOT NULL DEFAULT 0,
    volume NUMERIC NOT NULL DEFAULT 0,
    value NUMERIC NOT NULL DEFAULT 0,
    token0_price_usd NUMERIC NULL,
    token1_price_usd NUMERIC NULL,
    latest_trade_at BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_token0 ON pool (token0);
CREATE INDEX IF NOT EXISTS idx_pool_token1 ON pool (token1);

-- Idempotent ALTER for existing DBs that may have the pool table without
-- the volume column (schema drift: prod has it, the historical CREATE
-- TABLE definitions above did not). Safe to re-run; no-op if the column
-- already exists.
ALTER TABLE pool ADD COLUMN IF NOT EXISTS volume NUMERIC NOT NULL DEFAULT 0;


-- ----------------------------------------------------------------------
-- 12. Fee Config (per-pair fee rates from FeeCollector.Setup)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fee_config (
    pair_id VARCHAR(42) PRIMARY KEY,
    token_id VARCHAR(42) NOT NULL,
    creator_fee_rate SMALLINT NOT NULL,
    curve_protocol_fee_rate SMALLINT NOT NULL,
    dex_protocol_fee_rate SMALLINT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fee_config_token
    ON fee_config (token_id);


-- ----------------------------------------------------------------------
-- 13. V2 FeeTo Claim History (FeeTo.Claimed)
-- Each row = one successful claim() call on FeeTo. quoteIn fixed at ~1 MON
-- by txbot; quoteOut = excess routed to feeReceiver.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_fee_to_claim_history (
    token VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    quote_id VARCHAR(42) NOT NULL
        DEFAULT '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A',
    quote_in NUMERIC NOT NULL,
    quote_out NUMERIC NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_fee_to_claim_token
    ON v2_fee_to_claim_history (token);
CREATE INDEX IF NOT EXISTS idx_v2_fee_to_claim_pair_created
    ON v2_fee_to_claim_history (pair, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_v2_fee_to_claim_created
    ON v2_fee_to_claim_history (created_at DESC);

-- ----------------------------------------------------------------------
-- 14. dex_swap / dex_sync / dex_mint / dex_burn (drift sync)
-- ----------------------------------------------------------------------
-- These four tables exist on prod (verified via \d on mainnet) but were
-- never written into the migration definitions. Mirrors 0023_dex_event_tables.sql
-- so the upgrade path is in sync with the numbered base.

CREATE TABLE IF NOT EXISTS dex_swap (
    pool_id          VARCHAR(42)  NOT NULL,
    sender           VARCHAR(42)  NOT NULL,
    amount0_in       NUMERIC      NOT NULL,
    amount1_in       NUMERIC      NOT NULL,
    amount0_out      NUMERIC      NOT NULL,
    amount1_out      NUMERIC      NOT NULL,
    value            NUMERIC      NOT NULL DEFAULT 0,
    created_at       BIGINT       NOT NULL,
    block_number     BIGINT       NOT NULL,
    transaction_hash VARCHAR      NOT NULL,
    log_index        INTEGER      NOT NULL,
    tx_index         INTEGER      NOT NULL,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_dex_swap_block_desc
    ON dex_swap (block_number DESC, tx_index DESC, log_index DESC);
CREATE INDEX IF NOT EXISTS idx_dex_swap_pool_block
    ON dex_swap (pool_id, block_number DESC);

CREATE TABLE IF NOT EXISTS dex_sync (
    pool_id          VARCHAR(42) NOT NULL,
    reserve0         NUMERIC     NOT NULL,
    reserve1         NUMERIC     NOT NULL,
    value            NUMERIC     NOT NULL DEFAULT 0,
    token0_usd       NUMERIC     NOT NULL DEFAULT 0,
    token1_usd       NUMERIC     NOT NULL DEFAULT 0,
    created_at       BIGINT      NOT NULL,
    block_number     BIGINT      NOT NULL,
    transaction_hash VARCHAR     NOT NULL,
    log_index        INTEGER     NOT NULL,
    tx_index         INTEGER     NOT NULL,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_dex_sync_pool_block
    ON dex_sync (pool_id, block_number DESC);

CREATE TABLE IF NOT EXISTS dex_mint (
    pool_id          VARCHAR(42) NOT NULL,
    sender           VARCHAR(42) NOT NULL,
    amount0          NUMERIC     NOT NULL,
    amount1          NUMERIC     NOT NULL,
    value            NUMERIC     NOT NULL DEFAULT 0,
    token0_usd       NUMERIC     NOT NULL DEFAULT 0,
    token1_usd       NUMERIC     NOT NULL DEFAULT 0,
    created_at       BIGINT      NOT NULL,
    block_number     BIGINT      NOT NULL,
    transaction_hash VARCHAR     NOT NULL,
    log_index        INTEGER     NOT NULL,
    tx_index         INTEGER     NOT NULL,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_dex_mint_pool_block
    ON dex_mint (pool_id, block_number DESC);

CREATE TABLE IF NOT EXISTS dex_burn (
    pool_id          VARCHAR(42) NOT NULL,
    sender           VARCHAR(42) NOT NULL,
    to_address       VARCHAR(42) NOT NULL,
    amount0          NUMERIC     NOT NULL,
    amount1          NUMERIC     NOT NULL,
    value            NUMERIC     NOT NULL DEFAULT 0,
    token0_usd       NUMERIC     NOT NULL DEFAULT 0,
    token1_usd       NUMERIC     NOT NULL DEFAULT 0,
    created_at       BIGINT      NOT NULL,
    block_number     BIGINT      NOT NULL,
    transaction_hash VARCHAR     NOT NULL,
    log_index        INTEGER     NOT NULL,
    tx_index         INTEGER     NOT NULL,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_dex_burn_pool_block
    ON dex_burn (pool_id, block_number DESC);

-- ----------------------------------------------------------------------
-- 15. pool.volume statement-level trigger (mirrors 0024_pool_volume_trigger.sql)
-- ----------------------------------------------------------------------
-- After observer's BATCH_INSERT_DEX_SWAPS_SQL simplification, the
-- application no longer runs UPDATE_POOL_VOLUME_SQL. Existing prod DBs
-- that ran an earlier v2_upgrade pass need this trigger installed too,
-- otherwise pool.volume stops accumulating after deployment.

CREATE OR REPLACE FUNCTION update_pool_volume()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE pool p
       SET volume = p.volume + d.swap_volume
      FROM (
          SELECT pool_id, SUM(value) AS swap_volume
            FROM new_dex_swaps
           GROUP BY pool_id
      ) d
     WHERE p.pool_id = d.pool_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_pool_volume ON dex_swap;
CREATE TRIGGER trg_update_pool_volume
    AFTER INSERT ON dex_swap
    REFERENCING NEW TABLE AS new_dex_swaps
    FOR EACH STATEMENT
    EXECUTE FUNCTION update_pool_volume();

-- ----------------------------------------------------------------------
-- 16. Idempotent value columns (mirrors 0025_dex_event_value_tvl.sql)
-- ----------------------------------------------------------------------
-- dex_sync.value = pool TVL at this Sync (snapshot history)
-- dex_mint.value = USD added this Mint
-- dex_burn.value = USD withdrawn this Burn
-- pool.value     = current TVL (snapshot, updated by app alongside reserves)

ALTER TABLE dex_sync ADD COLUMN IF NOT EXISTS value NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE dex_mint ADD COLUMN IF NOT EXISTS value NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE dex_burn ADD COLUMN IF NOT EXISTS value NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE pool     ADD COLUMN IF NOT EXISTS value NUMERIC NOT NULL DEFAULT 0;

-- per-token USD unit price (observer feat/v2-dex-pool-price-usd). Nullable:
-- NULL = orphan token (no WMON-implied price) or not yet synced.
ALTER TABLE pool     ADD COLUMN IF NOT EXISTS token0_price_usd NUMERIC;
ALTER TABLE pool     ADD COLUMN IF NOT EXISTS token1_price_usd NUMERIC;

-- per-side USD (mirrors 0026_dex_event_per_side_usd.sql)
ALTER TABLE dex_sync ADD COLUMN IF NOT EXISTS token0_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE dex_sync ADD COLUMN IF NOT EXISTS token1_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE dex_mint ADD COLUMN IF NOT EXISTS token0_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE dex_mint ADD COLUMN IF NOT EXISTS token1_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE dex_burn ADD COLUMN IF NOT EXISTS token0_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE dex_burn ADD COLUMN IF NOT EXISTS token1_usd NUMERIC NOT NULL DEFAULT 0;


-- ----------------------------------------------------------------------
-- whitelist_token (mirrors 0031_whitelist_token.sql)
-- Select Token 모달 고정순서 화이트리스트.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS whitelist_token (
    token_id   VARCHAR(42) PRIMARY KEY,
    sort_order INT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE
);

-- 고정순서: MON(1), WMON(2), USDC(3), USDT(4), LVMON(5)
-- NOTE: WMON/USDC/USDT 온체인 주소 미확정 → MON/LVMON만 seed, 나머지는 주소 확정 후 후속 커밋.
INSERT INTO whitelist_token (token_id, sort_order) VALUES
    ('0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A', 1),  -- MON (quote_token 기존 주소)
    ('0xBe3fa50514D9617ce645a02B34F595541AF02b6b', 5)   -- LVMON (quote_token 기존 주소)
ON CONFLICT (token_id) DO NOTHING;

COMMIT;
