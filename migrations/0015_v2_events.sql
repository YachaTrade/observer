-- V2-specific event history tables. Populated from on-chain V2 contract
-- events: BondingCurve, LPManager, FeeCollector, CreatorFeeProcessor,
-- BurnVault, GiftVault, LPVault, CreatorFeeVault.
--
-- All quote_id DEFAULTs are the GIWA WETH predeploy (chain-agnostic
-- OP Stack address, valid on testnet and mainnet).

-- 1. V2 Sniping Penalties (BondingCurve.SnipingFeeCollected)
CREATE TABLE IF NOT EXISTS v2_sniping_history (
    token_id VARCHAR(42) NOT NULL,
    buyer VARCHAR(42) NOT NULL,
    sniping_fee NUMERIC NOT NULL, -- quote raw (wei): BondingCurve.SnipingPenalty.snipingFee uint256
    penalty_bps NUMERIC NOT NULL, -- bps: BondingCurve.SnipingPenalty.penaltyBps uint256
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL, -- unix seconds (block timestamp)
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_sniping_history_token ON v2_sniping_history (token_id);

-- 2. V2 LP Allocate History (LPManager.LPAllocated)
CREATE TABLE IF NOT EXISTS v2_lp_allocate_history (
    token_id VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    caller VARCHAR(42) NOT NULL,
    dex_type SMALLINT NOT NULL,
    token_in NUMERIC NOT NULL, -- token raw (wei): LPManager.Allocate.tokenIn uint256
    quote_in NUMERIC NOT NULL, -- quote raw (wei): LPManager.Allocate.quoteIn uint256
    liquidity NUMERIC NOT NULL, -- token raw (wei): LP token amount minted; LPManager.Allocate.liquidity uint256
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL, -- unix seconds (block timestamp)
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_lp_allocate_token ON v2_lp_allocate_history (token_id);

-- 3. V2 Fee Collect History (FeeCollector.Collect)
CREATE TABLE IF NOT EXISTS v2_fee_collect_history (
    token VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    quote_id VARCHAR(42) NOT NULL DEFAULT '0x4200000000000000000000000000000000000006',
    amount NUMERIC NOT NULL, -- quote raw (wei): FeeCollector.Collect.amount uint256 (fees denominated in quote_id)
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL, -- unix seconds (block timestamp)
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_fee_collect_token ON v2_fee_collect_history (token);
CREATE INDEX IF NOT EXISTS idx_v2_fee_collect_pair ON v2_fee_collect_history (pair);

-- 4. V2 Fee Settle History (FeeCollector.Settle)
CREATE TABLE IF NOT EXISTS v2_fee_settle_history (
    token VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    quote_id VARCHAR(42) NOT NULL DEFAULT '0x4200000000000000000000000000000000000006',
    total_fee NUMERIC NOT NULL, -- quote raw (wei): FeeCollector.Settle.totalFee uint256 (fees denominated in quote_id)
    creator_fee NUMERIC NOT NULL, -- quote raw (wei): FeeCollector.Settle.creatorFee uint256 (fees denominated in quote_id)
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL, -- unix seconds (block timestamp)
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_fee_settle_token ON v2_fee_settle_history (token);
CREATE INDEX IF NOT EXISTS idx_v2_fee_settle_pair ON v2_fee_settle_history (pair);

-- 5. V2 Creator Fee Distribution (CreatorFeeProcessor.Distribute / CallbackFail)
CREATE TABLE IF NOT EXISTS v2_creator_fee_distribution (
    event_type VARCHAR NOT NULL,
    token VARCHAR(42),
    quote_id VARCHAR(42) NOT NULL DEFAULT '0x4200000000000000000000000000000000000006',
    vault VARCHAR(42),
    amount NUMERIC NOT NULL, -- quote raw (wei): CreatorFeeProcessor.Distribute.amount uint256 (denominated in quote_id; usd_enrich.rs divides by quote decimals)
    usd_value NUMERIC NOT NULL DEFAULT 0, -- USD (human): amount/10^quote_decimals * quote USD price (usd_enrich.rs)
    reason BYTEA,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL, -- unix seconds (block timestamp)
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_creator_fee_dist_token ON v2_creator_fee_distribution (token);


-- All vault-related schema (event logs, registry, metadata, aggregates,
-- triggers, backfill) lives in migrations/vault.sql.

-- 6. V2 FeeTo Claim History (FeeTo.Claimed)
-- Each row = one successful claim() call on FeeTo. quoteIn fixed at ~1 MON
-- by txbot; quoteOut = excess routed to feeReceiver.
CREATE TABLE IF NOT EXISTS v2_fee_to_claim_history (
    token VARCHAR(42) NOT NULL,
    pair VARCHAR(42) NOT NULL,
    quote_id VARCHAR(42) NOT NULL DEFAULT '0x4200000000000000000000000000000000000006',
    quote_in NUMERIC NOT NULL, -- quote raw (wei): FeeTo.Claimed.quoteIn uint256
    quote_out NUMERIC NOT NULL, -- quote raw (wei): FeeTo.Claimed.quoteOut uint256 (excess routed to feeReceiver)
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL, -- unix seconds (block timestamp)
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_v2_fee_to_claim_token ON v2_fee_to_claim_history (token);
CREATE INDEX IF NOT EXISTS idx_v2_fee_to_claim_pair_created ON v2_fee_to_claim_history (pair, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_v2_fee_to_claim_created ON v2_fee_to_claim_history (created_at DESC);
