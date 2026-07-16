-- V2 DEX infrastructure: pool (pairs), dex_token (external tokens),
-- fee_config, raw event tables (dex_swap / dex_sync / dex_mint / dex_burn),
-- and the statement-level update_pool_volume() trigger.
--
-- Consolidated from prior incremental migrations (0022_pool_volume,
-- 0023_dex_event_tables, 0024_pool_volume_trigger, 0025_dex_event_value_tvl,
-- 0026_dex_event_per_side_usd) into one file representing the final V2 DEX
-- schema. Existing prod DBs are upgraded by the idempotent
-- migrations/v2_upgrade_new_tables.sql; this file is for fresh DBs.

-- ---------------------------------------------------------------------------
-- 1. DEX Token (external tokens discovered via PairCreated)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dex_token (
    token_id   VARCHAR(42) PRIMARY KEY,
    name       VARCHAR     NOT NULL DEFAULT '',
    symbol     VARCHAR     NOT NULL DEFAULT '',
    decimals   INT         NOT NULL DEFAULT 18,
    image_uri  VARCHAR     NOT NULL DEFAULT '',
    created_at BIGINT      NOT NULL
);

-- GIN trigram indexes for /dex/search ILIKE substring acceleration.
-- Requires the pg_trgm extension (already enabled in production per the
-- existing idx_token_*_gin declarations in 0002_token.sql).
-- Folded in from the former standalone 0029_dex_search_indexes.sql now that
-- the indexes are applied on prod/dev — keeps the canonical dex_token DDL
-- and its indexes in one file for fresh-DB loads.
CREATE INDEX IF NOT EXISTS idx_dex_token_symbol_gin
    ON dex_token USING GIN (symbol gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_dex_token_name_gin
    ON dex_token USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_dex_token_token_id_gin
    ON dex_token USING GIN (token_id gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- 2. Pool (DEX pairs — both graduated launchpad and pure DEX)
--
-- volume = lifetime trade volume in USD (accumulated by update_pool_volume()
--          trigger below from dex_swap inserts).
-- value  = current TVL snapshot in USD (updated alongside reserves by the
--          indexer when prices are known).
-- token0_price_usd / token1_price_usd = per-token USD unit price
--          (WMON-implied price x Pyth WMON/USD), set by the indexer's RawSync
--          inference. NULL when the token has no WMON-reachable price (orphan)
--          or before the first priced sync.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pool (
    pool_id         VARCHAR(42) PRIMARY KEY,
    token0          VARCHAR(42) NOT NULL,
    token1          VARCHAR(42) NOT NULL,
    reserve0        NUMERIC     NOT NULL DEFAULT 0, -- token raw (wei) of token0
    reserve1        NUMERIC     NOT NULL DEFAULT 0, -- token raw (wei) of token1
    price           NUMERIC     NOT NULL DEFAULT 0, -- quote per token (native_reserve/token_reserve; 0 for pure-DEX RawSync arm)
    volume          NUMERIC     NOT NULL DEFAULT 0, -- USD (human); lifetime SUM(dex_swap.value)
    value           NUMERIC     NOT NULL DEFAULT 0, -- USD (human); current TVL snapshot
    token0_price_usd NUMERIC    NULL, -- USD per token (token0)
    token1_price_usd NUMERIC    NULL, -- USD per token (token1)
    latest_trade_at BIGINT      NOT NULL DEFAULT 0,
    created_at      BIGINT      NOT NULL,
    block_number    BIGINT      NOT NULL,
    tx_hash         VARCHAR     NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_token0 ON pool (token0);
CREATE INDEX IF NOT EXISTS idx_pool_token1 ON pool (token1);

-- Idempotent ALTERs cover DBs created before volume/value joined the
-- canonical CREATE TABLE definition. Safe no-op on fresh DBs.
ALTER TABLE pool
    ADD COLUMN IF NOT EXISTS volume NUMERIC NOT NULL DEFAULT 0, -- USD (human); lifetime SUM(dex_swap.value)
    ADD COLUMN IF NOT EXISTS value  NUMERIC NOT NULL DEFAULT 0, -- USD (human); current TVL snapshot
    -- Per-token USD unit price (observer feat/v2-dex-pool-price-usd). Nullable:
    -- NULL = orphan token (no WMON-implied price) or not yet synced.
    ADD COLUMN IF NOT EXISTS token0_price_usd NUMERIC, -- USD per token (token0)
    ADD COLUMN IF NOT EXISTS token1_price_usd NUMERIC; -- USD per token (token1)

-- ---------------------------------------------------------------------------
-- 3. Fee Config (per-pair fee rates from FeeCollector.Setup)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fee_config (
    pair_id                 VARCHAR(42) PRIMARY KEY,
    token_id                VARCHAR(42) NOT NULL,
    creator_fee_rate        SMALLINT    NOT NULL,
    curve_protocol_fee_rate SMALLINT    NOT NULL,
    dex_protocol_fee_rate   SMALLINT    NOT NULL,
    created_at              BIGINT      NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fee_config_token ON fee_config (token_id);

-- ---------------------------------------------------------------------------
-- 4. Raw V2 DEX event tables (dex_swap / dex_sync / dex_mint / dex_burn)
--
-- value      = total USD value of the event (token0_usd + token1_usd).
-- token0_usd = USD value of the token0 side at this event.
-- token1_usd = USD value of the token1 side at this event.
--
-- Per-side preservation lets partial-orphan cases (one side priced, the
-- other unknown) keep the known side's USD rather than collapsing the
-- whole row to value=0.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dex_swap (
    pool_id          VARCHAR(42) NOT NULL,
    sender           VARCHAR(42) NOT NULL,
    amount0_in       NUMERIC     NOT NULL, -- token raw (wei) of token0 in
    amount1_in       NUMERIC     NOT NULL, -- token raw (wei) of token1 in
    amount0_out      NUMERIC     NOT NULL, -- token raw (wei) of token0 out
    amount1_out      NUMERIC     NOT NULL, -- token raw (wei) of token1 out
    value            NUMERIC     NOT NULL DEFAULT 0, -- USD (human); priced-side flow x token USD price (0 = orphan/Pyth miss)
    created_at       BIGINT      NOT NULL, -- unix seconds (block timestamp)
    block_number     BIGINT      NOT NULL,
    transaction_hash VARCHAR     NOT NULL,
    log_index        INTEGER     NOT NULL,
    tx_index         INTEGER     NOT NULL,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_dex_swap_block_desc
    ON dex_swap (block_number DESC, tx_index DESC, log_index DESC);
CREATE INDEX IF NOT EXISTS idx_dex_swap_pool_block
    ON dex_swap (pool_id, block_number DESC);

CREATE TABLE IF NOT EXISTS dex_sync (
    pool_id          VARCHAR(42) NOT NULL,
    reserve0         NUMERIC     NOT NULL, -- token raw (wei) of token0
    reserve1         NUMERIC     NOT NULL, -- token raw (wei) of token1
    value            NUMERIC     NOT NULL DEFAULT 0, -- USD (human); token0_usd + token1_usd (TVL at this sync)
    token0_usd       NUMERIC     NOT NULL DEFAULT 0, -- USD (human); USD value of the token0 reserve side
    token1_usd       NUMERIC     NOT NULL DEFAULT 0, -- USD (human); USD value of the token1 reserve side
    created_at       BIGINT      NOT NULL, -- unix seconds (block timestamp)
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
    amount0          NUMERIC     NOT NULL, -- token raw (wei) of token0 added
    amount1          NUMERIC     NOT NULL, -- token raw (wei) of token1 added
    value            NUMERIC     NOT NULL DEFAULT 0, -- USD (human); token0_usd + token1_usd
    token0_usd       NUMERIC     NOT NULL DEFAULT 0, -- USD (human); USD value of the token0 side
    token1_usd       NUMERIC     NOT NULL DEFAULT 0, -- USD (human); USD value of the token1 side
    created_at       BIGINT      NOT NULL, -- unix seconds (block timestamp)
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
    amount0          NUMERIC     NOT NULL, -- token raw (wei) of token0 removed
    amount1          NUMERIC     NOT NULL, -- token raw (wei) of token1 removed
    value            NUMERIC     NOT NULL DEFAULT 0, -- USD (human); token0_usd + token1_usd
    token0_usd       NUMERIC     NOT NULL DEFAULT 0, -- USD (human); USD value of the token0 side
    token1_usd       NUMERIC     NOT NULL DEFAULT 0, -- USD (human); USD value of the token1 side
    created_at       BIGINT      NOT NULL, -- unix seconds (block timestamp)
    block_number     BIGINT      NOT NULL,
    transaction_hash VARCHAR     NOT NULL,
    log_index        INTEGER     NOT NULL,
    tx_index         INTEGER     NOT NULL,
    PRIMARY KEY (pool_id, transaction_hash, tx_index, log_index)
);
CREATE INDEX IF NOT EXISTS idx_dex_burn_pool_block
    ON dex_burn (pool_id, block_number DESC);

-- Idempotent ALTERs cover DBs that ran an older revision of this file
-- where value / token0_usd / token1_usd were added in a follow-up.
ALTER TABLE dex_sync
    ADD COLUMN IF NOT EXISTS value      NUMERIC NOT NULL DEFAULT 0, -- USD (human); token0_usd + token1_usd (TVL)
    ADD COLUMN IF NOT EXISTS token0_usd NUMERIC NOT NULL DEFAULT 0, -- USD (human); token0 side
    ADD COLUMN IF NOT EXISTS token1_usd NUMERIC NOT NULL DEFAULT 0; -- USD (human); token1 side
ALTER TABLE dex_mint
    ADD COLUMN IF NOT EXISTS value      NUMERIC NOT NULL DEFAULT 0, -- USD (human); token0_usd + token1_usd
    ADD COLUMN IF NOT EXISTS token0_usd NUMERIC NOT NULL DEFAULT 0, -- USD (human); token0 side
    ADD COLUMN IF NOT EXISTS token1_usd NUMERIC NOT NULL DEFAULT 0; -- USD (human); token1 side
ALTER TABLE dex_burn
    ADD COLUMN IF NOT EXISTS value      NUMERIC NOT NULL DEFAULT 0, -- USD (human); token0_usd + token1_usd
    ADD COLUMN IF NOT EXISTS token0_usd NUMERIC NOT NULL DEFAULT 0, -- USD (human); token0 side
    ADD COLUMN IF NOT EXISTS token1_usd NUMERIC NOT NULL DEFAULT 0; -- USD (human); token1 side

-- ---------------------------------------------------------------------------
-- 5. pool.volume accumulator trigger
--
-- Statement-level: a single batch INSERT fires the trigger once with the
-- full new-rows view, so we GROUP BY pool_id and emit one UPDATE per pool
-- instead of one plpgsql call per row.
--
-- Idempotency: when a dex_swap INSERT uses ON CONFLICT DO NOTHING and the
-- row is skipped, the conflicted row is NOT included in the AFTER INSERT
-- transition table (PostgreSQL semantics). The trigger therefore only sums
-- the values of rows that were actually inserted — safe under replay.
-- ---------------------------------------------------------------------------
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
