-- ======================================================================
-- V2 Upgrade: ALTER statements only
-- ----------------------------------------------------------------------
-- Run this AGAINST a production DB that already has migrations 0001-0013
-- applied. Brings existing tables forward to match the v2 observer
-- schema (column renames, new columns, CHECK constraint updates, PK
-- changes).
--
-- Every ALTER/RENAME is wrapped in IF EXISTS / DO blocks so the whole
-- script is idempotent and can be re-run safely. Also bundles the
-- `price` table unification and the position `native -> quote` rename
-- (previously separate files 0018/0019).
--
-- Pair this with `v2_upgrade_new_tables.sql` for the v2-specific tables.
-- ======================================================================

BEGIN;

-- ----------------------------------------------------------------------
-- 1. token: add version column + CHECK constraint
-- ----------------------------------------------------------------------
-- Production may or may not have the `version` column depending on which
-- rev of 0002 was applied. Add it defensively, normalize existing 'v1'
-- rows to 'V1', then install the CHECK constraint.
ALTER TABLE token
    ADD COLUMN IF NOT EXISTS version VARCHAR NOT NULL DEFAULT 'V1';
UPDATE token SET version = 'V1' WHERE version = 'v1';
ALTER TABLE token ALTER COLUMN version SET DEFAULT 'V1';
ALTER TABLE token DROP CONSTRAINT IF EXISTS token_version_check;
ALTER TABLE token ADD CONSTRAINT token_version_check CHECK (version IN ('V1', 'V2'));
CREATE INDEX IF NOT EXISTS idx_token_version ON token (version);


-- ----------------------------------------------------------------------
-- 2. market: rename reserve_native -> reserve_quote, add quote_id,
--    update market_type CHECK constraint
-- ----------------------------------------------------------------------
ALTER TABLE market DROP CONSTRAINT IF EXISTS market_market_type_check;
ALTER TABLE market ADD CONSTRAINT market_market_type_check
    CHECK (market_type IN ('CURVE', 'DEX', 'V2_CURVE', 'V2_DEX'));

-- Add quote_id column. The DEFAULT must match the WMON address for the
-- target environment. The value below is TESTNET WMON -- override for
-- mainnet deployments before running.
ALTER TABLE market
    ADD COLUMN IF NOT EXISTS quote_id VARCHAR(42) NOT NULL
        DEFAULT '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'market' AND column_name = 'reserve_native'
    ) THEN
        ALTER TABLE market RENAME COLUMN reserve_native TO reserve_quote;
    END IF;
END $$;

-- Rename ath_price_native -> ath_price_quote (v2 multi-quote terminology).
-- The old "native" name was a v1 relic when every market was WMON-denominated.
-- In v2 multi-quote, "quote" is the accurate generic term for the reserve
-- currency (WMON, USDC, etc.).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'market' AND column_name = 'ath_price_native'
    ) THEN
        ALTER TABLE market RENAME COLUMN ath_price_native TO ath_price_quote;
    END IF;
END $$;


-- ----------------------------------------------------------------------
-- 3. swap: update market_type CHECK, rename native_amount/reserve_native
-- ----------------------------------------------------------------------
ALTER TABLE swap DROP CONSTRAINT IF EXISTS swap_market_type_check;
ALTER TABLE swap ADD CONSTRAINT swap_market_type_check
    CHECK (market_type IN ('CURVE', 'DEX', 'V2_CURVE', 'V2_DEX'));

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'swap' AND column_name = 'native_amount'
    ) THEN
        ALTER TABLE swap RENAME COLUMN native_amount TO quote_amount;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'swap' AND column_name = 'reserve_native'
    ) THEN
        ALTER TABLE swap RENAME COLUMN reserve_native TO reserve_quote;
    END IF;
END $$;

-- Rebuild index that referenced the old column name
DROP INDEX IF EXISTS idx_swap_token_buy_volume_created;
CREATE INDEX IF NOT EXISTS idx_swap_token_buy_volume_created
    ON swap (token_id, is_buy, quote_amount DESC, created_at DESC);

-- Rebuild the swap trigger function with the new column name.
CREATE OR REPLACE FUNCTION update_market_volume()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE market
    SET volume = volume + NEW.quote_amount
    WHERE token_id = NEW.token_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------
-- 4. mint / burn: rename native_amount/reserve_native
-- ----------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'mint' AND column_name = 'native_amount'
    ) THEN
        ALTER TABLE mint RENAME COLUMN native_amount TO quote_amount;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'mint' AND column_name = 'reserve_native'
    ) THEN
        ALTER TABLE mint RENAME COLUMN reserve_native TO reserve_quote;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'burn' AND column_name = 'native_amount'
    ) THEN
        ALTER TABLE burn RENAME COLUMN native_amount TO quote_amount;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'burn' AND column_name = 'reserve_native'
    ) THEN
        ALTER TABLE burn RENAME COLUMN reserve_native TO reserve_quote;
    END IF;
END $$;


-- ----------------------------------------------------------------------
-- 5. lp_allocate_history / lp_collect_history: rename native_amount
-- ----------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'lp_allocate_history' AND column_name = 'native_amount'
    ) THEN
        ALTER TABLE lp_allocate_history RENAME COLUMN native_amount TO quote_amount;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'lp_collect_history' AND column_name = 'native_amount'
    ) THEN
        ALTER TABLE lp_collect_history RENAME COLUMN native_amount TO quote_amount;
    END IF;
END $$;


-- ----------------------------------------------------------------------
-- 6. fee_distribute_history: add tx_index, swap PK to (tx, tx_index, log)
-- ----------------------------------------------------------------------
ALTER TABLE fee_distribute_history
    ADD COLUMN IF NOT EXISTS tx_index INT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'fee_distribute_history'::regclass
          AND i.indisprimary
          AND a.attname = 'tx_index'
    ) THEN
        ALTER TABLE fee_distribute_history DROP CONSTRAINT IF EXISTS fee_distribute_history_pkey;
        ALTER TABLE fee_distribute_history
            ADD PRIMARY KEY (transaction_hash, tx_index, log_index);
    END IF;
END $$;


-- ----------------------------------------------------------------------
-- 7. fee_history: add tx_index, rename native_amount, swap PK
-- ----------------------------------------------------------------------
ALTER TABLE fee_history
    ADD COLUMN IF NOT EXISTS tx_index INT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'fee_history'::regclass
          AND i.indisprimary
          AND a.attname = 'tx_index'
    ) THEN
        ALTER TABLE fee_history DROP CONSTRAINT IF EXISTS fee_history_pkey;
        ALTER TABLE fee_history
            ADD PRIMARY KEY (transaction_hash, tx_index, log_index);
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'fee_history' AND column_name = 'native_amount'
    ) THEN
        ALTER TABLE fee_history RENAME COLUMN native_amount TO quote_amount;
    END IF;
END $$;


-- ----------------------------------------------------------------------
-- 8. fee (aggregate): rename native_amount + rebuild trigger function
-- ----------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'fee' AND column_name = 'native_amount'
    ) THEN
        ALTER TABLE fee RENAME COLUMN native_amount TO quote_amount;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION update_fee_on_history()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO fee (
        account_id, token_id,
        quote_amount, usd_amount,
        created_at, updated_at
    )
    VALUES (
        NEW.account_id, NEW.token_id,
        NEW.quote_amount, NEW.usd_amount,
        NEW.created_at, NEW.created_at
    )
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        quote_amount = fee.quote_amount + EXCLUDED.quote_amount,
        usd_amount = fee.usd_amount + EXCLUDED.usd_amount,
        updated_at = EXCLUDED.updated_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------
-- 9. price: add quote_id + swap PK (was migrations/0018_unify_price_table.sql)
-- ----------------------------------------------------------------------
-- Extends the `price` table to support multiple quote tokens. The DEFAULT
-- backfills legacy rows with the mainnet WMON address (metadata-only on
-- Postgres >= 11). The PK swap is required so WMON and non-WMON quotes
-- can coexist at the same block number.
ALTER TABLE price
    ADD COLUMN IF NOT EXISTS quote_id VARCHAR(42) NOT NULL
        DEFAULT '0x3bd359c1119da7da1d913d1c4d2b7c461115433a';

-- Only swap the PK if it's still single-column (idempotent on re-run).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'price'
          AND c.conname = 'price_pkey'
          AND array_length(c.conkey, 1) = 1
    ) THEN
        ALTER TABLE price DROP CONSTRAINT price_pkey;
        ALTER TABLE price ADD CONSTRAINT price_pkey PRIMARY KEY (quote_id, block_number);
    END IF;
END $$;

DROP INDEX IF EXISTS idx_price_block_number;
CREATE INDEX IF NOT EXISTS idx_price_quote_block
    ON price (quote_id, block_number DESC);
-- idx_price_created_at is unchanged.


-- ----------------------------------------------------------------------
-- 10. position_history / position: native_in/out -> quote_in/out
--     (was migrations/0019_rename_position_native_to_quote.sql)
-- ----------------------------------------------------------------------
-- No semantic change: these columns continue to hold WMON flows only
-- until the tracking logic itself is generalized. The trigger function
-- is recreated via CREATE OR REPLACE so the existing trigger keeps
-- pointing at it.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'position_history' AND column_name = 'native_in'
    ) THEN
        ALTER TABLE position_history RENAME COLUMN native_in TO quote_in;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'position_history' AND column_name = 'native_out'
    ) THEN
        ALTER TABLE position_history RENAME COLUMN native_out TO quote_out;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'position' AND column_name = 'native_in'
    ) THEN
        ALTER TABLE position RENAME COLUMN native_in TO quote_in;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'position' AND column_name = 'native_out'
    ) THEN
        ALTER TABLE position RENAME COLUMN native_out TO quote_out;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION update_position_on_history()
RETURNS TRIGGER AS $$
DECLARE
    sender_position RECORD;
    avg_cost_quote NUMERIC;
    avg_cost_usd NUMERIC;
    transfer_cost_quote NUMERIC;
    transfer_cost_usd NUMERIC;
    current_balance NUMERIC;
BEGIN
    -- transfer_out: compute sender cost basis and store it as quote_in
    IF NEW.transfer_type = 'transfer_out' THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.account_id AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;

            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd := sender_position.usd_out / sender_position.token_in;

                transfer_cost_quote := avg_cost_quote * NEW.token_out;
                transfer_cost_usd := avg_cost_usd * NEW.token_out;

                NEW.quote_in := transfer_cost_quote;
                NEW.usd_in := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    -- transfer_in: pull sender_address cost basis into quote_out
    IF NEW.transfer_type = 'transfer_in' AND NEW.sender_address IS NOT NULL THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.sender_address AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;

            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd := sender_position.usd_out / sender_position.token_in;

                transfer_cost_quote := avg_cost_quote * NEW.token_in;
                transfer_cost_usd := avg_cost_usd * NEW.token_in;

                NEW.quote_out := transfer_cost_quote;
                NEW.usd_out := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    -- Accumulate into `position`
    INSERT INTO position (
        account_id, token_id,
        quote_in, quote_out,
        usd_in, usd_out,
        token_in, token_out,
        created_at, updated_at
    )
    VALUES (
        NEW.account_id, NEW.token_id,
        NEW.quote_in, NEW.quote_out,
        NEW.usd_in, NEW.usd_out,
        NEW.token_in, NEW.token_out,
        NEW.created_at, NEW.created_at
    )
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        quote_in = position.quote_in + EXCLUDED.quote_in,
        quote_out = position.quote_out + EXCLUDED.quote_out,
        usd_in = position.usd_in + EXCLUDED.usd_in,
        usd_out = position.usd_out + EXCLUDED.usd_out,
        token_in = position.token_in + EXCLUDED.token_in,
        token_out = position.token_out + EXCLUDED.token_out,
        updated_at = EXCLUDED.updated_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------
-- 11. quote_token: add pyth_feed_id column + backfill MON feed
-- ----------------------------------------------------------------------
-- Observer reads pyth_feed_id + decimals from this table at startup to
-- build its quote price config (replaces QUOTE_CONFIGS env var).
ALTER TABLE quote_token
    ADD COLUMN IF NOT EXISTS pyth_feed_id VARCHAR NOT NULL DEFAULT '';

-- Backfill the MON feed ID for the existing seed row.
UPDATE quote_token
SET pyth_feed_id = '0x31491744e2dbf6df7fcf4ac0820d18a609b49076d45066d3568424e62f686cd1'
WHERE quote_id = '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A'
  AND pyth_feed_id = '';

-- Seed LVMON quote token (idempotent — safe to re-run).
INSERT INTO quote_token (quote_id, name, symbol, decimals, pyth_feed_id, image_uri)
VALUES (
    '0xBe3fa50514D9617ce645a02B34F595541AF02b6b',
    'LeverUpMon',
    'LVMON',
    18,
    '0x31491744e2dbf6df7fcf4ac0820d18a609b49076d45066d3568424e62f686cd1',
    'https://storage.nadapp.net/quote/lvmon.webp'
) ON CONFLICT (quote_id) DO NOTHING;

-- ----------------------------------------------------------------------
-- 11b. quote_token: is_native flag (mirrors 0028_quote_token_is_native.sql)
--      Allows multiple MON-pegged wrappers (WMON, LVMON, future variants)
--      to all act as "native" in chain-implied price cache propagation.
--
--      DEFAULT TRUE because current quote_token only holds MON-pegged rows;
--      backfill to TRUE in one shot. Future non-native quotes (USDC, USDT,
--      ...) must INSERT with explicit is_native = FALSE.
-- ----------------------------------------------------------------------
ALTER TABLE quote_token
    ADD COLUMN IF NOT EXISTS is_native BOOLEAN NOT NULL DEFAULT TRUE;


-- ----------------------------------------------------------------------
-- 12. set_creator_history: drop derived-state columns from PK
--     old PK: (token_id, old_creator, new_creator, transaction_hash,
--              block_number, tx_index, log_index)
--     new PK: (transaction_hash, tx_index, log_index)
-- old_creator/new_creator are derived from token.creator at insert time,
-- not from the on-chain event. That broke idempotency: replaying the same
-- log under a different token state could produce extra history rows.
-- New PK matches the natural event key (same shape as fee_distribute_history
-- and the v2_*_history tables).
-- ----------------------------------------------------------------------
DO $$
BEGIN
    -- Idempotency guard: only run while old PK still includes old_creator.
    IF EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'set_creator_history'::regclass
          AND i.indisprimary
          AND a.attname = 'old_creator'
    ) THEN
        -- Dedup any rows that share the new natural key but differ on the
        -- derived columns. Keep the row with the smallest
        -- (created_at, token_id, old_creator, new_creator) tuple.
        DELETE FROM set_creator_history a
        USING set_creator_history b
        WHERE a.transaction_hash = b.transaction_hash
          AND a.tx_index          = b.tx_index
          AND a.log_index         = b.log_index
          AND (a.created_at, a.token_id, a.old_creator, a.new_creator)
            > (b.created_at, b.token_id, b.old_creator, b.new_creator);

        ALTER TABLE set_creator_history DROP CONSTRAINT IF EXISTS set_creator_history_pkey;
        ALTER TABLE set_creator_history
            ADD CONSTRAINT set_creator_history_pkey PRIMARY KEY (transaction_hash, tx_index, log_index);
    END IF;
END $$;

COMMIT;
