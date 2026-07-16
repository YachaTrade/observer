-- Price table: multi-quote aware.
-- Supports multiple quote tokens (WMON, USDC, etc.) via a composite
-- primary key (quote_id, block_number). The default quote_id is the
-- mainnet WMON address, matching the legacy V1 single-quote behavior.
CREATE TABLE IF NOT EXISTS price (
    quote_id VARCHAR(42) NOT NULL DEFAULT '0x3bd359c1119da7da1d913d1c4d2b7c461115433a',
    block_number BIGINT NOT NULL,
    price NUMERIC NOT NULL, -- USD per quote: Pyth oracle USD price of quote_id at this block

    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (quote_id, block_number)
);

-- Per-quote range scan index (descending block for latest-first queries)
CREATE INDEX IF NOT EXISTS idx_price_quote_block ON price (quote_id, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_price_created_at ON price (created_at DESC);
