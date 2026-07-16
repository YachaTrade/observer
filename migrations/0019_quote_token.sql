-- Quote token metadata for multi-quote support
-- Stores name, symbol, decimals, pyth feed, and image for each quote asset (WMON, USDC, etc.)
-- Referenced by market.quote_id via LEFT JOIN in api-server queries
-- Observer reads pyth_feed_id + decimals at startup to build its quote price config
-- (replaces the old QUOTE_CONFIGS env var)

CREATE TABLE IF NOT EXISTS quote_token (
    quote_id VARCHAR(42) PRIMARY KEY,
    name VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    decimals INT NOT NULL DEFAULT 18,
    pyth_feed_id VARCHAR NOT NULL,
    image_uri VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO quote_token (quote_id, name, symbol, decimals, pyth_feed_id, image_uri)
VALUES (
    '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A',
    'MONAD',
    'MON',
    18,
    '0x31491744e2dbf6df7fcf4ac0820d18a609b49076d45066d3568424e62f686cd1',
    'https://storage.nadapp.net/quote/mon.webp'
) ON CONFLICT (quote_id) DO NOTHING;

INSERT INTO quote_token (quote_id, name, symbol, decimals, pyth_feed_id, image_uri)
VALUES (
    '0xBe3fa50514D9617ce645a02B34F595541AF02b6b',
    'LeverUpMon',
    'LVMON',
    18,
    '0x31491744e2dbf6df7fcf4ac0820d18a609b49076d45066d3568424e62f686cd1',
    'https://storage.nadapp.net/quote/lvmon.webp'
) ON CONFLICT (quote_id) DO NOTHING;
