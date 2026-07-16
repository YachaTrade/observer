-- Quote token metadata for multi-quote support
-- Stores name, symbol, decimals, pyth feed, and image for each quote asset (WETH, USDC, etc.)
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
    '0x4200000000000000000000000000000000000006',
    'Wrapped Ether',
    'WETH',
    18,
    '0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace',
    'https://storage.nadapp.net/quote/weth.webp'
) ON CONFLICT (quote_id) DO NOTHING;
