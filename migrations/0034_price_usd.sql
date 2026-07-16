-- 0034_price_usd.sql
--
-- price_usd: per-(whitelist token, block) USD unit price sourced from the
-- DefiLlama coins API (free tier). SEPARATE from `price` (Pyth, quote_token,
-- indexing path) — price_usd feeds downstream `balance_usd` display only and is
-- NOT read by get_quote_usd_price / forward-propagation.
--
-- Block-keyed dense (mirrors `price`, 0007_price.sql): the observer price_usd
-- refresher writes one row per block for each enabled whitelist_token, applying
-- the latest DefiLlama price (fetched at most once / 60s) to every block in the
-- elapsed range, so there are no gap blocks. `created_at` = block_timestamp
-- (same meaning as in `price`). `confidence` = DefiLlama 0..1 quality score
-- (NULL permitted).
--
-- Read contract (downstream balance_usd):
--   latest   : ORDER BY block_number DESC LIMIT 1
--   as-of(b) : WHERE block_number <= b ORDER BY block_number DESC LIMIT 1
--              (carry-forward — never a future price).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_usd (
    token_id     VARCHAR(42) NOT NULL,
    block_number BIGINT      NOT NULL,
    price        NUMERIC     NOT NULL,   -- USD unit price (DefiLlama)
    confidence   NUMERIC,                -- DefiLlama confidence (0..1), NULL allowed
    created_at   BIGINT      NOT NULL,   -- block_timestamp
    PRIMARY KEY (token_id, block_number)
);

CREATE INDEX IF NOT EXISTS idx_price_usd_token_block ON price_usd (token_id, block_number DESC);
