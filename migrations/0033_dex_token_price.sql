-- 0033_dex_token_price.sql
--
-- dex_token_price: per-token USD unit price, taken from the deepest-TVL pool
-- the token trades in. Read-time aggregation over pool.token0_price_usd /
-- pool.token1_price_usd (set by the observer's RawSync inference; columns added
-- in 0014_dex.sql). pool is the single source, so no denormalized column and no
-- trigger/indexer write is needed — the view is always consistent with the
-- latest synced prices.
--
-- Selection: DISTINCT ON (token_id) ORDER BY value DESC picks the token's price
-- from the pool where it holds the largest USD TVL (most liquid = canonical
-- market price, manipulation-resistant). Ties are broken by latest_trade_at
-- then pool_id so output is deterministic (no flapping between equal-TVL pools).
--
-- Coverage: a token appears only when at least one of its pools has a non-NULL
-- side price. The NULL side of a half-priced pool drops out of the UNION, and
-- orphan tokens (no WMON-reachable price) have no row at all — callers read
-- "no row" as price unknown (NULL).
--
-- Consumers: api-server /dex/tokens resolves price_usd as
--   COALESCE(dex_token_price.price_usd, market.price * quote->USD)
-- i.e. this pool-view first, legacy market path as fallback.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW dex_token_price AS
SELECT DISTINCT ON (token_id)
       token_id,
       price_usd,
       pool_id,            -- source pool (debug / trace)
       value AS pool_value
FROM (
    SELECT token0 AS token_id, token0_price_usd AS price_usd,
           pool_id, value, latest_trade_at
    FROM pool
    WHERE token0_price_usd IS NOT NULL
    UNION ALL
    SELECT token1 AS token_id, token1_price_usd AS price_usd,
           pool_id, value, latest_trade_at
    FROM pool
    WHERE token1_price_usd IS NOT NULL
) t
ORDER BY token_id, value DESC NULLS LAST, latest_trade_at DESC, pool_id;
