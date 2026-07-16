-- v2_upgrade_dex_token_price.sql
--
-- Idempotent prod upgrade for the dex_token_price view added by
-- migrations/0033_dex_token_price.sql. Apply manually on prod where the
-- numbered migration cannot run (pre-existing DB state).
--
-- Depends on pool.token0_price_usd / pool.token1_price_usd, added by
-- v2_upgrade_new_tables.sql — apply that first.
--
-- Keep the body below in sync with 0033_dex_token_price.sql.
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
