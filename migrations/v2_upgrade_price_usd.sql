-- v2_upgrade_price_usd.sql
--
-- PROD upgrade track for price_usd (fresh-DB base lives in 0034_price_usd.sql).
-- Idempotent (IF NOT EXISTS) so it is safe to re-run on existing prod DBs.
-- NOT applied by the integration test harness (fresh test DBs get the table
-- from the numbered baseline file). See 0034_price_usd.sql for column/semantic
-- documentation.
-- ---------------------------------------------------------------------------
BEGIN;

CREATE TABLE IF NOT EXISTS price_usd (
    token_id     VARCHAR(42) NOT NULL,
    block_number BIGINT      NOT NULL,
    price        NUMERIC     NOT NULL,
    confidence   NUMERIC,
    created_at   BIGINT      NOT NULL,
    PRIMARY KEY (token_id, block_number)
);

CREATE INDEX IF NOT EXISTS idx_price_usd_token_block ON price_usd (token_id, block_number DESC);

COMMIT;
