-- v2_upgrade_whitelist_price_source_id.sql
--
-- PROD upgrade track for whitelist_token.price_source_id (fresh-DB base lives in
-- 0035_whitelist_price_source_id.sql). Idempotent (IF NOT EXISTS) so it is safe
-- to re-run on existing prod DBs. NOT applied by the integration test harness
-- (fresh test DBs get the column from the numbered baseline file). See
-- 0035_whitelist_price_source_id.sql for column semantics.
--
-- Data seeding is environment-specific and applied separately, NOT here:
--   * mainnet: leave price_source_id NULL except native/non-priceable tokens
--     (MON 0x0000…, LVMON) which point at mainnet WMON.
--   * testnet: token_id = mock address, price_source_id = mainnet equivalent.
-- ---------------------------------------------------------------------------
BEGIN;

ALTER TABLE whitelist_token
    ADD COLUMN IF NOT EXISTS price_source_id VARCHAR(42);

COMMIT;
