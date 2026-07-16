-- 0035_whitelist_price_source_id.sql
--
-- Add `price_source_id` to whitelist_token: the address used to QUERY DefiLlama
-- for this token's USD price, decoupled from `token_id` (the on-chain address
-- that is the price_usd storage / join key for this deployment).
--
-- DefiLlama only knows major-chain MAINNET addresses (ethereum for GIWA tokens). On testnet, pools and balances
-- reference mock token addresses DefiLlama cannot price, so `token_id` holds the
-- testnet mock address and `price_source_id` holds the mainnet equivalent. On
-- mainnet, `price_source_id` stays NULL and the indexer falls back to token_id.
--
-- Indexer rule (src/event/common/price_usd):
--   DefiLlama coin ref = COALESCE(NULLIF(price_source_id, ''), token_id)
--   price_usd rows are stored under token_id.
-- Multiple tokens may share one price_source_id (multiple native-pegged tokens
-- may price via one mainnet address), so a single fetched price fans out to several tokens.
--
-- NULL default: mainnet rows need no value. Per-environment data seeding
-- (testnet token_id remap + price_source_id) is applied separately, not here.
-- Idempotent: ADD COLUMN IF NOT EXISTS. Safe to re-run.

ALTER TABLE whitelist_token
    ADD COLUMN IF NOT EXISTS price_source_id VARCHAR(42);
