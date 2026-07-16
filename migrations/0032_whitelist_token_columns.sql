-- whitelist_token: CMS 큐레이션 컬럼 추가 (0031 CREATE 이후 append-only).
--   price_feed_id : Pyth Hermes feed id. NULL이면 가격 미조회 → balance_usd null.
--   name/symbol/image_uri/decimals : 표시·계산 메타. 값이 있으면 우선,
--                                    NULL이면 token/dex_token/quote_token에서 fallback.
-- 기존 DB는 0032만 새로 적용(0031 체크섬 불변). 모두 IF NOT EXISTS라 idempotent.
ALTER TABLE whitelist_token
    ADD COLUMN IF NOT EXISTS price_feed_id VARCHAR,
    ADD COLUMN IF NOT EXISTS name          VARCHAR,
    ADD COLUMN IF NOT EXISTS symbol        VARCHAR,
    ADD COLUMN IF NOT EXISTS image_uri     VARCHAR,
    ADD COLUMN IF NOT EXISTS decimals      INT;
