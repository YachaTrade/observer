CREATE TABLE IF NOT EXISTS whitelist_token (
    token_id   VARCHAR(42) PRIMARY KEY,
    sort_order INT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE
);

-- 고정순서: MON(1), WMON(2), USDC(3), USDT(4), LVMON(5)
-- NOTE: WMON/USDC/USDT 온체인 주소 미확정 → MON/LVMON만 seed, 나머지는 주소 확정 후 후속 커밋.
INSERT INTO whitelist_token (token_id, sort_order) VALUES
    ('0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A', 1),  -- MON (quote_token 기존 주소)
    ('0xBe3fa50514D9617ce645a02B34F595541AF02b6b', 5)   -- LVMON (quote_token 기존 주소)
ON CONFLICT (token_id) DO NOTHING;
