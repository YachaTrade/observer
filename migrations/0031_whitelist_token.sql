CREATE TABLE IF NOT EXISTS whitelist_token (
    token_id   VARCHAR(42) PRIMARY KEY,
    sort_order INT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE
);

-- 고정순서: WETH(1), USDC(2), USDT(3), ...
-- NOTE: USDC/USDT GIWA 온체인 주소 미확정 → WETH만 seed, 나머지는 주소 확정 후 후속 커밋.
INSERT INTO whitelist_token (token_id, sort_order) VALUES
    ('0x4200000000000000000000000000000000000006', 1)   -- WETH (quote_token 시드 주소)
ON CONFLICT (token_id) DO NOTHING;
