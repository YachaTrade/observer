-- PnL Aggregator: account별 실현 + 미실현 손익 집계
-- Scheduler에서 5분마다 갱신
-- API에서 SELECT만 하면 되므로 빠름

CREATE TABLE IF NOT EXISTS pnl_aggregator (
    account_id VARCHAR(42) PRIMARY KEY,
    total_invested_native NUMERIC NOT NULL DEFAULT 0,  -- UNIT: quote raw (wei)
    total_invested_usd NUMERIC NOT NULL DEFAULT 0,     -- UNIT: USD (human)
    realized_native NUMERIC NOT NULL DEFAULT 0,        -- UNIT: quote raw (wei)
    realized_usd NUMERIC NOT NULL DEFAULT 0,           -- UNIT: USD (human)
    unrealized_native NUMERIC NOT NULL DEFAULT 0,      -- UNIT: quote raw (wei)
    unrealized_usd NUMERIC NOT NULL DEFAULT 0,         -- UNIT: USD (human)
    updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pnl_aggregator_total ON pnl_aggregator((realized_native + unrealized_native) DESC);
