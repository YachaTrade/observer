-- Swap History
CREATE TABLE IF NOT EXISTS swap (
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    -- market type
    market_type VARCHAR NOT NULL CHECK (market_type IN ('NADFUN', 'UNISWAPV3')),
    is_buy BOOLEAN NOT NULL,
    quote_amount NUMERIC NOT NULL,   -- UNIT: quote raw (wei) (buy=amount_in / sell=amount_out; observer src/event/v1/curve/receive.rs:431,530)
    token_amount NUMERIC NOT NULL,   -- UNIT: token raw (wei) (buy=amount_out / sell=amount_in; observer src/event/v1/curve/receive.rs:432,531)
    reserve_quote NUMERIC NULL,   -- UNIT: quote raw (wei) (curve/pool quote reserve snapshot; observer src/event/v1/curve/receive.rs:433)
    reserve_token NUMERIC NULL,   -- UNIT: token raw (wei) (curve/pool token reserve snapshot; observer src/event/v1/curve/receive.rs:434)
    value NUMERIC NOT NULL DEFAULT 0,   -- UNIT: USD (human) ((quote_amount / 10^decimals) * USD-per-quote price; observer src/event/v1/curve/receive.rs:409-410)
    created_at BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL DEFAULT 0,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    PRIMARY KEY (account_id,token_id, transaction_hash,tx_index,log_index)
);

CREATE INDEX IF NOT EXISTS idx_swap_account_created_at ON swap (account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_swap_token_account_buy_created ON swap (token_id, account_id, is_buy, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_swap_token_buy_volume_created ON swap (token_id, is_buy, quote_amount DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_swap_is_buy_created_at ON swap (is_buy, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_swap_block_number_tx_index_log_index ON swap (block_number ASC, tx_index ASC, log_index ASC);
CREATE INDEX IF NOT EXISTS idx_swap_token_created ON swap (token_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_swap_block_number_tx_index_log_index_desc
ON swap (block_number DESC, tx_index DESC, log_index DESC);

-- Trend job optimization: 4h tx count query
-- Query: WHERE created_at >= $1 GROUP BY token_id ORDER BY COUNT(*) DESC
CREATE INDEX IF NOT EXISTS idx_swap_created_at_token
ON swap (created_at, token_id);


-- 1. 트리거 함수 생성 (swap INSERT 시 market volume 증가)
CREATE OR REPLACE FUNCTION update_market_volume()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE market
    SET volume = volume + NEW.quote_amount
    WHERE token_id = NEW.token_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 트리거 생성
CREATE TRIGGER trg_update_market_volume
AFTER INSERT ON swap
FOR EACH ROW
EXECUTE FUNCTION update_market_volume();

-- 3. 기존 데이터로 market의 volume 업데이트
UPDATE market m
SET volume = COALESCE(
    (
        SELECT SUM(s.quote_amount)
        FROM swap s
        WHERE s.token_id = m.token_id
    ),
    0
);



-- -- API New Content 모듈 최적화: latest buy/sell 조회용 인덱스
-- CREATE INDEX IF NOT EXISTS idx_swap_is_buy_created_at ON swap (is_buy, created_at DESC);

-- CREATE INDEX IF NOT EXISTS idx_swap_time_token_buy ON swap (token_id , created_at DESC, is_buy);
-- -- API Trading 모듈 최적화: swap history 조회용 복합 인덱스들
-- CREATE INDEX IF NOT EXISTS idx_swap_account_created_at ON swap (account_id, created_at DESC);
-- CREATE INDEX IF NOT EXISTS idx_swap_token_created_at ON swap (token_id, created_at DESC);
-- CREATE INDEX IF NOT EXISTS idx_swap_token_account ON swap (token_id, account_id, created_at DESC);
-- CREATE INDEX IF NOT EXISTS idx_swap_token_buy ON swap (token_id, is_buy, created_at DESC);
-- CREATE INDEX IF NOT EXISTS idx_swap_token_volume ON swap (token_id, quote_amount, created_at DESC);
-- CREATE INDEX IF NOT EXISTS idx_swap_token_account_buy_created_at ON swap (token_id, account_id, is_buy, created_at DESC);
-- CREATE INDEX IF NOT EXISTS idx_swap_token_buy_amount_created_at ON swap (token_id, is_buy, quote_amount, created_at DESC);

CREATE TABLE IF NOT EXISTS swap_count (
    token_id VARCHAR(42) PRIMARY KEY,
    count BIGINT NOT NULL DEFAULT 0,
    buy_count BIGINT NOT NULL DEFAULT 0,
    sell_count BIGINT NOT NULL DEFAULT 0
);
-- Swap Count 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요

-- 기존 데이터 초기화
UPDATE swap_count SET 
    count = (
        SELECT COUNT(*) FROM swap 
        WHERE token_id = swap_count.token_id
    ),
    buy_count = (
        SELECT COUNT(*) FROM swap 
        WHERE token_id = swap_count.token_id AND is_buy = true
    ),
    sell_count = (
        SELECT COUNT(*) FROM swap 
        WHERE token_id = swap_count.token_id AND is_buy = false
    );

-- Swap Count 트리거
CREATE OR REPLACE FUNCTION update_swap_count()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.token_id IS NULL THEN
        RETURN NEW;
    END IF;

    INSERT INTO public.swap_count (token_id, count, buy_count, sell_count)
    VALUES (
        NEW.token_id, 
        1,
        CASE WHEN NEW.is_buy THEN 1 ELSE 0 END,
        CASE WHEN NEW.is_buy THEN 0 ELSE 1 END
    )
    ON CONFLICT (token_id)
    DO UPDATE SET 
        count = public.swap_count.count + 1,
        buy_count = public.swap_count.buy_count + CASE WHEN NEW.is_buy THEN 1 ELSE 0 END,
        sell_count = public.swap_count.sell_count + CASE WHEN NEW.is_buy THEN 0 ELSE 1 END;
    
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS swap_count_trigger ON public.swap;
CREATE TRIGGER swap_count_trigger
    AFTER INSERT ON public.swap
    FOR EACH ROW
    EXECUTE FUNCTION update_swap_count();

ALTER TABLE public.swap ENABLE TRIGGER swap_count_trigger;



-- Account Swap Count 집계 테이블 (Trading 모듈 최적화)
CREATE TABLE IF NOT EXISTS account_swap_count (
    account_id VARCHAR(42) PRIMARY KEY,
    total_count BIGINT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- Account Swap Count 초기 데이터 삽입
INSERT INTO account_swap_count (account_id, total_count)
SELECT 
    s.account_id,
    COUNT(*) as total_count
FROM swap s
GROUP BY s.account_id
ON CONFLICT (account_id) DO UPDATE 
SET total_count = EXCLUDED.total_count,
    last_updated = NOW();

-- Account Swap Count 트리거 함수
CREATE OR REPLACE FUNCTION update_account_swap_count() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO account_swap_count (account_id, total_count)
    VALUES (NEW.account_id, 1)
    ON CONFLICT (account_id) 
    DO UPDATE SET 
        total_count = account_swap_count.total_count + 1,
        last_updated = NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_update_account_swap_count ON swap;
CREATE TRIGGER trg_update_account_swap_count
AFTER INSERT ON swap
FOR EACH ROW
EXECUTE FUNCTION update_account_swap_count();



CREATE TABLE IF NOT EXISTS mint(
    token_id VARCHAR(42) NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    market_id VARCHAR(42) NOT NULL,
    quote_amount NUMERIC NOT NULL,   -- UNIT: quote raw (wei) (decoded log amount; observer src/event/v1/dex/receive.rs:570)
    token_amount NUMERIC NOT NULL,   -- UNIT: token raw (wei) (decoded log amount; observer src/event/v1/dex/receive.rs:571)
    reserve_quote NUMERIC NOT NULL,   -- UNIT: quote raw (wei) (pool reserve snapshot; observer src/event/v1/dex/receive.rs:572)
    reserve_token NUMERIC NOT NULL,   -- UNIT: token raw (wei) (pool reserve snapshot; observer src/event/v1/dex/receive.rs:573)
    created_at BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    PRIMARY KEY (token_id, transaction_hash, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_mint_block_number_tx_index_log_index ON mint (block_number ASC, tx_index ASC, log_index ASC);


CREATE TABLE IF NOT EXISTS burn(
    token_id VARCHAR(42) NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    market_id VARCHAR(42) NOT NULL,
    quote_amount NUMERIC NOT NULL,   -- UNIT: quote raw (wei) (decoded log amount; observer src/event/v1/dex/receive.rs:590)
    token_amount NUMERIC NOT NULL,   -- UNIT: token raw (wei) (decoded log amount; observer src/event/v1/dex/receive.rs:591)
    reserve_quote NUMERIC NOT NULL,   -- UNIT: quote raw (wei) (pool reserve snapshot; observer src/event/v1/dex/receive.rs:592)
    reserve_token NUMERIC NOT NULL,   -- UNIT: token raw (wei) (pool reserve snapshot; observer src/event/v1/dex/receive.rs:593)
    created_at BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    PRIMARY KEY (token_id, transaction_hash, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_burn_block_number_tx_index_log_index ON burn (block_number ASC, tx_index ASC, log_index ASC);
