
-- =====================================================
-- PRICE HISTORY 트리거 기반 차트 업데이트 시스템
-- price_history 테이블 INSERT → 자동으로 모든 차트 테이블 업데이트
-- =====================================================

-- Price History 테이블 생성
CREATE TABLE IF NOT EXISTS price_history (
    token_id VARCHAR(42) NOT NULL,
    price NUMERIC(15,10) NOT NULL,   -- UNIT: quote per token (chart price = virtual_native/virtual_token; observer src/types/chart.rs:19)
    volume NUMERIC NOT NULL DEFAULT 0,   -- UNIT: quote raw (wei) (amount_in on buy / amount_out on sell; observer src/types/chart.rs:35,48)
    created_at BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    PRIMARY KEY (token_id, block_number, transaction_hash, tx_index,log_index)
);
CREATE INDEX IF NOT EXISTS idx_price_history_token_id_created_at ON price_history (token_id, created_at DESC);

-- Trend job optimization: 24h gain rate query
-- Query: WHERE created_at <= $1 ORDER BY token_id, created_at DESC
CREATE INDEX IF NOT EXISTS idx_price_history_created_at_token
ON price_history (created_at DESC, token_id);

CREATE INDEX IF NOT EXISTS idx_price_history_token_created_asc
ON price_history (token_id, created_at ASC);


-- Chart 테이블 (파티션)
CREATE TABLE IF NOT EXISTS chart (
    token_id VARCHAR(42) NOT NULL,
    interval_type VARCHAR(2) NOT NULL CHECK (interval_type IN ('1', '5', '15', '30', '1H', '4H', 'D', 'W', 'M')),
    open_price NUMERIC(15,10) NOT NULL,   -- UNIT: quote per token (OHLC of price_history.price; 0003_chart.sql trigger lines 199-202)
    close_price NUMERIC(15,10) NOT NULL,   -- UNIT: quote per token
    high_price NUMERIC(15,10) NOT NULL,   -- UNIT: quote per token
    low_price NUMERIC(15,10) NOT NULL,   -- UNIT: quote per token
    volume NUMERIC NOT NULL DEFAULT 0,   -- UNIT: quote raw (wei) (sum of price_history.volume; trigger line 203,216)
    usd_open_price NUMERIC(15,10) NOT NULL,   -- UNIT: USD per token (price * latest_usd_price, USD-per-quote; trigger line 205)
    usd_close_price NUMERIC(15,10) NOT NULL,   -- UNIT: USD per token (trigger line 206)
    usd_high_price NUMERIC(15,10) NOT NULL,   -- UNIT: USD per token (trigger line 207)
    usd_low_price NUMERIC(15,10) NOT NULL,   -- UNIT: USD per token (trigger line 208)
    usd_volume NUMERIC NOT NULL DEFAULT 0,   -- UNIT: USD scaled by 10^quote_decimals -- NOT human USD! (= volume[quote raw wei] * USD-per-quote; divide by 10^quote_decimals for human USD; trigger line 209)
    total_supply NUMERIC NOT NULL,   -- UNIT: token raw (wei) (copied from token.total_supply; trigger line 204)
    time_stamp BIGINT NOT NULL,   -- UNIT: unix seconds (interval-bucketed candle start; convert_chart_timestamp)
    PRIMARY KEY (token_id, interval_type, time_stamp)
) PARTITION BY HASH (token_id);

-- 파티션 생성
CREATE TABLE IF NOT EXISTS chart_0 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE IF NOT EXISTS chart_1 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE IF NOT EXISTS chart_2 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE IF NOT EXISTS chart_3 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE IF NOT EXISTS chart_4 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE IF NOT EXISTS chart_5 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE IF NOT EXISTS chart_6 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE IF NOT EXISTS chart_7 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE IF NOT EXISTS chart_8 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE IF NOT EXISTS chart_9 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE IF NOT EXISTS chart_10 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE IF NOT EXISTS chart_11 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE IF NOT EXISTS chart_12 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE IF NOT EXISTS chart_13 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE IF NOT EXISTS chart_14 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE IF NOT EXISTS chart_15 PARTITION OF chart FOR VALUES WITH (modulus 16, remainder 15);

-- Chart 복합 인덱스 (observer의 INSERT에는 불필요하지만 API 조회용)
CREATE INDEX IF NOT EXISTS idx_chart_lookup ON chart (token_id, interval_type, time_stamp DESC);

-- Trend job optimization: 4h volume query
-- Query: WHERE interval_type = '4H' AND time_stamp >= $1 ORDER BY volume DESC
CREATE INDEX IF NOT EXISTS idx_chart_interval_timestamp
ON chart (interval_type, time_stamp);




-- Rust convert_chart_timestamp와 동일한 PostgreSQL 함수
CREATE OR REPLACE FUNCTION convert_chart_timestamp(
    input_timestamp BIGINT, 
    interval_type TEXT
) RETURNS BIGINT AS $$
DECLARE
    total_minutes BIGINT;
    rounded_minutes BIGINT;
BEGIN
    -- 1단계: 초를 분으로 변환
    total_minutes := input_timestamp / 60;
    
    -- 2단계: interval에 따라 분 단위 반올림
    CASE interval_type
        WHEN '1' THEN 
            rounded_minutes := total_minutes;
        WHEN '5' THEN 
            rounded_minutes := (total_minutes / 5) * 5;
        WHEN '15' THEN 
            rounded_minutes := (total_minutes / 15) * 15;
        WHEN '30' THEN 
            rounded_minutes := (total_minutes / 30) * 30;
        WHEN '1H' THEN 
            rounded_minutes := (total_minutes / 60) * 60;
        WHEN '4H' THEN 
            rounded_minutes := (total_minutes / 240) * 240;
        WHEN 'D' THEN 
            rounded_minutes := (total_minutes / 1440) * 1440;
        WHEN 'W' THEN 
            -- 주 단위: 월요일 자정 기준 (분 단위로 변환)
            rounded_minutes := EXTRACT(EPOCH FROM DATE_TRUNC('week', TO_TIMESTAMP(input_timestamp)))::BIGINT / 60;
        WHEN 'M' THEN 
            -- 월 단위: 월초 자정 기준 (분 단위로 변환)
            rounded_minutes := EXTRACT(EPOCH FROM DATE_TRUNC('month', TO_TIMESTAMP(input_timestamp)))::BIGINT / 60;
        ELSE 
            rounded_minutes := total_minutes;
    END CASE;
    
    -- 3단계: 분을 다시 초로 변환
    RETURN rounded_minutes * 60;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 6단계: 트리거 함수 업데이트 (USD OHLC 지원)
CREATE OR REPLACE FUNCTION update_charts_on_price_insert()
RETURNS TRIGGER AS $$
DECLARE
    interval_val TEXT;
    converted_timestamp BIGINT;
    prev_close_price NUMERIC(15,10);
    prev_usd_close_price NUMERIC(15,10);
    token_supply NUMERIC;
    latest_usd_price NUMERIC;
    token_quote_id VARCHAR(42);
BEGIN
    -- token total_supply + market quote_id 1회 PK 조회로 통합
    SELECT t.total_supply, m.quote_id
      INTO token_supply, token_quote_id
      FROM token t
      JOIN market m ON m.token_id = t.token_id
     WHERE t.token_id = NEW.token_id;

    -- USD 가격 조회 (해당 quote_id의 해당 블록 이하 최신)
    -- quote_id 등호 필터로 idx_price_quote_block 인덱스 사용
    SELECT price INTO latest_usd_price
    FROM price
    WHERE quote_id = token_quote_id
      AND block_number <= NEW.block_number
    ORDER BY block_number DESC
    LIMIT 1;

    -- 해당 블록 이하에 USD 가격이 없으면 최신 USD 가격 사용 (같은 quote 한정)
    IF latest_usd_price IS NULL THEN
        SELECT price INTO latest_usd_price
        FROM price
        WHERE quote_id = token_quote_id
        ORDER BY block_number DESC
        LIMIT 1;
    END IF;

    -- USD 환율이 없으면 1로 설정
    IF latest_usd_price IS NULL THEN
        latest_usd_price := 1;
    END IF;

    -- 각 시간대별로 차트 업데이트
    FOREACH interval_val IN ARRAY ARRAY['1', '5', '15', '30', '1H', '4H', 'D', 'W', 'M']
    LOOP
        -- 타임스탬프 변환
        converted_timestamp := convert_chart_timestamp(NEW.created_at, interval_val);

        -- 이전 캔들의 close_price, usd_close_price 조회 (새 캔들의 open_price로 사용)
        SELECT close_price, usd_close_price INTO prev_close_price, prev_usd_close_price
        FROM chart
        WHERE chart.token_id = NEW.token_id
          AND chart.interval_type = interval_val
          AND chart.time_stamp < converted_timestamp
        ORDER BY chart.time_stamp DESC
        LIMIT 1;

        -- OHLCV + Market Cap + USD OHLCV 업데이트
        INSERT INTO chart (
            token_id,
            interval_type,
            time_stamp,
            open_price,
            close_price,
            high_price,
            low_price,
            volume,
            total_supply,
            usd_open_price,
            usd_close_price,
            usd_high_price,
            usd_low_price,
            usd_volume
        )
        VALUES (
            NEW.token_id,
            interval_val,
            converted_timestamp,
            COALESCE(prev_close_price, NEW.price),                                      -- open_price
            NEW.price,                                                                  -- close_price
            NEW.price,                                                                  -- high_price
            NEW.price,                                                                  -- low_price
            NEW.volume,                                                                 -- volume
            COALESCE(token_supply, 0),                                                  -- total_supply
            COALESCE(prev_usd_close_price, NEW.price * latest_usd_price),              -- usd_open_price
            NEW.price * latest_usd_price,                                              -- usd_close_price
            NEW.price * latest_usd_price,                                              -- usd_high_price
            NEW.price * latest_usd_price,                                              -- usd_low_price
            NEW.volume * latest_usd_price                                               -- usd_volume
        )
        ON CONFLICT (token_id, interval_type, time_stamp)
        DO UPDATE SET
            close_price = EXCLUDED.close_price,
            high_price = GREATEST(chart.high_price, EXCLUDED.high_price),
            low_price = LEAST(chart.low_price, EXCLUDED.low_price),
            volume = chart.volume + EXCLUDED.volume,
            total_supply = EXCLUDED.total_supply,
            usd_close_price = EXCLUDED.usd_close_price,
            usd_high_price = GREATEST(chart.usd_high_price, EXCLUDED.usd_high_price),
            usd_low_price = LEAST(chart.usd_low_price, EXCLUDED.usd_low_price),
            usd_volume = chart.usd_volume + EXCLUDED.usd_volume;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- price_history INSERT 트리거 생성
DROP TRIGGER IF EXISTS trigger_update_charts_on_price_insert ON price_history;
CREATE TRIGGER trigger_update_charts_on_price_insert
    AFTER INSERT ON price_history
    FOR EACH ROW
    EXECUTE FUNCTION update_charts_on_price_insert();

-- 트리거 활성화
ALTER TABLE price_history ENABLE TRIGGER trigger_update_charts_on_price_insert;




-- Chart count table
CREATE TABLE IF NOT EXISTS chart_count(
    token_id VARCHAR(42) NOT NULL,
    interval_type VARCHAR(2) NOT NULL CHECK (interval_type IN ('1', '5', '15', '30', '1H', '4H', 'D', 'W', 'M')),
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (token_id, interval_type)
);
-- Chart Count 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요

-- Chart Count 트리거
CREATE OR REPLACE FUNCTION update_chart_count()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO chart_count (token_id, interval_type, count)
    VALUES (NEW.token_id, NEW.interval_type, 1)
    ON CONFLICT (token_id, interval_type)
    DO UPDATE SET count = chart_count.count + 1;
    
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS chart_count_trigger ON public.chart;
CREATE TRIGGER chart_count_trigger
    AFTER INSERT OR UPDATE ON chart
    FOR EACH ROW
    EXECUTE FUNCTION update_chart_count();

ALTER TABLE chart ENABLE TRIGGER chart_count_trigger;



