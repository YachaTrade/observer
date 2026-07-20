CREATE TABLE IF NOT EXISTS token (
    token_id VARCHAR(42) PRIMARY KEY,
    name VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    image_uri VARCHAR NOT NULL,
    creator VARCHAR(42)NOT NULL,
    description TEXT NULL,
    twitter VARCHAR NULL,
    telegram VARCHAR NULL,
    website VARCHAR NULL,
    is_nsfw BOOLEAN NOT NULL DEFAULT FALSE,
    is_graduated BOOLEAN NOT NULL DEFAULT FALSE,
    is_cto BOOLEAN NOT NULL DEFAULT FALSE,
    created_at BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    total_supply NUMERIC NOT NULL, -- token raw (wei): ERC20 raw supply (init 1e9 * 1e18), decremented by burns

    token_holder_count BIGINT NOT NULL DEFAULT 0,
    version VARCHAR NOT NULL DEFAULT 'V1' CHECK (version IN ('V1', 'V2'))
);


-- Token 테이블 복합 인덱스 (cache에서 token_id와 creator 동시 조회)
CREATE INDEX IF NOT EXISTS idx_token_token_id_creator ON token (token_id, creator);


-- API New Content 모듈 최적화: latest token 조회용 인덱스
CREATE INDEX IF NOT EXISTS idx_token_created_at ON token (created_at DESC);

-- API Token 모듈 최적화: creator로 조회하는 쿼리용 인덱스
CREATE INDEX IF NOT EXISTS idx_token_creator ON token (creator);
CREATE INDEX IF NOT EXISTS idx_token_creator_created_at ON token (creator, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_token_symbol ON token (symbol);
CREATE INDEX IF NOT EXISTS idx_token_name ON token (name);
-- Search 모듈 최적화: name, symbol lower로 조회하는 쿼리용 인덱스
CREATE INDEX IF NOT EXISTS idx_token_name_gin ON token USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_token_symbol_gin ON token USING GIN (symbol gin_trgm_ops);  
CREATE INDEX IF NOT EXISTS idx_token_token_id_lower ON token (LOWER(token_id));
CREATE INDEX IF NOT EXISTS idx_token_version ON token (version);
CREATE INDEX IF NOT EXISTS idx_token_is_nsfw ON token (is_nsfw);





-- Count Tables
CREATE TABLE IF NOT EXISTS token_count (
    total_count BIGINT NOT NULL DEFAULT 0,
    graduated_count BIGINT NOT NULL DEFAULT 0,
    nsfw_count BIGINT NOT NULL DEFAULT 0,
    sfw_count BIGINT NOT NULL DEFAULT 0,
    id SERIAL PRIMARY KEY
);
-- 단일 행 테이블이므로 인덱스 불필요

-- 초기 데이터 삽입
INSERT INTO token_count (total_count, graduated_count, nsfw_count, sfw_count)
SELECT
    (SELECT COUNT(*) FROM token),
    (SELECT COUNT(*) FROM token WHERE is_graduated = true),
    (SELECT COUNT(*) FROM token WHERE is_nsfw = true),
    (SELECT COUNT(*) FROM token WHERE is_nsfw IS NOT true);

-- 트리거 함수들
CREATE OR REPLACE FUNCTION update_token_count_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.token_count
    SET
        total_count = total_count + 1,
        nsfw_count = CASE WHEN NEW.is_nsfw = true THEN nsfw_count + 1 ELSE nsfw_count END,
        sfw_count = CASE WHEN NEW.is_nsfw IS NOT true THEN sfw_count + 1 ELSE sfw_count END,
        graduated_count = CASE WHEN NEW.is_graduated = true THEN graduated_count + 1 ELSE graduated_count END;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_token_count_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.token_count
    SET
        total_count = total_count - 1,
        nsfw_count = CASE WHEN OLD.is_nsfw = true THEN nsfw_count - 1 ELSE nsfw_count END,
        sfw_count = CASE WHEN OLD.is_nsfw IS NOT true THEN sfw_count - 1 ELSE sfw_count END,
        graduated_count = CASE WHEN OLD.is_graduated = true THEN graduated_count - 1 ELSE graduated_count END;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION update_graduated_count()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.is_graduated IS NOT true AND NEW.is_graduated = true THEN
        UPDATE public.token_count SET graduated_count = graduated_count + 1;
    ELSIF OLD.is_graduated = true AND NEW.is_graduated IS NOT true THEN
        UPDATE public.token_count SET graduated_count = graduated_count - 1;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_nsfw_count()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.is_nsfw IS NOT true AND NEW.is_nsfw = true THEN
        UPDATE public.token_count
        SET
            nsfw_count = nsfw_count + 1,
            sfw_count = sfw_count - 1;
    ELSIF OLD.is_nsfw = true AND NEW.is_nsfw IS NOT true THEN
        UPDATE public.token_count
        SET
            nsfw_count = nsfw_count - 1,
            sfw_count = sfw_count + 1;
    END IF;
    RETURN NEW;
END;
$$;

-- 트리거 생성
DROP TRIGGER IF EXISTS token_insert_trigger ON public.token;
CREATE TRIGGER token_insert_trigger
AFTER INSERT ON public.token
FOR EACH ROW EXECUTE FUNCTION update_token_count_insert();

DROP TRIGGER IF EXISTS token_delete_trigger ON public.token;
CREATE TRIGGER token_delete_trigger
AFTER DELETE ON public.token
FOR EACH ROW EXECUTE FUNCTION update_token_count_delete();

DROP TRIGGER IF EXISTS token_graduated_count_trigger ON public.token;
CREATE TRIGGER token_graduated_count_trigger
AFTER UPDATE OF is_graduated ON public.token
FOR EACH ROW EXECUTE FUNCTION update_graduated_count();

DROP TRIGGER IF EXISTS token_nsfw_count_trigger ON public.token;
CREATE TRIGGER token_nsfw_count_trigger
AFTER UPDATE OF is_nsfw ON public.token
FOR EACH ROW EXECUTE FUNCTION update_nsfw_count();

ALTER TABLE public.token ENABLE TRIGGER token_insert_trigger;
ALTER TABLE public.token ENABLE TRIGGER token_delete_trigger;
ALTER TABLE public.token ENABLE TRIGGER token_graduated_count_trigger;
ALTER TABLE public.token ENABLE TRIGGER token_nsfw_count_trigger;


CREATE TABLE IF NOT EXISTS token_metadata(
    metadata_url VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    description TEXT NULL,
    image_url VARCHAR,
    website VARCHAR,
    twitter VARCHAR,
    telegram VARCHAR,
    is_nsfw BOOLEAN NOT NULL DEFAULT FALSE
);





-- Market
CREATE TABLE IF NOT EXISTS market (
    market_type VARCHAR NOT NULL CHECK (market_type IN ('CURVE', 'DEX', 'V2_CURVE', 'V2_DEX')),
    token_id VARCHAR NOT NULL,
    pool_id VARCHAR NULL,
    reserve_quote NUMERIC NULL, --liquidity; quote raw (wei): raw on-chain reserve of the quote token
    reserve_token NUMERIC NULL, -- token raw (wei): raw on-chain reserve of the traded token
    volume NUMERIC NOT NULL DEFAULT 0, -- quote raw (wei): cumulative sum of swap.quote_amount (raw on-chain amount_in/out)
    ath_price NUMERIC(15,10) NOT NULL DEFAULT 0, --USD; USD per token (ath_price_quote * quote USD price)
    ath_price_quote NUMERIC(15,10) NOT NULL DEFAULT 0, --Quote; quote per token (all-time-high in quote terms)
    price NUMERIC(15,10) NOT NULL, -- quote per token: virtual_quote_reserve / virtual_token_reserve (NOT USD)
    quote_id VARCHAR(42) NOT NULL DEFAULT '0x4200000000000000000000000000000000000006',
    latest_trade_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (token_id)
);

-- Market 테이블 복합 인덱스 (observer는 쓰기 위주이므로 필요한 것만)
CREATE INDEX IF NOT EXISTS idx_market_token_id_market_type ON market (token_id, market_type) WHERE market_type = 'DEX';
CREATE INDEX IF NOT EXISTS idx_market_pool_dex ON market (pool_id, market_type) WHERE market_type = 'DEX';

-- API Token 모듈 최적화: 정렬 쿼리용 인덱스
CREATE INDEX IF NOT EXISTS idx_market_price ON market (price DESC);
CREATE INDEX IF NOT EXISTS idx_market_latest_trade_at ON market (latest_trade_at DESC);



-- Burn History
CREATE TABLE IF NOT EXISTS burn_history(
    token_id VARCHAR(42) NOT NULL ,
    account_id VARCHAR(42) NOT NULL,
    token_amount NUMERIC NOT NULL, -- token raw (wei): ERC20 Transfer value burned to zero address
    transaction_hash VARCHAR NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    log_index INT NOT NULL,
    PRIMARY KEY(token_id,account_id,transaction_hash,log_index)
);
-- Burn History 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요 (INSERT만 수행)



CREATE TABLE IF NOT EXISTS set_creator_history(
    token_id VARCHAR(42) NOT NULL,
    old_creator VARCHAR(42) NOT NULL,
    new_creator VARCHAR(42) NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    block_number BIGINT NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY(transaction_hash, tx_index, log_index)
);
