-- =====================================================
-- OBSERVER 전용 최적화된 데이터베이스 스키마
-- 불필요한 인덱스 제거로 쓰기 성능 최적화
-- =====================================================

-- Account Management Tables
CREATE TABLE IF NOT EXISTS account (
    account_id VARCHAR(42) PRIMARY KEY,
    nickname VARCHAR(42) NOT NULL,
    bio VARCHAR(255) NOT NULL DEFAULT '',
    image_uri VARCHAR NOT NULL,
    follower_count INT NOT NULL DEFAULT 0,
    following_count INT NOT NULL DEFAULT 0
);

-- API Search 모듈 최적화: nickname 검색 + follower_count 정렬을 위한 복합 인덱스  
CREATE INDEX IF NOT EXISTS idx_account_nickname_follower ON account (nickname, follower_count DESC);

-- API Social 모듈 최적화: get_follows에서 follower_count 정렬용 인덱스
CREATE INDEX IF NOT EXISTS idx_account_follower_count ON account (follower_count DESC);

-- Search 모듈 최적화: nickname 조회하는 쿼리용 인덱스
CREATE INDEX IF NOT EXISTS idx_account_nickname_gin ON account USING gin (nickname gin_trgm_ops);
-- EVM 주소 대소문자 무관 검색용 LOWER 인덱스
CREATE INDEX IF NOT EXISTS idx_account_account_id_lower ON account (LOWER(account_id));

-- 불필요한 trigram 인덱스 제거 (LOWER 방식으로 대체됨)
DROP INDEX IF EXISTS idx_account_account_id_gin;



-- Session Management
CREATE TABLE IF NOT EXISTS account_session (
    id VARCHAR(64) NOT NULL,
    account_id VARCHAR(42) PRIMARY KEY
);

-- API Auth 모듈 최적화: session_id로 조회하는 쿼리용 인덱스
CREATE INDEX IF NOT EXISTS idx_account_session_id ON account_session (id);

CREATE TABLE IF NOT EXISTS account_x(
    account_id VARCHAR(42) NOT NULL,
    x_handle VARCHAR(16) NOT NULL,
    x_image_uri VARCHAR NOT NULL,
    is_blue_label BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (account_id)
);

-- API Search 모듈 최적화: Twitter 핸들 검색 시 account 테이블과의 JOIN 성능을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_account_x_account_id ON account_x (account_id);
CREATE INDEX IF NOT EXISTS idx_account_x_handle ON account_x (x_handle);


-- Search 모듈 최적화: Twitter 핸들 gin_trgm_ops 조회하는 쿼리용 인덱스
 CREATE INDEX IF NOT EXISTS idx_account_x_handle_gin ON account_x USING GIN (x_handle gin_trgm_ops);



CREATE TABLE IF NOT EXISTS account_verified(
    x_handle VARCHAR(16) NOT NULL,
    PRIMARY KEY (x_handle)
);



CREATE TABLE IF NOT EXISTS account_wallet(
    account_id VARCHAR(42) NOT NULL,
    wallet VARCHAR(10) NOT NULL CHECK (wallet IN ('METAMASK', 'KEPLR', 'BACKPACK', 'HAHA', 'OKX', 'PHANTOM', 'RABBY', 'OTHER')),
    PRIMARY KEY (account_id)
);
-- Account Wallet 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요


CREATE TABLE IF NOT EXISTS auth_nonce (
    address VARCHAR(42) PRIMARY KEY,
    message TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);







































-- Hype Token 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요 (INSERT만 수행)

CREATE OR REPLACE FUNCTION search_everything(
    search_query TEXT,
    token_limit INT DEFAULT 50,
    account_limit INT DEFAULT 20
)
RETURNS TABLE (
    result_type VARCHAR,
    token_id VARCHAR(42),
    name VARCHAR,
    symbol VARCHAR,
    image_uri VARCHAR,
    created_at BIGINT,
    total_supply NUMERIC,
    market_type VARCHAR,
    price NUMERIC,
    account_id VARCHAR(42),
    nickname VARCHAR,
    follower_count INT,
    following_count INT,
    similarity_score REAL
) AS $$
BEGIN
    -- 토큰 검색 결과
    RETURN QUERY
    SELECT 
        'token'::VARCHAR as result_type,
        t.token_id,
        t.name,
        t.symbol,
        t.image_uri,
        t.created_at,
        t.total_supply,
        m.market_type,
        m.price,
        NULL::VARCHAR(42) as account_id,
        NULL::VARCHAR as nickname,
        NULL::INT as follower_count,
        NULL::INT as following_count,
        GREATEST(
            similarity(LOWER(t.name), LOWER(search_query)),
            similarity(LOWER(t.symbol), LOWER(search_query)),
            similarity(LOWER(t.token_id), LOWER(search_query))
        ) as similarity_score
    FROM token t
    JOIN market m ON t.token_id = m.token_id
    WHERE 
        -- 정확한 매칭 (최우선)
        LOWER(t.name) = LOWER(search_query)
        OR LOWER(t.symbol) = LOWER(search_query)
        OR LOWER(t.token_id) = LOWER(search_query)
        -- Trigram 매칭
        OR LOWER(t.name) % LOWER(search_query)
        OR LOWER(t.symbol) % LOWER(search_query)
        OR LOWER(t.token_id) % LOWER(search_query)
    ORDER BY 
        CASE 
            WHEN LOWER(t.name) = LOWER(search_query) 
            OR LOWER(t.symbol) = LOWER(search_query)
            OR LOWER(t.token_id) = LOWER(search_query) THEN 0
            ELSE 1
        END,
        similarity_score DESC,
        m.price DESC
    LIMIT token_limit;

    -- 계정 검색 결과
    RETURN QUERY
    SELECT 
        'account'::VARCHAR as result_type,
        NULL::VARCHAR(42) as token_id,
        NULL::VARCHAR as name,
        NULL::VARCHAR as symbol,
        a.image_uri,
        NULL::BIGINT as created_at,
        NULL::NUMERIC as total_supply,
        NULL::VARCHAR as market_type,
        NULL::NUMERIC as price,
        a.account_id,
        a.nickname,
        a.follower_count,
        a.following_count,
        GREATEST(
            similarity(LOWER(a.nickname), LOWER(search_query)),
            similarity(LOWER(a.account_id), LOWER(search_query))
        ) as similarity_score
    FROM account a
    WHERE 
        -- 정확한 매칭
        LOWER(a.nickname) = LOWER(search_query)
        OR LOWER(a.account_id) = LOWER(search_query)
        -- Trigram 매칭
        OR LOWER(a.nickname) % LOWER(search_query)
        OR LOWER(a.account_id) % LOWER(search_query)
    ORDER BY 
        CASE 
            WHEN LOWER(a.nickname) = LOWER(search_query)
            OR LOWER(a.account_id) = LOWER(search_query) THEN 0
            ELSE 1
        END,
        similarity_score DESC,
        a.follower_count DESC
    LIMIT account_limit;
END;
$$ LANGUAGE plpgsql;





-- =====================================================
-- Observer 최적화 완료
-- 총 인덱스 수: 최소화 (쓰기 성능 우선)
-- 주요 최적화:
-- 1. Primary Key 외 불필요한 인덱스 대부분 제거
-- 2. 실제 WHERE 조건에서만 사용하는 인덱스만 유지
-- 3. 쓰기 성능 최적화 완료
-- =====================================================

