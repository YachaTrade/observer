
-- LP Manager
CREATE TABLE IF NOT EXISTS lp_allocate_history(
    token_id VARCHAR(42) NOT NULL,
    quote_amount NUMERIC NOT NULL, -- quote raw (wei); LpManagerAllocate.monAmount
    token_amount NUMERIC NOT NULL, -- token raw (wei); LpManagerAllocate.tokenAmount
    transaction_hash VARCHAR NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (token_id,transaction_hash)
);
-- LP Allocate History 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요 (INSERT만 수행)

-- 1. 트리거 함수 생성                                                                                                                                                                                                                                                                                                                                                                                                                       
CREATE OR REPLACE FUNCTION update_lp_collect_status_from_allocate()                                                                                                                                                                                                                                                                                                                                                                          
RETURNS TRIGGER AS $$                                                                                                                                                                                                                                                                                                                                                                                                                        
BEGIN                                                                                                                                                                                                                                                                                                                                                                                                                                        
    -- 새로 INSERT된 allocate 정보로 lp_collect_status 업데이트                                                                                                                                                                                                                                                                                                                                                                              
    INSERT INTO lp_collect_status (token_id, last_collect_at)                                                                                                                                                                                                                                                                                                                                                                                
    VALUES (NEW.token_id, NEW.created_at)                                                                                                                                                                                                                                                                                                                                                                                                    
    ON CONFLICT (token_id)                                                                                                                                                                                                                                                                                                                                                                                                                   
    DO NOTHING;  -- 이미 존재하면 아무것도 하지 않음 (collect가 더 최신일 수 있음)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
    RETURN NEW;                                                                                                                                                                                                                                                                                                                                                                                                                              
END;                                                                                                                                                                                                                                                                                                                                                                                                                                         
$$ LANGUAGE plpgsql;                                                                                                                                                                                                                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                                                                                             
-- 2. 트리거 생성                                                                                                                                                                                                                                                                                                                                                                                                                            
DROP TRIGGER IF EXISTS trigger_update_lp_collect_status_from_allocate ON lp_allocate_history;                                                                                                                                                                                                                                                                                                                                                
CREATE TRIGGER trigger_update_lp_collect_status_from_allocate                                                                                                                                                                                                                                                                                                                                                                                
    AFTER INSERT ON lp_allocate_history                                                                                                                                                                                                                                                                                                                                                                                                      
    FOR EACH ROW                                                                                                                                                                                                                                                                                                                                                                                                                             
    EXECUTE FUNCTION update_lp_collect_status_from_allocate();                                                                                                                                                                                                                                                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                                                                                             
-- 3. 트리거 활성화                                                                                                                                                                                                                                                                                                                                                                                                                          
ALTER TABLE lp_allocate_history ENABLE TRIGGER trigger_update_lp_collect_status_from_allocate;    





CREATE TABLE IF NOT EXISTS lp_collect_history(
    token_id VARCHAR(42) NOT NULL,
    quote_amount NUMERIC NOT NULL, -- quote raw (wei); LpManagerCollect.monAmount
    token_amount NUMERIC NOT NULL, --token treasury -- token raw (wei); LpManagerCollect.tokenAmount
    c_amount NUMERIC NOT NULL, --creator treasury -- quote raw (wei); quote_amount * creatorTreasuryFeeRate / 1e6
    ft_amount NUMERIC NOT NULL, --foundation treasury -- quote raw (wei); quote_amount * foundationTreasuryFeeRate / 1e6
    ct_amount NUMERIC NOT NULL, --community treasury -- quote raw (wei); quote_amount * communityTreasuryFeeRate / 1e6
    transaction_hash VARCHAR NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (token_id,transaction_hash,tx_index,log_index)
);
-- LP Collect History 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요 (INSERT만 수행)


CREATE OR REPLACE FUNCTION update_lp_collect_status_from_collect()
RETURNS TRIGGER AS $$
BEGIN
    -- 새로 INSERT된 collect 정보로 lp_collect_status 업데이트
    INSERT INTO lp_collect_status (token_id, last_collect_at)
    VALUES (NEW.token_id, NEW.created_at)
    ON CONFLICT (token_id)
    DO UPDATE SET
        last_collect_at = NEW.created_at;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 트리거 생성
DROP TRIGGER IF EXISTS trigger_update_lp_collect_status_from_collect ON lp_collect_history;
CREATE TRIGGER trigger_update_lp_collect_status_from_collect
    AFTER INSERT ON lp_collect_history
    FOR EACH ROW
    EXECUTE FUNCTION update_lp_collect_status_from_collect();

-- 3. 트리거 활성화
ALTER TABLE lp_collect_history ENABLE TRIGGER trigger_update_lp_collect_status_from_collect;




CREATE TABLE IF NOT EXISTS lp_collect_status(
    token_id VARCHAR(42) NOT NULL,
    last_collect_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (token_id)
);
-- LP Collect Status는 UPDATE에서 WHERE token_id = $1 사용하므로 PRIMARY KEY만으로 충분





-- Fee Distribution History (Distributed event)
CREATE TABLE IF NOT EXISTS fee_distribute_history (
    transaction_hash VARCHAR(66) NOT NULL,
    tx_index INT NOT NULL DEFAULT 0,
    log_index INT NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    token_amount NUMERIC(78, 0) NOT NULL, -- token raw (wei); Distributed.tokenAmount
    mon_received NUMERIC(78, 0) NOT NULL, -- quote raw (wei); Distributed.monReceived
    foundation_amount NUMERIC(78, 0) NOT NULL, -- quote raw (wei); Distributed.foundationAmount (split of monReceived)
    creator_amount NUMERIC(78, 0) NOT NULL, -- quote raw (wei); Distributed.creatorAmount (split of monReceived)
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (transaction_hash, tx_index, log_index)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fee_distribute_history_token ON fee_distribute_history(token_id);
CREATE INDEX IF NOT EXISTS idx_fee_distribute_history_block ON fee_distribute_history(block_number);
CREATE INDEX IF NOT EXISTS idx_fee_distribute_history_created_at ON fee_distribute_history(created_at);

-- =====================================================
-- Trigger: Update creator_treasury_balance from fee_distribute_history
-- =====================================================

-- 1. 트리거 함수 생성
CREATE OR REPLACE FUNCTION update_creator_treasury_balance_from_distribute()
RETURNS TRIGGER AS $$
BEGIN
    -- token 테이블에서 creator를 조회하여 creator_treasury_balance 업데이트
    INSERT INTO creator_treasury_balance (account_id, token_id, amount)
    SELECT creator, NEW.token_id, NEW.creator_amount
    FROM token
    WHERE token_id = NEW.token_id
    ON CONFLICT (account_id, token_id)
    DO UPDATE SET
        amount = creator_treasury_balance.amount + EXCLUDED.amount;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 트리거 생성
DROP TRIGGER IF EXISTS trigger_update_creator_treasury_balance_from_distribute ON fee_distribute_history;
CREATE TRIGGER trigger_update_creator_treasury_balance_from_distribute
    AFTER INSERT ON fee_distribute_history
    FOR EACH ROW
    EXECUTE FUNCTION update_creator_treasury_balance_from_distribute();

-- 3. 트리거 활성화
ALTER TABLE fee_distribute_history ENABLE TRIGGER trigger_update_creator_treasury_balance_from_distribute;
