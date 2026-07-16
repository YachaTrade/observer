


CREATE TABLE IF NOT EXISTS token_treasury_balance(
    token_id VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL, -- UNIT: token raw (wei); accumulates lp_collect_history.token_amount (LpManagerCollect.tokenAmount). --collect history 가 insert 되면 업데이트
    PRIMARY KEY (token_id)
);


-- =====================================================
-- lp_collect_history INSERT 시 token_treasury_balance 업데이트
-- =====================================================

-- 1. 트리거 함수 생성
CREATE OR REPLACE FUNCTION update_token_treasury_balance_from_collect()
RETURNS TRIGGER AS $$
BEGIN
    -- token_treasury_balance 업데이트 (token_amount 누적)
    INSERT INTO token_treasury_balance (token_id, amount)
    VALUES (NEW.token_id, NEW.token_amount)
    ON CONFLICT (token_id)
    DO UPDATE SET
        amount = token_treasury_balance.amount + EXCLUDED.amount;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 트리거 생성
DROP TRIGGER IF EXISTS trigger_update_token_treasury_balance_from_collect ON lp_collect_history;
CREATE TRIGGER trigger_update_token_treasury_balance_from_collect
    AFTER INSERT ON lp_collect_history
    FOR EACH ROW
    EXECUTE FUNCTION update_token_treasury_balance_from_collect();

-- 3. 트리거 활성화
ALTER TABLE lp_collect_history ENABLE TRIGGER trigger_update_token_treasury_balance_from_collect;



-- Creator Treasury Balance(각 creator 가 받아야할 양)
CREATE TABLE IF NOT EXISTS creator_treasury_balance(
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL, -- UNIT: quote raw (wei); accumulates lp_collect_history.c_amount = monAmount * creatorTreasuryFeeRate / 1e6. --collect history 가 insert 되면 업데이트(token_id 로 token의 creator 로 accuont_id 찾음)
    PRIMARY KEY (account_id, token_id)
);

 -- 1. 트리거 함수 생성                                                                                                                                                                                                                                                                                                                                                                                                                       
 CREATE OR REPLACE FUNCTION update_creator_treasury_balance_from_collect()                                                                                                                                                                                                                                                                                                                                                                    
 RETURNS TRIGGER AS $$                                                                                                                                                                                                                                                                                                                                                                                                                        
 BEGIN                                                                                                                                                                                                                                                                                                                                                                                                                                        
     -- token 테이블에서 creator를 조회하여 creator_treasury_balance 업데이트                                                                                                                                                                                                                                                                                                                                                                 
     INSERT INTO creator_treasury_balance (account_id, token_id, amount)                                                                                                                                                                                                                                                                                                                                                                      
     SELECT creator, NEW.token_id, NEW.c_amount                                                                                                                                                                                                                                                                                                                                                                                               
     FROM token                                                                                                                                                                                                                                                                                                                                                                                                                               
     WHERE token_id = NEW.token_id                                                                                                                                                                                                                                                                                                                                                                                                            
     ON CONFLICT (account_id, token_id)                                                                                                                                                                                                                                                                                                                                                                                                       
     DO UPDATE SET                                                                                                                                                                                                                                                                                                                                                                                                                            
         amount = creator_treasury_balance.amount + EXCLUDED.amount;                                                                                                                                                                                                                                                                                                                                                                          
                                                                                                                                                                                                                                                                                                                                                                                                                                              
     RETURN NEW;                                                                                                                                                                                                                                                                                                                                                                                                                              
 END;                                                                                                                                                                                                                                                                                                                                                                                                                                         
 $$ LANGUAGE plpgsql;                                                                                                                                                                                                                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                                                                                              
 -- 2. 트리거 생성                                                                                                                                                                                                                                                                                                                                                                                                                            
 DROP TRIGGER IF EXISTS trigger_update_creator_treasury_balance_from_collect ON lp_collect_history;                                                                                                                                                                                                                                                                                                                                           
 CREATE TRIGGER trigger_update_creator_treasury_balance_from_collect                                                                                                                                                                                                                                                                                                                                                                          
     AFTER INSERT ON lp_collect_history                                                                                                                                                                                                                                                                                                                                                                                                       
     FOR EACH ROW                                                                                                                                                                                                                                                                                                                                                                                                                             
     EXECUTE FUNCTION update_creator_treasury_balance_from_collect();                                                                                                                                                                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                                                                                              
 -- 3. 트리거 활성화                                                                                                                                                                                                                                                                                                                                                                                                                          
 ALTER TABLE lp_collect_history ENABLE TRIGGER trigger_update_creator_treasury_balance_from_collect;  






CREATE TABLE IF NOT EXISTS creator_treasury_claim_history(
    token_id VARCHAR(42) NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL, -- UNIT: quote raw (wei); claimed amount deducted from creator_treasury_balance.amount (same unit).
    transaction_hash VARCHAR NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (token_id,account_id,transaction_hash,tx_index,log_index)
);

CREATE INDEX IF NOT EXISTS idx_creator_treasury_claim_history_account_id ON creator_treasury_claim_history (account_id,created_at DESC);

-- =====================================================
-- creator_treasury_claim_history INSERT 시 creator_treasury_balance 차감
-- =====================================================

-- 1. 트리거 함수 생성
CREATE OR REPLACE FUNCTION deduct_creator_treasury_balance_from_claim()
RETURNS TRIGGER AS $$
BEGIN
    -- creator_treasury_balance에서 amount 차감
    UPDATE creator_treasury_balance
    SET amount = amount - NEW.amount
    WHERE account_id = NEW.account_id
      AND token_id = NEW.token_id;

    -- 차감 후 잔액이 0 이하인 경우 행 삭제 (선택사항)
    DELETE FROM creator_treasury_balance
    WHERE account_id = NEW.account_id
      AND token_id = NEW.token_id
      AND amount <= 0;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 트리거 생성
DROP TRIGGER IF EXISTS trigger_deduct_creator_treasury_balance_from_claim ON creator_treasury_claim_history;
CREATE TRIGGER trigger_deduct_creator_treasury_balance_from_claim
    AFTER INSERT ON creator_treasury_claim_history
    FOR EACH ROW
    EXECUTE FUNCTION deduct_creator_treasury_balance_from_claim();

-- 3. 트리거 활성화
ALTER TABLE creator_treasury_claim_history ENABLE TRIGGER trigger_deduct_creator_treasury_balance_from_claim;


-- =====================================================
-- 참고:
-- - creator_treasury_claim_history INSERT 시 자동으로 creator_treasury_balance 차감
-- - claim한 amount만큼 차감됨
-- =====================================================



CREATE TABLE IF NOT EXISTS creator_treasury_merkle_root(
    merkle_root VARCHAR NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (merkle_root)
);

CREATE INDEX IF NOT EXISTS idx_creator_treasury_merkle_root_created_at ON creator_treasury_merkle_root (created_at DESC);




CREATE TABLE IF NOT EXISTS creator_reward(
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    amount NUMERIC(38, 0) NOT NULL, -- UNIT: quote raw (wei); sourced from creator_treasury_balance.amount, floored to integer (with_scale(0)) for merkle/contract.
    status VARCHAR NOT NULL CHECK (status IN ('AWAITING', 'CLAIMED')),
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    proof TEXT[] NOT NULL,
    PRIMARY KEY (account_id, token_id)
);





-- =====================================================
-- creator_treasury_claim_history INSERT 시 creator_reward status를 CLAIMED로 업데이트
-- =====================================================

-- 1. 트리거 함수 생성
CREATE OR REPLACE FUNCTION update_creator_reward_status_from_claim()
RETURNS TRIGGER AS $$
BEGIN
    -- creator_reward의 status를 CLAIMED로 업데이트
    UPDATE creator_reward
    SET status = 'CLAIMED'
    WHERE account_id = NEW.account_id
      AND token_id = NEW.token_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 트리거 생성
DROP TRIGGER IF EXISTS trigger_update_creator_reward_status_from_claim ON creator_treasury_claim_history;
CREATE TRIGGER trigger_update_creator_reward_status_from_claim
    AFTER INSERT ON creator_treasury_claim_history
    FOR EACH ROW
    EXECUTE FUNCTION update_creator_reward_status_from_claim();

-- 3. 트리거 활성화
ALTER TABLE creator_treasury_claim_history ENABLE TRIGGER trigger_update_creator_reward_status_from_claim;


-- =====================================================
-- 참고:
-- - lp_collect_history INSERT 시 자동으로 token_treasury_balance 업데이트
-- - token_amount를 누적 합산
-- =====================================================



