
-- =====================================================
-- 참고: 이제 Rust 코드에서는 balance_history만 INSERT하면 됨
-- balance 테이블은 트리거가 자동으로 관리
-- =====================================================



-- =====================================================
-- balance_history INSERT 시 자동으로 balance 테이블 업데이트
-- =====================================================

CREATE TABLE IF NOT EXISTS balance_history(
    token_id VARCHAR(42) NOT NULL ,
    account_id VARCHAR(42) NOT NULL,
    balance NUMERIC NOT NULL,   -- UNIT: token raw (wei) (ERC20 balanceOf at block, no scaling; observer src/event/common/token/stream.rs:702-713)
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    log_index INT NOT NULL,
    tx_index INT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (token_id, account_id, transaction_hash,tx_index, log_index)
);

-- Backfill / migration support: DISTINCT ON (account_id, token_id) ORDER BY
-- block_number DESC, tx_index DESC, log_index DESC is the canonical
-- "latest balance per (account, token)" query (used by
-- v2_upgrade_triggers_and_backfill.sql). Without this index the sort spills
-- to disk on production-size history tables.
CREATE INDEX IF NOT EXISTS idx_balance_history_acct_token_block
    ON balance_history (account_id, token_id, block_number DESC, tx_index DESC, log_index DESC);



-- 1. 트리거 함수 생성
-- The `WHERE balance.created_at <= EXCLUDED.created_at` guard prevents an
-- out-of-order balance_history INSERT (older row arriving after a newer one,
-- e.g., from parallel indexer workers or a backfill) from overwriting the
-- current balance with stale data. Without the guard, the ON CONFLICT path
-- would unconditionally write the older `balance` value.
CREATE OR REPLACE FUNCTION update_balance_from_history()
RETURNS TRIGGER AS $$
BEGIN
    -- 새로 INSERT된 balance로 balance 테이블 업데이트
    INSERT INTO balance (account_id, token_id, balance, created_at)
    VALUES (NEW.account_id, NEW.token_id, NEW.balance, NEW.created_at)
    ON CONFLICT (account_id, token_id)
    DO UPDATE SET
        balance = EXCLUDED.balance,
        created_at = EXCLUDED.created_at
    WHERE balance.created_at <= EXCLUDED.created_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. 트리거 생성
DROP TRIGGER IF EXISTS trigger_update_balance_from_history ON balance_history;
CREATE TRIGGER trigger_update_balance_from_history
    AFTER INSERT ON balance_history
    FOR EACH ROW
    EXECUTE FUNCTION update_balance_from_history();

-- 3. 트리거 활성화
ALTER TABLE balance_history ENABLE TRIGGER trigger_update_balance_from_history;

CREATE TABLE IF NOT EXISTS balance(
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    balance NUMERIC NOT NULL DEFAULT 0,   -- UNIT: token raw (wei) (latest balance_history.balance; trigger update_balance_from_history)
    created_at BIGINT NOT NULL,
    PRIMARY KEY (account_id, token_id)
);

-- API Search 모듈 최적화: 계정별 총 자산 계산 서브쿼리용 인덱스
CREATE INDEX IF NOT EXISTS idx_balance_account_balance ON balance (account_id, balance DESC) WHERE balance >= 1000000000000000000;
CREATE INDEX IF NOT EXISTS idx_balance_account_token ON balance (account_id,token_id,balance DESC);
CREATE INDEX IF NOT EXISTS idx_balance_token_account ON balance (token_id,account_id,balance DESC);

-- API Trading 모듈 최적화: position 조회용 복합 인덱스들
CREATE INDEX IF NOT EXISTS idx_balance_token_balance ON balance (token_id, balance DESC) WHERE balance > 0;
CREATE INDEX IF NOT EXISTS idx_balance_account_positive ON balance (account_id, balance DESC) WHERE balance > 0;
-- (Two unconditional duplicates of idx_balance_token_balance and
--  idx_balance_account_balance used to live here. CREATE INDEX IF NOT EXISTS
--  checks the name only, so on fresh DBs the partial-index versions above
--  always won and the unconditional copies were dead code. Removed.)

CREATE OR REPLACE FUNCTION delete_zero_balance()
  RETURNS TRIGGER AS $$
  BEGIN
      IF NEW.balance = 0 THEN
          DELETE FROM balance WHERE account_id = NEW.account_id AND token_id = NEW.token_id;
          RETURN NULL;
      END IF;
      RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;

  CREATE TRIGGER trigger_delete_zero_balance
  AFTER UPDATE ON balance
  FOR EACH ROW
  EXECUTE FUNCTION delete_zero_balance();


CREATE OR REPLACE FUNCTION update_token_holder_count_v2() 
RETURNS TRIGGER AS $$
DECLARE
  v_old_positive BOOLEAN;
  v_new_positive BOOLEAN;
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- INSERT: balance > 0이면 holder_count 증가
    IF NEW.balance > 0 THEN
      UPDATE token 
      SET token_holder_count = token_holder_count + 1
      WHERE token_id = NEW.token_id;
    END IF;
    
  ELSIF TG_OP = 'UPDATE' THEN
    v_old_positive := OLD.balance > 0;
    v_new_positive := NEW.balance > 0;
    
    IF NOT v_old_positive AND v_new_positive THEN
      -- 0 이하 → 양수: holder 증가
      UPDATE token 
      SET token_holder_count = token_holder_count + 1
      WHERE token_id = NEW.token_id;
      
    ELSIF v_old_positive AND NOT v_new_positive THEN
      -- 양수 → 0 이하: holder 감소
      UPDATE token 
      SET token_holder_count = GREATEST(token_holder_count - 1, 0)
      WHERE token_id = NEW.token_id;
    END IF;
    
  ELSIF TG_OP = 'DELETE' THEN
    -- DELETE: balance > 0이었으면 holder_count 감소
    IF OLD.balance > 0 THEN
      UPDATE token 
      SET token_holder_count = GREATEST(token_holder_count - 1, 0)
      WHERE token_id = OLD.token_id;
    END IF;
  END IF;
  
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 새로운 트리거 생성
CREATE TRIGGER trg_update_holder_count
AFTER INSERT OR UPDATE OR DELETE ON balance
FOR EACH ROW
EXECUTE FUNCTION update_token_holder_count_v2();
