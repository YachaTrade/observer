-- Position V2: Transfer 기반 PnL 추적 (현금 흐름 기반)
-- 모든 케이스 커버: Buy, Sell, LP Mint, LP Burn, Transfer, Airdrop

-- 1. 기존 트리거 및 함수 삭제
DROP TRIGGER IF EXISTS trg_position_on_swap ON swap;
DROP FUNCTION IF EXISTS update_position_on_swap();

-- 2. 기존 position 테이블 삭제
DROP TABLE IF EXISTS position CASCADE;

-- 3. position_history 테이블 생성 (분석된 position 변화 기록)
CREATE TABLE IF NOT EXISTS position_history (
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,

    -- Quote 흐름 (이 TX에서의 변화량)
    quote_in NUMERIC NOT NULL DEFAULT 0,   -- UNIT: quote raw (wei)
    quote_out NUMERIC NOT NULL DEFAULT 0,  -- UNIT: quote raw (wei)

    -- USD 흐름
    usd_in NUMERIC NOT NULL DEFAULT 0,   -- UNIT: USD (human)
    usd_out NUMERIC NOT NULL DEFAULT 0,  -- UNIT: USD (human)

    -- Token 흐름
    token_in NUMERIC NOT NULL DEFAULT 0,   -- UNIT: token raw (wei)
    token_out NUMERIC NOT NULL DEFAULT 0,  -- UNIT: token raw (wei)

    -- Transfer 메타데이터
    -- transfer_type: 'buy', 'sell', 'transfer_out', 'transfer_in', NULL(LP 등)
    transfer_type VARCHAR(20),
    sender_address VARCHAR(42),  -- transfer_in 시 sender 주소

    -- TX 정보
    transaction_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    created_at BIGINT NOT NULL,

    PRIMARY KEY (account_id, token_id, transaction_hash, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_position_history_account ON position_history(account_id);
CREATE INDEX IF NOT EXISTS idx_position_history_token ON position_history(token_id);
CREATE INDEX IF NOT EXISTS idx_position_history_tx ON position_history(transaction_hash);
CREATE INDEX IF NOT EXISTS idx_position_history_block ON position_history(block_number, tx_index, log_index);
CREATE INDEX IF NOT EXISTS idx_position_history_transfer_type ON position_history(transfer_type);
CREATE INDEX IF NOT EXISTS idx_position_history_sender_address ON position_history(sender_address);

-- 4. position 테이블 생성 (누적 position)
CREATE TABLE IF NOT EXISTS position (
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,

    -- Quote 흐름 (누적)
    quote_in NUMERIC NOT NULL DEFAULT 0,       -- 수입 (매도, LP 제거 시 받음) | UNIT: quote raw (wei)
    quote_out NUMERIC NOT NULL DEFAULT 0,      -- 지출 (매수, LP 추가 시 지불) | UNIT: quote raw (wei)

    -- USD 흐름 (누적)
    usd_in NUMERIC NOT NULL DEFAULT 0,         -- 수입 (USD) | UNIT: USD (human)
    usd_out NUMERIC NOT NULL DEFAULT 0,        -- 지출 (USD) | UNIT: USD (human)

    -- Token 흐름 (누적)
    token_in NUMERIC NOT NULL DEFAULT 0,       -- 획득 (매수, LP 제거, Transfer 받음) | UNIT: token raw (wei)
    token_out NUMERIC NOT NULL DEFAULT 0,      -- 지출 (매도, LP 추가, Transfer 보냄) | UNIT: token raw (wei)

    -- 메타데이터
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,

    PRIMARY KEY (account_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_position_account ON position(account_id);
CREATE INDEX IF NOT EXISTS idx_position_token ON position(token_id);

-- 5. position_history INSERT 시 position 자동 업데이트 트리거 (cost basis 이전 포함)
CREATE OR REPLACE FUNCTION update_position_on_history()
RETURNS TRIGGER AS $$
DECLARE
    sender_position RECORD;
    avg_cost_quote NUMERIC;
    avg_cost_usd NUMERIC;
    transfer_cost_quote NUMERIC;
    transfer_cost_usd NUMERIC;
    current_balance NUMERIC;
BEGIN
    -- transfer_out인 경우: sender의 cost basis 계산하여 quote_in에 기록
    IF NEW.transfer_type = 'transfer_out' THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.account_id AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;

            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd := sender_position.usd_out / sender_position.token_in;

                transfer_cost_quote := avg_cost_quote * NEW.token_out;
                transfer_cost_usd := avg_cost_usd * NEW.token_out;

                NEW.quote_in := transfer_cost_quote;
                NEW.usd_in := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    -- transfer_in인 경우: sender_address의 cost basis 가져와서 quote_out에 기록
    IF NEW.transfer_type = 'transfer_in' AND NEW.sender_address IS NOT NULL THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.sender_address AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;

            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd := sender_position.usd_out / sender_position.token_in;

                transfer_cost_quote := avg_cost_quote * NEW.token_in;
                transfer_cost_usd := avg_cost_usd * NEW.token_in;

                NEW.quote_out := transfer_cost_quote;
                NEW.usd_out := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    -- position 테이블 업데이트
    INSERT INTO position (
        account_id, token_id,
        quote_in, quote_out,
        usd_in, usd_out,
        token_in, token_out,
        created_at, updated_at
    )
    VALUES (
        NEW.account_id, NEW.token_id,
        NEW.quote_in, NEW.quote_out,
        NEW.usd_in, NEW.usd_out,
        NEW.token_in, NEW.token_out,
        NEW.created_at, NEW.created_at
    )
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        quote_in = position.quote_in + EXCLUDED.quote_in,
        quote_out = position.quote_out + EXCLUDED.quote_out,
        usd_in = position.usd_in + EXCLUDED.usd_in,
        usd_out = position.usd_out + EXCLUDED.usd_out,
        token_in = position.token_in + EXCLUDED.token_in,
        token_out = position.token_out + EXCLUDED.token_out,
        updated_at = EXCLUDED.updated_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- BEFORE INSERT trigger (NEW 값 수정 가능)
CREATE TRIGGER trg_position_on_history
BEFORE INSERT ON position_history
FOR EACH ROW
EXECUTE FUNCTION update_position_on_history();



-- Fee History: Buy 이벤트에서 발생한 fee 추적
-- Curve, DEX Swap, DexRouter Buy 이벤트에서 fee 계산 및 누적
-- PnL 조회 시 position과 JOIN해서 사용

-- 1. fee_history 테이블 생성 (개별 fee 이벤트)
CREATE TABLE IF NOT EXISTS fee_history (
    transaction_hash VARCHAR(66) NOT NULL,
    tx_index INT NOT NULL DEFAULT 0,
    log_index INT NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    quote_amount NUMERIC NOT NULL,      -- Quote token 기준 fee | UNIT: quote raw (wei)
    usd_amount NUMERIC NOT NULL,         -- USD 기준 fee | UNIT: USD (human)
    fee_type VARCHAR(20) NOT NULL,       -- 'create', 'curve_buy', 'swap_buy', 'dex_router_buy'
    block_number BIGINT NOT NULL,
    created_at BIGINT NOT NULL,

    PRIMARY KEY (transaction_hash, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_fee_history_tx ON fee_history(transaction_hash);
CREATE INDEX IF NOT EXISTS idx_fee_history_account_token ON fee_history(account_id, token_id);
CREATE INDEX IF NOT EXISTS idx_fee_history_block ON fee_history(block_number);

-- 2. fee 테이블 생성 (account, token별 누적)
CREATE TABLE IF NOT EXISTS fee (
    account_id VARCHAR(42) NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    quote_amount NUMERIC NOT NULL DEFAULT 0,  -- 누적 fee (Quote) | UNIT: quote raw (wei)
    usd_amount NUMERIC NOT NULL DEFAULT 0,     -- 누적 fee (USD) | UNIT: USD (human)
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,

    PRIMARY KEY (account_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_fee_account ON fee(account_id);
CREATE INDEX IF NOT EXISTS idx_fee_token ON fee(token_id);

-- 3. fee_history INSERT 시 fee 자동 업데이트 트리거
CREATE OR REPLACE FUNCTION update_fee_on_history()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO fee (
        account_id, token_id,
        quote_amount, usd_amount,
        created_at, updated_at
    )
    VALUES (
        NEW.account_id, NEW.token_id,
        NEW.quote_amount, NEW.usd_amount,
        NEW.created_at, NEW.created_at
    )
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        quote_amount = fee.quote_amount + EXCLUDED.quote_amount,
        usd_amount = fee.usd_amount + EXCLUDED.usd_amount,
        updated_at = EXCLUDED.updated_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_fee_on_history
AFTER INSERT ON fee_history
FOR EACH ROW
EXECUTE FUNCTION update_fee_on_history();
