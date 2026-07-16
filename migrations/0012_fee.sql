CREATE TABLE IF NOT EXISTS set_fee_history(
    pool_id VARCHAR(42) NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    fee_protocol0_old SMALLINT NOT NULL,
    fee_protocol1_old SMALLINT NOT NULL,
    fee_protocol0_new SMALLINT NOT NULL,
    fee_protocol1_new SMALLINT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, block_number, transaction_hash, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_set_fee_history_pool ON set_fee_history (pool_id);
