CREATE TABLE IF NOT EXISTS chester_reward_token(
    token_id VARCHAR(42) PRIMARY KEY,
    name VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    image_uri VARCHAR NOT NULL
);

-- Seed from existing chester_reward + token
-- INSERT INTO chester_reward_token (token_id, name, symbol, image_uri)
-- SELECT DISTINCT rw.token_id, t.name, t.symbol, t.image_uri
-- FROM chester_reward rw
-- INNER JOIN token t ON t.token_id = rw.token_id
-- ON CONFLICT (token_id) DO NOTHING;



-- Chester Round configuration (raffle_round 패턴)
CREATE TABLE IF NOT EXISTS chester_round(
    round BIGINT NOT NULL,
    start_at BIGINT NOT NULL,
    end_at BIGINT NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('ACTIVE', 'COMPLETED', 'READY')),
    merkle_root VARCHAR,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (round)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chester_round_single_active
    ON chester_round ((1)) WHERE status = 'ACTIVE';

-- Chester Reward (라운드별 보상 토큰)
CREATE TABLE IF NOT EXISTS chester_reward(
    round BIGINT NOT NULL REFERENCES chester_round(round),
    token_id VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL,  -- UNIT: token raw (wei) — reward pool size per token; box rewards scaled by 1e18 are validated against this (mock seed 1e21 = 1000 tokens)
    PRIMARY KEY (round, token_id)
);

-- -- Mock data: Round 1 (2026-02-06 ~ 2026-02-13 23:59 KST)
-- INSERT INTO chester_round (round, start_at, end_at, status)
-- VALUES (1, 1770392544, 1770994740, 'ACTIVE')
-- ON CONFLICT (round) DO NOTHING;

-- INSERT INTO chester_reward (round, token_id, amount)
-- VALUES (1, '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A', 1e21)
-- ON CONFLICT (round, token_id) DO NOTHING;

-- Volume/Fee는 별도 테이블 없이 실시간 쿼리로 계산:
-- total_usd_volume = SUM(swap.value) WHERE created_at BETWEEN start_at AND end_at
-- total_fee_usd = SUM(point_history.value) WHERE created_at BETWEEN start_at AND end_at
--
-- Reward usd_value 계산:
-- WMON: reward.amount * price.price (최신 block의 MON/USD)
-- 기타: reward.amount * market.price * price.price (토큰→MON→USD)


-- 상자별 보상 결과 (외부 정산 프로세스에서 INSERT)
CREATE TABLE IF NOT EXISTS chester_box_reward (
    round           BIGINT NOT NULL REFERENCES chester_round(round),
    level           INT NOT NULL,              -- 상자 레벨 1~4
    token_id        VARCHAR(42) NOT NULL,
    account_id      VARCHAR(42) NOT NULL,
    amount          NUMERIC NOT NULL,          -- 토큰 수량 | UNIT: token raw (wei) — (expected_qty * multiplier).floor() * 1e18 (chester_settlement_service.rs:171-175)
    status          VARCHAR NOT NULL CHECK (status IN ('AWAITING', 'CLAIMED')),
    proof           TEXT[] NOT NULL,            -- 머클 프루프
    transaction_hash VARCHAR NULL,             -- Claim 시 기록
    claimed_at      BIGINT NULL,
    created_at      BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (round, level, token_id, account_id)
);

CREATE INDEX IF NOT EXISTS idx_chester_box_reward_account
    ON chester_box_reward (account_id);
CREATE INDEX IF NOT EXISTS idx_chester_box_reward_round_account
    ON chester_box_reward (round, account_id);

-- AddReward 이벤트 이력 테이블
CREATE TABLE IF NOT EXISTS chester_add_reward_history (
    round           BIGINT NOT NULL,
    token_id        VARCHAR(42) NOT NULL,
    account_id      VARCHAR(42) NOT NULL,
    amount          NUMERIC NOT NULL,  -- UNIT: token raw (wei) — on-chain AddReward event amount (U256); mirrors chester_box_reward.amount scale (no INSERT writer in observer/scheduler src)
    transaction_hash VARCHAR NOT NULL,
    log_index       BIGINT NOT NULL,
    block_number    BIGINT NOT NULL,
    block_timestamp BIGINT NOT NULL,
    created_at      BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (round, account_id, token_id, transaction_hash, log_index)
);

-- BoxRewardClaimed 이벤트 이력 테이블 (중복 인덱싱 방지)
CREATE TABLE IF NOT EXISTS chester_box_reward_claim_history (
    round           BIGINT NOT NULL,
    level           BIGINT NOT NULL,
    token_id        VARCHAR(42) NOT NULL,
    account_id      VARCHAR(42) NOT NULL,
    amount          NUMERIC NOT NULL,  -- UNIT: token raw (wei) — on-chain BoxRewardClaimed event amount (U256); equals the claimed chester_box_reward.amount (no INSERT writer in observer/scheduler src)
    transaction_hash VARCHAR NOT NULL,
    log_index       BIGINT NOT NULL,
    block_number    BIGINT NOT NULL,
    block_timestamp BIGINT NOT NULL,
    created_at      BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (round, level, token_id, account_id, transaction_hash, log_index)
);

-- point_distribution에 'CHEST' activity_type 추가
ALTER TABLE point_distribution DROP CONSTRAINT IF EXISTS point_distribution_activity_type_check;
ALTER TABLE point_distribution ADD CONSTRAINT point_distribution_activity_type_check
    CHECK (activity_type IN ('CREATE','CURVE','GRADUATE','DEX','HYPEBOARD','RAFFLE','CHEST'));

-- 트리거 함수 업데이트: CHEST도 hype_point로 처리
CREATE OR REPLACE FUNCTION update_point_on_distribution_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.activity_type IN ('RAFFLE', 'CHEST') THEN
        -- RAFFLE, CHEST updates hype_point
        INSERT INTO point (account_id, hype_point)
        VALUES (NEW.account_id, NEW.amount)
        ON CONFLICT (account_id)
        DO UPDATE SET hype_point = point.hype_point + NEW.amount;
    ELSE
        -- Other activities update round_point
        INSERT INTO point (account_id, round_point)
        VALUES (NEW.account_id, NEW.amount)
        ON CONFLICT (account_id)
        DO UPDATE SET round_point = point.round_point + NEW.amount;
    END IF;

    RETURN NEW;
END;
$$;
