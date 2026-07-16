CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS monad_airdrop(
   account_id VARCHAR(42) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS raffle_round(
   round BIGINT NOT NULL,
   start_at BIGINT NOT NULL,
   end_at BIGINT NOT NULL,
   status VARCHAR NOT NULL CHECK (status IN ('ACTIVE', 'COMPLETED','READY')),
   sequence_number BIGINT,
   random_number VARCHAR(66),
   created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
   PRIMARY KEY (round),
   -- 시간 범위 겹침 방지
   EXCLUDE USING gist (int8range(start_at, end_at) WITH &&)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_round_single_active
  ON raffle_round ((1)) WHERE status = 'ACTIVE';


CREATE TABLE IF NOT EXISTS raffle(
    id SERIAL PRIMARY KEY,
    round BIGINT NOT NULL,
    epoch BIGINT NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
);

CREATE INDEX IF NOT EXISTS idx_raffle_account_created_at ON raffle (account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_raffle_round_account ON raffle (round, account_id);
CREATE INDEX IF NOT EXISTS idx_raffle_epoch ON raffle (epoch);
CREATE INDEX IF NOT EXISTS idx_raffle_round_epoch_account ON raffle (round, epoch, account_id);

-- Create raffle_winner table
-- GENERAL_MONAD: General raffle, MON prize
-- GENERAL_HYPE: General raffle, HYPE prize
-- MONAD_AIRDROP_MONAD: Monad Airdrop raffle, MON prize
-- MONAD_AIRDROP_HYPE: Monad Airdrop raffle, HYPE prize
CREATE TABLE IF NOT EXISTS raffle_winner (
    id SERIAL PRIMARY KEY,
    round BIGINT NOT NULL,
    raffle_id INT NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    rank INT NOT NULL,
    type VARCHAR(25) NOT NULL DEFAULT 'GENERAL_MONAD' CHECK (type IN ('GENERAL_MONAD', 'GENERAL_HYPE', 'MONAD_AIRDROP_MONAD', 'MONAD_AIRDROP_HYPE','WHALE_MONAD','WHALE_HYPE')),
    transaction_hash VARCHAR(66),
    amount NUMERIC NOT NULL, -- UNIT: dual by type -> *_MONAD = quote raw (wei) (prize_mon * 1e18, raffle_service.rs get_prize_whale/general); *_HYPE = points (int) (RAFFLE_HYPE_POINT_REWARD = 200).
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    CONSTRAINT fk_raffle_winner_round FOREIGN KEY (round) REFERENCES raffle_round(round),
    CONSTRAINT fk_raffle_winner_raffle FOREIGN KEY (raffle_id) REFERENCES raffle(id)
);

CREATE INDEX IF NOT EXISTS idx_raffle_winner_round ON raffle_winner (round);
CREATE INDEX IF NOT EXISTS idx_raffle_winner_account ON raffle_winner (account_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raffle_winner_round_type_rank ON raffle_winner (round, type, rank);





-- =====================================================
-- 테스트 쿼리문
-- =====================================================
/*
-- 1. raffle_round 데이터 생성 (현재시간 기준)
DO $$
DECLARE
    now_ts BIGINT := EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT;
BEGIN
    -- Round 1: 30일 전 ~ 1일 전 (완료)
    INSERT INTO raffle_round (round, start_at, end_at, status) VALUES
    (1, now_ts - (30 * 24 * 60 * 60), now_ts - (1 * 24 * 60 * 60), 'COMPLETED')
    ON CONFLICT (round) DO NOTHING;

    -- Round 2: 1일 전 ~ 29일 후 (현재 진행중)
    INSERT INTO raffle_round (round, start_at, end_at, status) VALUES
    (2, now_ts - (1 * 24 * 60 * 60), now_ts + (29 * 24 * 60 * 60), 'ACTIVE')
    ON CONFLICT (round) DO NOTHING;

    -- Round 3: 30일 후 ~ 60일 후 (대기중)
    INSERT INTO raffle_round (round, start_at, end_at, status) VALUES
    (3, now_ts + (30 * 24 * 60 * 60), now_ts + (60 * 24 * 60 * 60), 'READY')
    ON CONFLICT (round) DO NOTHING;
END $$;

-- 2. 테스트용 account 생성
INSERT INTO account (account_id) VALUES
('0x1111111111111111111111111111111111111111'),
('0x2222222222222222222222222222222222222222'),
('0x3333333333333333333333333333333333333333')
ON CONFLICT (account_id) DO NOTHING;

-- 3. monad_airdrop에 일부 계정만 추가
-- 0x1111... : monad_airdrop O -> raffle 생성됨
-- 0x2222... : monad_airdrop O -> raffle 생성됨
-- 0x3333... : monad_airdrop X -> raffle 생성 안됨
INSERT INTO monad_airdrop (account_id) VALUES
('0x1111111111111111111111111111111111111111'),
('0x2222222222222222222222222222222222222222')
ON CONFLICT (account_id) DO NOTHING;

-- 4. point 테스트 케이스

-- Case 1: 0x1111 (monad_airdrop O) - 0 -> 50 round_point (raffle 2개 생성)
INSERT INTO point (account_id, round_point) VALUES
('0x1111111111111111111111111111111111111111', 50)
ON CONFLICT (account_id) DO UPDATE SET round_point = 50, raffle_count = 0;

-- Case 2: 0x1111 (monad_airdrop O) - 50 -> 100 round_point (raffle 3개 추가: 5-2=3)
UPDATE point SET round_point = 100
WHERE account_id = '0x1111111111111111111111111111111111111111';

-- Case 3: 0x1111 (monad_airdrop O) - 100 -> 150 round_point (raffle 2개 추가: 7-5=2)
UPDATE point SET round_point = 150
WHERE account_id = '0x1111111111111111111111111111111111111111';

-- Case 4: 0x2222 (monad_airdrop O) - 0 -> 80 round_point (raffle 4개 생성)
INSERT INTO point (account_id, round_point) VALUES
('0x2222222222222222222222222222222222222222', 80)
ON CONFLICT (account_id) DO UPDATE SET round_point = 80, raffle_count = 0;

-- Case 5: 0x3333 (monad_airdrop X) - 0 -> 100 round_point (raffle 0개, 생성 안됨!)
INSERT INTO point (account_id, round_point) VALUES
('0x3333333333333333333333333333333333333333', 100)
ON CONFLICT (account_id) DO UPDATE SET round_point = 100, raffle_count = 0;

-- Case 6: 0x3333 (monad_airdrop X) - 100 -> 200 round_point (여전히 raffle 0개)
UPDATE point SET round_point = 200
WHERE account_id = '0x3333333333333333333333333333333333333333';

-- 5. 결과 확인

-- raffle_round 상태 확인
SELECT
    round,
    status,
    TO_TIMESTAMP(start_at) as start_time,
    TO_TIMESTAMP(end_at) as end_time,
    CASE
        WHEN start_at <= EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
         AND end_at >= EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
        THEN 'CURRENT'
        WHEN start_at > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
        THEN 'FUTURE'
        ELSE 'PAST'
    END as time_status
FROM raffle_round
ORDER BY round;

-- monad_airdrop 확인
SELECT
    ma.account_id,
    CASE WHEN ma.account_id IS NOT NULL THEN 'O' ELSE 'X' END as in_airdrop
FROM account a
LEFT JOIN monad_airdrop ma ON a.account_id = ma.account_id
WHERE a.account_id IN (
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222',
    '0x3333333333333333333333333333333333333333'
)
ORDER BY a.account_id;

-- point 확인 (monad_airdrop 여부와 함께)
SELECT
    p.account_id,
    CASE WHEN ma.account_id IS NOT NULL THEN 'O' ELSE 'X' END as in_airdrop,
    p.round_point,
    p.raffle_count,
    p.round_point / 20 as expected_raffle_count
FROM point p
LEFT JOIN monad_airdrop ma ON p.account_id = ma.account_id
WHERE p.account_id IN (
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222',
    '0x3333333333333333333333333333333333333333'
)
ORDER BY p.account_id;

-- raffle 개수 확인
SELECT
    r.account_id,
    COUNT(*) as actual_raffle_count,
    r.round
FROM raffle r
WHERE r.account_id IN (
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222',
    '0x3333333333333333333333333333333333333333'
)
GROUP BY r.account_id, r.round
ORDER BY r.account_id, r.round;

-- 전체 raffle 상세
SELECT
    id,
    round,
    account_id,
    TO_TIMESTAMP(created_at) as created_time
FROM raffle
WHERE account_id IN (
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222',
    '0x3333333333333333333333333333333333333333'
)
ORDER BY account_id, created_at;

-- 6. 예상 결과:
-- account 0x1111... (monad_airdrop O): raffle 7개 (2+3+2), round_point=150, round=2
-- account 0x2222... (monad_airdrop O): raffle 4개, round_point=80, round=2
-- account 0x3333... (monad_airdrop X): raffle 0개, round_point=200, raffle_count=0 ⭐

-- 7. 정리 (테스트 후)
DELETE FROM raffle WHERE account_id IN (
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222',
    '0x3333333333333333333333333333333333333333'
);
DELETE FROM point WHERE account_id IN (
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222',
    '0x3333333333333333333333333333333333333333'
);
DELETE FROM monad_airdrop WHERE account_id IN (
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222'
);
DELETE FROM raffle_round WHERE round IN (1, 2, 3);
*/

CREATE TABLE IF NOT EXISTS monad_airdrop(
   account_id VARCHAR(42) PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS prize(
    round BIGINT NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    amount BIGINT NOT NULL,
    PRIMARY KEY (round, account_id)
);

CREATE INDEX IF NOT EXISTS idx_round ON prize (round);