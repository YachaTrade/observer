CREATE EXTENSION IF NOT EXISTS btree_gist;

-- HYPE BOARD

CREATE TABLE IF NOT EXISTS hype_token(
    epoch BIGINT NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    vote NUMERIC NOT NULL DEFAULT 0, -- UNIT: points (int); total hype points voted on this token (api-server vote(): hype_token.vote += amount, where amount moves from point.round_point to point.hype_point).
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (epoch, token_id)
);




-- HYPE BOARD
CREATE TABLE IF NOT EXISTS epoch (
    epoch BIGINT PRIMARY KEY,
    start_at BIGINT NOT NULL,
    end_at BIGINT NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('ACTIVE', 'COMPLETED','READY')),
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    -- 시간 범위 겹침 방지
    EXCLUDE USING gist (int8range(start_at, end_at) WITH &&)
);


CREATE UNIQUE INDEX IF NOT EXISTS idx_epoch_single_active
  ON epoch ((1)) WHERE status = 'ACTIVE';



CREATE TABLE IF NOT EXISTS point_history(
    account_id VARCHAR(42) NOT NULL,
    point_type VARCHAR NOT NULL CHECK (point_type IN ('CREATE', 'GRADUATE', 'CURVE', 'DEX')),
    value NUMERIC NOT NULL, -- UNIT: USD (human); activity volume in USD (observer PointBatchData.value = quote_amount/decimals * price), later multiplied by point rate to derive points.
    transaction_hash VARCHAR NOT NULL,
    tx_index INT NOT NULL,
    log_index INT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (account_id, transaction_hash, tx_index,log_index)
);
-- Point History 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요 (INSERT만 수행)

CREATE TABLE IF NOT EXISTS point (
    account_id VARCHAR(42) NOT NULL,
    round_point BIGINT NOT NULL DEFAULT 0, --unused point
    hype_point BIGINT NOT NULL DEFAULT 0, --used point
    raffle_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id)
);
-- Point 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요


CREATE TABLE IF NOT EXISTS total_hype_point(
    id INTEGER PRIMARY KEY DEFAULT 1,
    hype_point BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT single_row CHECK (id = 1)
);

INSERT INTO total_hype_point (id, hype_point) 
SELECT 1, 0 
WHERE NOT EXISTS (SELECT 1 FROM total_hype_point WHERE id = 1);


-- 트리거 함수 생성
CREATE OR REPLACE FUNCTION update_total_hype_point()
RETURNS TRIGGER AS $$
BEGIN
    -- INSERT 시: 새로운 spend_point 추가
    IF TG_OP = 'INSERT' THEN
        UPDATE total_hype_point 
        SET hype_point = hype_point + NEW.hype_point
        WHERE id = 1;
        RETURN NEW;
    END IF;
    
    -- UPDATE 시: 차이만큼 업데이트
    IF TG_OP = 'UPDATE' THEN
        UPDATE total_hype_point 
        SET hype_point = hype_point + (NEW.hype_point - OLD.hype_point)
        WHERE id = 1;
        RETURN NEW;
    END IF;
    
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 트리거 생성
DROP TRIGGER IF EXISTS point_hype_trigger ON point;
CREATE TRIGGER point_hype_trigger
    AFTER INSERT OR UPDATE ON point
    FOR EACH ROW
    EXECUTE FUNCTION update_total_hype_point();

-- 기존 데이터가 있다면 총합 계산해서 초기화
UPDATE total_hype_point 
SET hype_point = (
    SELECT COALESCE(SUM(hype_point), 0) 
    FROM point
) 
WHERE id = 1;



-- 1. 테이블 생성
CREATE TABLE hype_point_leaderboard_count (
    id INTEGER NOT NULL DEFAULT 1 PRIMARY KEY,
    total_count BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT single_row_leaderboard CHECK (id = 1)
);

-- 2. 초기 row 삽입 (기존 point 테이블 count로 초기화)
INSERT INTO hype_point_leaderboard_count (id, total_count)
VALUES (1, (SELECT COUNT(*) FROM point));

-- 3. 트리거 함수 생성
CREATE OR REPLACE FUNCTION update_hype_point_leaderboard_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE hype_point_leaderboard_count SET total_count = total_count + 1 WHERE id = 1;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE hype_point_leaderboard_count SET total_count = total_count - 1 WHERE id = 1;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 4. 트리거 생성
CREATE TRIGGER hype_point_leaderboard_count_trigger
    AFTER INSERT OR DELETE ON point
    FOR EACH ROW
    EXECUTE FUNCTION update_hype_point_leaderboard_count();



CREATE TABLE IF NOT EXISTS point_distribution(
    epoch BIGINT NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    activity_type VARCHAR NOT NULL CHECK (activity_type IN ('CREATE','CURVE','GRADUATE','DEX','HYPEBOARD','RAFFLE')),
    amount BIGINT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (account_id, activity_type, epoch, created_at)
);
CREATE INDEX IF NOT EXISTS idx_point_distribution_account_id ON point_distribution (account_id);
CREATE INDEX IF NOT EXISTS idx_point_distribution_account_epoch ON point_distribution (account_id, epoch);
CREATE INDEX IF NOT EXISTS idx_point_distribution_created_at ON point_distribution (created_at DESC);


-- Update trigger function to handle RAFFLE type (update hype_point instead of round_point)
CREATE OR REPLACE FUNCTION update_point_on_distribution_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.activity_type = 'RAFFLE' THEN
        -- RAFFLE updates hype_point
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

  -- Create trigger to automatically update point table
  DROP TRIGGER IF EXISTS point_update_trigger ON point_distribution;
  CREATE TRIGGER point_update_trigger
      AFTER INSERT ON point_distribution
      FOR EACH ROW
      EXECUTE FUNCTION update_point_on_distribution_insert();

 CREATE TABLE IF NOT EXISTS account_point_distribution_count (
      account_id VARCHAR(42) PRIMARY KEY,
      total_count BIGINT NOT NULL DEFAULT 0,
      last_updated_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  );

  -- point_distribution에 INSERT될 때 카운트 증가
  CREATE OR REPLACE FUNCTION update_point_distribution_count_on_insert()
  RETURNS trigger
  LANGUAGE plpgsql
  AS $$
  BEGIN
      INSERT INTO account_point_distribution_count (account_id, total_count, last_updated_at)
      VALUES (NEW.account_id, 1, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT)
      ON CONFLICT (account_id)
      DO UPDATE SET
          total_count = account_point_distribution_count.total_count + 1,
          last_updated_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT;
      RETURN NEW;
  END;
  $$;

CREATE TRIGGER point_distribution_count_trigger
AFTER INSERT ON point_distribution
FOR EACH ROW EXECUTE FUNCTION update_point_distribution_count_on_insert();



-- Point Distribution Records 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요
CREATE TABLE IF NOT EXISTS reward_add_history(
      epoch BIGINT NOT NULL,
      token_id VARCHAR(42) NOT NULL,
      account_id VARCHAR(42) NOT NULL,
      amount NUMERIC NOT NULL, -- UNIT: token raw (wei); on-chain RewardPool AddReward.amount (reward/buy-back token, e.g. HYPE) stored raw by observer.
      total_amount NUMERIC NOT NULL DEFAULT 0, -- UNIT: token raw (wei); running SUM of amount per (epoch, account_id, token_id), computed by calculate_reward_total_amount() trigger.
      transaction_hash VARCHAR(66) NOT NULL,
      log_index BIGINT NOT NULL,
      created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
      PRIMARY KEY (epoch, account_id, token_id, transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_reward_add_history_sum_lookup
  ON reward_add_history(epoch, account_id, token_id, amount);

CREATE INDEX IF NOT EXISTS idx_reward_add_history_account_created_at
ON reward_add_history(account_id, created_at DESC);

CREATE OR REPLACE FUNCTION calculate_reward_total_amount()
RETURNS TRIGGER AS $$
BEGIN
    -- INSERT시 total_amount 계산
    NEW.total_amount := (
        SELECT COALESCE(SUM(amount), 0) + NEW.amount
        FROM reward_add_history
        WHERE epoch = NEW.epoch
        AND account_id = NEW.account_id
        AND token_id = NEW.token_id
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 트리거 생성
CREATE TRIGGER trigger_calculate_reward_total
BEFORE INSERT ON reward_add_history
FOR EACH ROW
EXECUTE FUNCTION calculate_reward_total_amount();


CREATE TABLE IF NOT EXISTS reward_add_history_count(
    account_id VARCHAR(42) PRIMARY KEY,
    total_count BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
);


CREATE OR REPLACE FUNCTION update_reward_add_history_count()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO reward_add_history_count (account_id, total_count, updated_at)
    VALUES (NEW.account_id, 1, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT)
    ON CONFLICT (account_id) 
    DO UPDATE SET 
        total_count = reward_add_history_count.total_count + 1,
        updated_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 트리거 생성
CREATE TRIGGER trigger_update_reward_add_history_count
    AFTER INSERT ON reward_add_history
    FOR EACH ROW
    EXECUTE FUNCTION update_reward_add_history_count();

-- 기존 데이터에 대한 초기 카운트 계산 (필요한 경우)
INSERT INTO reward_add_history_count (account_id, total_count, updated_at)
SELECT 
    account_id,
    COUNT(*) as total_count,
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT as updated_at
FROM reward_add_history
GROUP BY account_id
ON CONFLICT (account_id) DO NOTHING;


CREATE TABLE IF NOT EXISTS reward_pool(
    epoch BIGINT NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL, -- UNIT: token raw (wei); SUM of reward_add_history.amount per (epoch, token_id) (observer reward.rs pool upsert) = total reward pool to split among voters.
    merkle_root VARCHAR(66) DEFAULT NULL,
    PRIMARY KEY (epoch, token_id)
);


CREATE TABLE IF NOT EXISTS reward(
    epoch BIGINT NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL, -- UNIT: token raw (wei); voter's share of reward_pool.amount, = account_vote * total_reward / total_votes, floored (with_scale(0)) for contract.
    status VARCHAR NOT NULL CHECK (status IN ('AWAITING', 'CLAIMED')),
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    proof TEXT[] NOT NULL,
    vote_amount NUMERIC NOT NULL, -- UNIT: points (int); account's total votes in this epoch/token (sum of vote_history.vote = spent hype points), used as reward split weight.
    transaction_hash VARCHAR NULL, -- Claim 시 기록
    claim_at BIGINT NULL,
    PRIMARY KEY (epoch, token_id, account_id)
);


CREATE INDEX IF NOT EXISTS idx_reward_account_id ON reward (account_id);
CREATE INDEX IF NOT EXISTS idx_reward_epoch_token_account ON reward (epoch, token_id, account_id);

-- Reward Claimed History 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요 (INSERT만 수행)

CREATE TABLE IF NOT EXISTS vote_history(
    id SERIAL PRIMARY KEY,
    epoch BIGINT NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    account_id VARCHAR(42) NOT NULL,
    vote NUMERIC NOT NULL, -- UNIT: points (int); hype points spent on this single vote (api-server vote(): deducted from point.round_point, added to point.hype_point).
    total_vote_amount NUMERIC NOT NULL, -- UNIT: points (int); hype_token.vote running total after this vote (hype_token.vote + amount at insert time).
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
);
-- Vote History 테이블은 PRIMARY KEY 외에 추가 인덱스 불필요 (INSERT만 수행)

CREATE INDEX IF NOT EXISTS idx_vote_history_account_epoch_token ON vote_history (account_id, epoch DESC, token_id);


CREATE TABLE IF NOT EXISTS account_vote_history_count (
      account_id VARCHAR(42) NOT NULL,
      total_count BIGINT NOT NULL DEFAULT 0,
      PRIMARY KEY (account_id)
);


INSERT INTO account_vote_history_count (account_id, total_count)
  SELECT
      account_id,
      COUNT(*) as total_count
  FROM vote_history
  GROUP BY account_id
  ON CONFLICT (account_id)
  DO UPDATE SET total_count = EXCLUDED.total_count;

CREATE OR REPLACE FUNCTION update_vote_history_count()
  RETURNS TRIGGER AS $$
  BEGIN
      IF TG_OP = 'INSERT' THEN
          INSERT INTO account_vote_history_count (account_id, total_count)
          VALUES (NEW.account_id, 1)
          ON CONFLICT (account_id)
          DO UPDATE SET total_count = account_vote_history_count.total_count + 1;
          RETURN NEW;
      ELSIF TG_OP = 'DELETE' THEN
          UPDATE account_vote_history_count
          SET total_count = GREATEST(total_count - 1, 0)
          WHERE account_id = OLD.account_id;
          RETURN OLD;
      END IF;
      RETURN NULL;
  END;
  $$ LANGUAGE plpgsql;

CREATE TRIGGER vote_history_count_trigger
AFTER INSERT OR DELETE ON vote_history
FOR EACH ROW EXECUTE FUNCTION update_vote_history_count();


CREATE TABLE IF NOT EXISTS total_buy_back(
    epoch BIGINT NOT NULL,
    token_id VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL DEFAULT 0, -- UNIT? (unverified: token raw (wei)); no reader/writer found in observer/scheduler/api-server — appears unused/legacy.
    PRIMARY KEY (epoch, token_id)
);