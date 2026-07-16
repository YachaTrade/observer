-- =====================================================
-- gift_tweet: producer(트윗 스트림)와 consumer(on-chain setReceiver)
-- 사이의 크래시 안전 버퍼. 추가로 consumer가 setReceiver 성공 직후
-- nad.fun 프로필 링크로 답글을 다는 reply 워크플로 상태도 같이 보관.
--
-- 설계 문서: gift-bot/docs/plans/2026-04-24-split-architecture-design.md
--
-- Reply 상태머신 (reply_status):
--   none      — reply 기능 비활성/이 row는 reply 대상 아님
--   pending   — setReceiver 성공, reply 워커가 처리 대기
--   sent      — POST /2/tweets 성공, reply_tweet_id에 답글 id 저장
--   failed    — 최대 재시도 횟수 초과 / 비재시도 오류 (operator 액션 필요)
-- =====================================================

CREATE TABLE IF NOT EXISTS gift_tweet (
    tweet_id         VARCHAR(32)  PRIMARY KEY,                 -- X snowflake id
    token_id         VARCHAR(42)  NOT NULL,                    -- 0x... token contract
    receiver_id      VARCHAR(42)  NOT NULL,                    -- 0x... resolved EVM receiver
    handle           VARCHAR(16)  NOT NULL,                    -- tweet 작성자 X handle (@ 제외)

    status           VARCHAR(16)  NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','submitted','completed','rejected')),
    reject_reason    VARCHAR(32),                              -- ValidationReject variant name
    tx_hash          VARCHAR(66),                              -- submitted → completed 전이 시 기록
    last_error       TEXT,                                     -- transient 실패 최근 원인

    -- Reply-on-success 워크플로 (consumer가 setReceiver 성공 후 X에 답글)
    reply_status     VARCHAR(16)  NOT NULL DEFAULT 'none'
                     CHECK (reply_status IN ('none','pending','sent','failed')),
    reply_tweet_id   VARCHAR(32),                              -- X가 반환한 답글의 snowflake id
    reply_attempts   INT          NOT NULL DEFAULT 0,          -- 재시도 카운터
    reply_last_error TEXT,                                     -- 최근 reply 실패 사유
    reply_sent_at    TIMESTAMPTZ,                              -- 답글 성공 timestamp

    received_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- pending 큐 폴링 최적화 (보통의 work queue)
CREATE INDEX IF NOT EXISTS idx_gift_tweet_pending
    ON gift_tweet (received_at)
    WHERE status = 'pending';

-- submitted 스윕 최적화 (재시작 시 on-chain reconciliation)
CREATE INDEX IF NOT EXISTS idx_gift_tweet_submitted
    ON gift_tweet (received_at)
    WHERE status = 'submitted';

-- Reply 워커 폴링 최적화: 'pending'만 인덱싱 (partial index라
-- sent/failed/none이 누적돼도 인덱스가 부풀지 않음)
CREATE INDEX IF NOT EXISTS idx_gift_tweet_reply_pending
    ON gift_tweet (updated_at)
    WHERE reply_status = 'pending';

-- INSERT 시 consumer에게 신호 (트랜잭션 커밋 후 발송됨)
CREATE OR REPLACE FUNCTION notify_gift_tweet_new() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('gift_tweet_new', NEW.tweet_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS gift_tweet_notify ON gift_tweet;
CREATE TRIGGER gift_tweet_notify
    AFTER INSERT ON gift_tweet
    FOR EACH ROW
    EXECUTE FUNCTION notify_gift_tweet_new();
