-- =============================================================================
-- v2_upgrade_triggers_and_backfill.sql
-- =============================================================================
-- Purpose
--   1) Re-create every trigger function and trigger defined across the project
--      (using the v2 override version where applicable).
--   2) Backfill every aggregate / derived table from its source-of-truth tables
--      so the state matches "as if every trigger had been firing all along".
--
-- Idempotency
--   Every CREATE FUNCTION uses OR REPLACE.
--   Every CREATE TRIGGER is preceded by DROP TRIGGER IF EXISTS.
--   Every backfill DELETEs from its target first, then re-inserts.
--   Safe to re-run.
--
-- pgactive compatibility
--   This script is safe under active-active (pgactive) replication:
--     • Uses DELETE instead of TRUNCATE (TRUNCATE does not replicate).
--     • Does NOT touch ALTER TABLE … DISABLE/ENABLE TRIGGER (DDL doesn't
--       replicate). Triggers stay enabled during backfill; they fire on
--       the local source node but logical replication does not fire them
--       on replicas. The explicit downstream aggregate UPDATEs at the end
--       of each section (e.g. token.token_holder_count after balance
--       rebuild) are the source of truth and replicate to all nodes.
--   Run from a SINGLE node — DELETE and INSERT both replicate to peers.
--
-- Concurrency note
--   Backfills LOCK source tables EXCLUSIVE for the duration of the recompute.
--   Stop the observer / indexer before running this on a live database.
--
-- Layout
--   SECTION A  — trigger functions  (CREATE OR REPLACE)
--   SECTION B  — triggers           (DROP IF EXISTS + CREATE)
--   SECTION C  — backfills          (per-aggregate, DELETE + INSERT)
--
-- Excluded from backfill
--   • notify_gift_tweet_new            — pure NOTIFY, no state
--   • update_creator_reward_status_from_claim
--     This only flips creator_reward.status to 'CLAIMED'. We do flip status in
--     the backfill, but we never INSERT creator_reward rows (those come from
--     the merkle root upload, not from this trigger).
--
-- =============================================================================

-- Stop psql on the first error (including user Ctrl-C). Without this, psql
-- logs the error and continues to the next statement in the file — which is
-- almost always wrong for a multi-section migration. Equivalent to invoking
-- psql with `-v ON_ERROR_STOP=1`.
\set ON_ERROR_STOP on

BEGIN;

-- Prevent two concurrent runs of this migration.
SELECT pg_advisory_xact_lock(8470129331477219347);


-- =============================================================================
-- SECTION A — trigger functions
--
-- v2 override priority is applied: where v2_upgrade_alter.sql redefines a base
-- function, the v2 body is used. Where 0017_chester_round.sql redefines a base
-- function (point_distribution → RAFFLE+CHEST), the 0017 body is used.
-- =============================================================================

-- ------------------------------------------------------------ token counters
CREATE OR REPLACE FUNCTION update_token_count_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE public.token_count
    SET
        total_count     = total_count + 1,
        nsfw_count      = CASE WHEN NEW.is_nsfw      = true THEN nsfw_count      + 1 ELSE nsfw_count      END,
        sfw_count       = CASE WHEN NEW.is_nsfw     IS NOT true THEN sfw_count   + 1 ELSE sfw_count       END,
        graduated_count = CASE WHEN NEW.is_graduated = true THEN graduated_count + 1 ELSE graduated_count END;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_token_count_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE public.token_count
    SET
        total_count     = total_count - 1,
        nsfw_count      = CASE WHEN OLD.is_nsfw      = true THEN nsfw_count      - 1 ELSE nsfw_count      END,
        sfw_count       = CASE WHEN OLD.is_nsfw     IS NOT true THEN sfw_count   - 1 ELSE sfw_count       END,
        graduated_count = CASE WHEN OLD.is_graduated = true THEN graduated_count - 1 ELSE graduated_count END;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION update_graduated_count()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.is_graduated IS NOT true AND NEW.is_graduated = true THEN
        UPDATE public.token_count SET graduated_count = graduated_count + 1;
    ELSIF OLD.is_graduated = true AND NEW.is_graduated IS NOT true THEN
        UPDATE public.token_count SET graduated_count = graduated_count - 1;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_nsfw_count()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.is_nsfw IS NOT true AND NEW.is_nsfw = true THEN
        UPDATE public.token_count SET nsfw_count = nsfw_count + 1, sfw_count = sfw_count - 1;
    ELSIF OLD.is_nsfw = true AND NEW.is_nsfw IS NOT true THEN
        UPDATE public.token_count SET nsfw_count = nsfw_count - 1, sfw_count = sfw_count + 1;
    END IF;
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------ chart
CREATE OR REPLACE FUNCTION update_charts_on_price_insert()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    interval_val TEXT;
    converted_timestamp BIGINT;
    prev_close_price NUMERIC(15,10);
    prev_usd_close_price NUMERIC(15,10);
    token_supply NUMERIC;
    latest_usd_price NUMERIC;
    token_quote_id VARCHAR(42);
BEGIN
    -- token total_supply + market quote_id 1회 PK 조회로 통합
    SELECT t.total_supply, m.quote_id
      INTO token_supply, token_quote_id
      FROM token t
      JOIN market m ON m.token_id = t.token_id
     WHERE t.token_id = NEW.token_id;

    -- quote_id 등호 필터로 idx_price_quote_block 인덱스 사용
    SELECT price INTO latest_usd_price
    FROM price
    WHERE quote_id = token_quote_id
      AND block_number <= NEW.block_number
    ORDER BY block_number DESC
    LIMIT 1;

    IF latest_usd_price IS NULL THEN
        SELECT price INTO latest_usd_price
        FROM price
        WHERE quote_id = token_quote_id
        ORDER BY block_number DESC
        LIMIT 1;
    END IF;

    IF latest_usd_price IS NULL THEN
        latest_usd_price := 1;
    END IF;

    FOREACH interval_val IN ARRAY ARRAY['1','5','15','30','1H','4H','D','W','M']
    LOOP
        converted_timestamp := convert_chart_timestamp(NEW.created_at, interval_val);

        SELECT close_price, usd_close_price INTO prev_close_price, prev_usd_close_price
        FROM chart
        WHERE chart.token_id = NEW.token_id
          AND chart.interval_type = interval_val
          AND chart.time_stamp < converted_timestamp
        ORDER BY chart.time_stamp DESC
        LIMIT 1;

        INSERT INTO chart (
            token_id, interval_type, time_stamp,
            open_price, close_price, high_price, low_price, volume, total_supply,
            usd_open_price, usd_close_price, usd_high_price, usd_low_price, usd_volume
        )
        VALUES (
            NEW.token_id, interval_val, converted_timestamp,
            COALESCE(prev_close_price, NEW.price), NEW.price, NEW.price, NEW.price, NEW.volume,
            COALESCE(token_supply, 0),
            COALESCE(prev_usd_close_price, NEW.price * latest_usd_price),
            NEW.price * latest_usd_price,
            NEW.price * latest_usd_price,
            NEW.price * latest_usd_price,
            NEW.volume * latest_usd_price
        )
        ON CONFLICT (token_id, interval_type, time_stamp) DO UPDATE SET
            close_price     = EXCLUDED.close_price,
            high_price      = GREATEST(chart.high_price, EXCLUDED.high_price),
            low_price       = LEAST(chart.low_price,     EXCLUDED.low_price),
            volume          = chart.volume + EXCLUDED.volume,
            total_supply    = EXCLUDED.total_supply,
            usd_close_price = EXCLUDED.usd_close_price,
            usd_high_price  = GREATEST(chart.usd_high_price, EXCLUDED.usd_high_price),
            usd_low_price   = LEAST(chart.usd_low_price,     EXCLUDED.usd_low_price),
            usd_volume      = chart.usd_volume + EXCLUDED.usd_volume;
    END LOOP;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_chart_count()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO chart_count (token_id, interval_type, count)
    VALUES (NEW.token_id, NEW.interval_type, 1)
    ON CONFLICT (token_id, interval_type)
    DO UPDATE SET count = chart_count.count + 1;
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------ swap / market (v2 body)
CREATE OR REPLACE FUNCTION update_market_volume()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE market SET volume = volume + NEW.quote_amount WHERE token_id = NEW.token_id;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_swap_count()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.token_id IS NULL THEN
        RETURN NEW;
    END IF;
    INSERT INTO public.swap_count (token_id, count, buy_count, sell_count)
    VALUES (NEW.token_id, 1,
            CASE WHEN NEW.is_buy THEN 1 ELSE 0 END,
            CASE WHEN NEW.is_buy THEN 0 ELSE 1 END)
    ON CONFLICT (token_id) DO UPDATE SET
        count      = public.swap_count.count + 1,
        buy_count  = public.swap_count.buy_count  + CASE WHEN NEW.is_buy THEN 1 ELSE 0 END,
        sell_count = public.swap_count.sell_count + CASE WHEN NEW.is_buy THEN 0 ELSE 1 END;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_account_swap_count()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO account_swap_count (account_id, total_count)
    VALUES (NEW.account_id, 1)
    ON CONFLICT (account_id) DO UPDATE SET
        total_count  = account_swap_count.total_count + 1,
        last_updated = NOW();
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------ balance
-- The `WHERE balance.created_at <= EXCLUDED.created_at` guard prevents an
-- out-of-order balance_history INSERT from overwriting the current balance
-- with stale data. Must stay in lockstep with 0005_balance.sql.
CREATE OR REPLACE FUNCTION update_balance_from_history()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO balance (account_id, token_id, balance, created_at)
    VALUES (NEW.account_id, NEW.token_id, NEW.balance, NEW.created_at)
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        balance    = EXCLUDED.balance,
        created_at = EXCLUDED.created_at
    WHERE balance.created_at <= EXCLUDED.created_at;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION delete_zero_balance()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.balance = 0 THEN
        DELETE FROM balance WHERE account_id = NEW.account_id AND token_id = NEW.token_id;
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_token_holder_count_v2()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_old_positive BOOLEAN;
    v_new_positive BOOLEAN;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.balance > 0 THEN
            UPDATE token SET token_holder_count = token_holder_count + 1 WHERE token_id = NEW.token_id;
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        v_old_positive := OLD.balance > 0;
        v_new_positive := NEW.balance > 0;
        IF NOT v_old_positive AND v_new_positive THEN
            UPDATE token SET token_holder_count = token_holder_count + 1 WHERE token_id = NEW.token_id;
        ELSIF v_old_positive AND NOT v_new_positive THEN
            UPDATE token SET token_holder_count = GREATEST(token_holder_count - 1, 0) WHERE token_id = NEW.token_id;
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.balance > 0 THEN
            UPDATE token SET token_holder_count = GREATEST(token_holder_count - 1, 0) WHERE token_id = OLD.token_id;
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

-- ------------------------------------------------------------ lp / treasury
CREATE OR REPLACE FUNCTION update_lp_collect_status_from_allocate()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO lp_collect_status (token_id, last_collect_at)
    VALUES (NEW.token_id, NEW.created_at)
    ON CONFLICT (token_id) DO NOTHING;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_lp_collect_status_from_collect()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO lp_collect_status (token_id, last_collect_at)
    VALUES (NEW.token_id, NEW.created_at)
    ON CONFLICT (token_id) DO UPDATE SET last_collect_at = NEW.created_at;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_creator_treasury_balance_from_distribute()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO creator_treasury_balance (account_id, token_id, amount)
    SELECT creator, NEW.token_id, NEW.creator_amount
    FROM token
    WHERE token_id = NEW.token_id
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        amount = creator_treasury_balance.amount + EXCLUDED.amount;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_token_treasury_balance_from_collect()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO token_treasury_balance (token_id, amount)
    VALUES (NEW.token_id, NEW.token_amount)
    ON CONFLICT (token_id) DO UPDATE SET
        amount = token_treasury_balance.amount + EXCLUDED.amount;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_creator_treasury_balance_from_collect()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO creator_treasury_balance (account_id, token_id, amount)
    SELECT creator, NEW.token_id, NEW.c_amount
    FROM token
    WHERE token_id = NEW.token_id
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        amount = creator_treasury_balance.amount + EXCLUDED.amount;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION deduct_creator_treasury_balance_from_claim()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE creator_treasury_balance
    SET amount = amount - NEW.amount
    WHERE account_id = NEW.account_id AND token_id = NEW.token_id;

    DELETE FROM creator_treasury_balance
    WHERE account_id = NEW.account_id AND token_id = NEW.token_id AND amount <= 0;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_creator_reward_status_from_claim()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE creator_reward
    SET status = 'CLAIMED'
    WHERE account_id = NEW.account_id AND token_id = NEW.token_id;
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------ hype / point  (uses 0017 chester_round body)
CREATE OR REPLACE FUNCTION update_total_hype_point()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE total_hype_point SET hype_point = hype_point + NEW.hype_point WHERE id = 1;
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        UPDATE total_hype_point SET hype_point = hype_point + (NEW.hype_point - OLD.hype_point) WHERE id = 1;
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION update_hype_point_leaderboard_count()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE hype_point_leaderboard_count SET total_count = total_count + 1 WHERE id = 1;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE hype_point_leaderboard_count SET total_count = total_count - 1 WHERE id = 1;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION update_point_on_distribution_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.activity_type IN ('RAFFLE', 'CHEST') THEN
        INSERT INTO point (account_id, hype_point)
        VALUES (NEW.account_id, NEW.amount)
        ON CONFLICT (account_id) DO UPDATE SET hype_point = point.hype_point + NEW.amount;
    ELSE
        INSERT INTO point (account_id, round_point)
        VALUES (NEW.account_id, NEW.amount)
        ON CONFLICT (account_id) DO UPDATE SET round_point = point.round_point + NEW.amount;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_point_distribution_count_on_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO account_point_distribution_count (account_id, total_count, last_updated_at)
    VALUES (NEW.account_id, 1, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT)
    ON CONFLICT (account_id) DO UPDATE SET
        total_count    = account_point_distribution_count.total_count + 1,
        last_updated_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION calculate_reward_total_amount()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.total_amount := (
        SELECT COALESCE(SUM(amount), 0) + NEW.amount
        FROM reward_add_history
        WHERE epoch = NEW.epoch
          AND account_id = NEW.account_id
          AND token_id = NEW.token_id
    );
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_reward_add_history_count()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO reward_add_history_count (account_id, total_count, updated_at)
    VALUES (NEW.account_id, 1, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT)
    ON CONFLICT (account_id) DO UPDATE SET
        total_count = reward_add_history_count.total_count + 1,
        updated_at  = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_vote_history_count()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO account_vote_history_count (account_id, total_count)
        VALUES (NEW.account_id, 1)
        ON CONFLICT (account_id) DO UPDATE SET total_count = account_vote_history_count.total_count + 1;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE account_vote_history_count
        SET total_count = GREATEST(total_count - 1, 0)
        WHERE account_id = OLD.account_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$;

-- ------------------------------------------------------------ position / fee  (v2 bodies)
CREATE OR REPLACE FUNCTION update_position_on_history()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    sender_position RECORD;
    avg_cost_quote NUMERIC;
    avg_cost_usd NUMERIC;
    transfer_cost_quote NUMERIC;
    transfer_cost_usd NUMERIC;
    current_balance NUMERIC;
BEGIN
    IF NEW.transfer_type = 'transfer_out' THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.account_id AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;
            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd   := sender_position.usd_out   / sender_position.token_in;
                transfer_cost_quote := avg_cost_quote * NEW.token_out;
                transfer_cost_usd   := avg_cost_usd   * NEW.token_out;
                NEW.quote_in := transfer_cost_quote;
                NEW.usd_in   := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    IF NEW.transfer_type = 'transfer_in' AND NEW.sender_address IS NOT NULL THEN
        SELECT quote_out, usd_out, token_in, token_out
        INTO sender_position
        FROM position
        WHERE account_id = NEW.sender_address AND token_id = NEW.token_id;

        IF FOUND AND sender_position.token_in > 0 THEN
            current_balance := sender_position.token_in - sender_position.token_out;
            IF current_balance > 0 THEN
                avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                avg_cost_usd   := sender_position.usd_out   / sender_position.token_in;
                transfer_cost_quote := avg_cost_quote * NEW.token_in;
                transfer_cost_usd   := avg_cost_usd   * NEW.token_in;
                NEW.quote_out := transfer_cost_quote;
                NEW.usd_out   := transfer_cost_usd;
            END IF;
        END IF;
    END IF;

    INSERT INTO position (
        account_id, token_id,
        quote_in, quote_out, usd_in, usd_out, token_in, token_out,
        created_at, updated_at
    )
    VALUES (
        NEW.account_id, NEW.token_id,
        NEW.quote_in, NEW.quote_out, NEW.usd_in, NEW.usd_out, NEW.token_in, NEW.token_out,
        NEW.created_at, NEW.created_at
    )
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        quote_in  = position.quote_in  + EXCLUDED.quote_in,
        quote_out = position.quote_out + EXCLUDED.quote_out,
        usd_in    = position.usd_in    + EXCLUDED.usd_in,
        usd_out   = position.usd_out   + EXCLUDED.usd_out,
        token_in  = position.token_in  + EXCLUDED.token_in,
        token_out = position.token_out + EXCLUDED.token_out,
        updated_at = EXCLUDED.updated_at;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_fee_on_history()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO fee (account_id, token_id, quote_amount, usd_amount, created_at, updated_at)
    VALUES (NEW.account_id, NEW.token_id, NEW.quote_amount, NEW.usd_amount, NEW.created_at, NEW.created_at)
    ON CONFLICT (account_id, token_id) DO UPDATE SET
        quote_amount = fee.quote_amount + EXCLUDED.quote_amount,
        usd_amount   = fee.usd_amount   + EXCLUDED.usd_amount,
        updated_at   = EXCLUDED.updated_at;
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------ gift_tweet
CREATE OR REPLACE FUNCTION notify_gift_tweet_new()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('gift_tweet_new', NEW.tweet_id);
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------ vault.sql trigger functions
CREATE OR REPLACE FUNCTION update_vault_burn_stats()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.vault_type = 'BURN' THEN
        INSERT INTO v2_burn_vault_stats
            (token_id, quote_spent, quote_spent_usd, tokens_burned,
             burn_count, last_block, updated_at)
        VALUES (NEW.token_id, NEW.quote_in, NEW.usd_value, NEW.token_burned, 1,
                NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            quote_spent     = v2_burn_vault_stats.quote_spent     + EXCLUDED.quote_spent,
            quote_spent_usd = v2_burn_vault_stats.quote_spent_usd + EXCLUDED.quote_spent_usd,
            tokens_burned   = v2_burn_vault_stats.tokens_burned   + EXCLUDED.tokens_burned,
            burn_count      = v2_burn_vault_stats.burn_count      + 1,
            last_block      = GREATEST(v2_burn_vault_stats.last_block, EXCLUDED.last_block),
            updated_at      = GREATEST(v2_burn_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.vault_type = 'GIFT' THEN
        INSERT INTO v2_gift_vault_stats
            (token_id, buyback_quote_spent, buyback_quote_spent_usd, buyback_tokens,
             last_block, updated_at)
        VALUES (NEW.token_id, NEW.quote_in, NEW.usd_value, NEW.token_burned,
                NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            buyback_quote_spent     = v2_gift_vault_stats.buyback_quote_spent     + EXCLUDED.buyback_quote_spent,
            buyback_quote_spent_usd = v2_gift_vault_stats.buyback_quote_spent_usd + EXCLUDED.buyback_quote_spent_usd,
            buyback_tokens          = v2_gift_vault_stats.buyback_tokens          + EXCLUDED.buyback_tokens,
            last_block              = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at              = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_vault_lp_stats()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO v2_lp_vault_stats
        (token_id, quote_injected, quote_injected_usd, token_injected, lp_burned,
         inject_count, last_block, updated_at)
    VALUES (NEW.token_id, NEW.quote_used, NEW.usd_value, NEW.token_used, NEW.lp_burned, 1,
            NEW.block_number, NEW.created_at)
    ON CONFLICT (token_id) DO UPDATE SET
        quote_injected     = v2_lp_vault_stats.quote_injected     + EXCLUDED.quote_injected,
        quote_injected_usd = v2_lp_vault_stats.quote_injected_usd + EXCLUDED.quote_injected_usd,
        token_injected     = v2_lp_vault_stats.token_injected     + EXCLUDED.token_injected,
        lp_burned          = v2_lp_vault_stats.lp_burned          + EXCLUDED.lp_burned,
        inject_count       = v2_lp_vault_stats.inject_count       + 1,
        last_block         = GREATEST(v2_lp_vault_stats.last_block, EXCLUDED.last_block),
        updated_at         = GREATEST(v2_lp_vault_stats.updated_at, EXCLUDED.updated_at);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_creator_fee_vault_stats()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_type = 'DEPOSIT' THEN
        INSERT INTO v2_creator_fee_vault_stats
            (token_id, current_balance, total_deposited, total_deposited_usd,
             deposit_count, last_block, updated_at)
        VALUES (NEW.token_id, COALESCE(NEW.new_balance, 0), NEW.amount, NEW.usd_value, 1,
                NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance     = COALESCE(EXCLUDED.current_balance, v2_creator_fee_vault_stats.current_balance),
            total_deposited     = v2_creator_fee_vault_stats.total_deposited     + EXCLUDED.total_deposited,
            total_deposited_usd = v2_creator_fee_vault_stats.total_deposited_usd + EXCLUDED.total_deposited_usd,
            deposit_count       = v2_creator_fee_vault_stats.deposit_count       + 1,
            last_block          = GREATEST(v2_creator_fee_vault_stats.last_block, EXCLUDED.last_block),
            updated_at          = GREATEST(v2_creator_fee_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'CLAIM' THEN
        INSERT INTO v2_creator_fee_vault_stats
            (token_id, current_balance, total_claimed, total_claimed_usd,
             claim_count, last_block, updated_at)
        VALUES (NEW.token_id, 0, NEW.amount, NEW.usd_value, 1, NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance   = 0,
            total_claimed     = v2_creator_fee_vault_stats.total_claimed     + EXCLUDED.total_claimed,
            total_claimed_usd = v2_creator_fee_vault_stats.total_claimed_usd + EXCLUDED.total_claimed_usd,
            claim_count       = v2_creator_fee_vault_stats.claim_count       + 1,
            last_block        = GREATEST(v2_creator_fee_vault_stats.last_block, EXCLUDED.last_block),
            updated_at        = GREATEST(v2_creator_fee_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_gift_vault_stats()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_type = 'SETUP' THEN
        INSERT INTO v2_gift_vault_stats (token_id, current_state, platform, platform_id, expires_at, last_block, updated_at)
        VALUES (NEW.token_id, 'Accumulating', NEW.platform, NEW.platform_id, NEW.expires_at, NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            platform    = COALESCE(EXCLUDED.platform, v2_gift_vault_stats.platform),
            platform_id = COALESCE(EXCLUDED.platform_id, v2_gift_vault_stats.platform_id),
            expires_at  = EXCLUDED.expires_at,
            last_block  = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at  = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'DEPOSIT' THEN
        INSERT INTO v2_gift_vault_stats (token_id, current_balance, total_deposited, total_deposited_usd, last_block, updated_at)
        VALUES (NEW.token_id, COALESCE(NEW.new_balance, 0), NEW.amount, NEW.usd_value, NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance     = COALESCE(EXCLUDED.current_balance, v2_gift_vault_stats.current_balance),
            total_deposited     = v2_gift_vault_stats.total_deposited     + EXCLUDED.total_deposited,
            total_deposited_usd = v2_gift_vault_stats.total_deposited_usd + EXCLUDED.total_deposited_usd,
            last_block          = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at          = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'CLAIM' THEN
        INSERT INTO v2_gift_vault_stats (token_id, current_balance, total_claimed, total_claimed_usd, last_block, updated_at)
        VALUES (NEW.token_id, 0, NEW.amount, NEW.usd_value, NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_balance   = 0,
            total_claimed     = v2_gift_vault_stats.total_claimed     + EXCLUDED.total_claimed,
            total_claimed_usd = v2_gift_vault_stats.total_claimed_usd + EXCLUDED.total_claimed_usd,
            last_block        = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at        = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'EXPIRE' THEN
        INSERT INTO v2_gift_vault_stats (token_id, current_state, current_balance, total_expired, total_expired_usd, last_block, updated_at)
        VALUES (NEW.token_id, 'Burned', 0, NEW.amount, NEW.usd_value, NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_state     = 'Burned',
            current_balance   = 0,
            total_expired     = v2_gift_vault_stats.total_expired     + EXCLUDED.total_expired,
            total_expired_usd = v2_gift_vault_stats.total_expired_usd + EXCLUDED.total_expired_usd,
            last_block        = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at        = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    ELSIF NEW.event_type = 'RECEIVER_SET' THEN
        INSERT INTO v2_gift_vault_stats (token_id, current_state, receiver, expires_at, receiver_set_at, last_block, updated_at)
        VALUES (NEW.token_id, 'Active', NEW.receiver, 0, NEW.created_at, NEW.block_number, NEW.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            current_state   = CASE v2_gift_vault_stats.current_state WHEN 'Burned' THEN 'Burned' ELSE 'Active' END,
            receiver        = COALESCE(EXCLUDED.receiver, v2_gift_vault_stats.receiver),
            expires_at      = 0,
            receiver_set_at = EXCLUDED.receiver_set_at,
            last_block      = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at      = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION sync_token_creator_from_v2_updates()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE token SET creator = NEW.new_creator WHERE token_id = NEW.token_id;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_creator_fee_distribution_stats()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_type <> 'DISTRIBUTE'
       OR NEW.token IS NULL
       OR NEW.vault IS NULL THEN
        RETURN NEW;
    END IF;
    INSERT INTO v2_creator_fee_distribution_stats
        (token_id, vault_id, quote_id, distributed_quote, distributed_quote_usd,
         distribute_count, last_block, updated_at)
    VALUES (NEW.token, NEW.vault, NEW.quote_id, NEW.amount, NEW.usd_value,
            1, NEW.block_number, NEW.created_at)
    ON CONFLICT (token_id, vault_id) DO UPDATE SET
        distributed_quote     = v2_creator_fee_distribution_stats.distributed_quote     + EXCLUDED.distributed_quote,
        distributed_quote_usd = v2_creator_fee_distribution_stats.distributed_quote_usd + EXCLUDED.distributed_quote_usd,
        distribute_count      = v2_creator_fee_distribution_stats.distribute_count + 1,
        last_block            = GREATEST(v2_creator_fee_distribution_stats.last_block, EXCLUDED.last_block),
        updated_at            = GREATEST(v2_creator_fee_distribution_stats.updated_at, EXCLUDED.updated_at);
    RETURN NEW;
END;
$$;


-- =============================================================================
-- SECTION B — triggers (drop + recreate, all idempotent)
-- =============================================================================

-- token
DROP TRIGGER IF EXISTS token_insert_trigger          ON public.token;
DROP TRIGGER IF EXISTS token_delete_trigger          ON public.token;
DROP TRIGGER IF EXISTS token_graduated_count_trigger ON public.token;
DROP TRIGGER IF EXISTS token_nsfw_count_trigger      ON public.token;
CREATE TRIGGER token_insert_trigger          AFTER INSERT                 ON public.token FOR EACH ROW EXECUTE FUNCTION update_token_count_insert();
CREATE TRIGGER token_delete_trigger          AFTER DELETE                 ON public.token FOR EACH ROW EXECUTE FUNCTION update_token_count_delete();
CREATE TRIGGER token_graduated_count_trigger AFTER UPDATE OF is_graduated ON public.token FOR EACH ROW EXECUTE FUNCTION update_graduated_count();
CREATE TRIGGER token_nsfw_count_trigger      AFTER UPDATE OF is_nsfw      ON public.token FOR EACH ROW EXECUTE FUNCTION update_nsfw_count();

-- chart
DROP TRIGGER IF EXISTS trigger_update_charts_on_price_insert ON price_history;
DROP TRIGGER IF EXISTS chart_count_trigger                   ON chart;
CREATE TRIGGER trigger_update_charts_on_price_insert AFTER INSERT          ON price_history FOR EACH ROW EXECUTE FUNCTION update_charts_on_price_insert();
CREATE TRIGGER chart_count_trigger                   AFTER INSERT OR UPDATE ON chart         FOR EACH ROW EXECUTE FUNCTION update_chart_count();

-- swap / market
DROP TRIGGER IF EXISTS trg_update_market_volume     ON swap;
DROP TRIGGER IF EXISTS swap_count_trigger           ON public.swap;
DROP TRIGGER IF EXISTS trg_update_account_swap_count ON swap;
CREATE TRIGGER trg_update_market_volume      AFTER INSERT ON swap        FOR EACH ROW EXECUTE FUNCTION update_market_volume();
CREATE TRIGGER swap_count_trigger            AFTER INSERT ON public.swap FOR EACH ROW EXECUTE FUNCTION update_swap_count();
CREATE TRIGGER trg_update_account_swap_count AFTER INSERT ON swap        FOR EACH ROW EXECUTE FUNCTION update_account_swap_count();

-- balance
DROP TRIGGER IF EXISTS trigger_update_balance_from_history ON balance_history;
DROP TRIGGER IF EXISTS trigger_delete_zero_balance         ON balance;
DROP TRIGGER IF EXISTS trg_update_holder_count             ON balance;
CREATE TRIGGER trigger_update_balance_from_history AFTER INSERT                       ON balance_history FOR EACH ROW EXECUTE FUNCTION update_balance_from_history();
CREATE TRIGGER trigger_delete_zero_balance         AFTER UPDATE                       ON balance         FOR EACH ROW EXECUTE FUNCTION delete_zero_balance();
CREATE TRIGGER trg_update_holder_count             AFTER INSERT OR UPDATE OR DELETE   ON balance         FOR EACH ROW EXECUTE FUNCTION update_token_holder_count_v2();

-- lp / treasury
DROP TRIGGER IF EXISTS trigger_update_lp_collect_status_from_allocate         ON lp_allocate_history;
DROP TRIGGER IF EXISTS trigger_update_lp_collect_status_from_collect          ON lp_collect_history;
DROP TRIGGER IF EXISTS trigger_update_creator_treasury_balance_from_distribute ON fee_distribute_history;
DROP TRIGGER IF EXISTS trigger_update_token_treasury_balance_from_collect      ON lp_collect_history;
DROP TRIGGER IF EXISTS trigger_update_creator_treasury_balance_from_collect    ON lp_collect_history;
DROP TRIGGER IF EXISTS trigger_deduct_creator_treasury_balance_from_claim      ON creator_treasury_claim_history;
DROP TRIGGER IF EXISTS trigger_update_creator_reward_status_from_claim         ON creator_treasury_claim_history;
CREATE TRIGGER trigger_update_lp_collect_status_from_allocate           AFTER INSERT ON lp_allocate_history            FOR EACH ROW EXECUTE FUNCTION update_lp_collect_status_from_allocate();
CREATE TRIGGER trigger_update_lp_collect_status_from_collect            AFTER INSERT ON lp_collect_history             FOR EACH ROW EXECUTE FUNCTION update_lp_collect_status_from_collect();
CREATE TRIGGER trigger_update_creator_treasury_balance_from_distribute  AFTER INSERT ON fee_distribute_history         FOR EACH ROW EXECUTE FUNCTION update_creator_treasury_balance_from_distribute();
CREATE TRIGGER trigger_update_token_treasury_balance_from_collect       AFTER INSERT ON lp_collect_history             FOR EACH ROW EXECUTE FUNCTION update_token_treasury_balance_from_collect();
CREATE TRIGGER trigger_update_creator_treasury_balance_from_collect     AFTER INSERT ON lp_collect_history             FOR EACH ROW EXECUTE FUNCTION update_creator_treasury_balance_from_collect();
CREATE TRIGGER trigger_deduct_creator_treasury_balance_from_claim       AFTER INSERT ON creator_treasury_claim_history FOR EACH ROW EXECUTE FUNCTION deduct_creator_treasury_balance_from_claim();
CREATE TRIGGER trigger_update_creator_reward_status_from_claim          AFTER INSERT ON creator_treasury_claim_history FOR EACH ROW EXECUTE FUNCTION update_creator_reward_status_from_claim();

-- hype / point
DROP TRIGGER IF EXISTS point_hype_trigger                     ON point;
DROP TRIGGER IF EXISTS hype_point_leaderboard_count_trigger   ON point;
DROP TRIGGER IF EXISTS point_update_trigger                   ON point_distribution;
DROP TRIGGER IF EXISTS point_distribution_count_trigger       ON point_distribution;
DROP TRIGGER IF EXISTS trigger_calculate_reward_total         ON reward_add_history;
DROP TRIGGER IF EXISTS trigger_update_reward_add_history_count ON reward_add_history;
DROP TRIGGER IF EXISTS vote_history_count_trigger             ON vote_history;
CREATE TRIGGER point_hype_trigger                      AFTER INSERT OR UPDATE ON point             FOR EACH ROW EXECUTE FUNCTION update_total_hype_point();
CREATE TRIGGER hype_point_leaderboard_count_trigger    AFTER INSERT OR DELETE ON point             FOR EACH ROW EXECUTE FUNCTION update_hype_point_leaderboard_count();
CREATE TRIGGER point_update_trigger                    AFTER INSERT           ON point_distribution FOR EACH ROW EXECUTE FUNCTION update_point_on_distribution_insert();
CREATE TRIGGER point_distribution_count_trigger        AFTER INSERT           ON point_distribution FOR EACH ROW EXECUTE FUNCTION update_point_distribution_count_on_insert();
CREATE TRIGGER trigger_calculate_reward_total          BEFORE INSERT          ON reward_add_history FOR EACH ROW EXECUTE FUNCTION calculate_reward_total_amount();
CREATE TRIGGER trigger_update_reward_add_history_count AFTER INSERT           ON reward_add_history FOR EACH ROW EXECUTE FUNCTION update_reward_add_history_count();
CREATE TRIGGER vote_history_count_trigger              AFTER INSERT OR DELETE ON vote_history      FOR EACH ROW EXECUTE FUNCTION update_vote_history_count();

-- position / fee
DROP TRIGGER IF EXISTS trg_position_on_history ON position_history;
DROP TRIGGER IF EXISTS trg_fee_on_history      ON fee_history;
DROP TRIGGER IF EXISTS trg_position_on_swap    ON swap;  -- legacy, predecessor of position_on_history
CREATE TRIGGER trg_position_on_history BEFORE INSERT ON position_history FOR EACH ROW EXECUTE FUNCTION update_position_on_history();
CREATE TRIGGER trg_fee_on_history      AFTER  INSERT ON fee_history      FOR EACH ROW EXECUTE FUNCTION update_fee_on_history();

-- gift_tweet
DROP TRIGGER IF EXISTS gift_tweet_notify ON gift_tweet;
CREATE TRIGGER gift_tweet_notify AFTER INSERT ON gift_tweet FOR EACH ROW EXECUTE FUNCTION notify_gift_tweet_new();

-- vault triggers
DROP TRIGGER IF EXISTS trg_update_vault_burn_stats              ON v2_vault_burns;
DROP TRIGGER IF EXISTS trg_update_vault_lp_stats                ON v2_vault_lp_injections;
DROP TRIGGER IF EXISTS trg_update_creator_fee_vault_stats       ON v2_creator_fee_claims;
DROP TRIGGER IF EXISTS trg_update_gift_vault_stats              ON v2_gifts;
DROP TRIGGER IF EXISTS trg_sync_token_creator_from_v2_updates   ON v2_creator_updates;
DROP TRIGGER IF EXISTS trg_update_creator_fee_distribution_stats ON v2_creator_fee_distribution;
CREATE TRIGGER trg_update_vault_burn_stats               AFTER INSERT ON v2_vault_burns              FOR EACH ROW EXECUTE FUNCTION update_vault_burn_stats();
CREATE TRIGGER trg_update_vault_lp_stats                 AFTER INSERT ON v2_vault_lp_injections      FOR EACH ROW EXECUTE FUNCTION update_vault_lp_stats();
CREATE TRIGGER trg_update_creator_fee_vault_stats        AFTER INSERT ON v2_creator_fee_claims       FOR EACH ROW EXECUTE FUNCTION update_creator_fee_vault_stats();
CREATE TRIGGER trg_update_gift_vault_stats               AFTER INSERT ON v2_gifts                    FOR EACH ROW EXECUTE FUNCTION update_gift_vault_stats();
CREATE TRIGGER trg_sync_token_creator_from_v2_updates    AFTER INSERT ON v2_creator_updates          FOR EACH ROW EXECUTE FUNCTION sync_token_creator_from_v2_updates();
CREATE TRIGGER trg_update_creator_fee_distribution_stats AFTER INSERT ON v2_creator_fee_distribution FOR EACH ROW EXECUTE FUNCTION update_creator_fee_distribution_stats();

COMMIT;


-- =============================================================================
-- SECTION C — backfills
--
-- Each backfill is wrapped in its own BEGIN…COMMIT so partial failure doesn't
-- block other recomputes. Heavy ones (chart, position) come last.
-- =============================================================================


-- ----------------------------------------------------------------- token_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
UPDATE token_count
SET total_count     = (SELECT COUNT(*) FROM token),
    graduated_count = (SELECT COUNT(*) FROM token WHERE is_graduated = true),
    nsfw_count      = (SELECT COUNT(*) FROM token WHERE is_nsfw      = true),
    sfw_count       = (SELECT COUNT(*) FROM token WHERE is_nsfw IS NOT true);
INSERT INTO token_count (total_count, graduated_count, nsfw_count, sfw_count)
SELECT
    (SELECT COUNT(*) FROM token),
    (SELECT COUNT(*) FROM token WHERE is_graduated = true),
    (SELECT COUNT(*) FROM token WHERE is_nsfw      = true),
    (SELECT COUNT(*) FROM token WHERE is_nsfw IS NOT true)
WHERE NOT EXISTS (SELECT 1 FROM token_count);
COMMIT;


-- ------------------------------------------------------------------ swap_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM swap_count;
INSERT INTO swap_count (token_id, count, buy_count, sell_count)
SELECT
    token_id,
    COUNT(*),
    COUNT(*) FILTER (WHERE is_buy = true),
    COUNT(*) FILTER (WHERE is_buy = false)
FROM swap
WHERE token_id IS NOT NULL
GROUP BY token_id;
COMMIT;


-- ----------------------------------------------------------- account_swap_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM account_swap_count;
INSERT INTO account_swap_count (account_id, total_count, last_updated)
SELECT account_id, COUNT(*), NOW()
FROM swap
GROUP BY account_id;
COMMIT;


-- ---------------------------------------------------------------- market.volume
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
WITH vol AS (
    SELECT token_id, COALESCE(SUM(quote_amount), 0) AS v
    FROM swap
    GROUP BY token_id
)
UPDATE market m
SET volume = COALESCE(vol.v, 0)
FROM vol
WHERE m.token_id = vol.token_id;
-- markets with zero swaps get reset to 0:
UPDATE market SET volume = 0
WHERE token_id NOT IN (SELECT token_id FROM swap WHERE token_id IS NOT NULL);
COMMIT;


-- ---------------------------------------------------------------------- balance
-- balance is the latest balance_history snapshot per (account, token).
-- Triggers on balance (trg_update_holder_count, trigger_delete_zero_balance)
-- fire on the local source node during this rebuild but write transient
-- values; the token.token_holder_count UPDATE in the next section is the
-- source of truth and overwrites whatever the trigger wrote. Logical
-- replication does not fire triggers on replicas, so peers receive only
-- the final DELETE/INSERT row changes (pgactive-safe).
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);

DELETE FROM balance;
INSERT INTO balance (account_id, token_id, balance, created_at)
SELECT DISTINCT ON (account_id, token_id)
    account_id, token_id, balance, created_at
FROM balance_history
ORDER BY account_id, token_id, block_number DESC, tx_index DESC, log_index DESC;

-- enforce delete_zero_balance behavior at the end
DELETE FROM balance WHERE balance = 0;

COMMIT;


-- -------------------------------------------------------- token.token_holder_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
WITH hc AS (
    SELECT token_id, COUNT(*)::BIGINT AS c
    FROM balance
    WHERE balance > 0
    GROUP BY token_id
)
UPDATE token t
SET token_holder_count = COALESCE(hc.c, 0)
FROM hc
WHERE t.token_id = hc.token_id;
UPDATE token SET token_holder_count = 0
WHERE token_id NOT IN (SELECT token_id FROM balance WHERE balance > 0);
COMMIT;


-- ------------------------------------------------------------------------- fee
-- fee = SUM per (account, token) over fee_history.
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM fee;
INSERT INTO fee (account_id, token_id, quote_amount, usd_amount, created_at, updated_at)
SELECT account_id, token_id,
       SUM(quote_amount), SUM(usd_amount),
       MIN(created_at),   MAX(created_at)
FROM fee_history
GROUP BY account_id, token_id;
COMMIT;


-- ----------------------------------------------------- lp_collect_status
-- Trigger semantics:
--   allocate INSERTs do NOTHING on conflict (only fills a missing row).
--   collect  INSERTs UPDATE last_collect_at = NEW.created_at on every row.
-- Net effect over a full replay: last_collect_at =
--   MAX(created_at) across collect rows,
--   or, if there were no collect rows but allocate rows existed, the FIRST allocate's created_at
--   (because allocate's ON CONFLICT DO NOTHING means subsequent allocates are no-ops).
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM lp_collect_status;
WITH
collect_max AS (
    SELECT token_id, MAX(created_at) AS last_collect_at
    FROM lp_collect_history
    GROUP BY token_id
),
allocate_first AS (
    SELECT token_id, MIN(created_at) AS first_allocate_at
    FROM lp_allocate_history
    GROUP BY token_id
)
INSERT INTO lp_collect_status (token_id, last_collect_at)
SELECT
    COALESCE(c.token_id, a.token_id),
    COALESCE(c.last_collect_at, a.first_allocate_at)
FROM collect_max c
FULL OUTER JOIN allocate_first a USING (token_id)
WHERE COALESCE(c.last_collect_at, a.first_allocate_at) IS NOT NULL;
COMMIT;


-- ------------------------------------------------------- token_treasury_balance
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM token_treasury_balance;
INSERT INTO token_treasury_balance (token_id, amount)
SELECT token_id, SUM(token_amount)
FROM lp_collect_history
GROUP BY token_id;
COMMIT;


-- ----------------------------------------------------- creator_treasury_balance
-- Net = SUM(c_amount from lp_collect_history) where account = token.creator
--     + SUM(creator_amount from fee_distribute_history) where account = token.creator
--     - SUM(amount from creator_treasury_claim_history).
-- Per the trigger, rows are deleted when amount <= 0.
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM creator_treasury_balance;

WITH
collect_credits AS (
    SELECT t.creator AS account_id, lch.token_id, SUM(lch.c_amount) AS amount
    FROM lp_collect_history lch
    JOIN token t ON t.token_id = lch.token_id
    GROUP BY t.creator, lch.token_id
),
distribute_credits AS (
    SELECT t.creator AS account_id, fdh.token_id, SUM(fdh.creator_amount) AS amount
    FROM fee_distribute_history fdh
    JOIN token t ON t.token_id = fdh.token_id
    GROUP BY t.creator, fdh.token_id
),
claim_debits AS (
    SELECT account_id, token_id, SUM(amount) AS amount
    FROM creator_treasury_claim_history
    GROUP BY account_id, token_id
),
all_rows AS (
    SELECT account_id, token_id, amount FROM collect_credits
    UNION ALL
    SELECT account_id, token_id, amount FROM distribute_credits
    UNION ALL
    SELECT account_id, token_id, -amount FROM claim_debits
),
agg AS (
    SELECT account_id, token_id, SUM(amount) AS amount
    FROM all_rows
    WHERE account_id IS NOT NULL
    GROUP BY account_id, token_id
)
INSERT INTO creator_treasury_balance (account_id, token_id, amount)
SELECT account_id, token_id, amount
FROM agg
WHERE amount > 0;
COMMIT;


-- ----------------------------------------------------- creator_reward.status
-- Trigger flips status to 'CLAIMED' whenever the (account, token) pair has any claim_history row.
-- It does not create creator_reward rows.
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
UPDATE creator_reward cr
SET status = 'CLAIMED'
WHERE EXISTS (
    SELECT 1 FROM creator_treasury_claim_history h
    WHERE h.account_id = cr.account_id AND h.token_id = cr.token_id
);
COMMIT;


-- ------------------------------------------------------------------------ point
-- point.hype_point  = SUM(amount) WHERE activity_type IN ('RAFFLE','CHEST')
-- point.round_point = SUM(amount) WHERE activity_type NOT IN ('RAFFLE','CHEST')
-- point.raffle_count is updated elsewhere (not by these triggers) — preserve it.
-- Triggers on point (point_hype_trigger, hype_point_leaderboard_count_trigger)
-- fire on local source node during INSERT and write transient totals; the
-- total_hype_point + hype_point_leaderboard_count UPDATEs below overwrite
-- with the correct value. Logical replication does not fire triggers on
-- replicas (pgactive-safe).
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);

WITH agg AS (
    SELECT
        account_id,
        SUM(amount) FILTER (WHERE activity_type IN ('RAFFLE','CHEST'))     AS hype,
        SUM(amount) FILTER (WHERE activity_type NOT IN ('RAFFLE','CHEST')) AS round_
    FROM point_distribution
    GROUP BY account_id
)
INSERT INTO point (account_id, hype_point, round_point)
SELECT account_id, COALESCE(hype, 0), COALESCE(round_, 0)
FROM agg
ON CONFLICT (account_id) DO UPDATE SET
    hype_point  = EXCLUDED.hype_point,
    round_point = EXCLUDED.round_point;

-- accounts that exist in point but have no distribution rows → zero out the trigger-managed fields
UPDATE point p
SET hype_point = 0, round_point = 0
WHERE NOT EXISTS (SELECT 1 FROM point_distribution pd WHERE pd.account_id = p.account_id);

COMMIT;


-- -------------------------------------------------------------- total_hype_point
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
UPDATE total_hype_point
SET hype_point = COALESCE((SELECT SUM(hype_point) FROM point), 0)
WHERE id = 1;
INSERT INTO total_hype_point (id, hype_point)
SELECT 1, COALESCE((SELECT SUM(hype_point) FROM point), 0)
WHERE NOT EXISTS (SELECT 1 FROM total_hype_point WHERE id = 1);
COMMIT;


-- -------------------------------------------------- hype_point_leaderboard_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
UPDATE hype_point_leaderboard_count
SET total_count = (SELECT COUNT(*) FROM point)
WHERE id = 1;
INSERT INTO hype_point_leaderboard_count (id, total_count)
SELECT 1, (SELECT COUNT(*) FROM point)
WHERE NOT EXISTS (SELECT 1 FROM hype_point_leaderboard_count WHERE id = 1);
COMMIT;


-- --------------------------------------------------- account_point_distribution_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM account_point_distribution_count;
INSERT INTO account_point_distribution_count (account_id, total_count, last_updated_at)
SELECT account_id, COUNT(*), EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
FROM point_distribution
GROUP BY account_id;
COMMIT;


-- --------------------------------------------------- reward_add_history.total_amount
-- Trigger semantics (BEFORE INSERT): NEW.total_amount = (sum of prior rows with same
-- (epoch, account, token)) + NEW.amount. So row N (in chronological order) carries the
-- running sum up to and including itself.
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
WITH ranked AS (
    SELECT
        ctid,
        SUM(amount) OVER (
            PARTITION BY epoch, account_id, token_id
            ORDER BY created_at, transaction_hash, log_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS new_total
    FROM reward_add_history
)
UPDATE reward_add_history rah
SET total_amount = r.new_total
FROM ranked r
WHERE rah.ctid = r.ctid;
COMMIT;


-- ----------------------------------------------------- reward_add_history_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM reward_add_history_count;
INSERT INTO reward_add_history_count (account_id, total_count, updated_at)
SELECT account_id, COUNT(*), EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
FROM reward_add_history
GROUP BY account_id;
COMMIT;


-- ------------------------------------------------------ account_vote_history_count
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM account_vote_history_count;
INSERT INTO account_vote_history_count (account_id, total_count)
SELECT account_id, COUNT(*)
FROM vote_history
GROUP BY account_id;
COMMIT;


-- ============================================================ V2 vault stats
-- v2_burn_vault_stats  ← v2_vault_burns WHERE vault_type='BURN'
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM v2_burn_vault_stats;
INSERT INTO v2_burn_vault_stats (token_id, quote_spent, quote_spent_usd, tokens_burned, burn_count, last_block, updated_at)
SELECT token_id,
       SUM(quote_in), SUM(usd_value), SUM(token_burned),
       COUNT(*),
       MAX(block_number), MAX(created_at)
FROM v2_vault_burns
WHERE vault_type = 'BURN'
GROUP BY token_id;
COMMIT;


-- v2_lp_vault_stats  ← v2_vault_lp_injections
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM v2_lp_vault_stats;
INSERT INTO v2_lp_vault_stats (token_id, quote_injected, quote_injected_usd, token_injected, lp_burned, inject_count, last_block, updated_at)
SELECT token_id,
       SUM(quote_used), SUM(usd_value), SUM(token_used), SUM(lp_burned),
       COUNT(*),
       MAX(block_number), MAX(created_at)
FROM v2_vault_lp_injections
GROUP BY token_id;
COMMIT;


-- v2_creator_fee_vault_stats  ← v2_creator_fee_claims (DEPOSIT and CLAIM)
-- The trigger's "current_balance" semantics:
--   DEPOSIT → set current_balance = NEW.new_balance (if not null, else keep)
--   CLAIM   → set current_balance = 0
-- Net effect at any time = the new_balance of the latest event (DEPOSIT or CLAIM, by block).
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM v2_creator_fee_vault_stats;

WITH
deposit_agg AS (
    SELECT token_id,
           SUM(amount) AS total_deposited,
           SUM(usd_value) AS total_deposited_usd,
           COUNT(*) AS deposit_count
    FROM v2_creator_fee_claims
    WHERE event_type = 'DEPOSIT'
    GROUP BY token_id
),
claim_agg AS (
    SELECT token_id,
           SUM(amount) AS total_claimed,
           SUM(usd_value) AS total_claimed_usd,
           COUNT(*) AS claim_count
    FROM v2_creator_fee_claims
    WHERE event_type = 'CLAIM'
    GROUP BY token_id
),
last_event AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        CASE event_type
            WHEN 'CLAIM' THEN 0
            ELSE COALESCE(new_balance, 0)
        END AS current_balance,
        block_number AS last_block,
        created_at  AS updated_at
    FROM v2_creator_fee_claims
    ORDER BY token_id, block_number DESC, created_at DESC
)
INSERT INTO v2_creator_fee_vault_stats
    (token_id, current_balance, total_deposited, total_deposited_usd, deposit_count,
     total_claimed, total_claimed_usd, claim_count, last_block, updated_at)
SELECT
    COALESCE(d.token_id, c.token_id, l.token_id),
    COALESCE(l.current_balance, 0),
    COALESCE(d.total_deposited, 0),
    COALESCE(d.total_deposited_usd, 0),
    COALESCE(d.deposit_count, 0),
    COALESCE(c.total_claimed, 0),
    COALESCE(c.total_claimed_usd, 0),
    COALESCE(c.claim_count, 0),
    l.last_block,
    l.updated_at
FROM deposit_agg d
FULL OUTER JOIN claim_agg c USING (token_id)
FULL OUTER JOIN last_event l USING (token_id);
COMMIT;


-- v2_gift_vault_stats  ← v2_gifts (SETUP/DEPOSIT/CLAIM/EXPIRE/RECEIVER_SET)
--                     + v2_vault_burns WHERE vault_type='GIFT' (buyback_* fields)
-- This is a state machine. We replay events in chronological order via a PL/pgSQL
-- DO block so the merge logic matches the trigger exactly.
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM v2_gift_vault_stats;

DO $do$
DECLARE
    r RECORD;
BEGIN
    -- replay v2_gifts in event order
    FOR r IN
        SELECT *
        FROM v2_gifts
        ORDER BY block_number, COALESCE(tx_index, 0), COALESCE(log_index, 0), created_at
    LOOP
        IF r.event_type = 'SETUP' THEN
            INSERT INTO v2_gift_vault_stats (token_id, current_state, platform, platform_id, expires_at, last_block, updated_at)
            VALUES (r.token_id, 'Accumulating', r.platform, r.platform_id, r.expires_at, r.block_number, r.created_at)
            ON CONFLICT (token_id) DO UPDATE SET
                platform    = COALESCE(EXCLUDED.platform, v2_gift_vault_stats.platform),
                platform_id = COALESCE(EXCLUDED.platform_id, v2_gift_vault_stats.platform_id),
                expires_at  = EXCLUDED.expires_at,
                last_block  = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
                updated_at  = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
        ELSIF r.event_type = 'DEPOSIT' THEN
            INSERT INTO v2_gift_vault_stats (token_id, current_balance, total_deposited, total_deposited_usd, last_block, updated_at)
            VALUES (r.token_id, COALESCE(r.new_balance, 0), r.amount, r.usd_value, r.block_number, r.created_at)
            ON CONFLICT (token_id) DO UPDATE SET
                current_balance     = COALESCE(EXCLUDED.current_balance, v2_gift_vault_stats.current_balance),
                total_deposited     = v2_gift_vault_stats.total_deposited     + EXCLUDED.total_deposited,
                total_deposited_usd = v2_gift_vault_stats.total_deposited_usd + EXCLUDED.total_deposited_usd,
                last_block          = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
                updated_at          = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
        ELSIF r.event_type = 'CLAIM' THEN
            INSERT INTO v2_gift_vault_stats (token_id, current_balance, total_claimed, total_claimed_usd, last_block, updated_at)
            VALUES (r.token_id, 0, r.amount, r.usd_value, r.block_number, r.created_at)
            ON CONFLICT (token_id) DO UPDATE SET
                current_balance   = 0,
                total_claimed     = v2_gift_vault_stats.total_claimed     + EXCLUDED.total_claimed,
                total_claimed_usd = v2_gift_vault_stats.total_claimed_usd + EXCLUDED.total_claimed_usd,
                last_block        = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
                updated_at        = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
        ELSIF r.event_type = 'EXPIRE' THEN
            INSERT INTO v2_gift_vault_stats (token_id, current_state, current_balance, total_expired, total_expired_usd, last_block, updated_at)
            VALUES (r.token_id, 'Burned', 0, r.amount, r.usd_value, r.block_number, r.created_at)
            ON CONFLICT (token_id) DO UPDATE SET
                current_state     = 'Burned',
                current_balance   = 0,
                total_expired     = v2_gift_vault_stats.total_expired     + EXCLUDED.total_expired,
                total_expired_usd = v2_gift_vault_stats.total_expired_usd + EXCLUDED.total_expired_usd,
                last_block        = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
                updated_at        = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
        ELSIF r.event_type = 'RECEIVER_SET' THEN
            INSERT INTO v2_gift_vault_stats (token_id, current_state, receiver, expires_at, receiver_set_at, last_block, updated_at)
            VALUES (r.token_id, 'Active', r.receiver, 0, r.created_at, r.block_number, r.created_at)
            ON CONFLICT (token_id) DO UPDATE SET
                current_state   = CASE v2_gift_vault_stats.current_state WHEN 'Burned' THEN 'Burned' ELSE 'Active' END,
                receiver        = COALESCE(EXCLUDED.receiver, v2_gift_vault_stats.receiver),
                expires_at      = 0,
                receiver_set_at = EXCLUDED.receiver_set_at,
                last_block      = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
                updated_at      = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
        END IF;
    END LOOP;

    -- replay v2_vault_burns(GIFT) → buyback_* columns
    FOR r IN
        SELECT *
        FROM v2_vault_burns
        WHERE vault_type = 'GIFT'
        ORDER BY block_number, COALESCE(tx_index, 0), COALESCE(log_index, 0), created_at
    LOOP
        INSERT INTO v2_gift_vault_stats
            (token_id, buyback_quote_spent, buyback_quote_spent_usd, buyback_tokens, last_block, updated_at)
        VALUES (r.token_id, r.quote_in, r.usd_value, r.token_burned, r.block_number, r.created_at)
        ON CONFLICT (token_id) DO UPDATE SET
            buyback_quote_spent     = v2_gift_vault_stats.buyback_quote_spent     + EXCLUDED.buyback_quote_spent,
            buyback_quote_spent_usd = v2_gift_vault_stats.buyback_quote_spent_usd + EXCLUDED.buyback_quote_spent_usd,
            buyback_tokens          = v2_gift_vault_stats.buyback_tokens          + EXCLUDED.buyback_tokens,
            last_block              = GREATEST(v2_gift_vault_stats.last_block, EXCLUDED.last_block),
            updated_at              = GREATEST(v2_gift_vault_stats.updated_at, EXCLUDED.updated_at);
    END LOOP;
END;
$do$;
COMMIT;


-- v2_creator_fee_distribution_stats  ← v2_creator_fee_distribution
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
DELETE FROM v2_creator_fee_distribution_stats;
INSERT INTO v2_creator_fee_distribution_stats
    (token_id, vault_id, quote_id, distributed_quote, distributed_quote_usd,
     distribute_count, last_block, updated_at)
SELECT
    token,
    vault,
    MAX(quote_id),
    SUM(amount),
    SUM(usd_value),
    COUNT(*),
    MAX(block_number),
    MAX(created_at)
FROM v2_creator_fee_distribution
WHERE event_type = 'DISTRIBUTE'
  AND token IS NOT NULL
  AND vault IS NOT NULL
GROUP BY token, vault;
COMMIT;


-- token.creator  ←  latest v2_creator_updates per token
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
WITH latest AS (
    SELECT DISTINCT ON (token_id)
        token_id, new_creator
    FROM v2_creator_updates
    ORDER BY token_id, block_number DESC, created_at DESC
)
UPDATE token t
SET creator = latest.new_creator
FROM latest
WHERE t.token_id = latest.token_id
  AND latest.new_creator IS NOT NULL;
COMMIT;


-- =============================================================================
-- Heavy backfills (chart, position) — run LAST so prior recomputes succeed even
-- if these need to be re-run or fail.
-- =============================================================================

-- -------------------------------------------------------------------------- fee
-- already handled above. position next.


-- --------------------------------------------------------------------- position
-- position is order-dependent (transfer_in pulls sender's avg cost from the
-- CURRENT position state). We replay position_history through the trigger
-- function in chronological order via a DO block so cost-basis math matches
-- the trigger exactly.
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
LOCK TABLE position_history IN EXCLUSIVE MODE;
DELETE FROM position;

DO $do$
DECLARE
    r RECORD;
    sender_position RECORD;
    avg_cost_quote NUMERIC;
    avg_cost_usd NUMERIC;
    transfer_cost_quote NUMERIC;
    transfer_cost_usd NUMERIC;
    current_balance NUMERIC;
    eff_quote_in NUMERIC;
    eff_quote_out NUMERIC;
    eff_usd_in NUMERIC;
    eff_usd_out NUMERIC;
BEGIN
    FOR r IN
        SELECT *
        FROM position_history
        ORDER BY block_number, tx_index, log_index, created_at
    LOOP
        eff_quote_in  := r.quote_in;
        eff_quote_out := r.quote_out;
        eff_usd_in    := r.usd_in;
        eff_usd_out   := r.usd_out;

        IF r.transfer_type = 'transfer_out' THEN
            SELECT quote_out, usd_out, token_in, token_out
            INTO sender_position
            FROM position
            WHERE account_id = r.account_id AND token_id = r.token_id;

            IF FOUND AND sender_position.token_in > 0 THEN
                current_balance := sender_position.token_in - sender_position.token_out;
                IF current_balance > 0 THEN
                    avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                    avg_cost_usd   := sender_position.usd_out   / sender_position.token_in;
                    transfer_cost_quote := avg_cost_quote * r.token_out;
                    transfer_cost_usd   := avg_cost_usd   * r.token_out;
                    eff_quote_in := transfer_cost_quote;
                    eff_usd_in   := transfer_cost_usd;
                END IF;
            END IF;
        END IF;

        IF r.transfer_type = 'transfer_in' AND r.sender_address IS NOT NULL THEN
            SELECT quote_out, usd_out, token_in, token_out
            INTO sender_position
            FROM position
            WHERE account_id = r.sender_address AND token_id = r.token_id;

            IF FOUND AND sender_position.token_in > 0 THEN
                current_balance := sender_position.token_in - sender_position.token_out;
                IF current_balance > 0 THEN
                    avg_cost_quote := sender_position.quote_out / sender_position.token_in;
                    avg_cost_usd   := sender_position.usd_out   / sender_position.token_in;
                    transfer_cost_quote := avg_cost_quote * r.token_in;
                    transfer_cost_usd   := avg_cost_usd   * r.token_in;
                    eff_quote_out := transfer_cost_quote;
                    eff_usd_out   := transfer_cost_usd;
                END IF;
            END IF;
        END IF;

        INSERT INTO position (
            account_id, token_id,
            quote_in, quote_out, usd_in, usd_out, token_in, token_out,
            created_at, updated_at
        )
        VALUES (
            r.account_id, r.token_id,
            eff_quote_in, eff_quote_out, eff_usd_in, eff_usd_out, r.token_in, r.token_out,
            r.created_at, r.created_at
        )
        ON CONFLICT (account_id, token_id) DO UPDATE SET
            quote_in  = position.quote_in  + EXCLUDED.quote_in,
            quote_out = position.quote_out + EXCLUDED.quote_out,
            usd_in    = position.usd_in    + EXCLUDED.usd_in,
            usd_out   = position.usd_out   + EXCLUDED.usd_out,
            token_in  = position.token_in  + EXCLUDED.token_in,
            token_out = position.token_out + EXCLUDED.token_out,
            updated_at = EXCLUDED.updated_at;

        -- Note: we deliberately do NOT write the mutated quote_in/usd_in/quote_out/usd_out
        -- back into position_history. The base trigger is BEFORE INSERT so it only ever
        -- ran on incoming rows; existing rows have whatever value the trigger wrote at the
        -- time they landed. The aggregate in `position` is the source of truth.
    END LOOP;
END;
$do$;
COMMIT;


-- ------------------------------------------------------------------------- chart
-- chart is a row-by-row OHLC accumulator over price_history with per-row USD rate.
-- Faithful replay needs each row's contemporaneous USD rate. We use a CTE that joins
-- each price_history row to the latest USD price <= its block_number (with global
-- fallback), then aggregate per (token, interval, bucket).
BEGIN;
SELECT pg_advisory_xact_lock(8470129331477219347);
LOCK TABLE price_history IN EXCLUSIVE MODE;

-- chart_count_trigger fires on every INSERT into chart and inflates chart_count
-- to a transient (wrong) value during this rebuild. We DELETE+rebuild chart_count
-- from scratch below, so the trigger's mid-flight values are overwritten.
-- Logical replication does not fire triggers on replicas (pgactive-safe).

DELETE FROM chart;

-- Pre-resolve USD rate per price_history row. Fall back to global latest.
WITH
fallback AS (
    SELECT price FROM price ORDER BY block_number DESC LIMIT 1
),
ph_with_rate AS (
    SELECT
        ph.token_id,
        ph.price,
        ph.volume,
        ph.created_at,
        ph.block_number,
        COALESCE(
            (SELECT p.price FROM price p WHERE p.block_number <= ph.block_number ORDER BY p.block_number DESC LIMIT 1),
            (SELECT price FROM fallback),
            1
        ) AS usd_rate
    FROM price_history ph
),
ph_expanded AS (
    -- Cartesian: each price_history row × 9 intervals
    SELECT
        r.token_id, r.price, r.volume, r.created_at, r.block_number, r.usd_rate,
        i.interval_type,
        convert_chart_timestamp(r.created_at, i.interval_type) AS bucket_ts
    FROM ph_with_rate r
    CROSS JOIN (VALUES ('1'),('5'),('15'),('30'),('1H'),('4H'),('D'),('W'),('M')) i(interval_type)
),
ordered AS (
    SELECT
        token_id, interval_type, bucket_ts,
        price, volume,
        price * usd_rate AS usd_price,
        volume * usd_rate AS usd_vol_row,
        block_number, created_at,
        ROW_NUMBER() OVER (PARTITION BY token_id, interval_type, bucket_ts ORDER BY block_number, created_at)                        AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY token_id, interval_type, bucket_ts ORDER BY block_number DESC, created_at DESC)              AS rn_last
    FROM ph_expanded
),
per_bucket AS (
    SELECT
        token_id, interval_type, bucket_ts,
        MAX(CASE WHEN rn_first = 1 THEN price     END) AS first_price,
        MAX(CASE WHEN rn_last  = 1 THEN price     END) AS last_price,
        MAX(price)        AS high_price,
        MIN(price)        AS low_price,
        SUM(volume)       AS volume_sum,
        MAX(CASE WHEN rn_first = 1 THEN usd_price END) AS first_usd_price,
        MAX(CASE WHEN rn_last  = 1 THEN usd_price END) AS last_usd_price,
        MAX(usd_price)    AS high_usd_price,
        MIN(usd_price)    AS low_usd_price,
        SUM(usd_vol_row)  AS usd_volume_sum
    FROM ordered
    GROUP BY token_id, interval_type, bucket_ts
),
with_prev AS (
    SELECT
        pb.*,
        LAG(last_price)     OVER (PARTITION BY token_id, interval_type ORDER BY bucket_ts) AS prev_close,
        LAG(last_usd_price) OVER (PARTITION BY token_id, interval_type ORDER BY bucket_ts) AS prev_usd_close
    FROM per_bucket pb
)
INSERT INTO chart (
    token_id, interval_type, time_stamp,
    open_price, close_price, high_price, low_price, volume, total_supply,
    usd_open_price, usd_close_price, usd_high_price, usd_low_price, usd_volume
)
SELECT
    w.token_id,
    w.interval_type,
    w.bucket_ts,
    COALESCE(w.prev_close,     w.first_price)     AS open_price,
    w.last_price                                   AS close_price,
    w.high_price,
    w.low_price,
    w.volume_sum,
    COALESCE(t.total_supply, 0)                    AS total_supply,
    COALESCE(w.prev_usd_close, w.first_usd_price)  AS usd_open_price,
    w.last_usd_price                               AS usd_close_price,
    w.high_usd_price                               AS usd_high_price,
    w.low_usd_price                                AS usd_low_price,
    w.usd_volume_sum                               AS usd_volume
FROM with_prev w
LEFT JOIN token t ON t.token_id = w.token_id;

-- chart_count = COUNT(*) per (token, interval)
DELETE FROM chart_count;
INSERT INTO chart_count (token_id, interval_type, count)
SELECT token_id, interval_type, COUNT(*)
FROM chart
GROUP BY token_id, interval_type;

COMMIT;


-- =============================================================================
-- DONE. Validation queries (uncomment to inspect):
-- =============================================================================
-- SELECT 'token_count'           AS table, * FROM token_count;
-- SELECT 'swap_count rows'       AS table, COUNT(*) FROM swap_count;
-- SELECT 'balance rows'          AS table, COUNT(*) FROM balance;
-- SELECT 'token_holder_count >0' AS table, COUNT(*) FROM token WHERE token_holder_count > 0;
-- SELECT 'fee rows'              AS table, COUNT(*) FROM fee;
-- SELECT 'position rows'         AS table, COUNT(*) FROM position;
-- SELECT 'chart rows'            AS table, COUNT(*) FROM chart;
-- SELECT 'chart_count rows'      AS table, COUNT(*) FROM chart_count;
-- SELECT 'total_hype_point'      AS table, * FROM total_hype_point;
-- SELECT 'hype_lb_count'         AS table, * FROM hype_point_leaderboard_count;
