# V2 LP Position Tracking — Design Spec (Position Pattern)

**Date:** 2026-05-14
**Branch:** v2 → feat/v2-lp-position
**Supersedes:** `2026-05-13-v2-lp-tracking-design.md`

## Why rewrite

Initial design used direct ± mutation on `lp_position(balance, cost_amount0, cost_amount1)`. Codebase convention (`migrations/0013_position.sql`) uses an **accumulating in/out** pattern with a separate `*_history` table. Reasons to follow that pattern for LP:

1. **Mint/burn history is preserved automatically** — `lp_position_history` records every event with `event_type`. No need for a separate "did this user ever LP?" query.
2. **Burn 회수 금액 기록**: BURN trigger uses `dex_burn(pool, tx).amount0/1` (실제 회수량) instead of proportional cost reduction. Realized P&L is exact.
3. **Cost basis ↔ transfer pattern matches `update_position_on_history` exactly** — same avg-cost-from-sender logic, less risk of subtle drift.
4. **`balance = 0` row deletion is clean** — current LP holders only in `lp_position`. History stays in `lp_position_history`. Re-entry creates a fresh row.

## Data model

```sql
-- All LP events as account-scoped rows (chain Transfer → 1 row mint/burn, 2 rows holder transfer)
CREATE TABLE lp_position_history (
    account_id   VARCHAR(42) NOT NULL,
    pool_id      VARCHAR(42) NOT NULL,

    lp_in        NUMERIC NOT NULL DEFAULT 0,
    lp_out       NUMERIC NOT NULL DEFAULT 0,

    token0_in    NUMERIC NOT NULL DEFAULT 0,   -- mint 공급량 / transfer_in 평균cost
    token0_out   NUMERIC NOT NULL DEFAULT 0,   -- burn 실제 회수량 / transfer_out 평균cost
    token1_in    NUMERIC NOT NULL DEFAULT 0,
    token1_out   NUMERIC NOT NULL DEFAULT 0,

    event_type     VARCHAR(20) NOT NULL,   -- 'mint' | 'burn' | 'transfer_in' | 'transfer_out'
    counterparty   VARCHAR(42),            -- transfer 시 상대방 (mint/burn 시 NULL)

    transaction_hash VARCHAR(66) NOT NULL,
    block_number     BIGINT      NOT NULL,
    tx_index         INT         NOT NULL,
    log_index        INT         NOT NULL,
    created_at       BIGINT      NOT NULL,

    PRIMARY KEY (account_id, pool_id, transaction_hash, tx_index, log_index)
);

-- Current holders only (DELETE when lp_in == lp_out)
CREATE TABLE lp_position (
    account_id   VARCHAR(42) NOT NULL,
    pool_id      VARCHAR(42) NOT NULL,
    lp_in        NUMERIC NOT NULL DEFAULT 0,
    lp_out       NUMERIC NOT NULL DEFAULT 0,
    token0_in    NUMERIC NOT NULL DEFAULT 0,
    token0_out   NUMERIC NOT NULL DEFAULT 0,
    token1_in    NUMERIC NOT NULL DEFAULT 0,
    token1_out   NUMERIC NOT NULL DEFAULT 0,
    created_at   BIGINT NOT NULL,
    updated_at   BIGINT NOT NULL,
    PRIMARY KEY (account_id, pool_id)
    -- balance = lp_in - lp_out (가상 계산)
);

ALTER TABLE pool ADD COLUMN total_supply NUMERIC(78,0) NOT NULL DEFAULT 0;
```

## Trigger (`update_lp_position_on_history`) — BEFORE INSERT on `lp_position_history`

Single function, four `event_type` branches. Pattern mirrors `update_position_on_history` exactly.

```
BEGIN
    -- 1. Fill in cost basis based on event_type
    CASE NEW.event_type
        'mint':
            SELECT amount0, amount1 INTO v_a0, v_a1
            FROM dex_mint
            WHERE pool_id = NEW.pool_id AND transaction_hash = NEW.transaction_hash
            ORDER BY log_index LIMIT 1;
            NEW.token0_in := COALESCE(v_a0, 0);
            NEW.token1_in := COALESCE(v_a1, 0);
            UPDATE pool SET total_supply = total_supply + NEW.lp_in WHERE pool_id = NEW.pool_id;

        'burn':
            SELECT amount0, amount1 INTO v_a0, v_a1
            FROM dex_burn
            WHERE pool_id = NEW.pool_id AND transaction_hash = NEW.transaction_hash
            ORDER BY log_index LIMIT 1;
            NEW.token0_out := COALESCE(v_a0, 0);
            NEW.token1_out := COALESCE(v_a1, 0);
            UPDATE pool SET total_supply = total_supply - NEW.lp_out WHERE pool_id = NEW.pool_id;

        'transfer_out':
            SELECT lp_in - lp_out, token0_in, token1_in
            INTO sender_lp_balance, sender_cost0_in, sender_cost1_in
            FROM lp_position
            WHERE account_id = NEW.account_id AND pool_id = NEW.pool_id;
            IF sender_lp_balance > 0 THEN
                avg_cost0 := sender_cost0_in / (sender_lp_balance + NEW.lp_out);  -- pre-event balance
                avg_cost1 := sender_cost1_in / (sender_lp_balance + NEW.lp_out);
                -- Wait: avg = lifetime_token0_in / lifetime_lp_in, not / balance
                avg_cost0 := sender_cost0_in / (lifetime_lp_in);  -- use position pattern
                avg_cost1 := sender_cost1_in / (lifetime_lp_in);
                NEW.token0_out := avg_cost0 * NEW.lp_out;
                NEW.token1_out := avg_cost1 * NEW.lp_out;
            END IF;

        'transfer_in':
            -- Same avg cost as the sender (counterparty)'s lifetime
            SELECT lp_in, token0_in, token1_in INTO sender_lp_in, sender_cost0_in, sender_cost1_in
            FROM lp_position
            WHERE account_id = NEW.counterparty AND pool_id = NEW.pool_id;
            IF sender_lp_in > 0 THEN
                avg_cost0 := sender_cost0_in / sender_lp_in;
                avg_cost1 := sender_cost1_in / sender_lp_in;
                NEW.token0_in := avg_cost0 * NEW.lp_in;
                NEW.token1_in := avg_cost1 * NEW.lp_in;
            END IF;
    END CASE;

    -- 2. UPSERT lp_position (accumulate in/out)
    INSERT INTO lp_position(account_id, pool_id, lp_in, lp_out, token0_in, token0_out, token1_in, token1_out, created_at, updated_at)
    VALUES (NEW.account_id, NEW.pool_id, NEW.lp_in, NEW.lp_out, NEW.token0_in, NEW.token0_out, NEW.token1_in, NEW.token1_out, NEW.created_at, NEW.created_at)
    ON CONFLICT (account_id, pool_id) DO UPDATE SET
        lp_in = lp_position.lp_in + EXCLUDED.lp_in,
        lp_out = lp_position.lp_out + EXCLUDED.lp_out,
        token0_in = lp_position.token0_in + EXCLUDED.token0_in,
        token0_out = lp_position.token0_out + EXCLUDED.token0_out,
        token1_in = lp_position.token1_in + EXCLUDED.token1_in,
        token1_out = lp_position.token1_out + EXCLUDED.token1_out,
        updated_at = EXCLUDED.updated_at;

    -- 3. DELETE row if balance reached zero
    DELETE FROM lp_position
    WHERE account_id = NEW.account_id AND pool_id = NEW.pool_id
      AND lp_in = lp_out;

    RETURN NEW;
END;
```

## Indexing flow

Chain `Transfer(from, to, value)` on a Pair → Rust receive layer derives 1 or 2 `lp_position_history` rows:

```
from = 0x0         → 1 row: { account=to,   event='mint',  lp_in=amount, counterparty=NULL }
to   = 0x0         → 1 row: { account=from, event='burn',  lp_out=amount, counterparty=NULL }
else (holder→holder) → 2 rows:
   { account=from, event='transfer_out', lp_out=amount, counterparty=to }
   { account=to,   event='transfer_in',  lp_in=amount,  counterparty=from }
```

Rust changes from previous attempt:
- `LpTransferData` → `LpPositionHistoryEvent` (event_type + counterparty + in/out fields)
- One chain Transfer → `Vec<LpPositionHistoryEvent>` (1 or 2 entries)
- `LpTransferController` → `LpPositionController` with `batch_insert_lp_position_history` SQL

Stream/receive ordering (P1-2/P2 fixes from previous codex review) — **keep** as-is. Same dependencies needed.

## API surface

```sql
-- 내 LP 화면 (현재 보유)
SELECT
    lp.pool_id, p.token0, p.token1,
    (lp.lp_in - lp.lp_out)                                          AS my_lp,
    lp.token0_in                                                    AS lifetime_supplied_token0,
    lp.token1_in                                                    AS lifetime_supplied_token1,
    lp.token0_out                                                   AS lifetime_received_token0,
    lp.token1_out                                                   AS lifetime_received_token1,
    (lp.lp_in - lp.lp_out) * p.reserve0 / NULLIF(p.total_supply, 0) AS current_share_token0,
    (lp.lp_in - lp.lp_out) * p.reserve1 / NULLIF(p.total_supply, 0) AS current_share_token1
FROM lp_position lp
JOIN pool p USING (pool_id)
WHERE lp.account_id = $1;

-- Realized P&L (closed positions OR partially-closed):
-- realized_token0 = lp.token0_out - (lp.token0_in × lp.lp_out / lp.lp_in)
```

## Non-goals (unchanged)

APR / TVL / volume_24h / IL / 누적 fee earned / reorg / 기존 풀 백필 — all separate phases.

## Test plan

Match the position pattern's test coverage:
1. Schema smoke
2. Idempotent upgrade
3. MINT history records dex_mint amounts as token_in
4. BURN history records dex_burn amounts as token_out (actual withdrawn, not proportional)
5. Holder→holder produces 2 history rows; sender token_out and recipient token_in use same avg cost
6. Position table deletes when `lp_in == lp_out`
7. Re-entry after full burn creates fresh row
8. E2E sequence
9. Sync ordering (existing tests carry over)
