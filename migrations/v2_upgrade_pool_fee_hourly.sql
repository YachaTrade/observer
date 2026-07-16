-- v2_upgrade_pool_fee_hourly.sql
--
-- Idempotent prod upgrade for the schema/trigger/view added by
-- migrations/0027_pool_fee_hourly.sql. Apply manually on prod where the
-- numbered migration cannot run (pre-existing DB state).
--
-- Keep the body below in sync with 0027_pool_fee_hourly.sql.

-- (1) pool: baseline columns
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sqrt_k         NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_at        BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_block     BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_tx_index  INT     NOT NULL DEFAULT 0;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_sync_log_index INT     NOT NULL DEFAULT 0;

-- (2) hourly bucket
CREATE TABLE IF NOT EXISTS pool_fee_hourly (
    pool_id        VARCHAR(42) NOT NULL,
    bucket_hour    BIGINT      NOT NULL,
    fee_token0     NUMERIC     NOT NULL DEFAULT 0,
    fee_token1     NUMERIC     NOT NULL DEFAULT 0,
    fee_usd        NUMERIC     NOT NULL DEFAULT 0,
    tvl_usd_sum    NUMERIC     NOT NULL DEFAULT 0,
    sample_count   INT         NOT NULL DEFAULT 0,
    updated_at     BIGINT      NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
    PRIMARY KEY (pool_id, bucket_hour)
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_hourly_pool_hour
    ON pool_fee_hourly (pool_id, bucket_hour DESC);
CREATE INDEX IF NOT EXISTS idx_pool_fee_hourly_hour
    ON pool_fee_hourly (bucket_hour DESC);

-- (3) trigger function: sqrt(k) ratio fee accrual with codex P1/P2 fixes.
CREATE OR REPLACE FUNCTION update_pool_fee_accrual()
RETURNS TRIGGER AS $$
BEGIN
    WITH
    annotated AS (
        SELECT
            s.pool_id, s.reserve0, s.reserve1, s.created_at,
            s.block_number, s.tx_index, s.log_index,
            s.token0_usd, s.token1_usd,
            (m.pool_id IS NOT NULL OR b.pool_id IS NOT NULL) AS is_blocked
        FROM new_dex_syncs s
        LEFT JOIN dex_mint m
            ON m.pool_id = s.pool_id
           AND m.transaction_hash = s.transaction_hash
        LEFT JOIN dex_burn b
            ON b.pool_id = s.pool_id
           AND b.transaction_hash = s.transaction_hash
    ),
    ordered AS (
        SELECT
            a.*,
            sqrt(a.reserve0 * a.reserve1) AS sqrt_k_new,
            LAG(sqrt(a.reserve0 * a.reserve1)) OVER w AS sqrt_k_old_intra,
            (SELECT p.last_sqrt_k FROM pool p WHERE p.pool_id = a.pool_id) AS sqrt_k_pool,
            (SELECT (p.last_sync_block, p.last_sync_tx_index, p.last_sync_log_index)
               FROM pool p WHERE p.pool_id = a.pool_id) AS pool_freshness
        FROM annotated a
        WINDOW w AS (
            PARTITION BY a.pool_id
            ORDER BY a.block_number, a.tx_index, a.log_index
        )
    ),
    fee_rows AS (
        SELECT
            pool_id,
            (created_at / 3600)::BIGINT                      AS bucket_hour,
            (sqrt_k_new / sqrt_k_old - 1) * reserve0         AS fee_token0,
            (sqrt_k_new / sqrt_k_old - 1) * reserve1         AS fee_token1,
            (sqrt_k_new / sqrt_k_old - 1) * (token0_usd + token1_usd) AS fee_usd,
            (token0_usd + token1_usd)                        AS tvl_usd_at_evt
        FROM (
            SELECT
                pool_id, reserve0, reserve1, created_at, token0_usd, token1_usd, sqrt_k_new,
                COALESCE(
                    sqrt_k_old_intra,
                    CASE WHEN (block_number, tx_index, log_index) > pool_freshness
                         THEN sqrt_k_pool
                    END
                ) AS sqrt_k_old,
                is_blocked
            FROM ordered
        ) x
        WHERE NOT is_blocked
          AND sqrt_k_old IS NOT NULL
          AND sqrt_k_old > 0
          AND sqrt_k_new > sqrt_k_old
    )
    INSERT INTO pool_fee_hourly (
        pool_id, bucket_hour, fee_token0, fee_token1, fee_usd, tvl_usd_sum, sample_count, updated_at
    )
    SELECT
        pool_id, bucket_hour,
        SUM(fee_token0), SUM(fee_token1), SUM(fee_usd),
        SUM(tvl_usd_at_evt), COUNT(*)::INT,
        EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
    FROM fee_rows
    GROUP BY pool_id, bucket_hour
    ON CONFLICT (pool_id, bucket_hour) DO UPDATE SET
        fee_token0   = pool_fee_hourly.fee_token0   + EXCLUDED.fee_token0,
        fee_token1   = pool_fee_hourly.fee_token1   + EXCLUDED.fee_token1,
        fee_usd      = pool_fee_hourly.fee_usd      + EXCLUDED.fee_usd,
        tvl_usd_sum  = pool_fee_hourly.tvl_usd_sum  + EXCLUDED.tvl_usd_sum,
        sample_count = pool_fee_hourly.sample_count + EXCLUDED.sample_count,
        updated_at   = EXCLUDED.updated_at;

    UPDATE pool p
       SET last_sqrt_k         = d.sqrt_k_new,
           last_sync_at        = d.created_at,
           last_sync_block     = d.block_number,
           last_sync_tx_index  = d.tx_index,
           last_sync_log_index = d.log_index
      FROM (
          SELECT DISTINCT ON (pool_id)
                 pool_id, sqrt(reserve0 * reserve1) AS sqrt_k_new,
                 created_at, block_number, tx_index, log_index
            FROM new_dex_syncs
           ORDER BY pool_id, block_number DESC, tx_index DESC, log_index DESC
      ) d
     WHERE p.pool_id = d.pool_id
       AND (d.block_number, d.tx_index, d.log_index)
           > (p.last_sync_block, p.last_sync_tx_index, p.last_sync_log_index);

    PERFORM 1
      FROM new_dex_syncs s
      JOIN pool p ON p.pool_id = s.pool_id
     WHERE p.last_sqrt_k > 0
       AND sqrt(s.reserve0 * s.reserve1) < p.last_sqrt_k
       AND NOT EXISTS (
           SELECT 1 FROM dex_mint m
            WHERE m.pool_id = s.pool_id AND m.transaction_hash = s.transaction_hash
       )
       AND NOT EXISTS (
           SELECT 1 FROM dex_burn b
            WHERE b.pool_id = s.pool_id AND b.transaction_hash = s.transaction_hash
       );
    IF FOUND THEN
        RAISE WARNING 'pool_fee_accrual: sqrt_k decreased without mint/burn (mint/burn missing?)';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS a_trg_update_pool_fee_accrual ON dex_sync;
CREATE TRIGGER a_trg_update_pool_fee_accrual
    AFTER INSERT ON dex_sync
    REFERENCING NEW TABLE AS new_dex_syncs
    FOR EACH STATEMENT
    EXECUTE FUNCTION update_pool_fee_accrual();

-- (4) pool_apr view — exposes gross AND LP-net fee (after 20% _mintFee
-- protocol carve-out; LP_SHARE_FACTOR = 0.8 while factory.feeTo() != 0).
CREATE OR REPLACE VIEW pool_apr AS
WITH now_h AS (SELECT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600)::BIGINT AS h),
     params AS (SELECT 0.8::numeric AS lp_share_factor)
SELECT
    f.pool_id,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24)        AS fee_24h_usd,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)      AS fee_7d_usd,
    SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)     AS fee_30d_usd,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24)     AS fee_24h_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24)     AS fee_24h_token1,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)   AS fee_7d_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)   AS fee_7d_token1,
    SUM(f.fee_token0) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)  AS fee_30d_token0,
    SUM(f.fee_token1) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)  AS fee_30d_token1,
    params.lp_share_factor * SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24)
        AS lp_fee_24h_usd,
    params.lp_share_factor * SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)
        AS lp_fee_7d_usd,
    params.lp_share_factor * SUM(f.fee_usd) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)
        AS lp_fee_30d_usd,
    params.lp_share_factor AS lp_share_factor,
    SUM(f.tvl_usd_sum) FILTER (WHERE f.bucket_hour >= now_h.h - 24)
        / NULLIF(SUM(f.sample_count) FILTER (WHERE f.bucket_hour >= now_h.h - 24), 0)
        AS tvl_24h_usd_avg,
    SUM(f.tvl_usd_sum) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7)
        / NULLIF(SUM(f.sample_count) FILTER (WHERE f.bucket_hour >= now_h.h - 24*7), 0)
        AS tvl_7d_usd_avg,
    SUM(f.tvl_usd_sum) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30)
        / NULLIF(SUM(f.sample_count) FILTER (WHERE f.bucket_hour >= now_h.h - 24*30), 0)
        AS tvl_30d_usd_avg
FROM pool_fee_hourly f
CROSS JOIN now_h
CROSS JOIN params
WHERE f.bucket_hour >= now_h.h - 24*30
GROUP BY f.pool_id, now_h.h, params.lp_share_factor;
