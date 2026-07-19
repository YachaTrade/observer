use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use tracing::{debug, error, info, warn};

use crate::{
    client::RpcClient,
    config::WNATIVE_ADDRESS,
    db::{postgres::PostgresDatabase, redis::RedisDatabase},
    sync::{EventType, stream::STREAM_MANAGER},
};
use bigdecimal::BigDecimal;
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use sqlx::Row;
use tokio::sync::RwLock;

// 전역 CacheManager 인스턴스
static CACHE_MANAGER: OnceCell<Arc<CacheManager>> = OnceCell::new();

/// Token 정보 캐싱을 위한 관리자 구조체
/// Redis를 1차 캐시로 사용하고, PostgreSQL을 2차 저장소로 사용합니다.
pub struct CacheManager {
    redis: Arc<RedisDatabase>,
    postgres: Arc<PostgresDatabase>,
    // Per-quote USD price cache (Pyth feed). quote_id -> (block_number -> price)
    // Source: external oracle (Pyth). Meaning: USD price of `quote_id` at block.
    price_cache: Arc<DashMap<String, DashMap<i64, Arc<BigDecimal>>>>,
    // Per-quote insertion order for cleanup. quote_id -> VecDeque<block_number>
    price_insertion_order: Arc<RwLock<std::collections::HashMap<String, std::collections::VecDeque<i64>>>>,
    // Per-token WMON-implied price cache (on-chain inferred). token_id -> (block_number -> price-in-WMON).
    // Source: derived from RawSync reserve ratios as swaps are processed.
    // Meaning: how much WMON one unit of `token_id` is worth at block, expressed in
    // human-scaled units (already divided by token decimals on both sides).
    // Composed with price_cache[WMON][block] to produce USD value.
    token_price_cache: Arc<DashMap<String, DashMap<i64, Arc<BigDecimal>>>>,
    // Per-token insertion order for cleanup. token_id -> VecDeque<block_number>
    token_price_insertion_order:
        Arc<RwLock<std::collections::HashMap<String, std::collections::VecDeque<i64>>>>,
    // Per-token decimals factor cache (10^decimals as BigDecimal). Chain-immutable,
    // so entries are insert-once and never evicted. Looked up in quote_token /
    // dex_token / token tables on first miss.
    token_decimals_cache: Arc<DashMap<String, Arc<BigDecimal>>>,
    // Set of EVM addresses (EIP-55 normalized) treated as MON-native for the
    // chain-implied price graph. Loaded once at startup from
    // `quote_token WHERE is_native = TRUE`. Membership short-circuits price
    // resolution to "1 in WMON units" so LVMON-X and WMON-X pools both
    // cascade prices into token_price_cache.
    native_addresses: Arc<std::collections::HashSet<String>>,
}

impl CacheManager {
    /// 글로벌 인스턴스 초기화
    pub async fn init() -> Result<()> {
        if CACHE_MANAGER.get().is_some() {
            info!("CacheManager already initialized");
            return Ok(());
        }

        let instance = Self::new().await?;
        let arc_instance = Arc::new(instance);

        if CACHE_MANAGER.set(arc_instance).is_err() {
            info!("CacheManager was initialized by another task");
        } else {
            info!("CacheManager global instance initialized successfully");
        }

        Ok(())
    }

    /// 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<Arc<CacheManager>> {
        CACHE_MANAGER
            .get()
            .map(Arc::clone)
            .ok_or_else(|| anyhow!("CacheManager not initialized. Call CacheManager::init() first"))
    }

    pub async fn new() -> Result<Self> {
        let redis = RedisDatabase::instance()?;
        let postgres = PostgresDatabase::instance()?;
        let price_cache = Arc::new(DashMap::new());
        let price_insertion_order = Arc::new(RwLock::new(std::collections::HashMap::new()));
        let token_price_cache = Arc::new(DashMap::new());
        let token_price_insertion_order =
            Arc::new(RwLock::new(std::collections::HashMap::new()));
        let token_decimals_cache = Arc::new(DashMap::new());

        // Load native-equivalent quote addresses (WMON, LVMON, future MON-pegged
        // wrappers). Stored as EIP-55-normalized strings — match Address parse +
        // Display so lookups can be case-insensitive against any caller's casing.
        //
        // Propagate the query error instead of defaulting: a missing is_native
        // column (migration 0028 not applied) or any DB hiccup at startup would
        // otherwise silently collapse the native set to just WMON, leaving
        // LVMON-paired pools writing value=0 with no surfaced signal. Failing
        // loud forces the operator to apply migrations before the indexer runs.
        let native_addresses = {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT quote_id FROM quote_token WHERE is_native = TRUE",
            )
            .fetch_all(&postgres.pool)
            .await?;
            let mut set = std::collections::HashSet::new();
            for (raw,) in rows {
                if let Ok(addr) = raw.parse::<alloy::primitives::Address>() {
                    set.insert(addr.to_string());
                }
            }
            // Fail-safe (post-query, not error-swallow): always include the
            // env-configured WMON so a mis-seeded quote_token row — column
            // present, but WMON not flagged is_native — doesn't break value
            // computation. Different concern from the swallow-on-query-fail
            // case above; we still want loud failure if the query itself
            // errors out.
            set.insert(crate::config::WNATIVE_ADDRESS.clone());
            info!("[CACHE] Loaded {} native-equivalent quote addresses", set.len());
            Arc::new(set)
        };

        let manager = Self {
            redis,
            postgres,
            price_cache,
            price_insertion_order,
            token_price_cache,
            token_price_insertion_order,
            token_decimals_cache,
            native_addresses,
        };

        Ok(manager)
    }

    /// Returns true if `addr` is one of the MON-native quote addresses loaded
    /// at startup (WMON + every `quote_token.is_native = true` row).
    /// Case-insensitive — matches against EIP-55-normalized membership.
    pub fn is_native(&self, addr: &str) -> bool {
        self.native_addresses
            .iter()
            .any(|a| a.eq_ignore_ascii_case(addr))
    }

    /// StreamManager 초기화 후 호출하여 시작 블록부터 최신 블록까지의 price를 로드
    pub async fn load_initial_prices_from_stream(&self) -> Result<()> {
        let price_block_range = STREAM_MANAGER.get_event_block_range(EventType::Price).await;
        let start_block = price_block_range.from_block as i64;

        let prices: Vec<(String, i64, BigDecimal)> =
            sqlx::query_as::<_, (String, i64, BigDecimal)>(
                r#"
                SELECT quote_id, block_number, price
                FROM price
                WHERE block_number >= $1
                ORDER BY quote_id, block_number ASC
                "#,
            )
            .bind(start_block)
            .fetch_all(&self.postgres.pool)
            .await?;

        if prices.is_empty() {
            info!(
                "[CACHE] No prices found in DB to preload from start_block: {}",
                start_block
            );
            return Ok(());
        }

        let mut order_map = self.price_insertion_order.write().await;
        let mut total_loaded = 0usize;

        for (quote_id, block_number, price) in prices {
            let inner = self
                .price_cache
                .entry(quote_id.clone())
                .or_insert_with(|| DashMap::with_capacity(500));
            inner.insert(block_number, Arc::new(price));

            order_map
                .entry(quote_id)
                .or_insert_with(std::collections::VecDeque::new)
                .push_back(block_number);

            total_loaded += 1;
        }

        info!(
            "[CACHE] Preloaded {} prices into unified cache ({} quotes) from start_block={}",
            total_loaded,
            order_map.len(),
            start_block
        );
        Ok(())
    }

    //-------------------------------------------------------------------------
    // 화이트리스트 토큰 관련 메서드들
    //-------------------------------------------------------------------------

    /// 화이트리스트에 토큰 추가 (Redis + PostgreSQL)
    pub async fn insert_white_list_token(&self, token: &str, is_white: bool) -> Result<()> {
        // Redis에 먼저 추가
        self.redis.insert_white_list_token(token, is_white).await?;

        // PostgreSQL 관련 로직이 필요하면 여기에 추가

        Ok(())
    }

    /// 토큰이 화이트리스트에 있는지 확인 (Redis 캐시 우선, 없으면 PostgreSQL에서 조회)
    pub async fn check_white_list_token(&self, token: &str) -> Result<bool> {
        // Redis 캐시 확인 (기존 코드 유지)
        // Redis 캐시 확인
        match self.redis.check_white_list_token(token).await {
            Ok(Some(exists)) => return Ok(exists),
            Ok(None) => {
                // Redis에 없는 경우 PostgreSQL에서 확인
            }
            Err(e) => {
                error!("Error checking token in Redis whitelist: {}", e);
                // Redis 에러는 무시하고 PostgreSQL에서 계속 시도
            }
        }

        // PostgreSQL에서 토큰 존재 확인 - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500; // 기본 대기 시간 (밀리초)

        while retry_count < max_retries {
            // PostgreSQL 쿼리 실행
            let query = r#"SELECT EXISTS(SELECT 1 FROM token WHERE token_id = $1) as exists"#;
            match sqlx::query(query)
                .bind(token)
                .fetch_one(&self.postgres.pool)
                .await
            {
                Ok(row) => {
                    let exists: bool = row.get("exists");
                    debug!(
                        "Token existence check in PostgreSQL: token={}, exists={}",
                        token, exists
                    );

                    match exists {
                        true => {
                            // 존재하는 토큰은 화이트리스트로 간주하고 Redis에 캐싱
                            if let Err(e) = self.redis.insert_white_list_token(token, true).await {
                                warn!(
                                    "check_white_list_token - Failed to cache white list token in Redis: {}",
                                    e
                                );
                                continue;
                            }
                        }
                        false => {
                            // is_white false로 redis에 저장
                            if let Err(e) = self.redis.insert_white_list_token(token, false).await {
                                warn!(
                                    "check_white_list_token - Failed to cache white list token in Redis: {}",
                                    e
                                );
                                continue;
                            }
                        }
                    }

                    return Ok(exists);
                }
                Err(e) => {
                    // 연결 관련 오류인지 확인

                    // 재시도 가능한 오류
                    retry_count += 1;

                    // 지수 백오프 계산 (1차: 500ms, 2차: 1000ms, 3차: 2000ms)
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "check_white_list_token - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        token, backoff_time, retry_count, max_retries, e
                    );

                    // 대기 후 재시도
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error!(
            "check_white_list_token - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            token
        );
        Ok(false)
    }

    //-------------------------------------------------------------------------
    // Fee Config 관련 메서드들
    //-------------------------------------------------------------------------

    /// Fee config 저장
    pub async fn insert_fee_config(&self, token: &str, creator_rate: u16, curve_rate: u16, dex_rate: u16) -> Result<()> {
        self.redis.insert_fee_config(token, creator_rate, curve_rate, dex_rate).await?;
        Ok(())
    }

    /// Fee config 조회 (Redis → DB fallback) → (creator_rate, curve_rate, dex_rate)
    pub async fn get_fee_config(&self, token: &str) -> Result<Option<(u16, u16, u16)>> {
        // Redis 캐시 확인
        match self.redis.get_fee_config(token).await {
            Ok(Some(config)) => return Ok(Some(config)),
            Ok(None) => {}
            Err(e) => {
                error!("Error getting fee config from Redis: {}", e);
            }
        }

        // PostgreSQL에서 조회
        let db = PostgresDatabase::instance()?;
        match sqlx::query_as::<_, (i16, i16, i16)>(
            "SELECT creator_fee_rate, curve_protocol_fee_rate, dex_protocol_fee_rate FROM fee_config WHERE token_id = $1"
        )
        .bind(token)
        .fetch_optional(&db.pool)
        .await
        {
            Ok(Some((creator, curve, dex))) => {
                let config = (creator as u16, curve as u16, dex as u16);
                if let Err(e) = self
                    .redis
                    .insert_fee_config(token, config.0, config.1, config.2)
                    .await
                {
                    warn!("[CACHE] Failed to cache fee_config for {}: {}", token, e);
                }
                Ok(Some(config))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Failed to get fee_config from DB for token {}: {}", token, e);
                Ok(None)
            }
        }
    }

    //-------------------------------------------------------------------------
    // 토큰-QUOTE TOKEN 관련 메서드들
    //-------------------------------------------------------------------------

    /// 토큰-QUOTE TOKEN 관계 저장
    pub async fn insert_token_quote_id(&self, token: &str, quote_id: &str) -> Result<()> {
        self.redis.insert_token_quote_id(token, quote_id).await?;
        Ok(())
    }

    /// 토큰에 대한 QUOTE TOKEN 조회 (Redis 캐시 우선, 없으면 market 테이블에서 조회).
    ///
    /// 반환값은 EIP-55 checksum 으로 정규화됨 — Redis/PG legacy 행이
    /// LEAST/LOWER backfill 로 lowercase 인 경우에도 호출자가 string `==`
    /// 비교를 안전하게 사용할 수 있도록 보장 (downstream `get_quote_decimals`,
    /// price cache 키, `WNATIVE_ADDRESS` 동등 비교 등). [`get_pool_pair`] 와
    /// 동일한 패턴 (e3749c4 hotfix).
    pub async fn get_token_quote_id(&self, token: &str) -> Result<Option<String>> {
        // EIP-55 checksum 정규화. 파싱 실패 시 원본 유지 (방어적 fallback).
        fn to_checksum(s: &str) -> String {
            alloy::primitives::Address::from_str(s)
                .map(|a| a.to_checksum(None))
                .unwrap_or_else(|_| s.to_string())
        }

        // Redis 캐시 확인
        match self.redis.get_token_quote_id(token).await {
            Ok(Some(quote)) => {
                return Ok(Some(to_checksum(&quote)));
            }
            Ok(None) => {}
            Err(e) => {
                error!("Error getting token quote from Redis: {}", e);
            }
        }

        // PostgreSQL에서 조회
        let db = PostgresDatabase::instance()?;
        match sqlx::query_scalar::<_, String>(
            "SELECT quote_id FROM market WHERE token_id = $1"
        )
        .bind(token)
        .fetch_optional(&db.pool)
        .await
        {
            Ok(Some(quote)) => {
                let normalized = to_checksum(&quote);
                // Redis에 캐싱 (정규화된 값으로 — 다음 hit 부터 normalize skip)
                if let Err(e) = self.redis.insert_token_quote_id(token, &normalized).await {
                    warn!("[CACHE] Failed to cache quote_id for {}: {}", token, e);
                }
                Ok(Some(normalized))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Failed to get quote_id from DB for token {}: {}", token, e);
                Ok(None)
            }
        }
    }

    //-------------------------------------------------------------------------
    // dex_token 존재 캐시 (PairCreated handler 의 fast-path)
    //-------------------------------------------------------------------------

    /// "이 token 이 dex_token 테이블에 이미 등록돼있나" 를 Redis 캐시 우선으로
    /// 조회. cache miss 시 PostgreSQL 에서 확인 → 존재 확인되면 Redis 에
    /// marker 저장. 다음 hit 부터 PG hit 0.
    ///
    /// PairCreated 핸들러가 매 batch 호출하므로 cache miss 비용이 누적되지
    /// 않도록 hit/miss 양쪽 모두 정규화 패턴 (get_pool_pair / get_token_quote_id
    /// 와 동일) 따른다.
    pub async fn dex_token_exists(&self, token_id: &str) -> Result<bool> {
        // Redis 캐시 우선
        match self.redis.get_dex_token_exists(token_id).await {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(e) => {
                error!("[CACHE] Redis dex_token check failed for {}: {}", token_id, e);
            }
        }

        // PostgreSQL fallback
        let db = PostgresDatabase::instance()?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM dex_token WHERE token_id = $1)",
        )
        .bind(token_id)
        .fetch_one(&db.pool)
        .await
        .map_err(|e| anyhow!("dex_token existence query failed: {}", e))?;

        // PG hit 이면 Redis 에 marker 저장 (다음 lookup 부터 cache hit)
        if exists {
            if let Err(e) = self.redis.mark_dex_token_exists(token_id).await {
                warn!("[CACHE] Failed to cache dex_token marker for {}: {}", token_id, e);
            }
        }

        Ok(exists)
    }

    /// PairCreated 핸들러가 새 dex_token row 를 batch insert 한 직후 호출 —
    /// 다음 PairCreated 처리 (다른 stream / 다음 batch) 가 PG 조회 없이
    /// Redis 만으로 short-circuit 가능하도록 marker 를 미리 채움.
    pub async fn mark_dex_token_exists(&self, token_id: &str) {
        if let Err(e) = self.redis.mark_dex_token_exists(token_id).await {
            warn!("[CACHE] Failed to mark dex_token cache for {}: {}", token_id, e);
        }
    }

    //-------------------------------------------------------------------------
    // 토큰-POOL 관련 메서드들
    //-------------------------------------------------------------------------

    /// 토큰-POOL 관계 저장 (Redis + PostgreSQL)
    pub async fn insert_token_pool(&self, token: &str, pool: &str) -> Result<()> {
        // Redis에 먼저 추가
        self.redis.insert_token_pool(token, pool).await?;

        // PostgreSQL 관련 로직이 필요하면 여기에 추가

        Ok(())
    }

    /// 토큰에 대한 POOL 정보 조회 (Redis 캐시 우선, 없으면 PostgreSQL에서 조회)
    pub async fn get_token_pool(&self, token: &str) -> Result<Option<String>> {
        // Redis 캐시 확인
        match self.redis.get_token_pool(token).await {
            Ok(Some(pool)) => {
                debug!("Token pool found in Redis: token={}, pool={}", token, pool);
                return Ok(Some(pool));
            }
            Ok(None) => {
                debug!("Token pool not found in Redis: token={}", token);
            }
            Err(e) => {
                error!("Error getting token pool from Redis: {}", e);
                // Redis 에러는 무시하고 PostgreSQL에서 계속 시도
            }
        }

        // PostgreSQL에서 pool_id 조회 (POOL 유형의 마켓) - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500; // 기본 대기 시간 (밀리초)

        while retry_count < max_retries {
            // PostgreSQL 쿼리 실행
            let query = r#"SELECT pool_id FROM market WHERE token_id = $1 AND market_type = 'UNISWAPV3'"#;
            match sqlx::query(query)
                .bind(token)
                .fetch_optional(&self.postgres.pool)
                .await
            {
                Ok(Some(row)) => {
                    let pool: String = row.get("pool_id");
                    debug!(
                        "Token pool found in PostgreSQL: token={}, pool={}",
                        token, pool
                    );

                    // 찾은 정보를 Redis에 캐싱
                    if let Err(e) = self.redis.insert_token_pool(token, &pool).await {
                        error!(
                            "get_token_pool - Failed to cache token pool in Redis: {}",
                            e
                        );
                        // Redis 캐싱 실패는 치명적이지 않으므로 계속 진행
                    }

                    return Ok(Some(pool));
                }
                Ok(None) => {
                    debug!("DEX market not found in PostgreSQL: token={}", token);
                    return Ok(None);
                }
                Err(e) => {
                    // 재시도 가능한 오류
                    retry_count += 1;

                    // 지수 백오프 계산
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "get_token_pool - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        token, backoff_time, retry_count, max_retries, e
                    );

                    // 대기 후 재시도
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error!(
            "get_token_pool - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            token
        );
        Ok(None)
    }

    //-------------------------------------------------------------------------
    // POOL 관련 메서드들
    //-------------------------------------------------------------------------

    /// token_pool 플래그 등록 (본딩커브 Create/Graduate에서 호출)
    pub async fn set_token_pool_flag(&self, pool: &str, value: bool) -> Result<()> {
        self.redis.insert_token_pool_flag(pool, value).await?;
        Ok(())
    }

    /// token_pool 여부 확인 (Redis → DB fallback: market 테이블)
    pub async fn check_token_pool(&self, pool: &str) -> Result<bool> {
        match self.redis.check_token_pool_flag(pool).await {
            Ok(Some(exists)) => return Ok(exists),
            Ok(None) => {}
            Err(e) => {
                error!("Error checking token_pool in Redis: {}", e);
            }
        }

        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500;

        while retry_count < max_retries {
            let query = r#"SELECT EXISTS(SELECT 1 FROM market WHERE pool_id = $1 AND market_type IN ('UNISWAPV3', 'V2_DEX')) as exists"#;
            match sqlx::query(query)
                .bind(pool)
                .fetch_one(&self.postgres.pool)
                .await
            {
                Ok(row) => {
                    let exists: bool = row.get("exists");
                    if let Err(e) = self.redis.insert_token_pool_flag(pool, exists).await {
                        warn!("[CACHE] Failed to cache token_pool_flag for {}: {}", pool, e);
                    }
                    return Ok(exists);
                }
                Err(e) => {
                    retry_count += 1;
                    let backoff_time = backoff_base * (1 << (retry_count - 1));
                    warn!(
                        "check_token_pool - DB error ({}), retry {}/{}...: {}",
                        pool, retry_count, max_retries, e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        error!("check_token_pool - max retries exceeded ({})", pool);
        Ok(false)
    }

    /// dex_pool 등록 (PairCreated에서 호출)
    pub async fn insert_dex_pool(&self, pool: &str, value: bool) -> Result<()> {
        self.redis.insert_dex_pool_flag(pool, value).await?;
        Ok(())
    }

    /// dex_pool 여부 확인 (Redis → DB fallback: pool 테이블)
    pub async fn check_dex_pool(&self, pool: &str) -> Result<bool> {
        match self.redis.check_dex_pool_flag(pool).await {
            Ok(Some(exists)) => return Ok(exists),
            Ok(None) => {}
            Err(e) => {
                error!("Error checking dex_pool in Redis: {}", e);
            }
        }

        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500;

        while retry_count < max_retries {
            let query = r#"SELECT EXISTS(SELECT 1 FROM pool WHERE pool_id = $1) as exists"#;
            match sqlx::query(query)
                .bind(pool)
                .fetch_one(&self.postgres.pool)
                .await
            {
                Ok(row) => {
                    let exists: bool = row.get("exists");
                    if let Err(e) = self.redis.insert_dex_pool_flag(pool, exists).await {
                        warn!("[CACHE] Failed to cache dex_pool_flag for {}: {}", pool, e);
                    }
                    return Ok(exists);
                }
                Err(e) => {
                    retry_count += 1;
                    let backoff_time = backoff_base * (1 << (retry_count - 1));
                    warn!(
                        "check_dex_pool - DB error ({}), retry {}/{}...: {}",
                        pool, retry_count, max_retries, e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        error!("check_dex_pool - max retries exceeded ({})", pool);
        Ok(false)
    }

    //-------------------------------------------------------------------------
    // POOL 페어 관련 메서드들
    //-------------------------------------------------------------------------

    /// POOL 페어 정보 저장 (Redis + PostgreSQL)
    pub async fn insert_pool_pair(&self, pool: &str, token0: &str, token1: &str) -> Result<()> {
        // Redis에 먼저 추가
        self.redis.insert_pool_pair(pool, token0, token1).await?;

        Ok(())
    }

    /// POOL 페어 정보 조회 (Redis 캐시 우선, 없으면 PostgreSQL에서 조회)
    /// 반환값의 token0/token1 은 EIP-55 checksum 으로 정규화됨 — 호출자가
    /// `is_quote_token` 등 case-sensitive 비교에서 안전하게 사용 가능.
    pub async fn get_pool_pair(&self, pool: &str) -> Result<Option<(String, String)>> {
        // EIP-55 checksum 정규화. 파싱 실패 시 원본 유지 (방어적 fallback).
        fn to_checksum(s: &str) -> String {
            alloy::primitives::Address::from_str(s)
                .map(|a| a.to_checksum(None))
                .unwrap_or_else(|_| s.to_string())
        }

        // Redis 캐시 확인
        match self.redis.get_pool_pair(pool).await {
            Ok(Some(pair)) => {
                let normalized = (to_checksum(&pair.0), to_checksum(&pair.1));
                debug!(
                    "Pool pair found in Redis: pool={}, token0={}, token1={}",
                    pool, normalized.0, normalized.1
                );
                return Ok(Some(normalized));
            }
            Ok(None) => {
                debug!("Pool pair not found in Redis: pool={}", pool);
            }
            Err(e) => {
                error!("Error getting pool pair from Redis: {}", e);
                // Redis 에러는 무시하고 PostgreSQL에서 계속 시도
            }
        }

        // PostgreSQL에서 페어 정보 조회 - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500;

        while retry_count < max_retries {
            // V1/V2를 market_type으로 명시 분기.
            //   - V2_DEX: PairCreated 이벤트가 pool 테이블에 (pool_id,
            //     token0, token1)을 체인 그대로 저장 → pool 룩업이 정답.
            //   - UNISWAPV3 (V1): graduate는 market에만 (token_id, quote_id)를
            //     남기고 pool 테이블엔 INSERT 안 함 → market에서 가져와
            //     주소 비교로 (token0, token1) 정렬 (Uniswap V3 컨벤션
            //     address(token0) < address(token1)).
            let row = sqlx::query_as::<_, (String, Option<String>, String, String)>(
                r#"
                SELECT m.market_type,
                       p.token0 AS pool_token0,
                       m.token_id,
                       m.quote_id
                  FROM market m
                  LEFT JOIN pool p ON p.pool_id = m.pool_id
                 WHERE m.pool_id = $1
                "#,
            )
            .bind(pool)
            .fetch_optional(&self.postgres.pool)
            .await;
            match row {
                Ok(Some((market_type, _pool_t0_check, token_id, quote_id))) => {
                    let (token0, token1) = match market_type.as_str() {
                        "V2_DEX" => {
                            // V2: pool 테이블이 정답. 별도 쿼리로 token0/token1 가져옴.
                            let p = sqlx::query_as::<_, (String, String)>(
                                r#"SELECT token0, token1 FROM pool WHERE pool_id = $1"#,
                            )
                            .bind(pool)
                            .fetch_optional(&self.postgres.pool)
                            .await;
                            match p {
                                Ok(Some((t0, t1))) => (t0, t1),
                                Ok(None) => {
                                    debug!(
                                        "[V2_DEX] pool row missing for {}: PairCreated not yet processed",
                                        pool
                                    );
                                    return Ok(None);
                                }
                                Err(e) => {
                                    warn!(
                                        "get_pool_pair - V2 pool lookup error pool={}: {}",
                                        pool, e
                                    );
                                    return Ok(None);
                                }
                            }
                        }
                        "UNISWAPV3" => {
                            // V1: market의 (token_id, quote_id)를 주소 비교로 정렬.
                            if token_id.to_lowercase() < quote_id.to_lowercase() {
                                (token_id.clone(), quote_id.clone())
                            } else {
                                (quote_id.clone(), token_id.clone())
                            }
                        }
                        other => {
                            debug!(
                                "get_pool_pair - unexpected market_type='{}' for pool={}",
                                other, pool
                            );
                            return Ok(None);
                        }
                    };

                    debug!(
                        "Pool pair resolved: pool={}, market_type={}, token0={}, token1={}",
                        pool, market_type, token0, token1
                    );

                    let token0 = to_checksum(&token0);
                    let token1 = to_checksum(&token1);
                    if let Err(e) = self.redis.insert_pool_pair(pool, &token0, &token1).await {
                        warn!("get_pool_pair - Failed to cache pool pair in Redis: {}", e);
                    }
                    return Ok(Some((token0, token1)));
                }
                Ok(None) => {
                    debug!("Pool pair not found in market: pool={}", pool);
                    return Ok(None);
                }
                Err(e) => {
                    retry_count += 1;
                    let backoff_time = backoff_base * (1 << (retry_count - 1));
                    warn!(
                        "get_pool_pair - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        pool, backoff_time, retry_count, max_retries, e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error!(
            "get_pool_pair - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            pool
        );
        Ok(None)
    }

    //-------------------------------------------------------------------------
    // Price 캐시 관련 메서드들 (quote-aware)
    //-------------------------------------------------------------------------
    //
    // Storage is a nested DashMap: quote_id -> (block_number -> price).
    // All methods take quote_id explicitly. A WMON-defaulting wrapper layer
    // below preserves the legacy single-quote API for V1 call sites.

    /// Insert a single price into the memory cache for a specific quote.
    pub async fn insert_price_for_quote(
        &self,
        quote_id: &str,
        block_number: i64,
        price: BigDecimal,
    ) {
        // The order RwLock serializes inserts against
        // `remove_prices_before_or_equal_for_quote`. Acquiring it BEFORE the
        // inner DashMap mutation ensures cleanup never observes an inner entry
        // that hasn't been recorded in the order map yet — preventing spurious
        // deletes of just-inserted prices.
        let mut order_map = self.price_insertion_order.write().await;
        let inner = self
            .price_cache
            .entry(quote_id.to_string())
            .or_insert_with(|| DashMap::with_capacity(500));
        inner.insert(block_number, Arc::new(price));
        order_map
            .entry(quote_id.to_string())
            .or_insert_with(std::collections::VecDeque::new)
            .push_back(block_number);

        debug!(
            "Price cached: quote={} block={} cache_size={}",
            quote_id,
            block_number,
            inner.len()
        );
    }

    /// Batch insert prices into the memory cache for a specific quote.
    pub async fn insert_price_batch_for_quote(
        &self,
        quote_id: &str,
        prices: &[(i64, BigDecimal)],
    ) {
        if prices.is_empty() {
            return;
        }

        // Same invariant as `insert_price_for_quote`: acquire the order lock
        // before mutating the inner DashMap so cleanup cannot interleave and
        // delete inserts that aren't yet recorded in the order map.
        let mut order_map = self.price_insertion_order.write().await;
        let inner = self
            .price_cache
            .entry(quote_id.to_string())
            .or_insert_with(|| DashMap::with_capacity(500));
        for (block_number, price) in prices {
            inner.insert(*block_number, Arc::new(price.clone()));
        }
        let order = order_map
            .entry(quote_id.to_string())
            .or_insert_with(std::collections::VecDeque::new);
        for (block_number, _) in prices {
            order.push_back(*block_number);
        }

        debug!(
            "Price batch cached: quote={} count={} cache_size={}",
            quote_id,
            prices.len(),
            inner.len()
        );
    }

    /// Exact-block lookup for a specific quote.
    pub async fn get_price_for_quote(
        &self,
        quote_id: &str,
        block_number: i64,
    ) -> Option<Arc<BigDecimal>> {
        self.price_cache
            .get(quote_id)
            .and_then(|inner| inner.get(&block_number).map(|e| Arc::clone(e.value())))
    }

    /// Range scan for a specific quote.
    pub async fn get_prices_in_range_for_quote(
        &self,
        quote_id: &str,
        min_block: i64,
        max_block: i64,
    ) -> std::collections::HashMap<i64, Arc<BigDecimal>> {
        self.price_cache
            .get(quote_id)
            .map(|inner| {
                inner
                    .iter()
                    .filter(|entry| *entry.key() >= min_block && *entry.key() <= max_block)
                    .map(|entry| (*entry.key(), Arc::clone(entry.value())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Latest price at or before `block_number` for a specific quote.
    pub async fn get_latest_price_before_for_quote(
        &self,
        quote_id: &str,
        block_number: i64,
    ) -> Option<Arc<BigDecimal>> {
        self.price_cache.get(quote_id).and_then(|inner| {
            inner
                .iter()
                .filter(|entry| *entry.key() <= block_number)
                .max_by_key(|entry| *entry.key())
                .map(|entry| Arc::clone(entry.value()))
        })
    }

    /// Most recent price (any block) for a specific quote.
    pub async fn get_latest_price_for_quote(
        &self,
        quote_id: &str,
    ) -> Option<Arc<BigDecimal>> {
        self.price_cache.get(quote_id).and_then(|inner| {
            inner
                .iter()
                .max_by_key(|entry| *entry.key())
                .map(|entry| Arc::clone(entry.value()))
        })
    }

    /// DB fallback: latest price at-or-before `block_number`, then absolute latest.
    pub async fn get_price_from_db_for_quote(
        &self,
        quote_id: &str,
        block_number: i64,
    ) -> Option<BigDecimal> {
        let result = sqlx::query_scalar::<_, BigDecimal>(
            r#"
            SELECT price FROM price
            WHERE quote_id = $1 AND block_number <= $2
            ORDER BY block_number DESC
            LIMIT 1
            "#,
        )
        .bind(quote_id)
        .bind(block_number)
        .fetch_optional(&self.postgres.pool)
        .await;

        match result {
            Ok(Some(price)) => {
                debug!(
                    "[CACHE] Found price from DB: quote={} block<={}",
                    quote_id, block_number
                );
                Some(price)
            }
            Ok(None) => {
                match sqlx::query_scalar::<_, BigDecimal>(
                    r#"
                    SELECT price FROM price
                    WHERE quote_id = $1
                    ORDER BY block_number DESC
                    LIMIT 1
                    "#,
                )
                .bind(quote_id)
                .fetch_optional(&self.postgres.pool)
                .await
                {
                    Ok(Some(price)) => {
                        debug!("[CACHE] Found latest price from DB for quote={}", quote_id);
                        Some(price)
                    }
                    Ok(None) => None,
                    Err(e) => {
                        error!(
                            "[CACHE] Failed to get latest price from DB: quote={} err={}",
                            quote_id, e
                        );
                        None
                    }
                }
            }
            Err(e) => {
                error!(
                    "[CACHE] Failed to get price from DB: quote={} block={} err={}",
                    quote_id, block_number, e
                );
                None
            }
        }
    }

    pub async fn get_price_usd_before(
        &self,
        token_id: &str,
        block_number: i64,
    ) -> Option<BigDecimal> {
        let result = sqlx::query_scalar::<_, BigDecimal>(
            r#"
            SELECT price FROM price_usd
            WHERE token_id = $1 AND block_number <= $2
            ORDER BY block_number DESC
            LIMIT 1
            "#,
        )
        .bind(token_id)
        .bind(block_number)
        .fetch_optional(&self.postgres.pool)
        .await;

        match result {
            Ok(Some(price)) => {
                debug!(
                    "[CACHE] Found USD price from DB: token={} block<={}",
                    token_id, block_number
                );
                Some(price)
            }
            Ok(None) => None,
            Err(e) => {
                error!(
                    "[CACHE] Failed to get USD price from DB: token={} block={} err={}",
                    token_id, block_number, e
                );
                None
            }
        }
    }

    pub async fn get_token_quote_price_history_before(
        &self,
        token_id: &str,
        created_at: i64,
    ) -> Option<BigDecimal> {
        let result = sqlx::query_scalar::<_, BigDecimal>(
            r#"
            SELECT price FROM price_history
            WHERE token_id = $1 AND created_at <= $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(token_id)
        .bind(created_at)
        .fetch_optional(&self.postgres.pool)
        .await;

        match result {
            Ok(Some(price)) => {
                debug!(
                    "[CACHE] Found token quote price history from DB: token={} created_at<={}",
                    token_id, created_at
                );
                Some(price)
            }
            Ok(None) => None,
            Err(e) => {
                error!(
                    "[CACHE] Failed to get token quote price history from DB: token={} created_at={} err={}",
                    token_id, created_at, e
                );
                None
            }
        }
    }

    /// Unified USD price lookup with full fallback chain:
    /// cache exact -> cache latest-before -> cache latest -> DB fallback.
    pub async fn get_quote_usd_price(
        &self,
        quote_id: &str,
        block_num: i64,
    ) -> Option<Arc<BigDecimal>> {
        if let Some(price) = self.get_price_for_quote(quote_id, block_num).await {
            return Some(price);
        }
        if let Some(price) = self
            .get_latest_price_before_for_quote(quote_id, block_num)
            .await
        {
            return Some(price);
        }
        if let Some(price) = self.get_latest_price_for_quote(quote_id).await {
            return Some(price);
        }
        self.get_price_from_db_for_quote(quote_id, block_num)
            .await
            .map(Arc::new)
    }

    /// Total number of cached prices across all quotes.
    pub async fn get_price_cache_size(&self) -> usize {
        self.price_cache.iter().map(|e| e.value().len()).sum()
    }

    /// Cleanup: remove cached prices at or below `block_number` for a specific quote.
    pub async fn remove_prices_before_or_equal_for_quote(
        &self,
        quote_id: &str,
        block_number: i64,
    ) {
        let mut order_map = self.price_insertion_order.write().await;
        if let Some(order) = order_map.get_mut(quote_id) {
            while let Some(&oldest) = order.front() {
                if oldest <= block_number {
                    order.pop_front();
                    if let Some(inner) = self.price_cache.get(quote_id) {
                        inner.remove(&oldest);
                    }
                } else {
                    break;
                }
            }
        }
    }

    /// Cleanup across every known quote. Walks the current quote set and
    /// applies `remove_prices_before_or_equal_for_quote` to each. V2 receive
    /// paths use this because they may serve any number of quote tokens; the
    /// legacy WMON-only wrapper is insufficient there.
    pub async fn remove_prices_before_or_equal_all_quotes(&self, block_number: i64) {
        let quote_ids: Vec<String> = self
            .price_cache
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for quote_id in &quote_ids {
            self.remove_prices_before_or_equal_for_quote(quote_id, block_number)
                .await;
        }
    }

    //-------------------------------------------------------------------------
    // Token decimals lookup (chain-immutable, insert-once cache)
    //-------------------------------------------------------------------------

    /// Decimals factor (`10^decimals`) for a token. Looks up in `quote_token`
    /// then `dex_token`.
    ///
    /// Caching policy:
    ///   - Confirmed DB hit → cache forever (decimals are chain-immutable).
    ///   - DB miss (token not yet registered) or transient DB error → return
    ///     the 18 fallback but do NOT cache, so a later call can re-resolve
    ///     once the `dex_token` row arrives or the DB recovers. Caching the
    ///     fallback would permanently skew prices for any non-18 token whose
    ///     first lookup happened to race ahead of its registration.
    pub async fn get_token_decimals_factor(&self, token_id: &str) -> Arc<BigDecimal> {
        if let Some(entry) = self.token_decimals_cache.get(token_id) {
            return Arc::clone(entry.value());
        }
        let lookup = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT decimals FROM (
                SELECT decimals FROM quote_token WHERE quote_id = $1
                UNION ALL
                SELECT decimals FROM dex_token WHERE token_id = $1
            ) t LIMIT 1
            "#,
        )
        .bind(token_id)
        .fetch_optional(&self.postgres.pool)
        .await;

        match lookup {
            Ok(Some(decimals)) => {
                let factor = Arc::new(
                    BigDecimal::from_str(&format!("1{}", "0".repeat(decimals as usize)))
                        .expect("BigDecimal 10^n must parse"),
                );
                self.token_decimals_cache
                    .insert(token_id.to_string(), Arc::clone(&factor));
                factor
            }
            Ok(None) | Err(_) => {
                // Unregistered or DB error: return the 18 fallback for this
                // call only. Skip caching so the next call retries.
                Arc::new(
                    BigDecimal::from_str("1000000000000000000")
                        .expect("BigDecimal 10^18 must parse"),
                )
            }
        }
    }

    //-------------------------------------------------------------------------
    // Token-level WMON-implied price cache (on-chain inferred)
    //-------------------------------------------------------------------------
    //
    // Separate from `price_cache` (Pyth USD per registered quote). This cache
    // stores chain-derived WMON-denominated price per token at a block. USD
    // value of any token amount at any block is the composition:
    //
    //     usd_value = (amount / 10^decimals) * token_price_cache[token][block]
    //                 * price_cache[WMON][block]
    //
    // Updated by the V2 DEX receive path on every RawSync via forward
    // propagation: any pool with a WMON side or a token whose price is
    // already known produces a new entry for the other side.

    /// Insert a single WMON-implied token price into the cache.
    ///
    /// Acquires the insertion-order write lock before mutating the inner
    /// DashMap — same invariant as `insert_price_for_quote` to prevent the
    /// TOCTOU race between concurrent inserts and cleanup.
    ///
    /// The order queue de-duplicates on the most recent block: if the same
    /// block is updated twice (e.g. two RawSyncs in the same block for
    /// different pools touching this token), the inner map gets the latest
    /// price (last-write-wins) but the queue keeps a single entry. This
    /// mirrors inner cardinality so cleanup's "keep at least one" invariant
    /// remains correct.
    pub async fn insert_token_price(&self, token_id: &str, block_number: i64, price: BigDecimal) {
        let mut order_map = self.token_price_insertion_order.write().await;
        let inner = self
            .token_price_cache
            .entry(token_id.to_string())
            .or_insert_with(|| DashMap::with_capacity(500));
        inner.insert(block_number, Arc::new(price));
        let order = order_map
            .entry(token_id.to_string())
            .or_insert_with(std::collections::VecDeque::new);
        if order.back() != Some(&block_number) {
            order.push_back(block_number);
        }
    }

    /// Exact-block lookup for a token's WMON-implied price.
    pub async fn get_token_price(
        &self,
        token_id: &str,
        block_number: i64,
    ) -> Option<Arc<BigDecimal>> {
        self.token_price_cache
            .get(token_id)
            .and_then(|inner| inner.get(&block_number).map(|e| Arc::clone(e.value())))
    }

    /// Latest WMON-implied price at or before `block_number`. Used by the
    /// inference step when a swap's counterpart was priced in an earlier
    /// block and no exact-block entry exists yet.
    pub async fn get_latest_token_price_before(
        &self,
        token_id: &str,
        block_number: i64,
    ) -> Option<Arc<BigDecimal>> {
        self.token_price_cache.get(token_id).and_then(|inner| {
            inner
                .iter()
                .filter(|entry| *entry.key() <= block_number)
                .max_by_key(|entry| *entry.key())
                .map(|entry| Arc::clone(entry.value()))
        })
    }

    /// Forward-propagate a token's WMON-implied price into the cache using
    /// a (reserve0, reserve1) snapshot for a pool. Four-case algorithm:
    ///   1. t0 == WMON         → t1 price = r0 / r1
    ///   2. t1 == WMON         → t0 price = r1 / r0
    ///   3. t0 known           → t1 price = (r0 * t0_price) / r1
    ///   4. t1 known           → t0 price = (r1 * t1_price) / r0
    /// otherwise orphan — neither side has a WMON-reachable price yet.
    ///
    /// Reserves are first divided by their decimals factor so the resulting
    /// price is in human-scaled units ("how much WMON one whole unit of the
    /// other token is worth").
    ///
    /// Returns true if a new entry was written (used by warm-up fixpoint).
    pub async fn update_token_price_from_sync(
        &self,
        t0: &str,
        t1: &str,
        d0: &BigDecimal,
        d1: &BigDecimal,
        reserve0: &BigDecimal,
        reserve1: &BigDecimal,
        block: i64,
    ) -> bool {
        use bigdecimal::Zero;
        if reserve0.is_zero() || reserve1.is_zero() {
            return false;
        }
        let r0 = reserve0 / d0;
        let r1 = reserve1 / d1;
        if r0.is_zero() || r1.is_zero() {
            return false;
        }
        // self.is_native(addr) matches WMON and every quote_token row flagged
        // is_native (e.g. LVMON). All are MON-pegged 1:1, so they short-circuit
        // to "price = 1 in WMON units" for the purpose of priming the cache.
        if self.is_native(t0) {
            self.insert_token_price(t1, block, &r0 / &r1).await;
            true
        } else if self.is_native(t1) {
            self.insert_token_price(t0, block, &r1 / &r0).await;
            true
        } else if let Some(t0_price) = self.get_latest_token_price_before(t0, block).await {
            self.insert_token_price(t1, block, (&r0 * &*t0_price) / &r1).await;
            true
        } else if let Some(t1_price) = self.get_latest_token_price_before(t1, block).await {
            self.insert_token_price(t0, block, (&r1 * &*t1_price) / &r0).await;
            true
        } else {
            false
        }
    }

    /// Warm-up: walk every row in `pool` and try to forward-propagate prices
    /// from the current reserve snapshot. Runs to fixpoint (passes the pool
    /// set until no new price was added) so multi-hop tokens resolve too.
    ///
    /// Use a `sentinel_block` slightly older than the next batch's expected
    /// `block_number` — the regular inference path will overwrite these with
    /// fresh per-block entries as RawSyncs flow in.
    pub async fn warm_up_token_price_cache(&self, sentinel_block: i64) -> Result<()> {
        let rows: Vec<(String, String, String, BigDecimal, BigDecimal)> = sqlx::query_as(
            "SELECT pool_id, token0, token1, reserve0, reserve1 FROM pool",
        )
        .fetch_all(&self.postgres.pool)
        .await?;

        if rows.is_empty() {
            info!("[CACHE] warm_up_token_price_cache: pool table empty, skipping");
            return Ok(());
        }

        // Pre-fetch decimals for every unique token (chain-immutable cache).
        let mut decimals: std::collections::HashMap<String, Arc<BigDecimal>> = Default::default();
        for (_pool, t0, t1, _r0, _r1) in &rows {
            if !decimals.contains_key(t0) {
                decimals.insert(t0.clone(), self.get_token_decimals_factor(t0).await);
            }
            if !decimals.contains_key(t1) {
                decimals.insert(t1.clone(), self.get_token_decimals_factor(t1).await);
            }
        }

        // Fixpoint loop. Bounded by `rows.len()` worst case — at most each
        // pass settles one transitive hop.
        let total = rows.len();
        let mut passes = 0;
        loop {
            passes += 1;
            let mut new_prices = 0usize;
            for (_pool, t0, t1, r0, r1) in &rows {
                let d0 = decimals.get(t0).expect("pre-fetched");
                let d1 = decimals.get(t1).expect("pre-fetched");
                // Only count a write when the target side was unknown before.
                let t0_known_before = self.get_token_price(t0, sentinel_block).await.is_some();
                let t1_known_before = self.get_token_price(t1, sentinel_block).await.is_some();
                if t0_known_before && t1_known_before {
                    continue;
                }
                if self
                    .update_token_price_from_sync(t0, t1, d0, d1, r0, r1, sentinel_block)
                    .await
                {
                    if (!t0_known_before
                        && self.get_token_price(t0, sentinel_block).await.is_some())
                        || (!t1_known_before
                            && self.get_token_price(t1, sentinel_block).await.is_some())
                    {
                        new_prices += 1;
                    }
                }
            }
            if new_prices == 0 || passes > total {
                info!(
                    "[CACHE] warm_up_token_price_cache: {} pools, {} passes, cache size = {}",
                    total,
                    passes,
                    self.get_token_price_cache_size().await
                );
                break;
            }
        }
        Ok(())
    }

    /// Cleanup: remove cached token prices at or below `block_number`, but
    /// always keep at least one entry per token (the newest available).
    ///
    /// Why retain one: the inference algorithm consults
    /// `get_latest_token_price_before` to value swaps in token-token pools.
    /// If a token sees a long stretch (>1000 blocks) of inactivity and the
    /// usual sliding cleanup wipes every entry, the first swap after the gap
    /// would be valued at 0 even though the price could still be priced from
    /// the most recent known WMON-implied rate. Keeping the newest entry
    /// alive preserves that fallback.
    pub async fn remove_token_prices_before_or_equal_all_tokens(&self, block_number: i64) {
        let token_ids: Vec<String> = self
            .token_price_cache
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for token_id in &token_ids {
            let mut order_map = self.token_price_insertion_order.write().await;
            if let Some(order) = order_map.get_mut(token_id) {
                // Retain at least one (the newest) entry per token.
                while order.len() > 1 {
                    match order.front() {
                        Some(&oldest) if oldest <= block_number => {
                            order.pop_front();
                            if let Some(inner) = self.token_price_cache.get(token_id) {
                                inner.remove(&oldest);
                            }
                        }
                        _ => break,
                    }
                }
            }
        }
    }

    /// Total number of cached token prices across all tokens.
    pub async fn get_token_price_cache_size(&self) -> usize {
        self.token_price_cache.iter().map(|e| e.value().len()).sum()
    }

    //-------------------------------------------------------------------------
    // WMON-only wrappers (legacy API preserved for V1 call sites)
    //-------------------------------------------------------------------------

    /// Legacy WMON-only insert. Prefer [`insert_price_for_quote`].
    pub async fn insert_price(&self, block_number: i64, price: BigDecimal) {
        self.insert_price_for_quote(&WNATIVE_ADDRESS, block_number, price)
            .await
    }

    /// Legacy WMON-only batch insert. Prefer [`insert_price_batch_for_quote`].
    pub async fn insert_price_batch(&self, prices: &[(i64, BigDecimal)]) {
        self.insert_price_batch_for_quote(&WNATIVE_ADDRESS, prices)
            .await
    }

    /// Legacy WMON-only exact lookup. Prefer [`get_price_for_quote`].
    pub async fn get_price(&self, block_number: i64) -> Option<Arc<BigDecimal>> {
        self.get_price_for_quote(&WNATIVE_ADDRESS, block_number)
            .await
    }

    /// Legacy WMON-only range scan. Prefer [`get_prices_in_range_for_quote`].
    pub async fn get_prices_in_range(
        &self,
        min_block: i64,
        max_block: i64,
    ) -> std::collections::HashMap<i64, Arc<BigDecimal>> {
        self.get_prices_in_range_for_quote(&WNATIVE_ADDRESS, min_block, max_block)
            .await
    }

    /// Legacy WMON-only latest-before lookup.
    pub async fn get_latest_price_before(
        &self,
        block_number: i64,
    ) -> Option<Arc<BigDecimal>> {
        self.get_latest_price_before_for_quote(&WNATIVE_ADDRESS, block_number)
            .await
    }

    /// Legacy WMON-only absolute-latest lookup.
    pub async fn get_latest_price(&self) -> Option<Arc<BigDecimal>> {
        self.get_latest_price_for_quote(&WNATIVE_ADDRESS).await
    }

    /// Legacy WMON-only DB fallback.
    pub async fn get_price_from_db(&self, block_number: i64) -> Option<BigDecimal> {
        self.get_price_from_db_for_quote(&WNATIVE_ADDRESS, block_number)
            .await
    }

    /// Legacy WMON-only cleanup.
    pub async fn remove_prices_before_or_equal(&self, block_number: i64) {
        self.remove_prices_before_or_equal_for_quote(&WNATIVE_ADDRESS, block_number)
            .await
    }

    //-------------------------------------------------------------------------
    // Token Creator 관련 메서드들
    //-------------------------------------------------------------------------

    /// Token creator 매핑 저장 (Redis, TTL 1일)
    pub async fn insert_token_creator(&self, token: &str, creator: &str) -> Result<()> {
        self.redis.insert_token_creator(token, creator).await?;
        Ok(())
    }

    /// Token creator 조회 (Redis 캐시 우선, 없으면 PostgreSQL에서 조회)
    pub async fn get_token_creator(&self, token: &str) -> Result<Option<String>> {
        // Redis 캐시 확인
        match self.redis.get_token_creator(token).await {
            Ok(Some(creator)) => {
                debug!(
                    "Token creator found in Redis: token={}, creator={}",
                    token, creator
                );
                return Ok(Some(creator));
            }
            Ok(None) => {
                debug!("Token creator not found in Redis: token={}", token);
            }
            Err(e) => {
                error!("Error getting token creator from Redis: {}", e);
                // Redis 에러는 무시하고 PostgreSQL에서 계속 시도
            }
        }

        // PostgreSQL에서 creator 조회 - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500; // 기본 대기 시간 (밀리초)

        while retry_count < max_retries {
            let query = r#"SELECT creator FROM token WHERE token_id = $1"#;
            match sqlx::query(query)
                .bind(token)
                .fetch_optional(&self.postgres.pool)
                .await
            {
                Ok(Some(row)) => {
                    let creator: String = row.get("creator");
                    debug!(
                        "Token creator found in PostgreSQL: token={}, creator={}",
                        token, creator
                    );

                    // 찾은 정보를 Redis에 캐싱
                    if let Err(e) = self.redis.insert_token_creator(token, &creator).await {
                        warn!(
                            "get_token_creator - Failed to cache token creator in Redis: {}",
                            e
                        );
                        // Redis 캐싱 실패는 치명적이지 않으므로 계속 진행
                    }

                    return Ok(Some(creator));
                }
                Ok(None) => {
                    debug!("Token creator not found in PostgreSQL: token={}", token);
                    return Ok(None);
                }
                Err(e) => {
                    // 재시도 가능한 오류
                    retry_count += 1;

                    // 지수 백오프 계산
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "get_token_creator - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        token, backoff_time, retry_count, max_retries, e
                    );

                    // 대기 후 재시도
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error!(
            "get_token_creator - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            token
        );
        Ok(None)
    }

    //-------------------------------------------------------------------------
    // EOA (Externally Owned Account) 체크 메서드
    //-------------------------------------------------------------------------

    /// 주소가 EOA인지 확인 (Redis 캐시 우선, 없으면 RPC 호출 후 캐싱)
    pub async fn check_is_eoa(&self, address: &str) -> Result<bool> {
        // Redis 캐시 확인
        match self.redis.check_is_eoa(address).await {
            Ok(Some(is_eoa)) => {
                debug!(
                    "EOA status found in Redis: address={}, is_eoa={}",
                    address, is_eoa
                );
                return Ok(is_eoa);
            }
            Ok(None) => {
                debug!("EOA status not found in Redis: address={}", address);
            }
            Err(e) => {
                error!("Error checking EOA status in Redis: {}", e);
            }
        }

        // RPC로 코드 조회
        let client = RpcClient::instance()?;
        let addr = address
            .parse::<alloy::primitives::Address>()
            .map_err(|e| anyhow!("Invalid address: {}", e))?;

        let code = client.get_code(addr).await?;
        let is_eoa = code.is_empty();

        // Redis에 캐싱
        if let Err(e) = self.redis.insert_is_eoa(address, is_eoa).await {
            warn!("Failed to cache EOA status in Redis: {}", e);
        }

        debug!(
            "EOA status checked via RPC: address={}, is_eoa={}",
            address, is_eoa
        );
        Ok(is_eoa)
    }

    /// 주소가 EOA 또는 EIP-7702 delegated EOA인지 확인
    /// - code 없음 → EOA (true)
    /// - code가 0xef0100 prefix (23 bytes) → EIP-7702 delegated EOA (true)
    /// - 그 외 code → contract (false)
    /// - zero / 0xdead 같은 burn sentinel은 코드 없는 주소지만 actor가 아니므로 false
    pub async fn check_is_eoa_or_delegated(&self, address: &str) -> Result<bool> {
        // Burn sentinel 주소는 코드가 비어있어 EOA 판정에 걸리지만 실제 actor가
        // 아님. resolve_actor가 receipt 스캔 도중 Transfer.to=0x0/0xdead를 후보로
        // 잡으면 swap row의 sender가 0x0으로 저장되는 문제 방지.
        const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
        const DEAD_ADDRESS: &str = "0x000000000000000000000000000000000000dEaD";
        if address.eq_ignore_ascii_case(ZERO_ADDRESS)
            || address.eq_ignore_ascii_case(DEAD_ADDRESS)
        {
            return Ok(false);
        }

        // Redis 캐시 확인 (별도 키 eoa_delegated:{address})
        match self.redis.check_is_eoa_or_delegated(address).await {
            Ok(Some(result)) => {
                return Ok(result);
            }
            Ok(None) => {}
            Err(e) => {
                error!("Error checking EOA/delegated status in Redis: {}", e);
            }
        }

        let client = RpcClient::instance()?;
        let addr = address
            .parse::<alloy::primitives::Address>()
            .map_err(|e| anyhow!("Invalid address: {}", e))?;

        let code = client.get_code(addr).await?;
        let is_eoa_or_delegated = code.is_empty()
            || (code.len() == 23 && code[0] == 0xef && code[1] == 0x01 && code[2] == 0x00);

        // 별도 캐시 키에 저장 (check_is_eoa와 충돌 방지)
        if let Err(e) = self.redis.insert_is_eoa_or_delegated(address, is_eoa_or_delegated).await {
            warn!("Failed to cache EOA/delegated status in Redis: {}", e);
        }

        debug!(
            "EOA/delegated check via RPC: address={}, result={}, code_len={}",
            address, is_eoa_or_delegated, code.len()
        );
        Ok(is_eoa_or_delegated)
    }

    //-------------------------------------------------------------------------
    // TX Sender 조회 메서드
    //-------------------------------------------------------------------------

    /// TX sender 조회 (Redis 캐시 우선, 없으면 RPC 호출 후 캐싱)
    pub async fn get_tx_sender(
        &self,
        tx_hash: &str,
    ) -> Result<Option<alloy::primitives::Address>> {
        // Redis 캐시 확인
        match self.redis.get_tx_sender(tx_hash).await {
            Ok(Some(sender_str)) => {
                debug!("TX sender found in Redis: tx_hash={}, sender={}", tx_hash, sender_str);
                let sender = sender_str
                    .parse::<alloy::primitives::Address>()
                    .map_err(|e| anyhow!("Invalid cached sender address: {}", e))?;
                return Ok(Some(sender));
            }
            Ok(None) => {
                debug!("TX sender not found in Redis: tx_hash={}", tx_hash);
            }
            Err(e) => {
                error!("Error getting TX sender from Redis: {}", e);
            }
        }

        // RPC로 tx sender 조회
        let client = RpcClient::instance()?;
        let hash = tx_hash
            .parse::<alloy::primitives::TxHash>()
            .map_err(|e| anyhow!("Invalid tx_hash: {}", e))?;

        match client.get_transaction_by_hash(hash).await {
            Ok(Some(tx)) => {
                let sender = tx.inner.signer();

                // Redis에 sender 캐싱
                if let Err(e) = self
                    .redis
                    .insert_tx_sender(tx_hash, &sender.to_string())
                    .await
                {
                    warn!("Failed to cache TX sender in Redis: {}", e);
                }

                debug!(
                    "TX sender fetched via RPC: tx_hash={}, sender={}",
                    tx_hash, sender
                );
                Ok(Some(sender))
            }
            Ok(None) => {
                debug!("Transaction not found: tx_hash={}", tx_hash);
                Ok(None)
            }
            Err(e) => {
                error!("Failed to get transaction by hash: {}", e);
                Err(e)
            }
        }
    }

    /// 이벤트의 실제 행위자(actor)를 판별
    /// 1. event_sender가 EOA/EIP-7702 delegated → event_sender 반환
    /// 2. event_sender가 contract → tx receipt에서 ERC20 Transfer 분석
    ///    - is_buy=true: Transfer.to 중 EOA/delegated = 유저
    ///    - is_buy=false: Transfer.from 중 EOA/delegated = 유저
    /// 3. fallback: tx.origin 반환
    pub async fn resolve_actor(
        &self,
        tx_hash: &str,
        event_sender: &str,
        token: &str,
        is_buy: bool,
    ) -> Result<String> {
        const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

        // Zero address는 burn target — actor 자격 없음.
        // EOA 체크를 건너뛰고 receipt 스캔 / tx.origin 으로 fall through.
        let skip_eoa_check = event_sender.eq_ignore_ascii_case(ZERO_ADDRESS);

        if !skip_eoa_check {
            // 1. event_sender가 EOA/delegated인지 확인
            match self.check_is_eoa_or_delegated(event_sender).await {
                Ok(true) => return Ok(event_sender.to_string()),
                Ok(false) => {
                    debug!(
                        "event_sender is contract, resolving from receipt: tx={}, sender={}",
                        tx_hash, event_sender
                    );
                }
                Err(e) => {
                    warn!("Failed to check EOA status for {}: {}", event_sender, e);
                    return Ok(event_sender.to_string());
                }
            }
        }

        // 2. tx receipt에서 ERC20 Transfer 분석
        let client = RpcClient::instance()?;
        let hash = tx_hash
            .parse::<alloy::primitives::TxHash>()
            .map_err(|e| anyhow!("Invalid tx_hash: {}", e))?;

        let token_addr = token
            .parse::<alloy::primitives::Address>()
            .map_err(|e| anyhow!("Invalid token address: {}", e))?;

        // ERC20 Transfer(address indexed from, address indexed to, uint256 value)
        let transfer_sig: alloy::primitives::B256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            .parse()
            .unwrap();

        if let Ok(Some(receipt)) = client.get_transaction_receipt(hash).await {
            for log in receipt.inner.logs() {
                if log.address() != token_addr {
                    continue;
                }
                if log.topic0() != Some(&transfer_sig) {
                    continue;
                }
                if log.topics().len() < 3 {
                    continue;
                }

                // Extract from/to from indexed topics (last 20 bytes of 32-byte topic)
                let from_addr = alloy::primitives::Address::from_slice(&log.topics()[1][12..]);
                let to_addr = alloy::primitives::Address::from_slice(&log.topics()[2][12..]);

                let candidate = if is_buy {
                    to_addr.to_string()
                } else {
                    from_addr.to_string()
                };

                if let Ok(true) = self.check_is_eoa_or_delegated(&candidate).await {
                    debug!(
                        "Resolved actor from Transfer: tx={}, actor={}, is_buy={}",
                        tx_hash, candidate, is_buy
                    );
                    return Ok(candidate);
                }
            }
        }

        // 3. Fallback: tx.origin
        match self.get_tx_sender(tx_hash).await {
            Ok(Some(sender)) => {
                debug!("Fallback to tx.origin: tx={}, sender={}", tx_hash, sender);
                Ok(sender.to_string())
            }
            _ => {
                warn!("All resolution failed for tx={}, using event_sender", tx_hash);
                Ok(event_sender.to_string())
            }
        }
    }
}
