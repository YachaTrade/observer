use std::{env, sync::Arc};

use anyhow::{Result, anyhow};

use once_cell::sync::OnceCell;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use tracing::{debug, error, info};

// Redis에 저장할 데이터 유형별 키 접두사
const PREFIX_TOKEN_EXISTS: &str = "token_exists:";
const PREFIX_IS_TOKEN_POOL: &str = "is_token_pool:";

const PREFIX_TOKEN_PAIR: &str = "token_pair:";
const PREFIX_TOKEN_QUOTE: &str = "token_quote_id:";
const PREFIX_TOKEN_CREATOR: &str = "token_creator:";
const PREFIX_EOA: &str = "eoa:";
const PREFIX_EOA_DELEGATED: &str = "eoa_delegated:";
const PREFIX_TX_SENDER: &str = "tx_sender:";

/// All observer-owned Redis key prefixes, in declaration order.
///
/// Used by `RedisDatabase::flush_observer_caches` to drop every observer
/// cache key at startup. **If you add a new `PREFIX_*` constant above,
/// you MUST also add it here** — missing a prefix silently leaves stale
/// entries in Redis that can poison address-casing assumptions after
/// a restart.
const ALL_PREFIXES: &[&str] = &[
    PREFIX_TOKEN_EXISTS,
    PREFIX_IS_TOKEN_POOL,
    PREFIX_TOKEN_PAIR,
    PREFIX_TOKEN_QUOTE,
    PREFIX_TOKEN_CREATOR,
    PREFIX_EOA,
    PREFIX_EOA_DELEGATED,
    PREFIX_TX_SENDER,
];

// Redis 캐시 만료 시간(초) - 더 오래 유지하려면 값을 증가시키세요
const CACHE_EXPIRATION: u64 = 172_800; // 48시간
const TOKEN_CREATOR_EXPIRATION: u64 = 86_400; // 1일 (24시간)
const EOA_EXPIRATION: u64 = 2_592_000; // 30일 (1달)
const TX_SENDER_EXPIRATION: u64 = 3_600; // 1시간

static REDIS_DB: OnceCell<Arc<RedisDatabase>> = OnceCell::new();

/// Redis database wrapper for caching blockchain data
/// 블록체인 데이터 캐싱을 위한 Redis 데이터베이스 래퍼
#[derive(Clone)]
pub struct RedisDatabase {
    conn: Arc<ConnectionManager>,
}

impl RedisDatabase {
    pub async fn init() -> Result<()> {
        if REDIS_DB.get().is_some() {
            info!("[REDIS] Database already initialized");
            return Ok(());
        }

        let instance = Self::new().await;
        let arc_instance = Arc::new(instance);

        if REDIS_DB.set(arc_instance).is_err() {
            info!("[REDIS] Database was initialized by another task");
        } else {
            info!("[REDIS] Global instance initialized successfully");
        }

        Ok(())
    }

    /// 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<Arc<RedisDatabase>> {
        REDIS_DB.get().map(Arc::clone).ok_or_else(|| {
            anyhow!("RedisDatabase not initialized. Call RedisDatabase::init() first")
        })
    }

    /// Creates a new Redis database connection
    /// Redis 데이터베이스 연결을 생성합니다
    pub async fn new() -> Self {
        let url = env::var("REDIS_URL")
            .unwrap_or_else(|_| panic!("REDIS_URL must be set in environment variables"));

        // Create Redis client - will handle rediss:// URLs automatically with native TLS
        let client = Client::open(url).expect("Failed to create Redis client");

        // Create connection manager for automatic reconnection
        let conn = ConnectionManager::new(client)
            .await
            .expect("Failed to create Redis connection manager");

        info!("[REDIS] Connection established with ElastiCache");

        RedisDatabase {
            conn: Arc::new(conn),
        }
    }

    /// Test Redis connection with PING command
    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow!("Redis ping failed: {}", e))?;
        Ok(())
    }

    /// Gets a connection manager clone
    /// ConnectionManager 복제본을 가져옵니다
    pub async fn get_conn(&self) -> Result<ConnectionManager> {
        Ok(self.conn.as_ref().clone())
    }

    /// Flush all observer-owned cache keys at startup.
    ///
    /// Deletes every key matching any prefix in `ALL_PREFIXES` so that
    /// the cache rebuilds from Postgres after restart. Rebuilding establishes
    /// the invariant that cached addresses use EIP-55 checksum form, keeping
    /// pool ordering, swap direction, and quote_id lookups consistent.
    ///
    /// Uses a single non-blocking SCAN sweep over the full keyspace,
    /// dispatching matched keys into per-prefix buckets for reporting.
    /// This is O(keyspace) total rather than O(keyspace × prefix_count),
    /// which matters on dense DBs where SCAN's MATCH filter still samples
    /// the whole keyspace per pass.
    pub async fn flush_observer_caches(&self) -> Result<()> {
        info!(
            "[REDIS] flush_observer_caches: starting full-keyspace sweep for {} observer prefixes",
            ALL_PREFIXES.len()
        );

        let mut conn = self.get_conn().await?;
        let mut per_prefix_matched: Vec<u64> = vec![0; ALL_PREFIXES.len()];
        let mut total_deleted: u64 = 0;
        let mut cursor: u64 = 0;

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("COUNT")
                .arg(5000u64)
                .query_async(&mut conn)
                .await
                .map_err(|e| {
                    error!("[REDIS] flush_observer_caches SCAN failed: {}", e);
                    anyhow!("flush_observer_caches SCAN failed: {}", e)
                })?;

            // Filter to observer-owned keys and count per-prefix matches.
            let mut to_delete: Vec<String> = Vec::new();
            for key in keys {
                if let Some((idx, _)) = ALL_PREFIXES
                    .iter()
                    .enumerate()
                    .find(|(_, p)| key.starts_with(*p))
                {
                    per_prefix_matched[idx] += 1;
                    to_delete.push(key);
                }
            }

            if !to_delete.is_empty() {
                let deleted: u64 = conn.del(&to_delete).await.map_err(|e| {
                    error!("[REDIS] flush_observer_caches DEL failed: {}", e);
                    anyhow!("flush_observer_caches DEL failed: {}", e)
                })?;
                total_deleted += deleted;
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        for (idx, count) in per_prefix_matched.iter().enumerate() {
            if *count > 0 {
                info!(
                    "[REDIS] flush_observer_caches: matched {} keys under '{}'",
                    count, ALL_PREFIXES[idx]
                );
            }
        }

        info!(
            "[REDIS] flush_observer_caches: sweep complete — {} observer cache keys deleted across {} prefixes",
            total_deleted,
            ALL_PREFIXES.len()
        );
        Ok(())
    }

    /// TTL 갱신 헬퍼 메서드
    async fn refresh_ttl(&self, conn: &mut ConnectionManager, key: &str) -> Result<()> {
        conn.expire::<_, ()>(key, CACHE_EXPIRATION as i64)
            .await
            .map_err(|e| {
                error!("[REDIS] Failed to refresh TTL for key {}: {}", key, e);
                anyhow!("Failed to refresh TTL: {}", e)
            })?;

        debug!("TTL refreshed for key: {}", key);
        Ok(())
    }

    /// Sets block timestamp with 10 second expiration
    /// 블록 타임스탬프를 10초 만료시간과 함께 설정합니다
    pub async fn set_block_timestamp(&self, block_number: u64, timestamp: u64) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, u64, ()>(format!("block:{}:timestamp", block_number), timestamp, 10)
            .await
            .map_err(|e| {
                error!("[REDIS] Failed to set timestamp: {}", e);
                anyhow!("Failed to set timestamp in Redis: {}", e)
            })?;

        Ok(())
    }

    /// Gets cached block timestamp if available
    /// 캐시된 블록 타임스탬프를 조회합니다
    pub async fn get_block_timestamp(&self, block_number: u64) -> Result<Option<u64>> {
        let mut conn = self.get_conn().await?;

        conn.get(format!("block:{}:timestamp", block_number))
            .await
            .map_err(|e| {
                error!("[REDIS] Failed to get timestamp: {}", e);
                anyhow!("Failed to get timestamp from Redis: {}", e)
            })
    }

    //-------------------------------------------------------------------------
    // Indexed token membership
    //-------------------------------------------------------------------------

    pub async fn set_token_exists(&self, token_id: &str, exists: bool) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, bool, ()>(
            format!("{}{}", PREFIX_TOKEN_EXISTS, token_id),
            exists,
            CACHE_EXPIRATION,
        )
        .await
        .map_err(|e| {
            error!("[REDIS] Failed to cache token membership: {}", e);
            anyhow!("Failed to cache token membership: {}", e)
        })?;

        debug!(
            "Token membership cached in Redis: {} = {}",
            token_id, exists
        );
        Ok(())
    }

    pub async fn get_token_exists(&self, token_id: &str) -> Result<Option<bool>> {
        let mut conn = self.get_conn().await?;
        let key = format!("{}{}", PREFIX_TOKEN_EXISTS, token_id);

        let exists: Option<bool> = conn.get(&key).await.map_err(|e| {
            error!("[REDIS] Failed to get token membership: {}", e);
            anyhow!("Failed to get token membership from Redis: {}", e)
        })?;

        if exists.is_some() {
            self.refresh_ttl(&mut conn, &key).await?;
        }

        Ok(exists)
    }

    //-------------------------------------------------------------------------
    // 화이트리스트 POOL 관련 메서드들
    //-------------------------------------------------------------------------

    /// 화이트리스트에 POOL 추가
    pub async fn insert_token_pool_flag(&self, pool: &str, is_white: bool) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, bool, ()>(
            format!("{}{}", PREFIX_IS_TOKEN_POOL, pool),
            is_white,
            CACHE_EXPIRATION,
        )
        .await
        .map_err(|e| {
            error!("[REDIS] Failed to insert white list pool: {}", e);
            anyhow!("Failed to insert white list pool: {}", e)
        })?;

        debug!(
            "White list pool inserted into Redis: {} = {}",
            pool, is_white
        );
        Ok(())
    }

    /// POOL가 화이트리스트에 있는지 확인
    pub async fn check_token_pool_flag(&self, pool: &str) -> Result<Option<bool>> {
        let mut conn = self.get_conn().await?;
        let key = format!("{}{}", PREFIX_IS_TOKEN_POOL, pool);

        let exists: Option<bool> = conn.get(&key).await.map_err(|e| {
            error!("[REDIS] Failed to check white list pool: {}", e);
            anyhow!("Failed to check white list pool in Redis: {}", e)
        })?;

        // 풀이 존재하고 true인 경우만 TTL 갱신
        if let Some(is_white) = exists
            && is_white
        {
            self.refresh_ttl(&mut conn, &key).await?;
        }

        Ok(exists)
    }

    //-------------------------------------------------------------------------
    // 토큰-QUOTE TOKEN 관련 메서드들
    //-------------------------------------------------------------------------

    pub async fn insert_token_quote_id(&self, token: &str, quote_id: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, String, ()>(
            format!("{}{}", PREFIX_TOKEN_QUOTE, token),
            quote_id.to_string(),
            CACHE_EXPIRATION,
        )
        .await
        .map_err(|e| {
            error!("[REDIS] Failed to insert token quote: {}", e);
            anyhow!("Failed to insert token quote into Redis: {}", e)
        })?;

        Ok(())
    }

    pub async fn get_token_quote_id(&self, token: &str) -> Result<Option<String>> {
        let mut conn = self.get_conn().await?;
        let key = format!("{}{}", PREFIX_TOKEN_QUOTE, token);

        let quote: Option<String> = conn.get(&key).await.map_err(|e| {
            error!("[REDIS] Failed to get token quote: {}", e);
            anyhow!("Failed to get token quote from Redis: {}", e)
        })?;

        if quote.is_some() {
            self.refresh_ttl(&mut conn, &key).await?;
        }

        Ok(quote)
    }

    //-------------------------------------------------------------------------
    // POOL 페어 관련 메서드들
    //-------------------------------------------------------------------------

    /// POOL 페어 정보 저장 (token0, token1)
    pub async fn insert_pool_pair(&self, pool: &str, token0: &str, token1: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let pair_data = format!("{}:{}", token0, token1);

        conn.set_ex::<String, String, ()>(
            format!("{}{}", PREFIX_TOKEN_PAIR, pool),
            pair_data,
            CACHE_EXPIRATION,
        )
        .await
        .map_err(|e| {
            error!("[REDIS] Failed to insert pool pair: {}", e);
            anyhow!("Failed to insert pool pair: {}", e)
        })?;

        debug!(
            "Pool pair stored in Redis: pool={}, token0={}, token1={}",
            pool, token0, token1
        );
        Ok(())
    }

    /// POOL 페어 정보 조회
    pub async fn get_pool_pair(&self, pool: &str) -> Result<Option<(String, String)>> {
        let mut conn = self.get_conn().await?;
        let key = format!("{}{}", PREFIX_TOKEN_PAIR, pool);

        let pair_data: Option<String> = conn.get(&key).await.map_err(|e| {
            error!("[REDIS] Failed to get pool pair: {}", e);
            anyhow!("Failed to get pool pair from Redis: {}", e)
        })?;

        // 데이터가 존재하면 TTL 갱신
        if pair_data.is_some() {
            self.refresh_ttl(&mut conn, &key).await?;
        }

        match pair_data {
            Some(data) => {
                let parts: Vec<&str> = data.split(':').collect();
                if parts.len() == 2 {
                    Ok(Some((parts[0].to_string(), parts[1].to_string())))
                } else {
                    error!("[REDIS] Invalid pool pair format: {}", data);
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    //-------------------------------------------------------------------------
    // 유틸리티 메서드들
    //-------------------------------------------------------------------------

    /// Redis 연결 상태 조회 (ConnectionManager는 자동 재연결 지원)
    pub fn get_connection_status(&self) -> String {
        "ConnectionManager active (auto-reconnect enabled)".to_string()
    }

    /// 특정 패턴의 키 개수 조회
    pub async fn count_keys(&self, pattern: &str) -> Result<usize> {
        let mut conn = self.get_conn().await?;
        let keys: Vec<String> = conn.keys(pattern).await?;
        Ok(keys.len())
    }

    /// 캐시 통계 조회
    pub async fn get_cache_stats(&self) -> Result<CacheStats> {
        Ok(CacheStats {
            indexed_tokens: self
                .count_keys(&format!("{}*", PREFIX_TOKEN_EXISTS))
                .await?,
            white_list_pools: self
                .count_keys(&format!("{}*", PREFIX_IS_TOKEN_POOL))
                .await?,
            pool_pairs: self.count_keys(&format!("{}*", PREFIX_TOKEN_PAIR)).await?,
            connection_status: self.get_connection_status(),
        })
    }

    //-------------------------------------------------------------------------
    // Token Creator 관련 메서드들
    //-------------------------------------------------------------------------

    /// Token creator 매핑 저장 (TTL 1일)
    pub async fn insert_token_creator(&self, token: &str, creator: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, String, ()>(
            format!("{}{}", PREFIX_TOKEN_CREATOR, token),
            creator.to_string(),
            TOKEN_CREATOR_EXPIRATION,
        )
        .await
        .map_err(|e| anyhow!("Failed to insert token creator: {}", e))?;

        debug!(
            "Token creator cached: token={}, creator={}, TTL={}s",
            token, creator, TOKEN_CREATOR_EXPIRATION
        );
        Ok(())
    }

    /// Token creator 조회
    pub async fn get_token_creator(&self, token: &str) -> Result<Option<String>> {
        let mut conn = self.get_conn().await?;

        let creator: Option<String> = conn
            .get(format!("{}{}", PREFIX_TOKEN_CREATOR, token))
            .await
            .map_err(|e| anyhow!("Failed to get token creator: {}", e))?;

        Ok(creator)
    }

    //-------------------------------------------------------------------------
    // EOA (Externally Owned Account) 캐싱 메서드
    //-------------------------------------------------------------------------

    /// EOA 여부 캐싱 (true = EOA, false = Contract)
    pub async fn insert_is_eoa(&self, address: &str, is_eoa: bool) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, bool, ()>(
            format!("{}{}", PREFIX_EOA, address),
            is_eoa,
            EOA_EXPIRATION,
        )
        .await
        .map_err(|e| anyhow!("Failed to insert EOA status: {}", e))?;

        debug!(
            "EOA status cached: address={}, is_eoa={}, TTL={}s",
            address, is_eoa, EOA_EXPIRATION
        );
        Ok(())
    }

    /// EOA 여부 조회
    pub async fn check_is_eoa(&self, address: &str) -> Result<Option<bool>> {
        let mut conn = self.get_conn().await?;

        let is_eoa: Option<bool> = conn
            .get(format!("{}{}", PREFIX_EOA, address))
            .await
            .map_err(|e| anyhow!("Failed to check EOA status: {}", e))?;

        Ok(is_eoa)
    }

    /// EOA 또는 EIP-7702 delegated EOA 여부 캐싱 (별도 키)
    pub async fn insert_is_eoa_or_delegated(
        &self,
        address: &str,
        is_eoa_or_delegated: bool,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, bool, ()>(
            format!("{}{}", PREFIX_EOA_DELEGATED, address),
            is_eoa_or_delegated,
            EOA_EXPIRATION,
        )
        .await
        .map_err(|e| anyhow!("Failed to insert EOA/delegated status: {}", e))?;

        Ok(())
    }

    /// EOA 또는 EIP-7702 delegated EOA 여부 조회 (별도 키)
    pub async fn check_is_eoa_or_delegated(&self, address: &str) -> Result<Option<bool>> {
        let mut conn = self.get_conn().await?;

        let result: Option<bool> = conn
            .get(format!("{}{}", PREFIX_EOA_DELEGATED, address))
            .await
            .map_err(|e| anyhow!("Failed to check EOA/delegated status: {}", e))?;

        Ok(result)
    }

    //-------------------------------------------------------------------------
    // TX Sender 캐싱 메서드
    //-------------------------------------------------------------------------

    /// TX sender 캐싱 (tx_hash → sender address)
    pub async fn insert_tx_sender(&self, tx_hash: &str, sender: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;

        conn.set_ex::<String, String, ()>(
            format!("{}{}", PREFIX_TX_SENDER, tx_hash),
            sender.to_string(),
            TX_SENDER_EXPIRATION,
        )
        .await
        .map_err(|e| anyhow!("Failed to insert tx sender: {}", e))?;

        debug!(
            "TX sender cached: tx_hash={}, sender={}, TTL=1h",
            tx_hash, sender
        );
        Ok(())
    }

    /// TX sender 조회
    pub async fn get_tx_sender(&self, tx_hash: &str) -> Result<Option<String>> {
        let mut conn = self.get_conn().await?;

        let sender: Option<String> = conn
            .get(format!("{}{}", PREFIX_TX_SENDER, tx_hash))
            .await
            .map_err(|e| anyhow!("Failed to get tx sender: {}", e))?;

        Ok(sender)
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub indexed_tokens: usize,
    pub white_list_pools: usize,
    pub pool_pairs: usize,
    pub connection_status: String,
}
