pub mod controller;

use once_cell::sync::OnceCell;
use sqlx::postgres::PgPoolOptions;
use std::{env, error::Error, str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use tokio::time::interval;
use tracing::{error, info, warn};
static POSTGRES_DB: OnceCell<Arc<PostgresDatabase>> = OnceCell::new();

#[derive(Debug)]
pub struct PostgresDatabase {
    pub pool: sqlx::Pool<sqlx::Postgres>,
}
/*  sqlx::query: 구조체로 매핑할 필요 없이 쿼리를 실행할 때 사용
•	sqlx::query_as!: 쿼리 결과를 구조체로 매핑할 때 사용
•	sqlx::query!: 결과를 튜플로 가져오거나, 단순히 쿼리를 실행할 때 사용
*/
impl PostgresDatabase {
    // 글로벌 인스턴스 초기화
    pub async fn init() -> Result<(), sqlx::Error> {
        if POSTGRES_DB.get().is_some() {
            info!("PostgresDatabase already initialized");
            return Ok(());
        }

        let instance = Self::new().await;
        let arc_instance = Arc::new(instance);

        if POSTGRES_DB.set(arc_instance).is_err() {
            info!("PostgresDatabase was initialized by another task");
        } else {
            info!("PostgresDatabase global instance initialized successfully");
        }

        Ok(())
    }

    // 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<Arc<PostgresDatabase>> {
        POSTGRES_DB.get().map(Arc::clone).ok_or_else(|| {
            anyhow::anyhow!("PostgresDatabase not initialized. Call PostgresDatabase::init() first")
        })
    }

    pub async fn new() -> Self {
        let pool = sqlx_connect().await;
        // Spawn background task to log detailed pool metrics periodically
        {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(60));
                loop {
                    interval.tick().await;

                    // 기본 풀 메트릭 수집
                    let size = pool_clone.size();
                    let idle = pool_clone.num_idle();
                    let acquired = size - idle as u32;

                    // pg_stat_activity 쿼리를 통한 상세 메트릭 수집
                    if let Ok(mut conn) = pool_clone.acquire().await {
                        let result = sqlx::query!(
                            r#"
                            SELECT 
                                count(*) as "total_connections!",
                                count(*) FILTER (WHERE state = 'active') as "active_connections!",
                                count(*) FILTER (WHERE state = 'idle') as "idle_connections!",
                                count(*) FILTER (WHERE state = 'idle in transaction') as "idle_in_transaction!"
                            FROM pg_stat_activity 
                            WHERE datname = current_database()
                            "#
                        )
                        .fetch_one(&mut *conn)
                        .await;

                        // 통합된 모니터링 결과 로깅
                        match result {
                            Ok(row) => {
                                warn!(
                                    "Postgres pool metrics: size={}, idle={}, acquired={}, db_total={}, db_active={}, db_idle={}, db_idle_in_transaction={}",
                                    size,
                                    idle,
                                    acquired,
                                    row.total_connections,
                                    row.active_connections,
                                    row.idle_connections,
                                    row.idle_in_transaction
                                );
                            }
                            Err(_) => {
                                // 쿼리 실패 시 기본 메트릭만 출력
                                warn!(
                                    "Postgres pool metrics: size={}, idle={}, acquired={}",
                                    size, idle, acquired
                                );
                            }
                        }
                    } else {
                        // 연결 획득 실패 시 기본 메트릭만 출력
                        warn!(
                            "Postgres pool metrics: size={}, idle={}, acquired={} (failed to acquire connection for detailed metrics)",
                            size, idle, acquired
                        );
                    }
                }
            });
        }
        Self { pool }
    }
}

async fn sqlx_connect() -> sqlx::Pool<sqlx::Postgres> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL environment variable not set");

    // 환경 변수에서 pool 설정 값 가져오기
    let max_connections = env::var("PG_MAX_CONNECTIONS")
        .expect("PG_MAX_CONNECTIONS environment variable not set")
        .parse::<u32>()
        .expect("PG_MAX_CONNECTIONS must be a valid u32");

    let min_connections = env::var("PG_MIN_CONNECTIONS")
        .expect("PG_MIN_CONNECTIONS environment variable not set")
        .parse::<u32>()
        .expect("PG_MIN_CONNECTIONS must be a valid u32");

    let max_lifetime_secs = env::var("PG_MAX_LIFETIME")
        .expect("PG_MAX_LIFETIME environment variable not set")
        .parse::<u64>()
        .expect("PG_MAX_LIFETIME must be a valid u64");

    let acquire_timeout_secs = env::var("PG_ACQUIRE_TIMEOUT")
        .expect("PG_ACQUIRE_TIMEOUT environment variable not set")
        .parse::<u64>()
        .expect("PG_ACQUIRE_TIMEOUT must be a valid u64");

    let idle_timeout_secs = env::var("PG_IDLE_TIMEOUT")
        .expect("PG_IDLE_TIMEOUT environment variable not set")
        .parse::<u64>()
        .expect("PG_IDLE_TIMEOUT must be a valid u64");

    let statement_cache_capacity = env::var("PG_STATEMENT_CACHE_CAPACITY")
        .expect("PG_STATEMENT_CACHE_CAPACITY environment variable not set")
        .parse::<usize>()
        .expect("PG_STATEMENT_CACHE_CAPACITY must be a valid usize");

    let ssl_mode = match env::var("PG_SSL_MODE")
        .expect("PG_SSL_MODE environment variable not set")
        .as_str()
    {
        "disable" => sqlx::postgres::PgSslMode::Disable,
        "prefer" => sqlx::postgres::PgSslMode::Prefer,
        "require" => sqlx::postgres::PgSslMode::Require,
        "verify-ca" => sqlx::postgres::PgSslMode::VerifyCa,
        "verify-full" => sqlx::postgres::PgSslMode::VerifyFull,
        mode => panic!(
            "Invalid PG_SSL_MODE: {}. Must be one of: disable, prefer, require, verify-ca, verify-full",
            mode
        ),
    };

    // Neon PgBouncer에 최적화된 고부하 환경 설정
    // PgBouncer의 트랜잭션 풀링 모드와 함께 작동하도록 조정됨
    let pool = PgPoolOptions::new()
        // 최대 연결 수 - PgBouncer가 다중화하므로 실제 백엔드 연결보다 많이 설정
        // db_total=819 로그를 고려하여 더 효율적인 값으로 조정
        .max_connections(max_connections)
        // 최소 연결 수 - 기본 부하 처리를 위한 준비된 연결 유지
        // 전체 연결의 ~5%로 설정하여 자원 효율성 개선
        .min_connections(min_connections)
        // 최대 연결 수명 - PgBouncer 환경에서는 더 짧게 설정하여 연결 회전 촉진
        // Neon의 서버리스 특성상 10분으로 단축하여 리소스 해제 촉진
        .max_lifetime(Duration::from_secs(max_lifetime_secs))
        // 연결 획득 타임아웃 - PgBouncer가 연결 관리를 담당하므로 짧게 설정 가능
        // 트랜잭션 완료 후 연결이 즉시 반환되므로 짧은 시간으로 충분
        .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
        // 유휴 타임아웃 - PgBouncer 환경에서는 더 공격적으로 설정
        // 트랜잭션 풀링 모드에서는 연결이 더 빨리 회수되어도 성능 저하 없음
        .idle_timeout(Duration::from_secs(idle_timeout_secs))
        // 연결 테스트 - PgBouncer가 이미 연결 관리를 수행하므로 비용 감소
        .test_before_acquire(true)
        // 효율적인 커넥션 설정으로 연결
        .connect_with(
            sqlx::postgres::PgConnectOptions::from_str(&database_url)
                .expect("Invalid database URL")
                .application_name("observer")
                // PgBouncer 환경에서 준비된 문 캐시 크기 최적화
                // 트랜잭션 풀링 모드에서 더 많은 준비된 문 재사용을 허용
                .statement_cache_capacity(statement_cache_capacity)
                // SSL 모드 설정 (환경에 맞게 조정)
                .ssl_mode(ssl_mode),
        )
        .await
        .map_err(|err| {
            let source_err = match err.source() {
                Some(source) => format!(": {}", source),
                None => String::new(),
            };
            error!(
                "Failed to establish Postgres connection{}{}",
                source_err,
                if err.to_string() != source_err.trim_start_matches(": ") {
                    format!(": {}", err)
                } else {
                    String::new()
                }
            );
        })
        .expect("Failed to establish Postgres connection");

    // 주요 글로벌 세션 파라미터 설정
    sqlx::query!("SET synchronous_commit = 'on'")
        .execute(&pool)
        .await
        .expect("Failed to set synchronous_commit");

    info!("PostgreSQL pool initialized with optimized settings");
    pool
}
