use moka::future::Cache;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

/// 전역 캐시 인스턴스
///
/// 단일 캐시로 모든 요청을 처리합니다.
/// Redis가 이미 1초 캐싱을 하고 있으므로 동일하게 1초 TTL을 사용합니다.
pub struct GlobalCache {
    /// 모든 쿼리 결과를 저장하는 통합 캐시 (1초 TTL)
    /// - 용도: 모든 종류의 쿼리 결과 캐싱
    /// - 특징: Single Flight Pattern으로 동시 요청 처리
    pub cache: Cache<String, Arc<Vec<u8>>>,
}

/// 전역 캐시 인스턴스 (싱글톤 패턴)
///
/// `Lazy`를 사용하여 첫 접근 시에만 초기화됩니다.
/// 프로그램 전체에서 하나의 인스턴스만 존재하며,
/// 모든 스레드에서 안전하게 공유됩니다.
pub static GLOBAL_CACHE: Lazy<GlobalCache> = Lazy::new(|| GlobalCache {
    cache: Cache::builder()
        .time_to_live(Duration::from_millis(1000)) // 1초 후 자동 만료 (Redis와 동일)
        .max_capacity(20_000) // 충분한 용량 확보
        .build(),
});

/// Single Flight Pattern을 구현하는 헬퍼 함수
///
/// 이 함수는 동일한 키에 대한 동시 요청을 하나의 DB 쿼리로 통합합니다.
///
/// # 작동 원리
///
/// 1. 캐시에 데이터가 있으면 즉시 반환
/// 2. 캐시에 없고 첫 번째 요청이면 DB 쿼리 실행
/// 3. 캐시에 없고 동시 요청이면 첫 번째 요청의 결과를 기다림
///
/// # 예시
///
/// ```rust
/// // 1000명이 동시에 같은 토큰 정보를 요청해도
/// // DB에는 1번만 쿼리가 실행됨
/// let token = with_cache(&GLOBAL_CACHE.cache, "token:BTC", || async {
///     self.fetch_token_from_db("BTC").await
/// }).await?;
/// ```
///
/// # 타입 매개변수
///
/// - `K`: 캐시 키 타입 (문자열로 변환 가능해야 함)
/// - `V`: 저장할 값의 타입 (JSON 직렬화/역직렬화 가능해야 함)
/// - `F`: DB에서 데이터를 가져오는 비동기 함수
/// - `Fut`: F가 반환하는 Future 타입
pub async fn with_cache<K, V, F, Fut>(
    cache: &Cache<String, Arc<Vec<u8>>>,
    key: K,
    fetch_fn: F,
) -> anyhow::Result<V>
where
    K: Hash + ToString + Debug,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<V>>,
{
    // 1단계: 캐시 키를 문자열로 변환
    let cache_key = key.to_string();

    // 2단계: Single Flight Pattern 핵심 - try_get_with 사용
    // 이 메소드가 동시 요청을 자동으로 통합해줍니다
    let result = cache
        .try_get_with(cache_key.clone(), async {
            // 캐시 미스 시에만 이 블록이 실행됨
            // 중요: 동시에 100개 요청이 와도 이 블록은 1번만 실행됨!

            // DB에서 실제 데이터 가져오기
            let value = fetch_fn().await?;

            // 데이터를 JSON으로 직렬화 (캐시에 저장하기 위해)
            let json_string = serde_json::to_string(&value)?;
            let bytes = json_string.into_bytes();

            // Arc로 감싸서 여러 스레드가 안전하게 공유
            Ok::<_, anyhow::Error>(Arc::new(bytes))
        })
        .await;

    // 3단계: 에러 처리
    let cached_bytes = match result {
        Ok(bytes) => bytes,
        Err(e) => return Err(anyhow::anyhow!("Cache operation failed: {}", e)),
    };

    // 4단계: 바이트를 원래 타입으로 역직렬화
    let json_string = String::from_utf8(cached_bytes.to_vec())
        .map_err(|e| anyhow::anyhow!("UTF-8 conversion failed: {}", e))?;
    let value = serde_json::from_str(&json_string)
        .map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))?;

    Ok(value)
}

/// 캐시 키를 생성하는 매크로
///
/// 여러 매개변수를 조합하여 고유한 캐시 키를 생성합니다.
///
/// # 예시
///
/// ```rust
/// // 단순한 키
/// let key = cache_key!("token", "BTC");
/// // 결과: "token:\"BTC\""
///
/// // 복잡한 키
/// let key = cache_key!("order_tokens", "market_cap", "DESC", 1, 10);
/// // 결과: "order_tokens:\"market_cap\":\"DESC\":1:10"
/// ```
#[macro_export]
macro_rules! cache_key {
    ($prefix:expr $(, $arg:expr)*) => {
        format!("{}:{}", $prefix, vec![$(format!("{:?}", $arg)),*].join(":"))
    };
}
