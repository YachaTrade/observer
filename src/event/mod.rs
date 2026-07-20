use crate::{
    cache::{GLOBAL_CACHE, with_cache},
    cache_key,
    client::RpcClient,
    db::redis::RedisDatabase,
};
use anyhow::Result;
use tracing::error;

pub mod common;
pub mod core;
pub mod curve;
pub mod dex;
pub mod error;
pub mod handler;
pub mod lp_manager;
pub(crate) mod usd_enrich;
pub mod vault;
pub mod vault_registry;

pub async fn get_block_timestamp(client: &RpcClient, block_number: u64) -> Result<u64> {
    let redis = RedisDatabase::instance()?;

    // 먼저 Redis에서 확인
    match redis.get_block_timestamp(block_number).await {
        Ok(Some(timestamp)) => Ok(timestamp),
        Ok(None) | Err(_) => {
            // Redis에 없거나 에러인 경우 Single Flight 패턴 적용
            let cache_key = cache_key!("block_timestamp", block_number);

            with_cache(&GLOBAL_CACHE.cache, cache_key, || async {
                // RPC에서 블록 타임스탬프 가져오기
                let block_timestamp = client.get_block_timestamp(block_number).await?;

                // Redis에 캐시 저장 (백그라운드로 처리)
                if let Err(e) = redis
                    .set_block_timestamp(block_number, block_timestamp)
                    .await
                {
                    error!(
                        "[EVENT] Failed to cache block timestamp for block {}: {}",
                        block_number, e
                    );
                }

                Ok(block_timestamp)
            })
            .await
        }
    }
}
