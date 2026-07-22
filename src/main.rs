use std::env;

use anyhow::Result;

use observer::{
    client,
    db::{cache::CacheManager, postgres::PostgresDatabase, redis::RedisDatabase},
    event::{
        common::{price as event_price, token as event_token},
        curve as event_curve, dex as event_dex,
        handler::run_event_handler as event_run_event_handler,
        lp_manager as event_lp_manager, vault as event_vault,
        vault_registry as event_vault_registry,
    },
    metrics::{run_metrics_logging, server::MetricsServer},
    sync::{EventType, stream::STREAM_MANAGER},
};

use tokio::task::JoinSet;
use tracing::{error, info, warn};
#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("main start");

    // Normalize address-bearing env config to EIP-55 checksum form and
    // force-init the statics so any env misconfiguration panics at boot
    // (not mid-stream on first consumer access).
    observer::config::force_init_address_configs();

    // Initialize database
    {
        PostgresDatabase::init().await?;
        RedisDatabase::init().await?;

        // Load quote token configs from DB (replaces QUOTE_CONFIGS env var).
        // Must run after PostgresDatabase::init() since it queries quote_token table.
        let db = PostgresDatabase::instance()?;
        observer::config::init_quote_configs_from_db(&db.pool).await?;

        // Flush observer-owned Redis caches at startup so every rebuilt
        // address-bearing value follows the EIP-55 checksum invariant.
        RedisDatabase::instance()?.flush_observer_caches().await?;

        // 새로운 캐시 매니저 초기화
        CacheManager::init().await?;
        info!("Cache Manager initialized");
    }
    // Initialize RPC client
    {
        let main_rpc_url = env::var("MAIN_RPC_URL").expect("MAIN_RPC_URL must be set");
        let sub_rpc_url_1 = env::var("SUB_RPC_URL_1").expect("SUB_RPC_URL_1 must be set");
        let sub_rpc_url_2 = env::var("SUB_RPC_URL_2").expect("SUB_RPC_URL_2 must be set");

        let rpc_urls = vec![main_rpc_url, sub_rpc_url_1, sub_rpc_url_2];
        let rpc_retry_count = rpc_urls.len();
        client::RpcClient::init(rpc_urls, Some(rpc_retry_count)).await?;
    }

    let _ = STREAM_MANAGER.initialize_block_range().await;

    // StreamManager 초기화 후 price 캐시 로드
    {
        let cache_manager = CacheManager::instance()?;
        if let Err(e) = cache_manager.load_initial_prices_from_stream().await {
            warn!("Failed to load initial prices from stream start: {}", e);
            // 에러가 나도 계속 진행 (캐시가 비어있어도 동작함)
        }
        // Warm up the chain-implied token_price_cache from the current `pool`
        // reserves so the first swap batch after restart doesn't fall through
        // to value=0 for every WMON-reachable token while the regular
        // RawSync-driven inference catches up. The sentinel block sits just
        // below the next batch's expected block_number; live inference
        // overwrites these with fresh per-block entries.
        let dex_range = STREAM_MANAGER.get_event_block_range(EventType::Dex).await;
        let sentinel = (dex_range.from_block as i64).saturating_sub(1);
        if let Err(e) = cache_manager.warm_up_token_price_cache(sentinel).await {
            warn!(
                "Failed to warm up token_price_cache from pool reserves: {}",
                e
            );
        }
    }

    let mut set = JoinSet::new();

    // Start unified metrics system (logging + RPC health checks)
    // Metrics logging task 실행
    set.spawn(run_metrics_logging());
    info!("[MAIN] Unified metrics system started");

    // 메트릭 서버 시작 - Prometheus /metrics 엔드포인트만 제공

    set.spawn(MetricsServer::start());

    set.spawn(event_run_event_handler::<event_curve::CurveEventHandler>(
        EventType::Curve,
    ));
    set.spawn(event_run_event_handler::<event_dex::DexEventHandler>(
        EventType::Dex,
    ));
    set.spawn(event_run_event_handler::<
        event_lp_manager::LpManagerEventHandler,
    >(EventType::LpManager));
    set.spawn(event_run_event_handler::<event_token::TokenEventHandler>(
        EventType::Token,
    ));
    set.spawn(event_run_event_handler::<event_price::PriceEventHandler>(
        EventType::Price,
    ));
    set.spawn(event_run_event_handler::<event_vault::VaultEventHandler>(
        EventType::Vault,
    ));
    set.spawn(event_run_event_handler::<
        event_vault_registry::VaultRegistryEventHandler,
    >(EventType::VaultRegistry));

    // 모든 태스크가 완료될 때까지 대기
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            error!("Task error: {:?}", e);
        }
    }

    info!("All tasks completed");
    Ok(())
}
