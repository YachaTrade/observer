use std::sync::Arc;
use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::ws::WebSocketConfig;
use anyhow::Result;
use reqwest::Url;
use tracing::{info, warn};

use crate::config::RPC_TIME_OUT;

// Helper function to create a single provider instance (3초 timeout)
pub async fn create_provider(url: &str) -> Result<DynProvider> {
    // WebSocket URL로 변환
    let ws_url = if url.starts_with("http://") {
        url.replace("http://", "ws://")
    } else if url.starts_with("https://") {
        url.replace("https://", "wss://")
    } else if url.starts_with("ws://") || url.starts_with("wss://") {
        url.to_string()
    } else {
        // 기본적으로 wss://로 가정
        format!("wss://{}", url)
    };
    let ws_confg = WebSocketConfig::default()
        .read_buffer_size(640 * 1024) // 5x: 640KB
        .write_buffer_size(640 * 1024) // 5x: 640KB
        .max_message_size(Some(320 << 20)) // 5x: 320MB
        .max_frame_size(Some(80 << 20)); // 5x: 80MB

    // WebSocket 연결 (3초 timeout)
    let ws = WsConnect::new(ws_url.clone()).with_config(ws_confg);
    let ws_provider = tokio::time::timeout(
        Duration::from_secs(3),
        ProviderBuilder::new().connect_ws(ws),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Provider connection timeout after 3 seconds"))?
    .map_err(|e| anyhow::anyhow!("Failed to create provider: {}", e))?;

    info!(
        "[HealthCheck] 🔧 Successfully created fresh provider: {}",
        ws_url
    );

    // 새로운 provider에 대해 즉시 ping-pong 테스트
    let provider = DynProvider::new(ws_provider);
    let ping_test_result =
        tokio::time::timeout(Duration::from_secs(3), provider.get_block_number()).await;

    match ping_test_result {
        Ok(Ok(_)) => {
            info!(
                "[HealthCheck] ✅ Fresh provider ping test successful: {}",
                ws_url
            );
            Ok(provider)
        }
        Ok(Err(e)) => {
            warn!(
                "[HealthCheck] ❌ Fresh provider ping test failed: {} - {}",
                ws_url, e
            );
            Err(anyhow::anyhow!("Provider ping test failed: {}", e))
        }
        Err(_) => {
            warn!(
                "[HealthCheck] ⏰ Fresh provider ping test timeout (3s): {}",
                ws_url
            );
            Err(anyhow::anyhow!("Provider ping test timeout"))
        }
    }
}

/// 지갑과 함께 프로바이더를 생성합니다.
/// 매번 호출될 때마다 새 프로바이더를 생성합니다.
pub async fn get_provider_with_wallet(wallet: EthereumWallet) -> Result<Arc<DynProvider>> {
    // URL 파싱
    let url = Url::parse(&std::env::var("MAIN_RPC_URL").expect("MAIN_RPC_URL must be set"))
        .expect("Invalid URL");

    // 지갑과 함께 새 프로바이더 생성
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(*RPC_TIME_OUT))
        .build()
        .expect("Failed to create HTTP client");
    let rpc_client = ClientBuilder::default().http_with_client(http_client, url);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_client(rpc_client);

    // 프로바이더 래핑하여 반환
    Ok(Arc::new(DynProvider::new(provider)))
}
