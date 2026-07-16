mod api;
mod block_stream;
mod config;
mod fallback;
mod health;
mod provider;

pub use config::ProviderConfig;
pub use provider::get_provider_with_wallet;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::providers::DynProvider;
use anyhow::Result;
use tokio::sync::{Mutex, OnceCell as TokioOnceCell};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

// Global RPC client instance
static RPC_CLIENT: TokioOnceCell<RpcClient> = TokioOnceCell::const_new();

pub struct RpcClient {
    // Vector of RPC providers (Arc<Mutex>로 변경하여 안전한 교체 가능, None if connection failed)
    providers: Arc<Mutex<Vec<Option<DynProvider>>>>,
    // Provider configurations with scoring
    provider_configs: Arc<Mutex<Vec<ProviderConfig>>>,
    // Current index to use (score-based selection)
    current_index: Arc<Mutex<usize>>,
    // Latest block number (thread-safe)
    latest_block: Arc<AtomicU64>,
    // Background task handle for block updates
    block_updater_handle: Option<JoinHandle<()>>,
    // Health check counter
    health_check_count: Arc<AtomicU64>,
}

impl RpcClient {
    // Initialize the global RPC client (심플하게)
    pub async fn init(urls: Vec<String>, _max_retries: Option<usize>) -> Result<&'static Self> {
        if let Some(client) = RPC_CLIENT.get() {
            return Ok(client);
        }

        let mut client = Self::create_client(urls).await?;
        client.start_block_updater().await?;

        match RPC_CLIENT.set(client) {
            Ok(_) => Ok(RPC_CLIENT.get().unwrap()),
            Err(_) => Ok(RPC_CLIENT.get().unwrap()),
        }
    }

    // Helper function to create a new RPC client (심플하게)
    async fn create_client(urls: Vec<String>) -> Result<Self> {
        if urls.is_empty() {
            return Err(anyhow::anyhow!("At least one RPC URL must be provided"));
        }

        let mut providers = Vec::new();
        let mut success_count = 0;
        let mut connection_errors = Vec::new();

        // 각 URL에 대해 연결 시도 (실패하면 None으로 설정)
        for (index, url) in urls.iter().enumerate() {
            let name = match index {
                0 => "Main".to_string(),
                1 => "Sub1".to_string(),
                2 => "Sub2".to_string(),
                _ => format!("Provider{}", index),
            };

            match provider::create_provider(url).await {
                Ok(provider) => {
                    info!("[CLIENT] ✓ Successfully connected to {}: {}", name, url);
                    providers.push(Some(provider));
                    success_count += 1;
                }
                Err(e) => {
                    error!("[CLIENT] ✗ Failed to connect to {}: {} - {}", name, url, e);
                    providers.push(None);
                    connection_errors.push((url.clone(), e.to_string()));
                }
            }
        }

        if success_count == 0 {
            return Err(anyhow::anyhow!(
                "Failed to connect to any RPC provider. Errors: {:?}",
                connection_errors
            ));
        }

        if !connection_errors.is_empty() {
            warn!(
                "[CLIENT] ⚠️ Partial initialization: {}/{} providers connected successfully. Failed providers will be retried during health checks.",
                success_count,
                urls.len()
            );
        }

        // Provider configs 생성 (Main 우선순위 시스템)
        let provider_configs = urls
            .iter()
            .enumerate()
            .map(|(i, url)| {
                let name = match i {
                    0 => "Main".to_string(),
                    1 => "Sub1".to_string(),
                    2 => "Sub2".to_string(),
                    _ => format!("Provider{}", i),
                };
                ProviderConfig::new(url.clone(), name, i) // index 추가로 우선순위 설정
            })
            .collect();

        let client = Self {
            providers: Arc::new(Mutex::new(providers)),
            provider_configs: Arc::new(Mutex::new(provider_configs)),
            current_index: Arc::new(Mutex::new(0)),
            latest_block: Arc::new(AtomicU64::new(0)),
            block_updater_handle: None,
            health_check_count: Arc::new(AtomicU64::new(0)),
        };

        // 초기 블록 번호 가져오기
        if let Ok(initial_block) = client.get_latest_block_number().await {
            client.latest_block.store(initial_block, Ordering::Relaxed);
            info!("Initial latest block: {}", initial_block);
        }

        Ok(client)
    }

    // 스마트 WebSocket 블록 업데이터 시작 (provider 교체 지원)
    async fn start_block_updater(&mut self) -> Result<()> {
        let latest_block = Arc::clone(&self.latest_block);
        let providers = Arc::clone(&self.providers);
        let provider_configs = Arc::clone(&self.provider_configs);

        let handle = tokio::spawn(async move {
            block_stream::smart_block_stream_loop(latest_block, providers, provider_configs).await;
        });

        self.block_updater_handle = Some(handle);
        info!(
            "[HealthCheck] ⚡ Started smart WebSocket block stream updater with provider switching"
        );
        Ok(())
    }

    // 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<&'static Self> {
        RPC_CLIENT.get().ok_or_else(|| {
            anyhow::anyhow!("RPC Client not initialized. Call RpcClient::init() first.")
        })
    }

    // Get the current provider based on the index
    pub async fn get_current_provider(&self) -> Result<DynProvider> {
        let index = *self.current_index.lock().await;
        let providers = self.providers.lock().await;
        providers[index]
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Current provider is None"))
    }

    // 현재 provider 인덱스 반환
    pub async fn get_current_provider_index(&self) -> usize {
        *self.current_index.lock().await
    }

    // Get provider for contract interactions (최신 alloy 패턴 지원)
    pub async fn get_provider(&self) -> Result<DynProvider> {
        self.get_current_provider().await
    }

    // 점수 기반 최적 provider 선택
    #[allow(dead_code)]
    async fn select_best_provider(&self) -> usize {
        fallback::select_best_provider(&self.provider_configs).await
    }

    // 성공 기록
    async fn record_provider_success(&self, provider_index: usize) {
        fallback::record_provider_success(&self.provider_configs, provider_index).await;
    }

    // 실패 기록
    async fn record_provider_failure(&self, provider_index: usize) {
        fallback::record_provider_failure(&self.provider_configs, provider_index).await;
    }

    // 점수 기반 요청 실행 (최고 점수부터 시도)
    async fn execute_with_fallback<F, T>(&self, operation: F) -> Result<T>
    where
        F: Fn(&DynProvider) -> Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + '_>>,
    {
        fallback::execute_with_fallback(
            &self.providers,
            &self.provider_configs,
            &self.current_index,
            operation,
        )
        .await
    }
}

// Drop trait 구현으로 자동 정리
impl Drop for RpcClient {
    fn drop(&mut self) {
        if let Some(handle) = self.block_updater_handle.take() {
            handle.abort();
        }
    }
}
