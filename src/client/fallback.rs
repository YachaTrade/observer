use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use alloy::providers::DynProvider;
use anyhow::Result;
use tokio::sync::Mutex;
use tracing::warn;

use super::config::ProviderConfig;
use crate::config::RPC_TIME_OUT;

// 점수 기반 최적 provider 선택
pub async fn select_best_provider(provider_configs: &Arc<Mutex<Vec<ProviderConfig>>>) -> usize {
    let configs = provider_configs.lock().await;
    let mut best_index = 0;
    let mut best_score = 0.0;

    for (index, config) in configs.iter().enumerate() {
        let score = config.calculate_current_score();
        if score > best_score {
            best_score = score;
            best_index = index;
        }
    }

    best_index
}

// 성공 기록
pub async fn record_provider_success(
    provider_configs: &Arc<Mutex<Vec<ProviderConfig>>>,
    provider_index: usize,
) {
    let mut configs = provider_configs.lock().await;
    if let Some(config) = configs.get_mut(provider_index) {
        config.record_success();
    }
}

// 실패 기록
pub async fn record_provider_failure(
    provider_configs: &Arc<Mutex<Vec<ProviderConfig>>>,
    provider_index: usize,
) {
    let mut configs = provider_configs.lock().await;
    if let Some(config) = configs.get_mut(provider_index) {
        config.record_failure();
    }
}

// 점수 기반 요청 실행 (최고 점수부터 시도)
pub async fn execute_with_fallback<F, T>(
    providers: &Arc<Mutex<Vec<Option<DynProvider>>>>,
    provider_configs: &Arc<Mutex<Vec<ProviderConfig>>>,
    current_index: &Arc<Mutex<usize>>,
    operation: F,
) -> Result<T>
where
    F: Fn(&DynProvider) -> Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + '_>>,
{
    // 최고 점수 provider부터 시작
    let best_index = select_best_provider(provider_configs).await;

    let providers_count = {
        let providers = providers.lock().await;
        providers.len()
    };

    for attempt in 0..providers_count {
        let provider_index = (best_index + attempt) % providers_count;

        let provider_opt = {
            let providers = providers.lock().await;
            providers[provider_index].clone()
        };

        // provider가 None이면 실패 기록하고 다음으로 넘어감
        let provider = match provider_opt {
            Some(p) => p,
            None => {
                record_provider_failure(provider_configs, provider_index).await;
                continue;
            }
        };

        let provider_name = {
            let configs = provider_configs.lock().await;
            configs
                .get(provider_index)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("Provider{}", provider_index))
        };

        match tokio::time::timeout(Duration::from_millis(*RPC_TIME_OUT), operation(&provider)).await
        {
            Ok(Ok(result)) => {
                // 성공 기록
                record_provider_success(provider_configs, provider_index).await;

                // current_index 업데이트
                {
                    let mut index = current_index.lock().await;
                    *index = provider_index;
                }
                return Ok(result);
            }
            Ok(Err(e)) => {
                record_provider_failure(provider_configs, provider_index).await;
                warn!(
                    "[HealthCheck] ❌ Provider[{}] {} RPC error in fallback: {}",
                    provider_index, provider_name, e
                );
            }
            Err(_) => {
                record_provider_failure(provider_configs, provider_index).await;
                warn!(
                    "[HealthCheck] ⏰ Provider[{}] {} timeout ({} ms) in fallback",
                    provider_index, provider_name, *RPC_TIME_OUT
                );
            }
        }
    }

    Err(anyhow::anyhow!("All providers failed"))
}
