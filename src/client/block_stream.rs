use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::providers::{DynProvider, Provider};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use super::config::ProviderConfig;
use crate::config::STREAM_TIMEOUT;

// 헬퍼: 최고 점수 provider 인덱스 반환 (None인 것은 스킵)
pub async fn select_best_provider_index(
    provider_configs: &Arc<Mutex<Vec<ProviderConfig>>>,
) -> usize {
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

// 스마트 WebSocket 블록 스트림 (provider 교체 및 장애 대응)
pub async fn smart_block_stream_loop(
    latest_block: Arc<AtomicU64>,
    providers: Arc<Mutex<Vec<Option<DynProvider>>>>,
    provider_configs: Arc<Mutex<Vec<ProviderConfig>>>,
) {
    info!("[HealthCheck] 🚀 Starting smart WebSocket block stream with provider switching");
    let mut current_provider_index = 0;
    let mut consecutive_failures = 0;

    loop {
        // 최고 점수 provider 선택 (주기적으로 체크)
        let best_provider_index = select_best_provider_index(&provider_configs).await;

        // provider 변경 필요 시 전환
        if current_provider_index != best_provider_index || consecutive_failures >= 3 {
            let old_provider_name = {
                let configs = provider_configs.lock().await;
                configs
                    .get(current_provider_index)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| format!("Provider{}", current_provider_index))
            };

            current_provider_index = best_provider_index;
            consecutive_failures = 0;

            let new_provider_name = {
                let configs = provider_configs.lock().await;
                configs
                    .get(current_provider_index)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| format!("Provider{}", current_provider_index))
            };

            info!(
                "[HealthCheck] 🔄 WebSocket switching: {} → provider[{}] {}",
                old_provider_name, current_provider_index, new_provider_name
            );
        }

        let provider_opt = {
            let providers = providers.lock().await;
            if current_provider_index < providers.len() {
                providers[current_provider_index].clone()
            } else {
                providers.first().and_then(|p| p.clone())
            }
        };

        // provider가 None이면 다음 provider로 전환
        let provider = match provider_opt {
            Some(p) => p,
            None => {
                warn!(
                    "[HealthCheck] ❌ Provider[{}] is None, switching to next...",
                    current_provider_index
                );
                current_provider_index =
                    (current_provider_index + 1) % providers.lock().await.len();
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        let provider_name = {
            let configs = provider_configs.lock().await;
            configs
                .get(current_provider_index)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("Provider{}", current_provider_index))
        };

        match provider.subscribe_blocks().await {
            Ok(subscription) => {
                info!(
                    "[HealthCheck] 🔗 WebSocket connected to provider[{}] {} for block stream",
                    current_provider_index, provider_name
                );
                let mut stream = subscription.into_stream();
                let mut blocks_received = 0;
                let block_timeout = Duration::from_millis(*STREAM_TIMEOUT);

                loop {
                    match tokio::time::timeout(block_timeout, stream.next()).await {
                        Ok(Some(header)) => {
                            let block_number = header.number;
                            let old_block = latest_block.load(Ordering::Relaxed);

                            if block_number > old_block {
                                latest_block.store(block_number, Ordering::Relaxed);
                                blocks_received += 1;
                                info!(
                                    "[HealthCheck] 📦 Block updated: {} → {} ({})",
                                    old_block, block_number, provider_name
                                );

                                // 성공적으로 블록을 받았으므로 실패 카운터 리셋 및 점수 증가
                                consecutive_failures = 0;
                                {
                                    let mut configs = provider_configs.lock().await;
                                    if let Some(config) = configs.get_mut(current_provider_index) {
                                        config.record_success();
                                    }
                                }

                                // 30블록마다 provider 재평가
                                if blocks_received % 30 == 0 {
                                    let new_best =
                                        select_best_provider_index(&provider_configs).await;
                                    if new_best != current_provider_index {
                                        let new_provider_name = {
                                            let configs = provider_configs.lock().await;
                                            configs
                                                .get(new_best)
                                                .map(|c| c.name.clone())
                                                .unwrap_or_else(|| format!("Provider{}", new_best))
                                        };
                                        info!(
                                            "[HealthCheck] 🔄 Better provider[{}] {} available, switching from {}...",
                                            new_best, new_provider_name, provider_name
                                        );
                                        break; // 현재 스트림 종료하고 새 provider로 전환
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            // 스트림 종료
                            warn!(
                                "[HealthCheck] ⚠️ WebSocket stream ended from provider[{}] {}, trying next...",
                                current_provider_index, provider_name
                            );
                            consecutive_failures += 1;

                            // 스트림 종료도 실패로 간주하여 점수 감소
                            {
                                let mut configs = provider_configs.lock().await;
                                if let Some(config) = configs.get_mut(current_provider_index) {
                                    config.record_failure();
                                }
                            }
                            break;
                        }
                        Err(_) => {
                            // 타임아웃 발생 - 블록을 받지 못함
                            error!(
                                "[HealthCheck] ⏰ WebSocket timeout: No block received for {} ms from provider[{}] {}, switching...",
                                block_timeout.as_millis(),
                                current_provider_index,
                                provider_name
                            );
                            consecutive_failures += 1;

                            // 타임아웃도 실패로 간주하여 점수 감소
                            {
                                let mut configs = provider_configs.lock().await;
                                if let Some(config) = configs.get_mut(current_provider_index) {
                                    config.record_failure();
                                }
                            }
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "[HealthCheck] ❌ Failed to subscribe WebSocket to provider[{}] {}: {}",
                    current_provider_index, provider_name, e
                );
                consecutive_failures += 1;

                // 실패한 provider의 점수 감소
                {
                    let mut configs = provider_configs.lock().await;
                    if let Some(config) = configs.get_mut(current_provider_index) {
                        config.record_failure();
                    }
                }

                // 연속 실패 시 다른 provider로 전환
                if consecutive_failures >= 2 {
                    current_provider_index =
                        (current_provider_index + 1) % providers.lock().await.len();
                    let next_provider_name = {
                        let configs = provider_configs.lock().await;
                        configs
                            .get(current_provider_index)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| format!("Provider{}", current_provider_index))
                    };
                    warn!(
                        "[HealthCheck] 🔄 WebSocket switching to next provider[{}] {} due to consecutive failures",
                        current_provider_index, next_provider_name
                    );
                }

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
