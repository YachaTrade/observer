use std::sync::atomic::Ordering;
use std::time::Duration;

use alloy::providers::Provider;
use tracing::{error, info, warn};

use super::RpcClient;
use super::provider::create_provider;
use crate::metrics::METRICS;

impl RpcClient {
    // 점수 기반 헬스체크 (실패한 provider 교체 포함)
    pub async fn health_check_all_providers(&self) {
        info!("[HealthCheck] 🏥 Starting health check for all providers");

        // providers를 복사해서 lock 해제 (데드락 방지)
        let providers_to_check = {
            let providers = self.providers.lock().await;
            providers.clone()
        };

        for (i, provider_opt) in providers_to_check.iter().enumerate() {
            // provider가 None인 경우 재연결 시도
            if provider_opt.is_none() {
                let provider_name = {
                    let configs = self.provider_configs.lock().await;
                    configs
                        .get(i)
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| format!("Provider{}", i))
                };

                warn!(
                    "[HealthCheck] 🔄 Provider[{}] {} is None, attempting reconnection",
                    i, provider_name
                );

                // None이면 무조건 재연결 시도
                self.try_replace_failed_provider(i).await;
                continue;
            }

            let provider = provider_opt.as_ref().unwrap();
            let provider_name = {
                let configs = self.provider_configs.lock().await;
                configs
                    .get(i)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| format!("Provider{}", i))
            };

            // 첫 번째 블록 쿼리
            let first_result =
                tokio::time::timeout(Duration::from_secs(5), provider.get_block_number()).await;

            match first_result {
                Ok(Ok(first_block)) => {
                    // 2초 대기 후 두 번째 블록 쿼리
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let second_result =
                        tokio::time::timeout(Duration::from_secs(5), provider.get_block_number())
                            .await;

                    let old_score = {
                        let configs = self.provider_configs.lock().await;
                        configs
                            .get(i)
                            .map(|c| c.calculate_current_score())
                            .unwrap_or(0.0)
                    };

                    match second_result {
                        Ok(Ok(second_block)) => {
                            // 블록 비교: 증가했으면 성공, 같으면 실패
                            let is_live = second_block > first_block;

                            match is_live {
                                true => {
                                    let new_score = {
                                        let configs = self.provider_configs.lock().await;
                                        configs
                                            .get(i)
                                            .map(|c| c.calculate_current_score())
                                            .unwrap_or(0.0)
                                    };
                                    info!(
                                        "[HealthCheck] ✅ Provider[{}] {} - block: {} [LIVE] | Score: {:.2} → {:.2}",
                                        i, provider_name, second_block, old_score, new_score
                                    );
                                    {
                                        self.record_provider_success(i).await;
                                        METRICS.provider.set_provider_health(&provider_name, true);
                                    }
                                }
                                false => {
                                    let (new_score, fail_count) = {
                                        let configs = self.provider_configs.lock().await;
                                        if let Some(config) = configs.get(i) {
                                            (config.calculate_current_score(), config.fail_count)
                                        } else {
                                            (0.0, 0)
                                        }
                                    };
                                    warn!(
                                        "[HealthCheck] ⚠️ Provider[{}] {} - block: {} [STALE] | Score: {:.2} → {:.2} (fails: {})",
                                        i,
                                        provider_name,
                                        first_block,
                                        old_score,
                                        new_score,
                                        fail_count
                                    );
                                    {
                                        self.record_provider_failure(i).await;
                                        METRICS.provider.set_provider_health(&provider_name, false);
                                    }
                                }
                            }
                        }
                        _ => {
                            // 두 번째 쿼리 실패
                            self.record_provider_failure(i).await;
                            let (new_score, fail_count) = {
                                let configs = self.provider_configs.lock().await;
                                if let Some(config) = configs.get(i) {
                                    (config.calculate_current_score(), config.fail_count)
                                } else {
                                    (0.0, 0)
                                }
                            };
                            warn!(
                                "[HealthCheck] ❌ Provider[{}] {} - second query failed | Score: {:.2} → {:.2} (fails: {})",
                                i, provider_name, old_score, new_score, fail_count
                            );
                            {
                                self.record_provider_failure(i).await;
                                METRICS.provider.set_provider_health(&provider_name, false);
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    let old_score = {
                        let configs = self.provider_configs.lock().await;
                        configs
                            .get(i)
                            .map(|c| c.calculate_current_score())
                            .unwrap_or(0.0)
                    };
                    {
                        self.record_provider_failure(i).await;
                        METRICS.provider.set_provider_health(&provider_name, false);
                    }

                    let (new_score, fail_count) = {
                        let configs = self.provider_configs.lock().await;
                        if let Some(config) = configs.get(i) {
                            (config.calculate_current_score(), config.fail_count)
                        } else {
                            (0.0, 0)
                        }
                    };

                    warn!(
                        "[HealthCheck] ❌ Provider[{}] {} RPC error - {} | Score: {:.2} → {:.2} (fails: {})",
                        i, provider_name, e, old_score, new_score, fail_count
                    );

                    // 실패한 provider 교체 시도
                    self.try_replace_failed_provider(i).await;
                }
                Err(_) => {
                    let old_score = {
                        let configs = self.provider_configs.lock().await;
                        configs
                            .get(i)
                            .map(|c| c.calculate_current_score())
                            .unwrap_or(0.0)
                    };

                    {
                        self.record_provider_failure(i).await;
                        METRICS.provider.set_provider_health(&provider_name, false);
                    }
                    let (new_score, fail_count) = {
                        let configs = self.provider_configs.lock().await;
                        if let Some(config) = configs.get(i) {
                            (config.calculate_current_score(), config.fail_count)
                        } else {
                            (0.0, 0)
                        }
                    };

                    warn!(
                        "[HealthCheck] ⏰ Provider[{}] {} timeout (5s) | Score: {:.2} → {:.2} (fails: {})",
                        i, provider_name, old_score, new_score, fail_count
                    );

                    // 타임아웃된 provider 교체 시도
                    self.try_replace_failed_provider(i).await;
                }
            }
        }

        // 모든 헬스체크 완료 후 최적 provider 재선택
        let (best_index, best_name, best_score) = {
            let configs = self.provider_configs.lock().await;
            let mut best_idx = 0;
            let mut best_score = 0.0;
            let mut best_name = String::new();

            for (index, config) in configs.iter().enumerate() {
                let current_score = config.calculate_current_score();
                if current_score > best_score {
                    best_score = current_score;
                    best_idx = index;
                    best_name = config.name.clone();
                }
            }
            (best_idx, best_name, best_score)
        };

        info!(
            "[HealthCheck] 🎯 Selected best provider: [{}] {} (Score: {:.2})",
            best_index, best_name, best_score
        );

        // 점수 현황 출력 (5분마다만)
        if self
            .health_check_count
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(5)
        {
            self.print_provider_scores().await;
        }
    }

    // 실패한 provider 교체 시도 (안전한 버전)
    async fn try_replace_failed_provider(&self, provider_index: usize) {
        // 점수가 너무 낮은 경우에만 교체 시도 (과도한 교체 방지)
        let should_replace = {
            let configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get(provider_index) {
                let score = config.calculate_current_score();

                // 점수가 30 이하이거나 연속 실패가 3회 이상인 경우 교체
                score <= 30.0 || config.fail_count >= 3
            } else {
                false
            }
        };

        if !should_replace {
            return;
        }

        // 기존 URL 가져오기
        let original_url = {
            let configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get(provider_index) {
                config.url.clone()
            } else {
                return;
            }
        };

        let provider_name = {
            let configs = self.provider_configs.lock().await;
            configs
                .get(provider_index)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("Provider{}", provider_index))
        };

        warn!(
            "[HealthCheck] 🔄 Attempting to replace failed provider[{}] {} - URL: {}",
            provider_index, provider_name, original_url
        );

        // 새로운 provider 생성 시도
        match create_provider(&original_url).await {
            Ok(new_provider) => {
                // providers Vec 안전하게 업데이트 (Some으로 래핑)
                {
                    let mut providers = self.providers.lock().await;
                    if provider_index < providers.len() {
                        providers[provider_index] = Some(new_provider);
                    } else {
                        error!(
                            "[HealthCheck] ❌ Invalid provider index for replacement: {}",
                            provider_index
                        );
                        return;
                    }
                }

                // 설정 리셋 (새로운 연결이므로 점수 초기화)
                {
                    let mut configs = self.provider_configs.lock().await;
                    if let Some(config) = configs.get_mut(provider_index) {
                        config.score = 100.0;
                        config.success_count = 0;
                        config.fail_count = 0;
                        config.last_used = Some(std::time::Instant::now());
                    }
                }

                warn!(
                    "[HealthCheck] ✅ Successfully replaced provider[{}] {} - Score reset to 100.0",
                    provider_index, provider_name
                );
            }
            Err(e) => {
                // 재연결 실패 시 None으로 설정
                {
                    let mut providers = self.providers.lock().await;
                    if provider_index < providers.len() {
                        providers[provider_index] = None;
                    }
                }
                warn!(
                    "[HealthCheck] ❌ Failed to replace provider[{}] {}, set to None - Error: {}",
                    provider_index, provider_name, e
                );
            }
        }
    }

    // Provider 점수 현황 출력
    async fn print_provider_scores(&self) {
        let configs = self.provider_configs.lock().await;
        warn!("[HealthCheck] 📊 Provider Score Summary:");
        for (i, config) in configs.iter().enumerate() {
            let score = config.calculate_current_score();
            let total_attempts = config.success_count + config.fail_count;
            let success_rate = if total_attempts > 0 {
                (config.success_count as f32 / total_attempts as f32) * 100.0
            } else {
                100.0 // 시도가 없을 때는 100%로 표시
            };

            warn!(
                "[HealthCheck] 📈 [{}] {} (P{}) - Score: {:.2}/100 | Success: {} | Fail: {} | Rate: {:.1}% | Total: {}",
                i,
                config.name,
                config.priority,
                score,
                config.success_count,
                config.fail_count,
                success_rate,
                total_attempts
            );
        }
    }
}
