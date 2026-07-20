use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};
use tracing::warn;

use super::EventType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DependencyWait {
    None,
    Timed,
    Strict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DependencyPolicy {
    wait: DependencyWait,
    dependencies: &'static [(EventType, u64)],
}

fn dependency_policy(event_type: EventType) -> DependencyPolicy {
    match event_type {
        EventType::Curve => DependencyPolicy {
            wait: DependencyWait::Timed,
            dependencies: &[(EventType::Price, 1)],
        },
        EventType::Dex | EventType::LpManager | EventType::Vault => DependencyPolicy {
            wait: DependencyWait::Timed,
            dependencies: &[(EventType::Curve, 1)],
        },
        EventType::Token => DependencyPolicy {
            wait: DependencyWait::Strict,
            dependencies: &[(EventType::Curve, 1)],
        },
        EventType::Price | EventType::PriceUsd | EventType::VaultRegistry => DependencyPolicy {
            wait: DependencyWait::None,
            dependencies: &[],
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReceiveType {
    Sync,
    Live,
}

#[derive(Debug)]
pub struct ReceiveManager {
    pub event_processed_block: RwLock<HashMap<EventType, AtomicU64>>,
    pub mode: Mutex<ReceiveType>,
}

lazy_static::lazy_static! {
    pub static ref RECEIVE_MANAGER: ReceiveManager = ReceiveManager {
        event_processed_block: {
            let mut map = HashMap::new();
            for event_type in EventType::all() {
                map.insert(event_type, AtomicU64::new(0));
            }
            RwLock::new(map)
        },
        mode: Mutex::new(ReceiveType::Sync),
    };
}

impl ReceiveManager {
    pub async fn set_last_processed_block(
        &self,
        event_type: EventType,
        processed_block: u64,
        latest_block: u64,
    ) {
        let map = self.event_processed_block.read().await;
        if let Some(block) = map.get(&event_type) {
            block.store(processed_block, Ordering::SeqCst);
        }

        if event_type == EventType::Curve {
            let mut mode = self.mode.lock().await;
            let is_live = latest_block.saturating_sub(processed_block) <= 1;

            if is_live {
                warn!("[RECEIVE] Live mode Change");
                *mode = ReceiveType::Live;
            } else {
                warn!("[RECEIVE] Sync mode Change");
                *mode = ReceiveType::Sync;
            }
        }
    }

    pub async fn get_last_processed_block(&self, event_type: EventType) -> u64 {
        let map = self.event_processed_block.read().await;
        let result = map
            .get(&event_type)
            .map(|block| block.load(Ordering::SeqCst))
            .unwrap_or(0);
        drop(map); // 읽기 락을 명시적으로 해제
        result
    }

    pub async fn check_last_processed_block(&self, block: u64, event_type: EventType) {
        let timeout = Duration::from_secs(60);
        let start = Instant::now();
        let policy = dependency_policy(event_type);

        match policy.wait {
            DependencyWait::Timed => {
                self.wait_for_dependency(start, timeout, block, event_type, policy.dependencies)
                    .await;
            }
            DependencyWait::Strict => {
                self.wait_for_dependency_strict(block, event_type, policy.dependencies)
                    .await;
            }
            DependencyWait::None => {}
        }
    }

    // 일반 의존성 대기 헬퍼 함수
    async fn wait_for_dependency(
        &self,
        start: Instant,
        timeout: Duration,
        block: u64,
        event_type: EventType,
        dependencies: &[(EventType, u64)],
    ) {
        while start.elapsed() < timeout {
            let mut all_ready = true;

            for &(dep_type, offset) in dependencies {
                let dep_block = self.get_last_processed_block(dep_type).await;
                if dep_block.saturating_sub(offset) < block {
                    all_ready = false;
                    break;
                }
            }

            if all_ready {
                return;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        warn!(
            "[RECEIVE] Timeout waiting for {} block {}",
            event_type.as_str(),
            block
        );

        // 타임아웃 발생해도 계속 진행
    }

    /// Strict version of `wait_for_dependency` — loops forever (no timeout) until
    /// all dependencies catch up. Token uses this to preserve Curve-before-Token
    /// checkpoint ordering without falling through after a timeout.
    ///
    /// Trade-off vs. `wait_for_dependency`: a genuinely broken dependency stalls
    /// this event type indefinitely (observable via metrics/logs) rather than
    /// producing wrong data.
    async fn wait_for_dependency_strict(
        &self,
        block: u64,
        event_type: EventType,
        dependencies: &[(EventType, u64)],
    ) {
        let mut warned = false;
        let start = Instant::now();
        loop {
            let mut all_ready = true;
            for &(dep_type, offset) in dependencies {
                let dep_block = self.get_last_processed_block(dep_type).await;
                if dep_block.saturating_sub(offset) < block {
                    all_ready = false;
                    break;
                }
            }
            if all_ready {
                return;
            }
            // After 60s, log a warning once so operators can see when a dep is
            // genuinely stuck. We don't keep re-warning to avoid log spam.
            if !warned && start.elapsed() >= Duration::from_secs(60) {
                warn!(
                    "[LP_TOKEN] {} strict-waiting > 60s for deps at block {} (correctness-critical, will not proceed)",
                    event_type.as_str(),
                    block
                );
                warned = true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DependencyPolicy, DependencyWait, dependency_policy};
    use crate::sync::EventType;

    #[test]
    fn curve_waits_for_price_with_offset_one() {
        assert_eq!(
            dependency_policy(EventType::Curve),
            DependencyPolicy {
                wait: DependencyWait::Timed,
                dependencies: &[(EventType::Price, 1)],
            }
        );
    }

    #[test]
    fn dex_waits_for_curve_with_offset_one() {
        assert_eq!(
            dependency_policy(EventType::Dex),
            DependencyPolicy {
                wait: DependencyWait::Timed,
                dependencies: &[(EventType::Curve, 1)],
            }
        );
    }

    #[test]
    fn lp_manager_waits_for_curve_with_offset_one() {
        assert_eq!(
            dependency_policy(EventType::LpManager),
            DependencyPolicy {
                wait: DependencyWait::Timed,
                dependencies: &[(EventType::Curve, 1)],
            }
        );
    }

    #[test]
    fn vault_waits_for_curve_with_offset_one() {
        assert_eq!(
            dependency_policy(EventType::Vault),
            DependencyPolicy {
                wait: DependencyWait::Timed,
                dependencies: &[(EventType::Curve, 1)],
            }
        );
    }

    #[test]
    fn token_strictly_waits_for_curve_only() {
        assert_eq!(
            dependency_policy(EventType::Token),
            DependencyPolicy {
                wait: DependencyWait::Strict,
                dependencies: &[(EventType::Curve, 1)],
            }
        );
    }

    #[test]
    fn price_is_independent() {
        assert_eq!(
            dependency_policy(EventType::Price),
            DependencyPolicy {
                wait: DependencyWait::None,
                dependencies: &[],
            }
        );
    }

    #[test]
    fn price_usd_is_independent() {
        assert_eq!(
            dependency_policy(EventType::PriceUsd),
            DependencyPolicy {
                wait: DependencyWait::None,
                dependencies: &[],
            }
        );
    }

    #[test]
    fn vault_registry_is_independent() {
        assert_eq!(
            dependency_policy(EventType::VaultRegistry),
            DependencyPolicy {
                wait: DependencyWait::None,
                dependencies: &[],
            }
        );
    }
}
