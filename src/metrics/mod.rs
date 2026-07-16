use anyhow::Result;
use once_cell::sync::Lazy;
use tracing::warn;

pub mod db_metrics;
pub mod event_metrics;
pub mod graduate_metrics;
pub mod monitor;
pub mod provider_metrics;
pub mod query;
pub mod server;

use db_metrics::DBMetrics;
use event_metrics::EventMetrics;
use graduate_metrics::GraduateMetrics;
use provider_metrics::ProviderMetrics;

// Re-export event metrics types
pub use event_metrics::{MonitoredReceiver, MonitoredSender, monitored_channel};

/// 중앙 집중화된 메트릭 관리
pub struct Metrics {
    pub event: EventMetrics,
    pub provider: ProviderMetrics,
    pub db: DBMetrics,
    pub graduate: GraduateMetrics,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            event: EventMetrics::new(),
            provider: ProviderMetrics::new(),
            db: DBMetrics::new(),
            graduate: GraduateMetrics::new(),
        }
    }
}

/// 전역 메트릭 인스턴스
pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::new);

pub use monitor::metrics_logging_task;

pub async fn run_metrics_logging() -> Result<()> {
    match metrics_logging_task().await {
        Ok(()) => Ok(()),
        Err(err) => {
            warn!("[METRICS] logging task stopped: {err}");
            Err(err)
        }
    }
}
