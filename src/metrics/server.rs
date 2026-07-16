use crate::{config::METRICS_PORT, metrics::METRICS};

use anyhow::Result;
use axum::{Router, http::StatusCode, response::Response, routing::get};
use std::net::SocketAddr;
use tracing::info;

pub struct MetricsServer;

impl MetricsServer {
    pub async fn start() -> Result<()> {
        let port = *METRICS_PORT;
        let app = Router::new().route("/metrics", get(Self::metrics_handler));

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        info!(
            "[METRICS] Metrics server listening on {} (serving /metrics only)",
            addr
        );

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Simple text metrics handler
    async fn metrics_handler() -> Result<Response<String>, StatusCode> {
        let mut output = String::new();

        // 1. 이벤트 채널 메트릭
        let (event_total, event_healthy, event_sent, event_received) =
            METRICS.event.get_event_values();
        output.push_str(&format!("event_channels_total {}\n", event_total));
        output.push_str(&format!("event_channels_healthy {}\n", event_healthy));
        output.push_str(&format!(
            "event_channels_dead {}\n",
            event_total - event_healthy
        ));
        output.push_str(&format!("event_channels_sent_total {}\n", event_sent));
        output.push_str(&format!(
            "event_channels_received_total {}\n",
            event_received
        ));

        // 2. 프로바이더 메트릭
        let (success_rate, health_rate, rpc_timeouts, avg_response_time) =
            METRICS.provider.get_values();
        output.push_str(&format!(
            "provider_success_rate_percent {:.2}\n",
            success_rate
        ));
        output.push_str(&format!(
            "provider_health_rate_percent {:.2}\n",
            health_rate
        ));
        output.push_str(&format!("provider_rpc_timeouts_total {}\n", rpc_timeouts));
        output.push_str(&format!(
            "provider_avg_response_time_ms {:.2}\n",
            avg_response_time
        ));

        // 3. DB 메트릭
        let (pg_timeouts, redis_timeouts, pg_avg_time, redis_avg_time) = METRICS.db.get_values();
        output.push_str(&format!("db_postgres_timeouts_total {}\n", pg_timeouts));
        output.push_str(&format!("db_redis_timeouts_total {}\n", redis_timeouts));
        output.push_str(&format!(
            "db_postgres_avg_query_time_ms {:.2}\n",
            pg_avg_time
        ));
        output.push_str(&format!(
            "db_redis_avg_query_time_ms {:.2}\n",
            redis_avg_time
        ));

        // 4. 졸업 메트릭
        let (lock_count, graduate_count, difference) = METRICS.graduate.get_values();
        output.push_str(&format!("lock_count {}\n", lock_count));
        output.push_str(&format!("graduate_count {}\n", graduate_count));
        output.push_str(&format!(
            "pending_graduate_tokens {}\n",
            difference.max(0) as u64
        ));

        let response = Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .body(output)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(response)
    }
}
