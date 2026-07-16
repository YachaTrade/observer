use std::time::Duration;
use tokio::time::interval;
use tracing::info;

use super::METRICS;
use crate::config::METRICS_REPORT_INTERVAL;

pub async fn metrics_logging_task() -> anyhow::Result<()> {
    let interval_ms = *METRICS_REPORT_INTERVAL;
    info!("[METRICS] logging started with {}ms interval", interval_ms);

    let mut ticker = interval(Duration::from_millis(interval_ms));

    loop {
        ticker.tick().await;
        log_metrics_snapshot().await;
    }
}

async fn log_metrics_snapshot() {
    info!("[METRICS] === METRICS REPORT ===");

    // 1. 이벤트 채널 메트릭
    let (event_total, event_healthy, event_sent, event_received) = METRICS.event.get_event_values();
    info!(
        "[METRICS] 📡 Event Channels: Total {}, Healthy {}, Sent {}, Received {}",
        event_total, event_healthy, event_sent, event_received
    );

    // 알람 체크: 건강하지 않은 채널이 있는 경우
    if event_healthy < event_total {
        info!(
            "[METRICS] ⚠️  ALARM: {} unhealthy channels detected!",
            event_total - event_healthy
        );
    }

    // 알람 체크: 보낸/받은 메시지 수 불일치
    if event_sent != event_received {
        info!(
            "[METRICS] ⚠️  ALARM: Message mismatch - Sent: {}, Received: {}",
            event_sent, event_received
        );
    }

    // 1-1. 이벤트 채널 상세 정보
    let event_details = METRICS.event.get_event_channel_details();
    for (name, healthy, sent, received) in event_details {
        let status = if healthy { "✅" } else { "❌" };
        info!(
            "[METRICS] 📡   └─ {}: {} Sent:{} Received:{}",
            name, status, sent, received
        );
    }

    // 2. 프로바이더 메트릭
    let (success_rate, health_rate, rpc_timeouts, avg_response_time) =
        METRICS.provider.get_values();
    info!(
        "[METRICS] 🌐 Providers: Success {:.1}%, Health {:.1}%, RPC Timeouts {}, Avg Response {:.1}ms",
        success_rate, health_rate, rpc_timeouts, avg_response_time
    );

    // 프로바이더 알람 체크
    if success_rate < 80.0 {
        info!(
            "[METRICS] ⚠️  ALARM: Provider success rate below 80%: {:.1}%",
            success_rate
        );
    }
    if avg_response_time >= 1000.0 {
        info!(
            "[METRICS] ⚠️  ALARM: Provider avg response time above 1000ms: {:.1}ms",
            avg_response_time
        );
    }
    if rpc_timeouts > 0 {
        info!(
            "[METRICS] ⚠️  ALARM: RPC timeouts detected: {}",
            rpc_timeouts
        );
    }
    if health_rate < 70.0 {
        info!(
            "[METRICS] ⚠️  ALARM: Provider health rate below 70%: {:.1}%",
            health_rate
        );
    }

    // 3. DB 메트릭
    let (pg_timeouts, redis_timeouts, pg_avg_time, redis_avg_time) = METRICS.db.get_values();
    info!(
        "[METRICS] 🗄️ Database: PostgreSQL Timeouts {}, Avg {:.1}ms | Redis Timeouts {}, Avg {:.1}ms",
        pg_timeouts, pg_avg_time, redis_timeouts, redis_avg_time
    );

    // DB 알람 체크
    if pg_avg_time >= 500.0 {
        info!(
            "[METRICS] ⚠️  ALARM: PostgreSQL avg query time above 500ms: {:.1}ms",
            pg_avg_time
        );
    }
    if redis_avg_time >= 100.0 {
        info!(
            "[METRICS] ⚠️  ALARM: Redis avg query time above 100ms: {:.1}ms",
            redis_avg_time
        );
    }
    if pg_timeouts > 0 {
        info!(
            "[METRICS] ⚠️  ALARM: PostgreSQL timeouts detected: {}",
            pg_timeouts
        );
    }
    if redis_timeouts > 0 {
        info!(
            "[METRICS] ⚠️  ALARM: Redis timeouts detected: {}",
            redis_timeouts
        );
    }

    // 4. 리스팅 메트릭
    let (_lock_count, _graduate_count, difference) = METRICS.graduate.get_values();

    // 리스팅 알람 체크
    if difference > 2 {
        info!(
            "[METRICS] ⚠️  ALARM: Too many pending graduate tokens: {} tokens locked but not graduated",
            difference
        );
    }
}
