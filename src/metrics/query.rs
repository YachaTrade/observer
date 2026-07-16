/// PostgreSQL 쿼리 성능 측정 매크로
#[macro_export]
macro_rules! measure_postgres {
    ($operation:expr, $query:expr) => {{
        let start_time = tokio::time::Instant::now();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(10000), async { $query }).await;
        let elapsed = start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;

        let query_result = result
            .map_err(|_| {
                $crate::metrics::METRICS.db.increment_postgres_timeout();
                tracing::warn!("[METRICS] PostgreSQL timeout - {} (10000ms)", $operation);
                anyhow::anyhow!("PostgreSQL timeout after 10000ms")
            })?
            .map_err(|e| anyhow::anyhow!("PostgreSQL error: {}", e))?;

        // 성공/실패 상관없이 응답시간 기록
        $crate::metrics::METRICS
            .db
            .record_postgres_query(elapsed_ms);

        if elapsed_ms >= 1000 {
            tracing::warn!(
                "[METRICS] PostgreSQL slow - {} ({}ms)",
                $operation,
                elapsed_ms
            );
        }

        anyhow::Ok(query_result)
    }};
}

/// Redis 명령 성능 측정 매크로
#[macro_export]
macro_rules! measure_redis {
    ($operation:expr, $query:expr) => {{
        let start_time = tokio::time::Instant::now();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(300), async { $query }).await;
        let elapsed = start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;

        let query_result = result
            .map_err(|_| {
                $crate::metrics::METRICS.db.increment_redis_timeout();
                tracing::warn!("[METRICS] Redis timeout - {} (300ms)", $operation);
                anyhow::anyhow!("Redis timeout after 300ms")
            })?
            .map_err(|e| anyhow::anyhow!("Redis error: {}", e))?;

        // 성공/실패 상관없이 응답시간 기록
        $crate::metrics::METRICS.db.record_redis_query(elapsed_ms);

        if elapsed_ms >= 250 {
            tracing::warn!("[METRICS] Redis slow - {} ({}ms)", $operation, elapsed_ms);
        }

        anyhow::Ok(query_result)
    }};
}

/// RPC 호출 성능 측정 매크로
#[macro_export]
macro_rules! measure_rpc {
    ($operation:expr, $rpc_call:expr) => {{
        let start_time = tokio::time::Instant::now();
        let result = tokio::time::timeout(std::time::Duration::from_millis(10000), $rpc_call).await;
        let elapsed = start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;

        let rpc_result = result.map_err(|_| {
            // 타임아웃 - 실패와 타임아웃 모두 기록
            $crate::metrics::METRICS.provider.record_rpc_timeout();
            $crate::metrics::METRICS
                .provider
                .record_request_with_time(false, elapsed_ms);
            tracing::warn!("[METRICS] RPC timeout - {} ({}ms)", $operation, elapsed_ms);
            anyhow::anyhow!("RPC timeout after {}ms", elapsed_ms)
        })?;

        // 성공/실패 상관없이 응답시간 기록
        match &rpc_result {
            Ok(_) => {
                // 성공 - 응답시간과 함께 기록
                $crate::metrics::METRICS
                    .provider
                    .record_request_with_time(true, elapsed_ms);
                if elapsed_ms >= 2000 {
                    tracing::warn!("[METRICS] RPC slow - {} ({}ms)", $operation, elapsed_ms);
                }
            }
            Err(_) => {
                // 실패 - 응답시간과 함께 기록
                $crate::metrics::METRICS
                    .provider
                    .record_request_with_time(false, elapsed_ms);
            }
        }

        rpc_result
    }};
}
