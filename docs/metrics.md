# Observer Metrics System

통합 메트릭 시스템을 통해 실시간으로 애플리케이션 상태를 모니터링하고 추적할 수 있습니다.

## 📊 무엇을 모니터링하는가?

### 1. 채널 메트릭 (Channel Metrics)
이벤트 처리 채널의 상태를 모니터링합니다.

- **메시지 전송 수**: 각 채널로 전송된 메시지 총 개수
- **메시지 수신 수**: 각 채널에서 수신된 메시지 총 개수  
- **에러 발생 수**: 채널에서 발생한 에러 총 개수

**모니터링 대상 채널:**
- `curve_events`: 본딩 커브 이벤트
- `dex_events`: DEX 거래 이벤트
- `price_events`: 가격 업데이트 이벤트
- `reward_events`: 리워드 이벤트
- `lp_manager_events`: LP 관리 이벤트
- `token_events`: 토큰 이벤트

### 2. 데이터베이스 메트릭 (DB Metrics)
PostgreSQL 데이터베이스 작업의 성능과 안정성을 추적합니다.

- **쿼리 총 실행 수**: 각 작업별 총 실행 횟수
- **성공한 쿼리 수**: 정상 완료된 쿼리 개수
- **실패한 쿼리 수**: 에러가 발생한 쿼리 개수
- **성공률**: (성공 / 총 실행) × 100%

**모니터링 대상 작업:**
- **모든 DB 쿼리**: `measure_query!` 매크로가 적용된 모든 쿼리가 자동으로 추적됨
- 예시: `token_insert_token_and_market`, `reward_handle_add_reward_event`, `position_handle_buy`, `chart_handle_chart_without_tx`, `market_get_price_by_token`, `lp_handle_lp_allocate` 등
- **100개 이상의 DB 작업**이 실시간으로 모니터링됨

### 3. RPC 프로바이더 메트릭 (RPC Provider Metrics)
블록체인 RPC 연결 상태와 성능을 모니터링합니다.

- **요청 총 개수**: 각 프로바이더로 보낸 요청 수
- **성공한 요청 수**: 정상 응답받은 요청 수
- **실패한 요청 수**: 에러가 발생한 요청 수
- **건강 상태**: 현재 프로바이더가 정상 작동하는지 여부
- **성공률**: (성공 / 총 요청) × 100%

### 4. 블록체인 동기화 메트릭 (Stream Metrics - StreamManager)
블록체인 이벤트 스트림 동기화 상태를 모니터링합니다.

- **현재 블록**: 각 이벤트 타입별 현재 처리 중인 블록 번호
- **성공 횟수**: 블록 처리가 성공적으로 완료된 총 횟수
- **에러 횟수**: RPC 요청 실패 등으로 발생한 에러 총 횟수
- **성공률**: (성공 / 전체) × 100%

**모니터링 대상 이벤트 타입:**
- `curve`, `token`, `dex`, `lp_manager`, `buy_back`, `creator_vault`, `token_management`, `reward`, `price`
- `recovery_curve`, `recovery_token`, `recovery_dex`, `recovery_buy_back`, `recovery_lp_manager`, `recovery_creator_vault`, `recovery_token_management`, `recovery_reward`

### 5. 이벤트 수신 메트릭 (Receive Metrics - ReceiveManager)
이벤트 처리 의존성 관리와 수신 상태를 모니터링합니다.

- **현재 블록**: 각 이벤트 타입별 현재 처리 중인 블록 번호
- **성공 횟수**: 이벤트 수신 처리가 성공적으로 완료된 총 횟수
- **에러 횟수**: 의존성 타임아웃 등으로 발생한 에러 총 횟수
- **성공률**: (성공 / 전체) × 100%

**의존성 관계:**
- `dex`, `token`, `lp_manager`, `buy_back` 등은 `curve` 이벤트 처리 완료 후 진행
- 모든 `recovery_*` 이벤트는 해당 원본 이벤트 처리 완료 후 진행
- 의존성 대기 타임아웃 발생 시 에러로 기록되며 처리는 계속 진행

### 6. 전역 메트릭 (Global Metrics)
애플리케이션 전체 상태를 추적합니다.

- **업타임**: 애플리케이션 실행 시간 (초)
- **처리된 이벤트 총 개수**: 지금까지 처리한 모든 이벤트 수

## 🔧 어떻게 작동하는가?

### 시스템 아키텍처

```
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Event Channels│  │  DB Operations  │  │  RPC Providers  │  │ Sync Managers   │
│                 │  │                 │  │                 │  │                 │
│ MonitoredSender │  │ measure_query!  │  │ health_check    │  │ StreamManager   │
│MonitoredReceiver│  │     macro       │  │   functions     │  │ ReceiveManager  │
└─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘
         │                     │                     │                     │
         ▼                     ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           METRICS (통합 매니저)                                  │
│                                                                                 │
│ ChannelMetrics   DbMetrics      RpcMetrics     SyncMetrics      ReceiveMetrics  │
│ - sent_count     - query_count  - request_count - current_block  - last_block   │
│ - received_count - success_count - success_count - target_block   - wait_count   │
│ - error_count    - error_count  - error_count   - processed      - timeout_count │
│                                 - is_healthy    - sync_errors    - is_live_mode  │
│                                                                                 │
│                              GlobalMetrics                                      │
│                              - uptime_seconds                                   │
│                              - total_events_processed                           │
└─────────────────────────────────────────────────────────────────────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────┐                          ┌─────────────────┐
│  Metrics Logger │                          │ Prometheus API  │
│  (10초마다)     │                          │  /metrics       │
│                 │                          │                 │
│ 📡🗄️🌐🔄📥🌍     │                          │  HTTP 엔드포인트 │
│  콘솔 로그 출력  │                          │                 │
└─────────────────┘                          └─────────────────┘
```

### 메트릭 수집 방식

1. **채널 메트릭**: `MonitoredSender`/`MonitoredReceiver`가 자동으로 전송/수신 시마다 카운트
2. **DB 메트릭**: `measure_query!` 매크로가 적용된 **모든 쿼리**에서 실행 전후에 자동으로 성공/실패 기록
3. **RPC 메트릭**: 60초마다 모든 프로바이더 상태 체크하여 기록
4. **동기화 메트릭**: `StreamManager`에서 블록 처리 성공 시 성공 기록, RPC 에러 시 에러 기록
5. **수신 메트릭**: `ReceiveManager`에서 블록 처리 성공 시 성공 기록, 의존성 타임아웃 시 에러 기록
6. **전역 메트릭**: 애플리케이션 시작 시점부터 누적 계산

## 🚀 사용 방법

### 환경 변수 설정

```bash
# .env 파일에 추가
METRICS_PORT=8080                    # Prometheus 메트릭 서버 포트
PROVIDER_CHECK_INTERVAL=60000        # RPC 프로바이더 체크 간격 (ms)
```

### 애플리케이션 시작

```rust
// main.rs에서 자동으로 시작됨
start_metrics_system();              // 메트릭 로깅 + RPC 체크 시작
set.spawn(MetricsServer::start());   // Prometheus 서버 시작
```

### 콘솔 로그 확인

애플리케이션을 실행하면 10초마다 다음과 같은 메트릭 리포트가 출력됩니다:

```
=== METRICS REPORT ===
📡 Channel Metrics:
  curve_events - Sent: 150, Received: 148, Errors: 2
  dex_events - Sent: 89, Received: 89, Errors: 0
  price_events - Sent: 203, Received: 203, Errors: 0

🗄️ Database Metrics:
  token_insert_token_and_market - Total: 45, Success: 45, Errors: 0, Rate: 100.0%
  reward_handle_add_reward_event - Total: 23, Success: 22, Errors: 1, Rate: 95.7%
  position_handle_buy - Total: 156, Success: 154, Errors: 2, Rate: 98.7%
  chart_handle_chart_without_tx - Total: 89, Success: 89, Errors: 0, Rate: 100.0%
  market_get_price_by_token - Total: 203, Success: 201, Errors: 2, Rate: 99.0%

🌐 RPC Provider Metrics:
  Provider0 - ✅ HEALTHY | Total: 234, Success: 230, Errors: 4, Rate: 98.3%
  Provider1 - ❌ UNHEALTHY | Total: 67, Success: 60, Errors: 7, Rate: 89.6%

🔄 Stream Metrics:
  curve - Block: 19850123, Success: 12500, Errors: 3, Rate: 99.9%
  token - Block: 19850120, Success: 9800, Errors: 2, Rate: 99.9%
  dex - Block: 19850119, Success: 8750, Errors: 5, Rate: 99.9%

📥 Receive Metrics:
  token - Block: 19850115, Success: 9800, Errors: 2, Rate: 99.9%
  dex - Block: 19850112, Success: 8750, Errors: 5, Rate: 99.9%
  lp_manager - Block: 19850110, Success: 7200, Errors: 1, Rate: 99.9%

🌍 Global - Uptime: 3450s, Events Processed: 1247
======================
```

### Prometheus 메트릭 API

브라우저 또는 Prometheus 서버에서 메트릭을 확인할 수 있습니다:

```bash
curl http://localhost:8080/metrics
```

**응답 예시:**
```
# Channel Metrics
observer_channel_messages_sent_total{channel_name="curve_events"} 150
observer_channel_messages_received_total{channel_name="curve_events"} 148
observer_channel_errors_total{channel_name="curve_events"} 2

# Database Metrics  
observer_db_operations_total{operation="token_insert_token_and_market"} 45
observer_db_operations_success_total{operation="token_insert_token_and_market"} 45
observer_db_operations_errors_total{operation="token_insert_token_and_market"} 0
observer_db_operations_total{operation="reward_handle_add_reward_event"} 23
observer_db_operations_success_total{operation="reward_handle_add_reward_event"} 22
observer_db_operations_errors_total{operation="reward_handle_add_reward_event"} 1

# RPC Provider Metrics
observer_rpc_requests_total{provider="Provider0"} 234
observer_rpc_requests_success_total{provider="Provider0"} 230
observer_rpc_provider_healthy{provider="Provider0"} 1

# Stream Metrics (StreamManager)
observer_stream_current_block{event_type="curve"} 19850123
observer_stream_success_total{event_type="curve"} 12500
observer_stream_errors_total{event_type="curve"} 3

# Receive Metrics (ReceiveManager)
observer_receive_current_block{event_type="token"} 19850115
observer_receive_success_total{event_type="token"} 9800
observer_receive_errors_total{event_type="token"} 2

# Global Metrics
observer_uptime_seconds 3450
observer_events_processed_total 1247
```

## 📈 메트릭 활용 방법

### 1. 실시간 모니터링

**Grafana 대시보드 설정:**
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'observer'
    static_configs:
      - targets: ['localhost:8080']
    scrape_interval: 15s
```

**주요 지표 패널:**
- Channel 처리량: `rate(observer_channel_messages_sent_total[5m])`
- DB 성공률: `observer_db_operations_success_total / observer_db_operations_total * 100`
- RPC 가용성: `observer_rpc_provider_healthy`
- Stream 성공률: `observer_stream_success_total / (observer_stream_success_total + observer_stream_errors_total) * 100`
- Receive 성공률: `observer_receive_success_total / (observer_receive_success_total + observer_receive_errors_total) * 100`
- 이벤트별 현재 블록: `observer_stream_current_block{event_type="curve"}`

### 2. 알림 설정

**Prometheus AlertManager 규칙:**
```yaml
groups:
- name: observer_alerts
  rules:
  # DB 성공률이 95% 이하로 떨어질 때
  - alert: DatabaseSuccessRateLow
    expr: (observer_db_operations_success_total / observer_db_operations_total) * 100 < 95
    for: 2m
    annotations:
      summary: "Database success rate below 95%"

  # RPC 프로바이더가 Unhealthy 상태일 때  
  - alert: RpcProviderDown
    expr: observer_rpc_provider_healthy == 0
    for: 1m
    annotations:
      summary: "RPC Provider {{ $labels.provider }} is unhealthy"

  # 채널 에러가 급증할 때
  - alert: ChannelErrorsHigh
    expr: rate(observer_channel_errors_total[5m]) > 0.1
    for: 30s
    annotations:
      summary: "High error rate on channel {{ $labels.channel_name }}"

  # Stream 성공률이 낮을 때
  - alert: StreamSuccessRateLow
    expr: (observer_stream_success_total / (observer_stream_success_total + observer_stream_errors_total)) * 100 < 95
    for: 2m
    annotations:
      summary: "Stream success rate below 95% for {{ $labels.event_type }}"

  # Receive 성공률이 낮을 때
  - alert: ReceiveSuccessRateLow
    expr: (observer_receive_success_total / (observer_receive_success_total + observer_receive_errors_total)) * 100 < 95
    for: 1m
    annotations:
      summary: "Receive success rate below 95% for {{ $labels.event_type }}"

  # Stream 에러가 증가할 때
  - alert: StreamErrorsHigh
    expr: rate(observer_stream_errors_total[5m]) > 0.05
    for: 1m
    annotations:
      summary: "High stream error rate for {{ $labels.event_type }}"

  # Receive 에러가 증가할 때
  - alert: ReceiveErrorsHigh
    expr: rate(observer_receive_errors_total[5m]) > 0.05
    for: 1m
    annotations:
      summary: "High receive error rate for {{ $labels.event_type }}"
```

### 3. 성능 분석

**병목 지점 식별:**
```bash
# 처리량이 가장 낮은 채널 찾기
curl -s http://localhost:8080/metrics | grep channel_messages_received_total

# 가장 많이 실패하는 DB 작업 찾기  
curl -s http://localhost:8080/metrics | grep db_operations_errors_total

# RPC 프로바이더 성능 비교
curl -s http://localhost:8080/metrics | grep rpc_requests_success_total

# Stream 상태 확인 (이벤트별)
curl -s http://localhost:8080/metrics | grep stream_current_block
curl -s http://localhost:8080/metrics | grep stream_success_total
curl -s http://localhost:8080/metrics | grep stream_errors_total

# Receive 상태 확인 (이벤트별)
curl -s http://localhost:8080/metrics | grep receive_current_block
curl -s http://localhost:8080/metrics | grep receive_success_total
curl -s http://localhost:8080/metrics | grep receive_errors_total
```

**최적화 포인트:**
- 채널 에러가 많으면 → 버퍼 크기 조정 또는 처리 로직 개선
- DB 성공률이 낮으면 → 쿼리 최적화 또는 연결 풀 설정 검토
- RPC 응답률이 낮으면 → 프로바이더 변경 또는 재시도 로직 강화
- Stream 성공률이 낮으면 → RPC 안정성 개선 또는 재시도 로직 강화
- Receive 성공률이 낮으면 → 의존성 대기 시간 조정 또는 타임아웃 설정 개선
- 특정 이벤트 타입 에러 집중 → 해당 이벤트 처리 로직 최적화

### 4. 용량 계획

**성장 추세 분석:**
- 시간당 이벤트 처리량: `rate(observer_events_processed_total[1h])`
- 피크 시간대 DB 부하: `max_over_time(observer_db_operations_total[24h])`
- 채널 포화도: `observer_channel_messages_sent_total - observer_channel_messages_received_total`
- 이벤트별 처리 성공률: `observer_stream_success_total / (observer_stream_success_total + observer_stream_errors_total)`
- 이벤트별 수신 성공률: `observer_receive_success_total / (observer_receive_success_total + observer_receive_errors_total)`

**확장 결정 기준:**
- 채널 백로그가 지속적으로 증가 → 워커 프로세스 추가
- DB 응답 시간 증가 → 읽기 전용 복제본 추가
- RPC 요청 실패율 증가 → 프로바이더 풀 확장
- 특정 이벤트 타입 성공률 저조 → 해당 이벤트 처리 로직 개선
- 전반적인 에러율 증가 → 인프라 스케일링 또는 최적화

## 🔍 트러블슈팅

### 메트릭이 수집되지 않을 때

1. **서비스 상태 확인:**
   ```bash
   curl http://localhost:8080/metrics
   # 연결이 안 되면 METRICS_PORT 환경변수 확인
   ```

2. **로그에서 에러 찾기:**
   ```bash
   # 메트릭 시스템 시작 로그 확인
   grep "Unified metrics system started" logs/app.log
   
   # RPC 체크 로그 확인  
   grep "RPC Provider health check started" logs/app.log
   ```

### 메트릭 값이 이상할 때

1. **채널 메트릭 불일치:**
   - 전송 > 수신: 정상 (버퍼에 대기 중)
   - 수신 > 전송: 비정상 (메트릭 버그 가능성)

2. **DB 성공률 0%:**
   - 데이터베이스 연결 확인
   - `measure_query!` 매크로 적용 여부 확인

3. **RPC 상태가 업데이트 안됨:**
   - `PROVIDER_CHECK_INTERVAL` 설정 확인
   - RPC 클라이언트 초기화 상태 확인

이 메트릭 시스템을 통해 Observer 애플리케이션의 전반적인 상태를 실시간으로 파악하고, 문제 발생 시 빠르게 대응할 수 있습니다.