# RPC Provider 스마트 Fallback 시스템 (v2.2)

## 개요
새롭게 설계된 100점 만점 스코어링 시스템과 스마트 WebSocket 블록 업데이터를 통해 인덱스 우선순위(Main → Sub1 → Sub2)를 보장하면서 실패에 엄격하게 대응하는 자동 장애 복구 시스템입니다.

## 시스템 구조

### Provider 우선순위 계층
```
Main Provider (P0)    - 최대 100점 (절대 우선)
Sub1 Provider (P1)    - 최대 90점
Sub2 Provider (P2)    - 최대 80점
```

### ProviderConfig 구조체 (v2.1)
```rust
pub struct ProviderConfig {
    pub url: String,              // RPC 엔드포인트 URL
    pub name: String,             // "Main", "Sub1", "Sub2"
    pub score: f32,               // 기본 점수 (초기 100.0)
    pub success_count: u32,       // 성공 횟수
    pub fail_count: u32,          // 실패 횟수
    pub last_used: Option<Instant>, // 마지막 사용 시간
    pub priority: usize,          // 우선순위 (0=Main, 1=Sub1, 2=Sub2)
}
```

## 100점 만점 스코어링 시스템

### 점수 구성 공식 (총 100점)
```rust
최종점수 = 성능점수(0-70점) + 우선순위보너스(0-30점) - 실패페널티(0-70점)
최종점수 = (base_score × success_rate × 0.7) + priority_bonus - failure_penalty
```

### 우선순위 보너스 (인덱스 기반)
```rust
let priority_bonus = match self.priority {
    0 => 30.0,  // Main: +30점 → 최대 100점
    1 => 20.0,  // Sub1: +20점 → 최대 90점
    2 => 10.0,  // Sub2: +10점 → 최대 80점
    _ => 0.0,   // 기타: +0점 → 최대 70점
};
```

### 엄격한 실패 페널티
```rust
let failure_penalty = match self.fail_count {
    1..=2 => 15.0,    // 1-2회 실패: -15점
    3..=5 => 30.0,    // 3-5회 실패: -30점  
    6..=10 => 50.0,   // 6-10회 실패: -50점
    _ => 70.0,        // 11회+ 실패: -70점
}
```

## 실패 처리 메커니즘

### 점수 변동 규칙
**성공 시:**
```rust
self.success_count += 1;

// 성공할 때마다 실패 카운트를 줄여서 회복 가능하게 함 (NEW v2.2)
if self.fail_count > 0 {
    // 성공할 때마다 실패 카운트 1개 감소 (1:1 비율)
    self.fail_count = self.fail_count.saturating_sub(1);
}

self.score = (self.score + 2.0).min(100.0); // +2점 (빠른 회복)
```

**실패 시:**
```rust
self.fail_count += 1;
let penalty = match self.fail_count {
    1..=2 => 10.0,    // 초기 실패: -10점
    3..=5 => 20.0,    // 반복 실패: -20점
    _ => 30.0,        // 연속 실패: -30점
};
self.score = (self.score - penalty).max(5.0); // 최소 5점 유지
```

### 자동 Provider 교체 조건
```rust
// 점수가 30 이하이거나 연속 실패가 3회 이상인 경우 즉시 교체
should_replace = score <= 30.0 || config.fail_count >= 3
```

## 스마트 WebSocket 블록 업데이터

### 동적 Provider 전환
```rust
// 30블록마다 더 나은 provider 체크
if blocks_received % 30 == 0 {
    let new_best = select_best_provider_index().await;
    if new_best != current_provider_index {
        info!("[HealthCheck] 🔄 Better provider[{}] available, switching...", new_best);
        break; // 현재 스트림 종료하고 새 provider로 전환
    }
}
```

### 장애 감지 및 복구
- **연속 실패 감지**: 2번 연속 실패 시 다음 provider로 자동 전환
- **빠른 복구**: Main이 복구되면 30블록 내 자동 감지하여 재전환
- **무중단 서비스**: WebSocket 연결 끊김 없이 provider 교체

## 실제 동작 시나리오

### 시나리오 1: 정상 상황
```
점수 상황:
- Main (P0): 70 + 30 - 0 = 100점 🥇
- Sub1 (P1): 70 + 20 - 0 = 90점
- Sub2 (P2): 70 + 10 - 0 = 80점

→ Main 선택, WebSocket도 Main 사용
```

### 시나리오 2: Main 3번 실패
```
Main 실패 후 점수:
- Main (P0): 70 + 30 - 30 = 70점
- Sub1 (P1): 70 + 20 - 0 = 90점 🥇
- Sub2 (P2): 70 + 10 - 0 = 80점

동작:
1. Sub1으로 즉시 전환 (90점 > 70점)
2. Main provider 교체 시도 (fail_count >= 3)
3. WebSocket도 Sub1으로 자동 전환
```

### 시나리오 3: Main 복구
```
Main 교체 후 점수 리셋:
- Main (P0): 70 + 30 - 0 = 100점 🥇 (점수 초기화)
- Sub1 (P1): 70 + 20 - 0 = 90점
- Sub2 (P2): 70 + 10 - 0 = 80점

동작:
1. Main이 다시 최고 점수
2. 다음 요청부터 Main 사용
3. WebSocket도 30블록 내 Main으로 복귀
```

## 헬스체크 시스템

### 스마트 라이브 체인 검증 (NEW v2.2)
```rust
// 블록 2번 체크해서 라이브인지 확인
if let Ok(Ok(block1)) = provider.get_block_number().await {
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    if let Ok(Ok(block2)) = provider.get_block_number().await {
        match block2 > block1 {
            true => {
                // 블록 증가 = 라이브 체인
                self.record_provider_success(i).await;
                info!("[HealthCheck] ✅ Provider[{}] {} - block: {} [LIVE]", i, name, block2);
            }
            false => {
                // 블록 같음 = 정지된 체인
                self.record_provider_failure(i).await;
                warn!("[HealthCheck] ⚠️ Provider[{}] {} - block: {} [STALE]", i, name, block1);
                self.try_replace_failed_provider(i).await;
            }
        }
    }
}
```

### 주기적 모니터링 (1분마다)
- **[LIVE]**: 2초 간격으로 블록이 증가하는 실시간 체인
- **[STALE]**: 블록이 정지되어 오래된 데이터를 제공하는 체인
- 단순한 연결 체크가 아닌 **실제 체인 진행 상태** 확인

### 최신 스코어 기반 최적 프로바이더 선택 (NEW v2.2)
```rust
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

info!("[HealthCheck] 🎯 Selected best provider: [{}] {} (Score: {:.2})", 
      best_index, best_name, best_score);
```

### 점수 모니터링 로그 (5분마다)
```
[HealthCheck] 📊 Provider Score Summary:
📈 [0] Main (P0) - Score: 100.00/100 | Success: 45 | Fail: 0 | Rate: 100.0% | Total: 45
📈 [1] Sub1 (P1) - Score: 67.98/100 | Success: 12 | Fail: 1 | Rate: 92.3% | Total: 13  
📈 [2] Sub2 (P2) - Score: 80.00/100 | Success: 8 | Fail: 2 | Rate: 80.0% | Total: 10
🎯 Selected best provider: [0] Main (Score: 100.00)
```

## TxBot 통합 시스템

### 이중 클라이언트 구조
```
ReadOnlyClient    - Observer와 동일한 100점 스코어링 시스템 적용
WalletClient      - 트랜잭션 전용, 지갑별 RPC 연결 관리
```

### 지갑 다중화 Fallback
```rust
// 자금 부족 시 모든 지갑을 시도하는 트랜잭션 실행
execute_transaction_with_wallet_fallback:
1. 첫 번째 지갑으로 트랜잭션 시도
2. InsufficientFunds 에러 시 다음 지갑으로 순차 시도
3. 다른 에러는 즉시 반환 (자금 문제가 아님)
4. 모든 지갑에서 자금 부족 시 최종 실패
```

### WalletClient Provider 생성
```rust
async fn create_provider(&self) -> Result<DynProvider> {
    let rpc_index = *self.current_index.lock().await;
    let rpc_url = self.config.rpc_urls.get(rpc_index)?;
    let wallet = self.config.wallets.get(self.wallet_index)?;
    
    // WSS → HTTPS 자동 변환 (트랜잭션용)
    let http_url = if rpc_url.starts_with("wss://") {
        rpc_url.replace("wss://", "https://")
    } else {
        rpc_url.clone()
    };
    
    let provider = ProviderBuilder::new()
        .wallet(wallet.wallet.clone())
        .connect_http(url);
        
    Ok(DynProvider::new(provider))
}
```

## Overflow 방지 시스템

### 카운터 리셋
```rust
// 카운터가 u32::MAX 근접 시 비율 유지하며 1/10 스케일로 축소
fn reset_counts_with_ratio(&mut self) {
    let scale_factor = 10;
    self.success_count = (self.success_count / scale_factor).max(1);
    self.fail_count = (self.fail_count / scale_factor).max(1);
    warn!("[HealthCheck] ⚠️ Provider {} counts reset to prevent overflow", self.name);
}
```

## 주요 개선사항 (v2.1 → v2.2)

### 1. 자동 회복 시스템 강화 (NEW)
**성공시 실패 카운트 감소:**
- 성공할 때마다 `fail_count` 1개씩 감소 (1:1 비율)
- 과도한 실패 페널티로 영구히 점수가 0인 문제 해결
- Main 프로바이더도 지속적 성공 시 점진적 회복 가능

### 2. 스마트 라이브 체인 검증 (NEW)
**2초 간격 블록 체크:**
- 첫 번째 블록 번호 → 2초 대기 → 두 번째 블록 번호 비교
- 블록 증가: `[LIVE]` 성공 처리
- 블록 정지: `[STALE]` 실패 처리 + 프로바이더 교체
- 단순 연결이 아닌 **실제 체인 진행 상태** 확인

### 3. 실시간 최적 프로바이더 선택 (NEW)
**헬스체크 완료 후 즉시 재계산:**
- 기존: 헬스체크 전 점수로 잘못된 선택
- 개선: 모든 헬스체크 완료 후 최신 점수로 정확한 선택
- 원자적 연산으로 일관성 보장

### 4. 로깅 정확도 향상 (NEW)
**상태별 명확한 표시:**
```
✅ Provider[0] Main - block: 35511550 [LIVE] | Score: 0.00 → 30.00
⚠️ Provider[1] Sub1 - block: 35611504 [STALE] | Score: 90.00 → 67.98
🎯 Selected best provider: [2] Sub2 (Score: 80.00)
```

## 이전 개선사항 (v2.0 → v2.1)

### 1. TxBot 완전 통합
**추가사항:**
- ReadOnlyClient에 Observer와 동일한 100점 시스템 적용
- WalletClient 트랜잭션 전용 최적화
- 지갑별 자금 부족 시 자동 순차 시도

### 2. 로깅 표준화
**통합 형식:**
```
[HealthCheck] 🔧 Successfully created fresh provider: wss://...
[HealthCheck] ✅ Provider[0] healthy: block 12345
[HealthCheck] ❌ Provider[1] failed: timeout
[HealthCheck] 🔄 Better provider[1] available, switching...
[HealthCheck] 📊 Provider Scores: (5분마다)
```

### 3. 헬스체크 주기 최적화
**이전:** 60초마다 헬스체크
**현재:** 1분마다 헬스체크 (Duration::from_secs(60))

### 4. 자동 URL 변환
**추가된 기능:**
- WSS → HTTPS 자동 변환 (트랜잭션용)
- WS → HTTP 자동 변환 (트랜잭션용)
- WebSocket과 HTTP 동시 지원

## 실제 사용 예시

### Observer에서 사용
```rust
// 자동으로 최고 점수 provider 선택
let client = RpcClient::instance()?;
let provider = client.get_current_provider().await;
let logs = provider.get_logs(filter).await?;
```

### TxBot에서 사용
```rust
// 읽기 전용 작업
let read_client = RpcManager::get_read_client().await?;
let block_number = read_client.get_latest_block_number().await?;

// 트랜잭션 작업 (자금 부족 시 자동 지갑 전환)
let result = RpcManager::execute_transaction_with_wallet_fallback(|wallet_client| {
    Box::pin(async move {
        wallet_client.execute_transaction(|provider| {
            Box::pin(async move {
                // 트랜잭션 로직
                Ok(receipt)
            })
        }).await
    })
}).await?;
```

## 장점

1. **직관성**: 0-100점 스케일로 이해하기 쉬움
2. **안정성**: Main 우선 + 엄격한 실패 처리
3. **신속성**: 실패 3회 시 즉시 교체 + 빠른 복구
4. **회복성**: 성공시 자동 실패 카운트 감소로 점진적 회복 (NEW v2.2)
5. **정확성**: 실제 체인 진행 상태 확인으로 정확한 라이브 감지 (NEW v2.2)
6. **실시간성**: 헬스체크 완료 즉시 최신 점수로 최적 프로바이더 선택 (NEW v2.2)
7. **지능성**: WebSocket도 점수 기반 동적 전환
8. **무중단**: Provider 교체 시에도 서비스 연속성 보장
9. **확장성**: Overflow 방지로 장기간 안정 운영
10. **통합성**: Observer + TxBot 완전 통합 운영
11. **유연성**: 지갑 다중화로 자금 관리 최적화

## 모니터링 로그 예시

### WebSocket 전환 로그
```
[HealthCheck] 🔧 Starting smart block stream with provider switching
[HealthCheck] 🔗 Connected to provider[0] for block stream
[HealthCheck] 📦 Block updated: 12345 → 12346 (Provider[0])
[HealthCheck] 🔄 Better provider[1] available, switching...
[HealthCheck] 🔗 Connected to provider[1] for block stream
[HealthCheck] 📦 Block updated: 12346 → 12347 (Provider[1])
```

### Provider 교체 로그
```
[HealthCheck] 🔄 Attempting to replace failed provider[0]: wss://main-rpc.com
[HealthCheck] ✅ Successfully replaced provider[0]: wss://main-rpc.com
```

### 스마트 라이브 체인 검증 로그 (NEW v2.2)
```
[HealthCheck] ✅ Provider[0] Main - block: 35511550 [LIVE] | Score: 0.00 → 30.00
[HealthCheck] ⚠️ Provider[1] Sub1 - block: 35611504 [STALE] | Score: 90.00 → 67.98 (fails: 1)
[HealthCheck] ✅ Provider[2] Sub2 - block: 35525870 [LIVE] | Score: 80.00 → 80.00
[HealthCheck] 🎯 Selected best provider: [2] Sub2 (Score: 80.00)
```

### 실패 처리 로그
```
[HealthCheck] ❌ Provider[0] Main - second query failed | Score: 100.00 → 85.00 (fails: 1)
[HealthCheck] ❌ Failed to replace provider[0] Main: timeout
[HealthCheck] ✅ Provider[1] Sub1 - block: 12347 [LIVE] | Score: 85.00 → 87.00
```

### TxBot 지갑 Fallback 로그
```
[Wallet] 💰 Using provider: RPC[0] Wallet[1] (0x123...) Balance: 0.5 ETH
[Wallet] ❌ Wallet[1] has insufficient funds, trying wallets sequentially
[Wallet] 🔄 Trying wallet[2] after wallet[1] had insufficient funds
[Wallet] ✅ Transaction successful with wallet[2] RPC[0]
```