use tracing::warn;

// 새로운 심플한 Scoring System
#[derive(Debug)]
pub struct ProviderConfig {
    pub url: String,
    pub name: String,
    pub score: f32,
    pub success_count: u32,
    pub fail_count: u32,
    pub last_used: Option<std::time::Instant>,
    pub priority: usize, // 0=Main(최우선), 1=Sub1, 2=Sub2, ...
}

impl ProviderConfig {
    pub fn new(url: String, name: String, priority: usize) -> Self {
        Self {
            url,
            name,
            score: 100.0, // 시작 점수
            success_count: 0,
            fail_count: 0,
            last_used: None,
            priority,
        }
    }

    // 100점 만점 스코어링 시스템
    pub fn calculate_current_score(&self) -> f32 {
        let total_attempts = self.success_count + self.fail_count;

        // 기본 성능 점수 (0-70점)
        let performance_score = if total_attempts == 0 {
            self.score * 0.7 // 초기에는 70점 만점
        } else {
            let success_rate = self.success_count as f32 / total_attempts as f32;
            self.score * success_rate * 0.7 // 성공률 반영하여 최대 70점
        };

        // 우선순위 보너스 (인덱스 낮을수록 높은 점수)
        let priority_bonus = match self.priority {
            0 => 30.0, // Main: +30점 (총 100점 가능)
            1 => 20.0, // Sub1: +20점 (총 90점 가능)
            2 => 10.0, // Sub2: +10점 (총 80점 가능)
            _ => 0.0,  // 기타: +0점 (총 70점 가능)
        };

        // 실패 페널티 (엄격하게)
        let failure_penalty = if self.fail_count > 0 {
            match self.fail_count {
                1..=2 => 15.0,  // 1-2회 실패: -15점
                3..=5 => 30.0,  // 3-5회 실패: -30점
                6..=10 => 50.0, // 6-10회 실패: -50점
                _ => 70.0,      // 11회+ 실패: -70점
            }
        } else {
            0.0
        };

        // 최종 점수 (0-100점)
        (performance_score + priority_bonus - failure_penalty).clamp(0.0, 100.0)
    }

    // 성공 기록 (overflow 방지)
    pub fn record_success(&mut self) {
        // Overflow 방지: 카운트가 너무 크면 비율을 유지하며 리셋
        if self.success_count > u32::MAX - 1000 || self.fail_count > u32::MAX - 1000 {
            self.reset_counts_with_ratio();
        }
        self.success_count = self.success_count.saturating_add(1);

        // 성공할 때마다 실패 카운트를 줄여서 회복 가능하게 함
        if self.fail_count > 0 {
            // 성공할 때마다 실패 카운트 1개 감소 (1:1 비율)
            self.fail_count = self.fail_count.saturating_sub(1);
        }

        // 성공 시 회복 속도를 실패 시 감소 속도와 비슷하게 조정 (5.0 -> 10회 성공으로 실패 1회 회복)
        self.score = (self.score + 5.0).min(100.0);
        self.last_used = Some(std::time::Instant::now());
    }

    // 실패 기록 (overflow 방지)
    pub fn record_failure(&mut self) {
        // Overflow 방지: 카운트가 너무 크면 비율을 유지하며 리셋
        if self.success_count > u32::MAX - 1000 || self.fail_count > u32::MAX - 1000 {
            self.reset_counts_with_ratio();
        }
        self.fail_count = self.fail_count.saturating_add(1);
        // 실패 시 더 엄격한 점수 감소
        let penalty = match self.fail_count {
            1..=2 => 10.0, // 초기 실패: -10점
            3..=5 => 20.0, // 반복 실패: -20점
            _ => 30.0,     // 연속 실패: -30점
        };
        self.score = (self.score - penalty).max(0.0); // 최소 0점 (calculate_current_score와 일관성)
        self.last_used = Some(std::time::Instant::now());
    }

    // 비율을 유지하면서 카운트 리셋 (overflow 방지)
    fn reset_counts_with_ratio(&mut self) {
        let total = self.success_count.saturating_add(self.fail_count);
        if total > 0 {
            // 1/10 스케일로 축소
            let scale_factor = 10;
            let new_success = self.success_count / scale_factor;
            let new_fail = self.fail_count / scale_factor;

            // 최소값 보장: 둘 다 0이면 비율 유지를 위해 원래 비율대로 최소값 설정
            if new_success == 0 && new_fail == 0 {
                // 원래 비율대로 최소값 할당 (총 10으로 스케일)
                let ratio = self.success_count as f32 / total as f32;
                self.success_count = (ratio * 10.0).round() as u32;
                self.fail_count = 10 - self.success_count;
            } else {
                // 최소 1로 보장
                self.success_count = new_success.max(1);
                self.fail_count = new_fail.max(1);
            }

            warn!(
                "Provider {} counts reset to prevent overflow: success={}, fail={}",
                self.name, self.success_count, self.fail_count
            );
        }
    }
}
