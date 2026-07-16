use std::sync::atomic::{AtomicU64, Ordering};

/// 리스팅 관련 메트릭
pub struct GraduateMetrics {
    pub lock_count: AtomicU64,     // 본딩 도달 수
    pub graduate_count: AtomicU64, // 상장 수
}

impl Default for GraduateMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl GraduateMetrics {
    pub fn new() -> Self {
        Self {
            lock_count: AtomicU64::new(0),
            graduate_count: AtomicU64::new(0),
        }
    }

    /// Lock 이벤트 카운트 증가 (본딩 도달)
    pub fn increment_lock_count(&self) {
        self.lock_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Graduate 이벤트 카운트 증가 (상장)
    pub fn increment_graduate_count(&self) {
        self.graduate_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 리스팅 메트릭 값들 반환: (lock_count, Graduate_count, difference)
    pub fn get_values(&self) -> (u64, u64, i64) {
        let lock_count = self.lock_count.load(Ordering::Relaxed);
        let graduate_count = self.graduate_count.load(Ordering::Relaxed);
        let difference = lock_count as i64 - graduate_count as i64;

        (lock_count, graduate_count, difference)
    }

    /// 리스팅 비율 계산 (Graduate/lock)
    pub fn get_graduate_ratio(&self) -> f64 {
        let lock_count = self.lock_count.load(Ordering::Relaxed);
        let graduate_count = self.graduate_count.load(Ordering::Relaxed);

        if lock_count > 0 {
            graduate_count as f64 / lock_count as f64
        } else {
            0.0
        }
    }
}
