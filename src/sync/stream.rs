use std::{collections::HashMap, env};

use anyhow::Result;
use sqlx::Row;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::BLOCK_OFFSET;
use crate::config::quote_configs;
use crate::db::postgres::PostgresDatabase;

use super::{BlockRange, EventType};

// 전역 인스턴스
lazy_static::lazy_static! {
    pub static ref STREAM_MANAGER: StreamManager = StreamManager::new();
}

fn normalize_quote_id(quote_id: &str) -> String {
    quote_id
        .trim_start_matches("0x")
        .trim_start_matches("0X")
        .to_ascii_lowercase()
}

pub(crate) fn select_price_resume_block(
    start_block: u64,
    configured_quotes: &[String],
    rows: &[(String, i64)],
) -> Option<u64> {
    let mut maxima = HashMap::new();
    for (quote_id, max_block) in rows {
        if *max_block >= start_block as i64 {
            maxima.insert(normalize_quote_id(quote_id), *max_block as u64);
        }
    }

    let mut quote_maxima = Vec::with_capacity(configured_quotes.len());
    for quote in configured_quotes {
        let block = maxima.get(&normalize_quote_id(quote))?;
        quote_maxima.push(*block);
    }

    quote_maxima
        .into_iter()
        .min()
        .map(|block| block.saturating_add(1))
}

fn build_initial_stream_ranges(
    default_range: BlockRange,
    price_resume_block: Option<u64>,
) -> HashMap<EventType, BlockRange> {
    let mut ranges = HashMap::new();
    for event_type in EventType::all() {
        ranges.insert(event_type, default_range.clone());
    }

    if let Some(resume_block) = price_resume_block {
        ranges.insert(
            EventType::Price,
            BlockRange {
                from_block: default_range.from_block,
                to_block: resume_block,
            },
        );
    }

    ranges
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamPolicy {
    Curve,
    CurveGated,
    TokenCurveGated,
    Price,
    Independent,
}

fn stream_policy(event_type: EventType) -> StreamPolicy {
    match event_type {
        EventType::Curve => StreamPolicy::Curve,
        EventType::Dex | EventType::LpManager | EventType::Vault => StreamPolicy::CurveGated,
        EventType::Token => StreamPolicy::TokenCurveGated,
        EventType::Price => StreamPolicy::Price,
        EventType::PriceUsd | EventType::VaultRegistry => StreamPolicy::Independent,
    }
}

impl StreamPolicy {
    fn waits_for_curve(self) -> bool {
        matches!(
            self,
            StreamPolicy::CurveGated | StreamPolicy::TokenCurveGated
        )
    }

    fn is_ready(self, from_block: u64, curve_from_block: u64) -> bool {
        !self.waits_for_curve() || from_block < curve_from_block
    }

    fn to_block(
        self,
        from_block: u64,
        block_batch_size: u64,
        latest_block: u64,
        curve_from_block: u64,
        block_offset: u64,
    ) -> u64 {
        match self {
            StreamPolicy::Curve => {
                (from_block + block_batch_size).min(latest_block.saturating_sub(block_offset))
            }
            StreamPolicy::CurveGated => (from_block + block_batch_size).min(latest_block),
            StreamPolicy::TokenCurveGated => (from_block + block_batch_size)
                .min(curve_from_block.saturating_sub(1))
                .min(latest_block.saturating_sub(1)),
            StreamPolicy::Price => {
                const PRICE_CYCLE_BLOCKS: u64 = 1_000;
                (from_block + PRICE_CYCLE_BLOCKS).min(latest_block.saturating_sub(5))
            }
            StreamPolicy::Independent => {
                (from_block + block_batch_size).min(latest_block.saturating_sub(block_offset))
            }
        }
    }
}

// 블록 동기화 관리자
#[derive(Debug)]
pub struct StreamManager {
    // 이벤트별 from_block
    stream_event_block: RwLock<HashMap<EventType, BlockRange>>,
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamManager {
    pub fn new() -> Self {
        let mut stream_event_block = HashMap::new();
        for event_type in EventType::all() {
            stream_event_block.insert(
                event_type,
                BlockRange {
                    from_block: 0,
                    to_block: 0,
                },
            );
        }

        Self {
            stream_event_block: RwLock::new(stream_event_block),
        }
    }

    // 이벤트 블록 가져오기
    pub async fn get_event_block_range(&self, event_type: EventType) -> BlockRange {
        let blocks = self.stream_event_block.read().await;
        blocks.get(&event_type).cloned().unwrap_or(BlockRange {
            from_block: 0,
            to_block: 0,
        })
    }

    // balance_history 테이블에서 가장 높은 블록 번호 가져오기
    async fn get_latest_block_from_history(&self) -> Result<Option<u64>> {
        match PostgresDatabase::instance() {
            Ok(db) => {
                match sqlx::query("SELECT MAX(block_number) as max_block FROM balance_history")
                    .fetch_optional(&db.pool)
                    .await
                {
                    Ok(row) => {
                        if let Some(row) = row {
                            let max_block: Option<i64> = row.try_get("max_block").ok();
                            if let Some(block) = max_block
                                && block > 0
                            {
                                info!(
                                    "[STREAM] Found latest block from balance_history: {}",
                                    block
                                );
                                return Ok(Some(block as u64));
                            }
                        }
                        info!("[STREAM] No blocks found in balance_history table");
                        Ok(None)
                    }
                    Err(e) => {
                        warn!("[STREAM] Error querying balance_history: {}", e);
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                warn!("[STREAM] Failed to get database instance: {}", e);
                Ok(None)
            }
        }
    }

    // 블록 범위 초기화
    pub async fn initialize_block_range(&self) -> Result<()> {
        // 환경 변수에서 기본 블록 범위 가져오기
        let env_start_block = env::var("START_BLOCK")
            .expect("START_BLOCK must be set")
            .parse::<u64>()
            .unwrap();

        let mut start_block = env_start_block;

        // start_block이 0이면 balance_history에서 최신 블록 조회
        if start_block == 0 {
            let latest_history_block = self.get_latest_block_from_history().await?;

            if let Some(history_block) = latest_history_block {
                // 마지막 처리된 블록 + 1부터 시작
                let new_start_block = history_block - 1;
                info!(
                    "[STREAM] Using latest block from balance_history: {} (next block: {})",
                    history_block, new_start_block
                );
                start_block = new_start_block;
            } else {
                panic!("[STREAM] No blocks found in balance_history");
            }
        } else {
            info!("[STREAM] Using configured start_block: {}", start_block);
        }

        let block_range = BlockRange {
            from_block: start_block - 100,
            to_block: start_block - 1,
        };

        let price_resume_block = match PostgresDatabase::instance() {
            Ok(db) => match sqlx::query(
                "SELECT quote_id, MAX(block_number) AS max_block FROM price GROUP BY quote_id",
            )
            .fetch_all(&db.pool)
            .await
            {
                Ok(rows) => {
                    let maxima: Vec<(String, i64)> = rows
                        .into_iter()
                        .filter_map(|row| {
                            Some((
                                row.try_get("quote_id").ok()?,
                                row.try_get("max_block").ok()?,
                            ))
                        })
                        .collect();
                    let configured: Vec<String> = quote_configs()
                        .iter()
                        .map(|quote| quote.address.clone())
                        .collect();
                    select_price_resume_block(start_block, &configured, &maxima)
                }
                Err(error) => {
                    warn!("[STREAM] Failed to load Price watermark: {}", error);
                    None
                }
            },
            Err(error) => {
                warn!(
                    "[STREAM] Failed to access database for Price watermark: {}",
                    error
                );
                None
            }
        };

        let initial_ranges = build_initial_stream_ranges(block_range, price_resume_block);
        *self.stream_event_block.write().await = initial_ranges;

        if let Some(resume_block) = price_resume_block {
            info!(
                "[STREAM] Resuming Price from complete quote watermark: {}",
                resume_block
            );
        }

        info!("[STREAM] Initialized block range - start: {}", start_block);

        Ok(())
    }

    pub async fn get_next_block_range(
        &self,
        event_type: EventType,
        block_batch_size: u64,
        latest_block: u64,
    ) -> BlockRange {
        let processed_range = self.get_event_block_range(event_type).await;

        // 이전에 처리한 블록 다음부터 시작 (to_block + 1)
        let from_block = processed_range.to_block;
        let policy = stream_policy(event_type);

        if policy.waits_for_curve() {
            loop {
                let curve_block = self.get_event_block_range(EventType::Curve).await;
                if policy.is_ready(from_block, curve_block.from_block) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            }
        }

        let curve_from_block = if policy == StreamPolicy::TokenCurveGated {
            self.get_event_block_range(EventType::Curve)
                .await
                .from_block
        } else {
            0
        };
        let block_offset = if matches!(policy, StreamPolicy::Curve | StreamPolicy::Independent) {
            *BLOCK_OFFSET
        } else {
            0
        };
        let to_block = policy.to_block(
            from_block,
            block_batch_size,
            latest_block,
            curve_from_block,
            block_offset,
        );

        BlockRange {
            from_block,
            to_block,
        }
    }

    pub async fn set_event_block_processed_block(
        &self,
        event_type: EventType,
        processed_block: u64,
    ) {
        let from_block = processed_block + 1;
        let mut blocks = self.stream_event_block.write().await;
        let block_range = BlockRange {
            from_block,
            to_block: from_block,
        };
        blocks.insert(event_type, block_range);
    }
}

#[cfg(test)]
mod tests {
    use super::{StreamPolicy, stream_policy};
    use crate::sync::EventType;

    #[test]
    fn curve_applies_the_block_offset_at_the_chain_head() {
        let policy = stream_policy(EventType::Curve);

        assert_eq!(policy, StreamPolicy::Curve);
        assert_eq!(policy.to_block(100, 100, 200, 0, 7), 193);
    }

    #[test]
    fn dex_waits_until_curve_is_ahead() {
        let policy = stream_policy(EventType::Dex);

        assert_eq!(policy, StreamPolicy::CurveGated);
        assert!(!policy.is_ready(100, 100));
        assert!(policy.is_ready(100, 101));
    }

    #[test]
    fn lp_manager_waits_until_curve_is_ahead() {
        let policy = stream_policy(EventType::LpManager);

        assert_eq!(policy, StreamPolicy::CurveGated);
        assert!(!policy.is_ready(100, 100));
        assert!(policy.is_ready(100, 101));
    }

    #[test]
    fn vault_waits_until_curve_is_ahead() {
        let policy = stream_policy(EventType::Vault);

        assert_eq!(policy, StreamPolicy::CurveGated);
        assert!(!policy.is_ready(100, 100));
        assert!(policy.is_ready(100, 101));
    }

    #[test]
    fn token_range_is_capped_by_the_last_curve_block() {
        let policy = stream_policy(EventType::Token);

        assert_eq!(policy, StreamPolicy::TokenCurveGated);
        assert!(!policy.is_ready(100, 100));
        assert!(policy.is_ready(100, 108));
        assert_eq!(policy.to_block(100, 20, 200, 108, 0), 107);
    }

    #[test]
    fn token_range_stays_one_block_behind_the_chain_head() {
        let policy = stream_policy(EventType::Token);

        assert_eq!(policy.to_block(100, 20, 105, 200, 0), 104);
    }

    #[test]
    fn price_range_uses_the_one_thousand_block_cycle_cap() {
        let policy = stream_policy(EventType::Price);

        assert_eq!(policy, StreamPolicy::Price);
        assert_eq!(policy.to_block(100, 20, 5_000, 0, 0), 1_100);
    }

    #[test]
    fn price_range_stays_five_blocks_behind_the_chain_head() {
        let policy = stream_policy(EventType::Price);

        assert_eq!(policy.to_block(100, 20, 600, 0, 0), 595);
    }

    #[test]
    fn vault_registry_uses_an_independent_offset_range() {
        let policy = stream_policy(EventType::VaultRegistry);

        assert_eq!(policy, StreamPolicy::Independent);
        assert!(policy.is_ready(100, 0));
        assert_eq!(policy.to_block(100, 20, 110, 0, 7), 103);
    }

    #[test]
    fn selects_the_only_quote_watermark() {
        let rows = vec![("quote-a".to_string(), 123_i64)];
        let configured = vec!["quote-a".to_string()];
        assert_eq!(
            super::select_price_resume_block(100, &configured, &rows),
            Some(124)
        );
    }

    #[test]
    fn selects_the_minimum_complete_watermark_across_quotes() {
        let rows = vec![
            ("quote-a".to_string(), 220_i64),
            ("quote-b".to_string(), 150_i64),
            ("quote-c".to_string(), 190_i64),
        ];
        let configured = vec![
            "quote-a".to_string(),
            "quote-b".to_string(),
            "quote-c".to_string(),
        ];
        assert_eq!(
            super::select_price_resume_block(100, &configured, &rows),
            Some(151)
        );
    }

    #[test]
    fn matches_quote_ids_case_insensitively() {
        let rows = vec![
            ("0xabc".to_string(), 200_i64),
            ("0XDEF".to_string(), 240_i64),
        ];
        let configured = vec!["0xABC".to_string(), "0xdef".to_string()];
        assert_eq!(
            super::select_price_resume_block(100, &configured, &rows),
            Some(201)
        );
    }

    #[test]
    fn falls_back_when_a_configured_quote_is_missing() {
        let rows = vec![("quote-a".to_string(), 123_i64)];
        let configured = vec!["quote-a".to_string(), "quote-b".to_string()];
        assert_eq!(
            super::select_price_resume_block(100, &configured, &rows),
            None
        );
    }

    #[test]
    fn falls_back_when_the_only_complete_maximum_is_before_start_block() {
        let rows = vec![("quote-a".to_string(), 80_i64)];
        let configured = vec!["quote-a".to_string()];
        assert_eq!(
            super::select_price_resume_block(100, &configured, &rows),
            None
        );
    }

    #[test]
    fn initial_ranges_apply_the_default_to_every_stream_and_override_only_price() {
        let ranges = super::build_initial_stream_ranges(
            crate::sync::BlockRange {
                from_block: 900,
                to_block: 999,
            },
            Some(1_234),
        );

        for event_type in crate::sync::EventType::all() {
            let range = ranges.get(&event_type).unwrap();
            if event_type == crate::sync::EventType::Price {
                assert_eq!((range.from_block, range.to_block), (900, 1_234));
            } else {
                assert_eq!((range.from_block, range.to_block), (900, 999));
            }
        }
    }
}
