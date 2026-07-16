use crate::types::v1::curve::{Buy, CreateCurve, Sell};
use bigdecimal::BigDecimal;

#[derive(Debug, Clone)]
pub struct ChartEvent {
    pub token_id: String,
    pub volume: BigDecimal,
    pub price: BigDecimal,
    pub block_number: i64,
    pub block_timestamp: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
}

impl ChartEvent {
    // CreateCurve에서 변환하는 정적 메서드
    pub fn from_create_curve(event: &CreateCurve) -> Self {
        let price = (*event.virtual_native).clone() / (*event.virtual_token).clone();
        Self {
            token_id: (*event.token).clone(),
            volume: BigDecimal::from(0), // 생성 이벤트는 volume이 0
            price,
            block_number: event.block_number as i64,
            block_timestamp: event.block_timestamp as i64,
            transaction_hash: (*event.transaction_hash).clone(),
            transaction_index: event.transaction_index as i64,
            log_index: event.log_index as i64,
        }
    }

    // Buy에서 변환하는 정적 메서드
    pub fn from_buy(event: &Buy, price: BigDecimal) -> Self {
        Self {
            token_id: (*event.token).clone(),
            volume: (*event.amount_in).clone(), // 구매시 volume은 amount_in (네이티브 토큰)
            price,
            block_number: event.block_number as i64,
            block_timestamp: event.block_timestamp as i64,
            transaction_hash: (*event.transaction_hash).clone(),
            transaction_index: event.transaction_index as i64,
            log_index: event.log_index as i64,
        }
    }

    // Sell에서 변환하는 정적 메서드
    pub fn from_sell(event: &Sell, price: BigDecimal) -> Self {
        Self {
            token_id: (*event.token).clone(),
            volume: (*event.amount_out).clone(), // 판매시 volume은 amount_out (네이티브 토큰)
            price,
            block_number: event.block_number as i64,
            block_timestamp: event.block_timestamp as i64,
            transaction_hash: (*event.transaction_hash).clone(),
            transaction_index: event.transaction_index as i64,
            log_index: event.log_index as i64,
        }
    }
}
