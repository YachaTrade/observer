use bigdecimal::BigDecimal;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Allocate {
    pub token: Arc<String>,
    pub pool: Arc<String>,
    pub quote_amount: Arc<BigDecimal>,
    pub token_amount: Arc<BigDecimal>,
    pub last_collect_time: u64,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct Collect {
    pub token: Arc<String>,
    pub pool: Arc<String>,
    pub quote_amount: Arc<BigDecimal>,
    pub token_amount: Arc<BigDecimal>,
    pub last_collect_time: u64,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub enum LpManagerEvent {
    Allocate(Allocate),
    Collect(Collect),
}

impl LpManagerEvent {
    pub fn block_number(&self) -> u64 {
        match self {
            Self::Allocate(event) => event.block_number,
            Self::Collect(event) => event.block_number,
        }
    }

    pub fn log_index(&self) -> u64 {
        match self {
            Self::Allocate(event) => event.log_index,
            Self::Collect(event) => event.log_index,
        }
    }

    pub fn transaction_index(&self) -> u64 {
        match self {
            Self::Allocate(event) => event.transaction_index,
            Self::Collect(event) => event.transaction_index,
        }
    }

    pub fn token(&self) -> &str {
        match self {
            Self::Allocate(event) => event.token.as_str(),
            Self::Collect(event) => event.token.as_str(),
        }
    }
}
