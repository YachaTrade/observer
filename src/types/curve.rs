use bigdecimal::BigDecimal;
use std::sync::Arc;

use crate::types::metadata::TokenMetadata;

#[derive(Debug, Clone)]
pub enum MarketType {
    Curve,
    Dex,
}

#[derive(Debug, Clone)]
pub struct CreateCurve {
    pub creator: Arc<String>,
    pub token: Arc<String>,
    pub pair: Arc<String>,
    pub quote_id: Arc<String>,
    pub name: Arc<String>,
    pub symbol: Arc<String>,
    pub token_uri: Arc<String>,
    pub token_metadata: TokenMetadata,
    pub virtual_quote_reserve: Arc<BigDecimal>,
    pub virtual_token_reserve: Arc<BigDecimal>,
    pub min_token_reserve: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub tx_sender: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct Buy {
    pub sender: Arc<String>,
    pub amount_in: Arc<BigDecimal>,
    pub amount_out: Arc<BigDecimal>,
    pub token: Arc<String>,
    pub market: Arc<String>,
    pub market_type: MarketType,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub tx_sender: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct Sell {
    pub sender: Arc<String>,
    pub amount_in: Arc<BigDecimal>,
    pub amount_out: Arc<BigDecimal>,
    pub token: Arc<String>,
    pub market: Arc<String>,
    pub market_type: MarketType,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub tx_sender: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct Graduate {
    pub token: Arc<String>,
    pub pool: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: Arc<String>,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct CurveSync {
    pub token: Arc<String>,
    pub real_quote_reserve: Arc<BigDecimal>,
    pub real_token_reserve: Arc<BigDecimal>,
    pub virtual_quote_reserve: Arc<BigDecimal>,
    pub virtual_token_reserve: Arc<BigDecimal>,
    pub price: Arc<BigDecimal>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: Arc<String>,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct SnipingPenalty {
    pub token: Arc<String>,
    pub buyer: Arc<String>,
    pub sniping_fee: Arc<BigDecimal>,
    pub penalty_bps: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub enum CurveEvent {
    Create(CreateCurve),
    Buy(Buy),
    Sell(Sell),
    Graduate(Graduate),
    Sync(CurveSync),
    SnipingPenalty(SnipingPenalty),
}

impl CurveEvent {
    pub fn block_number(&self) -> u64 {
        match self {
            Self::Create(e) => e.block_number,
            Self::Buy(e) => e.block_number,
            Self::Sell(e) => e.block_number,
            Self::Graduate(e) => e.block_number,
            Self::Sync(e) => e.block_number,
            Self::SnipingPenalty(e) => e.block_number,
        }
    }

    pub fn log_index(&self) -> u64 {
        match self {
            Self::Create(e) => e.log_index,
            Self::Buy(e) => e.log_index,
            Self::Sell(e) => e.log_index,
            Self::Graduate(e) => e.log_index,
            Self::Sync(e) => e.log_index,
            Self::SnipingPenalty(e) => e.log_index,
        }
    }

    pub fn transaction_index(&self) -> u64 {
        match self {
            Self::Create(e) => e.transaction_index,
            Self::Buy(e) => e.transaction_index,
            Self::Sell(e) => e.transaction_index,
            Self::Graduate(e) => e.transaction_index,
            Self::Sync(e) => e.transaction_index,
            Self::SnipingPenalty(e) => e.transaction_index,
        }
    }

    pub fn token(&self) -> Option<&str> {
        match self {
            Self::Create(e) => Some(e.token.as_str()),
            Self::Buy(e) => Some(e.token.as_str()),
            Self::Sell(e) => Some(e.token.as_str()),
            Self::Graduate(e) => Some(e.token.as_str()),
            Self::Sync(e) => Some(e.token.as_str()),
            Self::SnipingPenalty(e) => Some(e.token.as_str()),
        }
    }
}
