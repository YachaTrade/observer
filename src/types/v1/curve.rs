use bigdecimal::BigDecimal;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TokenMetadata {
    pub image_uri: String,
    pub description: Option<String>,
    pub website: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub is_nsfw: bool,
}

#[derive(Debug, Clone)]
pub struct CreateCurve {
    pub creator: Arc<String>,
    pub token: Arc<String>,
    pub virtual_token: Arc<BigDecimal>,
    pub virtual_native: Arc<BigDecimal>,
    pub token_metadata: TokenMetadata,
    pub name: Arc<String>,
    pub symbol: Arc<String>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub tx_sender: Arc<String>,
}

#[derive(Debug, Clone)]
pub enum MarketType {
    CURVE,
    DEX,
}
#[derive(Debug, Clone)]
pub struct Buy {
    pub sender: Arc<String>,
    pub to: Option<Arc<String>>,
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
    pub to: Option<Arc<String>>,
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
    pub reserve_quote_amount: Arc<BigDecimal>,
    pub reserve_token_amount: Arc<BigDecimal>,
    pub virtual_quote_amount: Arc<BigDecimal>,
    pub virtual_token_amount: Arc<BigDecimal>,
    pub price: Arc<BigDecimal>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: Arc<String>,
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
}

impl CurveEvent {
    pub fn block_number(&self) -> u64 {
        match self {
            Self::Create(event) => event.block_number,
            Self::Buy(event) => event.block_number,
            Self::Sell(event) => event.block_number,
            Self::Graduate(event) => event.block_number,
            Self::Sync(event) => event.block_number,
        }
    }

    pub fn log_index(&self) -> u64 {
        match self {
            Self::Create(event) => event.log_index,
            Self::Buy(event) => event.log_index,
            Self::Sell(event) => event.log_index,
            Self::Graduate(event) => event.log_index,
            Self::Sync(event) => event.log_index,
        }
    }

    pub fn transaction_index(&self) -> u64 {
        match self {
            Self::Create(event) => event.transaction_index,
            Self::Buy(event) => event.transaction_index,
            Self::Sell(event) => event.transaction_index,
            Self::Graduate(event) => event.transaction_index,
            Self::Sync(event) => event.transaction_index,
        }
    }

    pub fn token(&self) -> Option<&str> {
        match self {
            Self::Create(event) => Some(event.token.as_str()),
            Self::Buy(event) => Some(event.token.as_str()),
            Self::Sell(event) => Some(event.token.as_str()),
            Self::Graduate(event) => Some(event.token.as_str()),
            Self::Sync(event) => Some(event.token.as_str()),
        }
    }
}
