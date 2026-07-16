use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct UpdatePrice {
    pub quote_id: String,
    pub block_number: u64,
    pub price: BigDecimal,
    pub block_timestamp: u64,
}

// ---------------Pyth Response-------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceFeedResponse {
    pub binary: BinaryData,
    pub parsed: Vec<ParsedPrice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryData {
    pub encoding: String,
    pub data: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedPrice {
    pub id: String,
    pub price: PriceData,
    pub ema_price: PriceData,
    pub metadata: PriceMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceData {
    pub price: String,
    pub conf: String,
    pub expo: i8,
    pub publish_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceMetadata {
    pub slot: i64,
    pub proof_available_time: i64,
    pub prev_publish_time: i64,
}
