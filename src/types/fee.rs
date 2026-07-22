use bigdecimal::BigDecimal;
use std::sync::Arc;

/// Indexed fee type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeeType {
    Create,
    CurveBuy,
    CurveSell,
    SwapBuy,
    SwapSell,
    DexRouterBuy,
    DexRouterSell,
}

impl FeeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeeType::Create => "create",
            FeeType::CurveBuy => "curve_buy",
            FeeType::CurveSell => "curve_sell",
            FeeType::SwapBuy => "swap_buy",
            FeeType::SwapSell => "swap_sell",
            FeeType::DexRouterBuy => "dex_router_buy",
            FeeType::DexRouterSell => "dex_router_sell",
        }
    }
}

/// Indexed fee history event.
#[derive(Debug, Clone)]
pub struct FeeHistoryEvent {
    pub transaction_hash: Arc<String>,
    pub log_index: u64,
    pub tx_index: u64,
    pub account_id: Arc<String>,
    pub token_id: Arc<String>,
    pub quote_amount: Arc<BigDecimal>,
    pub usd_amount: Arc<BigDecimal>,
    pub fee_type: FeeType,
    pub block_number: u64,
    pub block_timestamp: u64,
}
