use bigdecimal::BigDecimal;
use std::sync::Arc;

use crate::types::v1::curve::{Buy, Sell};

#[derive(Debug, Clone)]
pub enum DexEvent {
    SwapBuy(Buy),
    SwapSell(Sell),
    Sync(DexSync),
    RouterBuy(DexRouterBuy),
    RouterSell(DexRouterSell),
    Mint(DexMint),
    Burn(DexBurn),
    SetFeeProtocol(SetFeeProtocol),
}

impl DexEvent {
    pub fn block_number(&self) -> u64 {
        match self {
            Self::SwapBuy(event) => event.block_number,
            Self::SwapSell(event) => event.block_number,
            Self::Sync(event) => event.block_number,
            Self::RouterBuy(event) => event.block_number,
            Self::RouterSell(event) => event.block_number,
            Self::Mint(event) => event.block_number,
            Self::Burn(event) => event.block_number,
            Self::SetFeeProtocol(event) => event.block_number,
        }
    }

    pub fn log_index(&self) -> u64 {
        match self {
            Self::SwapBuy(event) => event.log_index,
            Self::SwapSell(event) => event.log_index,
            Self::Sync(event) => event.log_index,
            Self::RouterBuy(event) => event.log_index,
            Self::RouterSell(event) => event.log_index,
            Self::Mint(event) => event.log_index,
            Self::Burn(event) => event.log_index,
            Self::SetFeeProtocol(event) => event.log_index,
        }
    }

    pub fn transaction_index(&self) -> u64 {
        match self {
            Self::SwapBuy(event) => event.transaction_index,
            Self::SwapSell(event) => event.transaction_index,
            Self::Sync(event) => event.transaction_index,
            Self::RouterBuy(event) => event.transaction_index,
            Self::RouterSell(event) => event.transaction_index,
            Self::Mint(event) => event.transaction_index,
            Self::Burn(event) => event.transaction_index,
            Self::SetFeeProtocol(event) => event.transaction_index,
        }
    }

    pub fn token(&self) -> Option<&str> {
        match self {
            Self::SwapBuy(event) => Some(event.token.as_str()),
            Self::SwapSell(event) => Some(event.token.as_str()),
            Self::Sync(event) => Some(event.token.as_str()),
            Self::RouterBuy(event) => Some(event.token.as_str()),
            Self::RouterSell(event) => Some(event.token.as_str()),
            Self::Mint(event) => Some(event.token_id.as_str()),
            Self::Burn(event) => Some(event.token_id.as_str()),
            Self::SetFeeProtocol(_) => None, // SetFeeProtocol has no token
        }
    }
}

#[derive(Debug, Clone)]
pub struct DexRouterBuy {
    pub token: Arc<String>,
    pub sender: Arc<String>,
    pub amount_in: Arc<BigDecimal>,
    pub amount_out: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub tx_sender: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct DexRouterSell {
    pub token: Arc<String>,
    pub sender: Arc<String>,
    pub amount_in: Arc<BigDecimal>,
    pub amount_out: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub tx_sender: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct DexSync {
    pub token: Arc<String>,
    pub pool: Arc<String>,
    pub price: Arc<BigDecimal>,
    pub reserve_quote: Arc<BigDecimal>, // Virtual reserve of native token (WETH)
    pub reserve_token: Arc<BigDecimal>,  // Virtual reserve of token
    pub transaction_hash: Arc<String>,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct DexMint {
    pub token_id: Arc<String>,
    pub account_id: Arc<String>,
    pub market_id: Arc<String>,
    pub quote_amount: Arc<BigDecimal>,  // amount1 (native token)
    pub token_amount: Arc<BigDecimal>,   // amount0 (token)
    pub liquidity: Arc<BigDecimal>,      // amount (liquidity)
    pub reserve_quote: Arc<BigDecimal>, // Virtual reserve of native token at mint time
    pub reserve_token: Arc<BigDecimal>,  // Virtual reserve of token at mint time
    pub transaction_hash: Arc<String>,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct DexBurn {
    pub token_id: Arc<String>,
    pub account_id: Arc<String>,
    pub market_id: Arc<String>,
    pub quote_amount: Arc<BigDecimal>,  // amount1 (native token)
    pub token_amount: Arc<BigDecimal>,   // amount0 (token)
    pub liquidity: Arc<BigDecimal>,      // amount (liquidity)
    pub reserve_quote: Arc<BigDecimal>, // Virtual reserve of native token at burn time
    pub reserve_token: Arc<BigDecimal>,  // Virtual reserve of token at burn time
    pub transaction_hash: Arc<String>,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

impl From<Buy> for DexEvent {
    fn from(value: Buy) -> Self {
        DexEvent::SwapBuy(value)
    }
}

impl From<Sell> for DexEvent {
    fn from(value: Sell) -> Self {
        DexEvent::SwapSell(value)
    }
}

impl From<DexSync> for DexEvent {
    fn from(value: DexSync) -> Self {
        DexEvent::Sync(value)
    }
}

impl From<DexRouterBuy> for DexEvent {
    fn from(value: DexRouterBuy) -> Self {
        DexEvent::RouterBuy(value)
    }
}

impl From<DexRouterSell> for DexEvent {
    fn from(value: DexRouterSell) -> Self {
        DexEvent::RouterSell(value)
    }
}

impl From<DexMint> for DexEvent {
    fn from(value: DexMint) -> Self {
        DexEvent::Mint(value)
    }
}

impl From<DexBurn> for DexEvent {
    fn from(value: DexBurn) -> Self {
        DexEvent::Burn(value)
    }
}

#[derive(Debug, Clone)]
pub struct SetFeeProtocol {
    pub pool_id: Arc<String>,
    pub fee_protocol0_old: u8,
    pub fee_protocol1_old: u8,
    pub fee_protocol0_new: u8,
    pub fee_protocol1_new: u8,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
}

impl From<SetFeeProtocol> for DexEvent {
    fn from(value: SetFeeProtocol) -> Self {
        DexEvent::SetFeeProtocol(value)
    }
}
