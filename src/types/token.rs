use anyhow::Result;
use bigdecimal::BigDecimal;
use std::sync::Arc;

// 새로운 구조체: 실제 잔액을 담는 용도
#[derive(Debug, Clone)]
pub struct TokenBalance {
    pub account_id: Arc<String>,
    pub token: Arc<String>,
    pub balance: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

impl TokenBalance {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_id: Arc<String>,
        token: Arc<String>,
        balance: Arc<BigDecimal>,
        block_timestamp: u64,
        block_number: u64,
        transaction_hash: Arc<String>,
        log_index: u64,
        transaction_index: u64,
    ) -> Result<Self> {
        Ok(TokenBalance {
            account_id,
            token,
            balance,
            transaction_hash,
            block_timestamp,
            block_number,
            log_index,
            transaction_index,
        })
    }
}

#[derive(Debug, Clone)]
pub enum TokenEvent {
    Balance(TokenBalance),
    Burn(TokenBurn),
    Transfer(TokenTransfer),
    PositionHistory(PositionHistoryEvent),
}

impl TokenEvent {
    pub fn token_address(&self) -> &str {
        match self {
            Self::Balance(event) => event.token.as_str(),
            Self::Burn(event) => event.token.as_str(),
            Self::Transfer(event) => event.token.as_str(),
            Self::PositionHistory(event) => event.token_id.as_str(),
        }
    }

    pub fn account_address(&self) -> &str {
        match self {
            Self::Balance(event) => event.account_id.as_str(),
            Self::Burn(event) => event.from.as_str(),
            Self::Transfer(event) => event.from_address.as_str(),
            Self::PositionHistory(event) => event.account_id.as_str(),
        }
    }

    pub fn block_number(&self) -> u64 {
        match self {
            Self::Balance(event) => event.block_number,
            Self::Burn(event) => event.block_number,
            Self::Transfer(event) => event.block_number,
            Self::PositionHistory(event) => event.block_number,
        }
    }

    pub fn log_index(&self) -> u64 {
        match self {
            Self::Balance(event) => event.log_index,
            Self::Burn(event) => event.log_index,
            Self::Transfer(event) => event.log_index,
            Self::PositionHistory(event) => event.log_index,
        }
    }

    pub fn transaction_index(&self) -> u64 {
        match self {
            Self::Balance(event) => event.transaction_index,
            Self::Burn(event) => event.transaction_index,
            Self::Transfer(event) => event.tx_index,
            Self::PositionHistory(event) => event.tx_index,
        }
    }

    pub fn timestamp(&self) -> u64 {
        match self {
            Self::Balance(event) => event.block_timestamp,
            Self::Burn(event) => event.block_timestamp,
            Self::Transfer(event) => event.block_timestamp,
            Self::PositionHistory(event) => event.block_timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TokenMetadata {
    pub dev_address: String,
    pub curve_address: String,
    pub dex_address: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TokenBurn {
    pub from: Arc<String>,
    pub token: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

impl TokenBurn {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        from: Arc<String>,
        token: Arc<String>,
        amount: Arc<BigDecimal>,
        block_timestamp: u64,
        block_number: u64,
        transaction_hash: Arc<String>,
        log_index: u64,
        transaction_index: u64,
    ) -> Result<Self> {
        Ok(TokenBurn {
            from,
            token,
            amount,
            transaction_hash,
            block_number,
            block_timestamp,
            log_index,
            transaction_index,
        })
    }
}

/// Token Transfer 이벤트 (PnL 추적용)
#[derive(Debug, Clone)]
pub struct TokenTransfer {
    pub token: Arc<String>,
    pub transaction_hash: Arc<String>,
    pub from_address: Arc<String>,
    pub to_address: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tx_index: u64,
    pub log_index: u64,
}

impl TokenTransfer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        token: Arc<String>,
        transaction_hash: Arc<String>,
        from_address: Arc<String>,
        to_address: Arc<String>,
        amount: Arc<BigDecimal>,
        block_number: u64,
        block_timestamp: u64,
        tx_index: u64,
        log_index: u64,
    ) -> Result<Self> {
        Ok(TokenTransfer {
            token,
            transaction_hash,
            from_address,
            to_address,
            amount,
            block_number,
            block_timestamp,
            tx_index,
            log_index,
        })
    }
}

/// Transfer 타입
#[derive(Debug, Clone, PartialEq)]
pub enum TransferType {
    Buy,         // 매수 (Pool/Curve → User, WMON out)
    Sell,        // 매도 (User → Pool/Curve, WMON in)
    TransferOut, // EOA→EOA 보내기
    TransferIn,  // EOA→EOA 받기
    LpAdd,       // LP 추가 (User → Pool)
    LpRemove,    // LP 제거 (Pool → User)
    Airdrop,     // 에어드랍 (Contract → User, no WMON)
    Other,       // 기타
}

impl TransferType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransferType::Buy => "buy",
            TransferType::Sell => "sell",
            TransferType::TransferOut => "transfer_out",
            TransferType::TransferIn => "transfer_in",
            TransferType::LpAdd => "lp_add",
            TransferType::LpRemove => "lp_remove",
            TransferType::Airdrop => "airdrop",
            TransferType::Other => "other",
        }
    }

    pub fn from_db_value(s: &str) -> Self {
        match s {
            "buy" => TransferType::Buy,
            "sell" => TransferType::Sell,
            "transfer_out" => TransferType::TransferOut,
            "transfer_in" => TransferType::TransferIn,
            "lp_add" => TransferType::LpAdd,
            "lp_remove" => TransferType::LpRemove,
            "airdrop" => TransferType::Airdrop,
            _ => TransferType::Other,
        }
    }
}

/// Position History 이벤트 (분석 완료된 PnL 데이터)
#[derive(Debug, Clone)]
pub struct PositionHistoryEvent {
    pub account_id: Arc<String>,
    pub token_id: Arc<String>,
    pub quote_in: Arc<BigDecimal>,
    pub quote_out: Arc<BigDecimal>,
    pub usd_in: Arc<BigDecimal>,
    pub usd_out: Arc<BigDecimal>,
    pub token_in: Arc<BigDecimal>,
    pub token_out: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tx_index: u64,
    pub log_index: u64,
    /// Transfer 타입 (buy, sell, transfer_out, transfer_in, etc.)
    pub transfer_type: TransferType,
    /// EOA→EOA transfer 시 상대방 주소
    pub sender_address: Option<Arc<String>>,
}
