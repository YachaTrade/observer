use bigdecimal::BigDecimal;
use std::sync::Arc;

// Mirror of GiftVault.Platform (solidity enum: GitHub=0, X=1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GiftPlatform {
    GitHub = 0,
    X = 1,
}

impl GiftPlatform {
    pub fn from_u8(v: u8) -> anyhow::Result<Self> {
        match v {
            0 => Ok(Self::GitHub),
            1 => Ok(Self::X),
            other => Err(anyhow::anyhow!("Unknown GiftVault.Platform value: {other}")),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GitHub => "GITHUB",
            Self::X => "X",
        }
    }
}

// BurnVault + GiftVault share the same Burn event structure
#[derive(Debug, Clone)]
pub struct VaultBurn {
    pub vault_type: VaultType,
    pub token: Arc<String>,
    pub pair: Arc<String>,
    pub quote_in: Arc<BigDecimal>,
    pub token_burned: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct LpInject {
    pub token: Arc<String>,
    pub pair: Arc<String>,
    pub quote_used: Arc<BigDecimal>,
    pub token_used: Arc<BigDecimal>,
    pub lp_burned: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct CreatorDeposit {
    pub token: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub new_balance: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct CreatorClaim {
    pub token: Arc<String>,
    pub creator: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

// CreatorFeeVault.VaultSetup — initial creator bind (one-shot per token).
#[derive(Debug, Clone)]
pub struct CreatorVaultSetup {
    pub token: Arc<String>,
    pub creator: Arc<String>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

// CreatorFeeVault.CreatorUpdate — subsequent creator change.
#[derive(Debug, Clone)]
pub struct CreatorUpdate {
    pub token: Arc<String>,
    pub old_creator: Arc<String>,
    pub new_creator: Arc<String>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct GiftVaultSetup {
    pub token: Arc<String>,
    pub platform: GiftPlatform,
    pub platform_id: Arc<String>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct GiftDeposit {
    pub token: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub new_balance: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct GiftClaim {
    pub token: Arc<String>,
    pub receiver: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct GiftExpire {
    pub token: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

// GiftVault.ReceiverSet — binds a receiver after platform verification.
#[derive(Debug, Clone)]
pub struct GiftReceiverSet {
    pub token: Arc<String>,
    pub receiver: Arc<String>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

// GiftVault.ExpiryUpdate — global expiry duration change (no token scope).
#[derive(Debug, Clone)]
pub struct GiftExpiryUpdate {
    pub old_duration: Arc<BigDecimal>,
    pub new_duration: Arc<BigDecimal>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum VaultType {
    Burn,
    Lp,
    CreatorFee,
    Gift,
}

#[derive(Debug, Clone)]
pub enum VaultEvent {
    Burn(VaultBurn),
    LpInject(LpInject),
    CreatorDeposit(CreatorDeposit),
    CreatorClaim(CreatorClaim),
    CreatorVaultSetup(CreatorVaultSetup),
    CreatorUpdate(CreatorUpdate),
    GiftVaultSetup(GiftVaultSetup),
    GiftDeposit(GiftDeposit),
    GiftClaim(GiftClaim),
    GiftExpire(GiftExpire),
    GiftReceiverSet(GiftReceiverSet),
    GiftExpiryUpdate(GiftExpiryUpdate),
    Dividend(crate::types::dividend::DividendEvent),
}

impl VaultEvent {
    pub fn block_number(&self) -> u64 {
        match self {
            Self::Burn(e) => e.block_number,
            Self::LpInject(e) => e.block_number,
            Self::CreatorDeposit(e) => e.block_number,
            Self::CreatorClaim(e) => e.block_number,
            Self::CreatorVaultSetup(e) => e.block_number,
            Self::CreatorUpdate(e) => e.block_number,
            Self::GiftVaultSetup(e) => e.block_number,
            Self::GiftDeposit(e) => e.block_number,
            Self::GiftClaim(e) => e.block_number,
            Self::GiftExpire(e) => e.block_number,
            Self::GiftReceiverSet(e) => e.block_number,
            Self::GiftExpiryUpdate(e) => e.block_number,
            Self::Dividend(e) => e.block_number(),
        }
    }

    pub fn log_index(&self) -> u64 {
        match self {
            Self::Burn(e) => e.log_index,
            Self::LpInject(e) => e.log_index,
            Self::CreatorDeposit(e) => e.log_index,
            Self::CreatorClaim(e) => e.log_index,
            Self::CreatorVaultSetup(e) => e.log_index,
            Self::CreatorUpdate(e) => e.log_index,
            Self::GiftVaultSetup(e) => e.log_index,
            Self::GiftDeposit(e) => e.log_index,
            Self::GiftClaim(e) => e.log_index,
            Self::GiftExpire(e) => e.log_index,
            Self::GiftReceiverSet(e) => e.log_index,
            Self::GiftExpiryUpdate(e) => e.log_index,
            Self::Dividend(e) => e.log_index(),
        }
    }

    pub fn transaction_index(&self) -> u64 {
        match self {
            Self::Burn(e) => e.transaction_index,
            Self::LpInject(e) => e.transaction_index,
            Self::CreatorDeposit(e) => e.transaction_index,
            Self::CreatorClaim(e) => e.transaction_index,
            Self::CreatorVaultSetup(e) => e.transaction_index,
            Self::CreatorUpdate(e) => e.transaction_index,
            Self::GiftVaultSetup(e) => e.transaction_index,
            Self::GiftDeposit(e) => e.transaction_index,
            Self::GiftClaim(e) => e.transaction_index,
            Self::GiftExpire(e) => e.transaction_index,
            Self::GiftReceiverSet(e) => e.transaction_index,
            Self::GiftExpiryUpdate(e) => e.transaction_index,
            Self::Dividend(e) => e.transaction_index(),
        }
    }
}
