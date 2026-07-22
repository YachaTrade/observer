use serde::Deserialize;
use std::sync::Arc;

// Mirror of IVaultRegistry.VaultType (solidity enum):
//   Custom=0, Burn=1, Lp=2, CreatorFee=3, Gift=4, Dividend=5
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RegisteredVaultType {
    Custom = 0,
    Burn = 1,
    Lp = 2,
    CreatorFee = 3,
    Gift = 4,
    Dividend = 5,
}

impl RegisteredVaultType {
    pub fn from_u8(v: u8) -> anyhow::Result<Self> {
        match v {
            0 => Ok(Self::Custom),
            1 => Ok(Self::Burn),
            2 => Ok(Self::Lp),
            3 => Ok(Self::CreatorFee),
            4 => Ok(Self::Gift),
            5 => Ok(Self::Dividend),
            other => Err(anyhow::anyhow!(
                "Unknown VaultRegistry.VaultType value: {other}"
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Custom => "CUSTOM",
            Self::Burn => "BURN",
            Self::Lp => "LP",
            Self::CreatorFee => "CREATOR_FEE",
            Self::Gift => "GIFT",
            Self::Dividend => "DIVIDEND",
        }
    }
}

// Off-chain metadata schema at the `metadataURI()` URL.
// Example shape: { "name": "...", "description": { "what": "...",
// "how": [...], "rules": [...], "importantNote": "..." },
// "imageUri": "https://..." }.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VaultMetadataDescription {
    #[serde(default)]
    pub what: String,
    #[serde(default)]
    pub how: Vec<String>,
    #[serde(default)]
    pub rules: Vec<String>,
    #[serde(default)]
    pub important_note: Option<String>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VaultMetadata {
    pub name: String,
    #[serde(default)]
    pub description: Option<VaultMetadataDescription>,
    #[serde(default)]
    pub image_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VaultRegister {
    pub vault: Arc<String>,
    pub name: Arc<String>,
    pub creator: Arc<String>,
    pub vault_type: RegisteredVaultType,
    // Populated by an eth_call to vault.metadataURI(). None if the call failed.
    pub metadata_uri: Option<Arc<String>>,
    // Populated by HTTP fetch of the URI above. None if fetch or parse failed.
    pub metadata: Option<VaultMetadata>,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct VaultDeactivate {
    pub vault: Arc<String>,
    pub active: bool,
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub enum VaultRegistryEvent {
    Register(VaultRegister),
    Deactivate(VaultDeactivate),
}

impl VaultRegistryEvent {
    pub fn block_number(&self) -> u64 {
        match self {
            Self::Register(e) => e.block_number,
            Self::Deactivate(e) => e.block_number,
        }
    }

    pub fn log_index(&self) -> u64 {
        match self {
            Self::Register(e) => e.log_index,
            Self::Deactivate(e) => e.log_index,
        }
    }

    pub fn transaction_index(&self) -> u64 {
        match self {
            Self::Register(e) => e.transaction_index,
            Self::Deactivate(e) => e.transaction_index,
        }
    }
}
