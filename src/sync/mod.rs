pub mod receive;
pub mod stream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    Curve,
    Dex,
    LpManager,
    Vault,
    VaultRegistry,
    Token,
    Price,
    PriceUsd,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Curve => "curve",
            EventType::Dex => "dex",
            EventType::LpManager => "lp_manager",
            EventType::Vault => "vault",
            EventType::VaultRegistry => "vault_registry",
            EventType::Token => "token",
            EventType::Price => "price",
            EventType::PriceUsd => "price_usd",
        }
    }

    pub fn all() -> [EventType; 8] {
        [
            EventType::Curve,
            EventType::Dex,
            EventType::LpManager,
            EventType::Vault,
            EventType::VaultRegistry,
            EventType::Token,
            EventType::Price,
            EventType::PriceUsd,
        ]
    }
}

// 블록 범위 구조체
#[derive(Debug, Clone)]
pub struct BlockRange {
    pub from_block: u64,
    pub to_block: u64,
}

// lazy_static::lazy_static! {
//     pub static ref RECIEVE_MANAGER: RecieveManager = RecieveManager {
//         last_processed_block: AtomicU64::new(0),
//         mode: AtomicCell::new(RecieveType::Sync),
//     };
// }

#[cfg(test)]
mod tests {
    use super::EventType;

    #[test]
    fn giwa_event_types_include_the_dormant_price_usd_checkpoint() {
        let names: Vec<&str> = EventType::all().iter().map(EventType::as_str).collect();
        assert_eq!(
            names,
            vec![
                "curve",
                "dex",
                "lp_manager",
                "vault",
                "vault_registry",
                "token",
                "price",
                "price_usd",
            ]
        );
    }
}
