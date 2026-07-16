use alloy::primitives::Address;
use bigdecimal::BigDecimal;
use lazy_static::lazy_static;
use std::env;
use std::str::FromStr;

/// Parse an address from a required env var, normalized to EIP-55
/// checksum form. Panics if the var is missing or not a valid address.
fn normalize_required_env_address(var: &str) -> String {
    let raw = env::var(var).unwrap_or_else(|_| panic!("{} must be set", var));
    raw.parse::<Address>()
        .unwrap_or_else(|e| panic!("{} env var is not a valid EVM address '{}': {}", var, raw, e))
        .to_string()
}

lazy_static! {
    pub static ref BONDING_CURVE_ADDRESS: String =
        normalize_required_env_address("BONDING_CURVE");
    pub static ref DEX_FACTORY_ADDRESS: String =
        normalize_required_env_address("DEX_FACTORY");
    pub static ref DEX_ROUTER_ADDRESS: String =
        normalize_required_env_address("DEX_ROUTER");
    pub static ref LP_MANAGER_ADDRESS: String =
        normalize_required_env_address("LP_MANAGER");
    // WMON address, normalized to EIP-55 checksum form at load time.
    // The env var may be set in any valid casing (lowercase, checksum,
    // etc.); we parse it through `alloy::primitives::Address` (case-
    // insensitive) and re-emit it via `Display`, which writes EIP-55.
    // This guarantees lex-equality with every alloy-derived address
    // downstream regardless of operator env casing.
    pub static ref WNATIVE_ADDRESS: String = {
        let raw = env::var("WMON").expect("WMON must be set");
        let parsed: Address = raw
            .parse()
            .unwrap_or_else(|e| panic!("WMON env var is not a valid EVM address '{}': {}", raw, e));
        parsed.to_string()
    };

    pub static ref VANITY_ADDRESS_SUFFIX: String =
        env::var("VANITY_ADDRESS_SUFFIX").unwrap_or_else(|_| "7777".to_string());

    pub static ref PYTH_API_URL: String =
        env::var("PYTH_API_URL").unwrap_or_else(|_| "https://hermes.pyth.network/v2/updates/price".to_string());

    // pub static ref UNISWAP_ROUTER_ADDRESS: String =
    //     env::var("UNISWAP_ROUTER").expect("UNISWAP_ROUTER must be set");
}

lazy_static! {
    pub static ref BLOCK_BATCH_SIZE: u64 = env::var("BLOCK_BATCH_SIZE")
        .expect("BLOCK_BATCH_SIZE must be set")
        .parse()
        .expect("BLOCK_BATCH_SIZE must be a number");
    pub static ref BLOCK_INTERVAL: u64 = env::var("BLOCK_INTERVAL")
        .expect("BLOCK_INTERVAL must be set")
        .parse()
        .expect("BLOCK_INTERVAL must be a number");
    pub static ref BLOCK_OFFSET: u64 = env::var("BLOCK_OFFSET")
        .expect("BLOCK_OFFSET must be set")
        .parse()
        .expect("BLOCK_OFFSET must be a valid u64");
}
lazy_static! {
    pub static ref GRADUATE_FEE_AMOUNT: BigDecimal = BigDecimal::from_str(
        &env::var("GRADUATE_FEE_AMOUNT")
            .expect("GRADUATE_FEE_AMOUNT must be set")
            .replace("_", ""),
    )
    .unwrap();

    pub static ref CREATE_FEE_AMOUNT: BigDecimal = BigDecimal::from_str(
        &env::var("CREATE_FEE_AMOUNT")
            .expect("CREATE_FEE_AMOUNT must be set")
            .replace("_", ""),
    )
    .unwrap();

    pub static ref BONDING_CURVE_FEE_RATE: BigDecimal = BigDecimal::from_str(
        &env::var("BONDING_CURVE_FEE_RATE")
            .expect("BONDING_CURVE_FEE_RATE must be set")
            .replace("_", ""),
    )
    .unwrap();

    pub static ref DEX_ROUTER_FEE_RATE: BigDecimal = BigDecimal::from_str(
        &env::var("DEX_ROUTER_FEE_RATE")
            .expect("DEX_ROUTER_FEE_RATE must be set")
            .replace("_", ""),
    )
    .unwrap();

    // pub static ref MIN_PRICE: BigDecimal =
    //     BigDecimal::from_str(
    //         &env::var("MIN_PRICE")
    //             .expect("MIN_PRICE must be set")
    //     )
    //     .unwrap();
}

#[derive(Debug, Clone)]
pub struct ChartConfig {
    pub chart_type: Vec<String>,
}

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            chart_type: vec![
                "1".to_string(),
                "5".to_string(),
                "15".to_string(),
                "30".to_string(),
                "1H".to_string(),
                "4H".to_string(),
                "D".to_string(),
                "W".to_string(),
                "M".to_string(),
            ],
        }
    }
}

impl ChartConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct RedisEnv {
    pub redis_url: String,
}

impl Default for RedisEnv {
    fn default() -> Self {
        Self {
            redis_url: env::var("REDIS_URL").expect("REDIS_URL must be set"),
        }
    }
}

impl RedisEnv {
    pub fn new() -> Self {
        Self::default()
    }
}

lazy_static! {
    pub static ref DEFAULT_DELAY: u64 = env::var("DEFAULT_DELAY")
        .expect("DEFAULT_DELAY must be set")
        .parse()
        .expect("DEFAULT_DELAY must be a number");
}

lazy_static! {
    pub static ref RPC_TIME_OUT: u64 = env::var("RPC_TIME_OUT")
        .expect("RPC_TIME_OUT must be set")
        .parse()
        .expect("RPC_TIME_OUT must be a number");
    pub static ref STREAM_TIMEOUT: u64 = env::var("STREAM_TIMEOUT")
        .unwrap_or_else(|_| "5000".to_string())
        .parse()
        .expect("STREAM_TIMEOUT must be a number");
    pub static ref METRICS_PORT: u16 = env::var("METRICS_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("METRICS_PORT must be a valid port number");
    pub static ref PROVIDER_CHECK_INTERVAL: u64 = env::var("PROVIDER_CHECK_INTERVAL")
        .expect("PROVIDER_CHECK_INTERVAL must be set")
        .parse()
        .expect("PROVIDER_CHECK_INTERVAL must be a number");
    pub static ref METRICS_REPORT_INTERVAL: u64 = env::var("METRICS_REPORT_INTERVAL")
        .expect("METRICS_REPORT_INTERVAL must be set")
        .parse()
        .expect("METRICS_REPORT_INTERVAL must be a number");
}

#[derive(Debug, Clone)]
pub struct QuoteConfig {
    pub address: String,
    pub pyth_feed_id: String,
    pub decimals: BigDecimal,
}

// ============================================================================
// Quote Config — loaded from DB `quote_token` table at startup
// ============================================================================

use std::sync::OnceLock;

static QUOTE_CONFIGS_STORE: OnceLock<Vec<QuoteConfig>> = OnceLock::new();

/// Load quote token configs from the `quote_token` DB table.
/// Replaces the old `QUOTE_CONFIGS` env var. Must be called after
/// `PostgresDatabase::init()` in main.rs startup.
///
/// Each row's `quote_id` is normalized to EIP-55 checksum via
/// `alloy::primitives::Address` parse + Display, same as all other
/// address statics in this module.
pub async fn init_quote_configs_from_db(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let rows: Vec<(String, String, i32)> = sqlx::query_as(
        "SELECT quote_id, pyth_feed_id, decimals FROM quote_token",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| anyhow::anyhow!("failed to load quote_token table: {}", e))?;

    if rows.is_empty() {
        panic!("quote_token table is empty — at least one quote (e.g. WMON) must be seeded");
    }

    let configs: Vec<QuoteConfig> = rows
        .into_iter()
        .map(|(quote_id, pyth_feed_id, decimals)| {
            let address: Address = quote_id
                .parse()
                .unwrap_or_else(|e| panic!("quote_token.quote_id '{}' is not a valid address: {}", quote_id, e));
            QuoteConfig {
                address: address.to_string(),
                pyth_feed_id,
                decimals: BigDecimal::from_str(&format!("1{}", "0".repeat(decimals as usize))).unwrap(),
            }
        })
        .collect();

    tracing::info!(
        "[CONFIG] loaded {} quote configs from DB: {:?}",
        configs.len(),
        configs.iter().map(|c| c.address.as_str()).collect::<Vec<_>>()
    );

    QUOTE_CONFIGS_STORE
        .set(configs)
        .map_err(|_| anyhow::anyhow!("QUOTE_CONFIGS already initialized"))?;

    Ok(())
}

/// Access the loaded quote configs. Panics if `init_quote_configs_from_db`
/// was not called yet.
pub fn quote_configs() -> &'static Vec<QuoteConfig> {
    QUOTE_CONFIGS_STORE
        .get()
        .expect("QUOTE_CONFIGS not initialized — call init_quote_configs_from_db first")
}

/// Get decimals for a quote token registered in `quote_token` table.
///
/// Lookup is case-insensitive — handles caller-supplied quote_id that may
/// be lowercase (legacy market.quote_id rows from LEAST/LOWER backfill) vs
/// QUOTE_CONFIGS env values stored in EIP-55 checksum form. Mirrors the
/// d2e5b7c hotfix on [`is_quote_token`].
///
/// **Panics** if `quote_id` is not present. Any failure here indicates a
/// bug in the upstream quote_id resolution or a missing row in the
/// `quote_token` table.
pub fn get_quote_decimals(quote_id: &str) -> &BigDecimal {
    quote_configs()
        .iter()
        .find(|q| q.address.eq_ignore_ascii_case(quote_id))
        .map(|q| &q.decimals)
        .unwrap_or_else(|| {
            panic!(
                "get_quote_decimals: quote_id '{}' not found in quote_token table",
                quote_id
            )
        })
}

/// Check if an address is a known quote token. Case-insensitive — handles
/// pool.token0/token1 stored in lowercase (from LEAST/LOWER backfill SQL)
/// vs QUOTE_CONFIGS env values stored in EIP-55 checksum form.
pub fn is_quote_token(address: &str) -> bool {
    quote_configs()
        .iter()
        .any(|q| q.address.eq_ignore_ascii_case(address))
}

/// Force eager init of address-bearing config statics at startup.
///
/// Every address static parses and normalizes its env input through
/// `alloy::primitives::Address` inside its lazy initializer — calling
/// this function during main startup ensures any env misconfiguration
/// (invalid hex, wrong length, missing required var) surfaces immediately
/// with a clear panic message instead of mid-stream on first consumer
/// access. It also applies the EIP-55 normalization step before any
/// downstream code reads these values.
pub fn force_init_address_configs() {
    let _ = &*WNATIVE_ADDRESS;
    let _ = &*BONDING_CURVE_ADDRESS;
    let _ = &*DEX_FACTORY_ADDRESS;
    let _ = &*DEX_ROUTER_ADDRESS;
    let _ = &*LP_MANAGER_ADDRESS;
    tracing::info!(
        "[CONFIG] GIWA address configs normalized to EIP-55 checksum (WNATIVE={})",
        *WNATIVE_ADDRESS,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(addr: &str, dec: u32) -> QuoteConfig {
        QuoteConfig {
            address: addr.to_string(),
            pyth_feed_id: String::new(),
            decimals: BigDecimal::from_str(&format!("1{}", "0".repeat(dec as usize))).unwrap(),
        }
    }

    /// Mirrors the production `find` predicate in [`get_quote_decimals`] /
    /// [`is_quote_token`]. Both must use case-insensitive matching so legacy
    /// lowercase market.quote_id rows resolve against EIP-55 checksum
    /// QUOTE_CONFIGS entries (d2e5b7c hotfix root cause).
    fn matches_quote(configs: &[QuoteConfig], quote_id: &str) -> bool {
        configs.iter().any(|q| q.address.eq_ignore_ascii_case(quote_id))
    }

    #[test]
    fn quote_lookup_is_case_insensitive() {
        // QUOTE_CONFIGS rows are normalized to EIP-55 checksum at load time
        // (see init_quote_configs_from_db).
        let configs = vec![cfg("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 18)];

        // Checksum query (canonical) — must hit.
        assert!(matches_quote(&configs, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"));
        // Lowercase query (legacy market.quote_id from LEAST/LOWER backfill) — must hit.
        assert!(matches_quote(&configs, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"));
        // Uppercase query (paranoia) — must hit.
        assert!(matches_quote(&configs, "0xC02AAA39B223FE8D0A0E5C4F27EAD9083C756CC2"));
        // Unrelated address — must miss.
        assert!(!matches_quote(&configs, "0x0000000000000000000000000000000000000001"));
    }
}
