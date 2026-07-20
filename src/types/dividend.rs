use bigdecimal::BigDecimal;
use std::sync::Arc;

/// Shared log coordinates for one decoded log.
#[derive(Debug, Clone)]
pub struct LogCoords {
    pub transaction_hash: Arc<String>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct DividendSetupEntry {
    pub source_token: Arc<String>,
    pub dividend_token: Arc<String>,
    pub ratio: i32,
    pub min_balance: Arc<BigDecimal>,
    pub entry_index: u64,
    pub coords: LogCoords,
}

#[derive(Debug, Clone)]
pub struct DividendDeposit {
    pub source_token: Arc<String>,
    pub dividend_token: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub pending: bool,
    pub entry_index: u64,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub coords: LogCoords,
}

#[derive(Debug, Clone)]
pub struct DividendConversion {
    pub source_token: Arc<String>,
    pub dividend_token: Arc<String>,
    pub consumed_quote: Arc<BigDecimal>,
    pub received: Arc<BigDecimal>,
    pub quote_id: Arc<String>,
    pub usd_value: Arc<BigDecimal>,
    pub entry_index: u64,
    pub coords: LogCoords,
}

#[derive(Debug, Clone)]
pub struct DividendMerkleRoot {
    pub merkle_root: Arc<String>,
    pub coords: LogCoords,
}

#[derive(Debug, Clone)]
pub struct DividendClaim {
    pub holder: Arc<String>,
    pub source_token: Arc<String>,
    pub dividend_token: Arc<String>,
    pub amount: Arc<BigDecimal>,
    pub usd_value: Arc<BigDecimal>,
    pub entry_index: u64,
    pub coords: LogCoords,
}

#[derive(Debug, Clone)]
pub enum DividendEvent {
    Setup(DividendSetupEntry),
    Deposit(DividendDeposit),
    Conversion(DividendConversion),
    MerkleRoot(DividendMerkleRoot),
    Claim(DividendClaim),
}

impl DividendEvent {
    pub fn coords(&self) -> &LogCoords {
        match self {
            Self::Setup(e) => &e.coords,
            Self::Deposit(e) => &e.coords,
            Self::Conversion(e) => &e.coords,
            Self::MerkleRoot(e) => &e.coords,
            Self::Claim(e) => &e.coords,
        }
    }

    pub fn block_number(&self) -> u64 {
        self.coords().block_number
    }

    pub fn log_index(&self) -> u64 {
        self.coords().log_index
    }

    pub fn transaction_index(&self) -> u64 {
        self.coords().transaction_index
    }

    /// Tie-breaker for exploded array entries sharing one log.
    pub fn entry_index(&self) -> u64 {
        match self {
            Self::Setup(e) => e.entry_index,
            Self::Deposit(e) => e.entry_index,
            Self::Conversion(e) => e.entry_index,
            Self::Claim(e) => e.entry_index,
            _ => 0,
        }
    }
}

/// Explode DividendSetup parallel arrays into per-entry items.
/// Errors on length mismatch (fail loud; the whole log is dropped upstream).
pub fn explode_setup(
    source_token: &str,
    dividend_tokens: Vec<String>,
    ratios: Vec<u16>,
    min_balance: BigDecimal,
    coords: LogCoords,
) -> anyhow::Result<Vec<DividendSetupEntry>> {
    if dividend_tokens.len() != ratios.len() {
        anyhow::bail!(
            "DividendSetup array length mismatch: tokens={} ratios={}",
            dividend_tokens.len(),
            ratios.len()
        );
    }

    let source = Arc::new(source_token.to_string());
    let min_balance = Arc::new(min_balance);
    Ok(dividend_tokens
        .into_iter()
        .zip(ratios)
        .enumerate()
        .map(
            |(entry_index, (dividend_token, ratio))| DividendSetupEntry {
                source_token: source.clone(),
                dividend_token: Arc::new(dividend_token),
                ratio: ratio as i32,
                min_balance: min_balance.clone(),
                entry_index: entry_index as u64,
                coords: coords.clone(),
            },
        )
        .collect())
}

/// Explode Deposit parallel arrays into per-entry items, SKIPPING zero slices.
/// `entry_index` preserves the ORIGINAL array position so the on-chain layout
/// stays reconstructable. Errors on any length mismatch.
pub fn explode_deposit(
    source_token: &str,
    dividend_tokens: Vec<String>,
    slices: Vec<BigDecimal>,
    pending: Vec<bool>,
    coords: LogCoords,
) -> anyhow::Result<Vec<DividendDeposit>> {
    let n = dividend_tokens.len();
    if slices.len() != n || pending.len() != n {
        anyhow::bail!(
            "Deposit array length mismatch: tokens={} slices={} pending={}",
            n,
            slices.len(),
            pending.len()
        );
    }

    let source = Arc::new(source_token.to_string());
    let zero = BigDecimal::from(0);
    Ok(dividend_tokens
        .into_iter()
        .zip(slices)
        .zip(pending)
        .enumerate()
        .filter(|(_, ((_, slice), _))| slice != &zero)
        .map(
            |(entry_index, ((dividend_token, amount), pending))| DividendDeposit {
                source_token: source.clone(),
                dividend_token: Arc::new(dividend_token),
                amount: Arc::new(amount),
                pending,
                entry_index: entry_index as u64,
                quote_id: Arc::new(String::new()),
                usd_value: Arc::new(BigDecimal::from(0)),
                coords: coords.clone(),
            },
        )
        .collect())
}

/// Explode Converted parallel arrays into per-order items.
/// Errors on any length mismatch.
pub fn explode_conversion(
    source_tokens: Vec<String>,
    dividend_tokens: Vec<String>,
    consumed_quote: Vec<BigDecimal>,
    received: Vec<BigDecimal>,
    coords: LogCoords,
) -> anyhow::Result<Vec<DividendConversion>> {
    let n = source_tokens.len();
    if dividend_tokens.len() != n || consumed_quote.len() != n || received.len() != n {
        anyhow::bail!(
            "Converted array length mismatch: sources={} dividends={} consumed={} received={}",
            n,
            dividend_tokens.len(),
            consumed_quote.len(),
            received.len()
        );
    }

    Ok(source_tokens
        .into_iter()
        .zip(dividend_tokens)
        .zip(consumed_quote.into_iter().zip(received))
        .enumerate()
        .map(
            |(entry_index, ((source_token, dividend_token), (consumed_quote, received)))| {
                DividendConversion {
                    source_token: Arc::new(source_token),
                    dividend_token: Arc::new(dividend_token),
                    consumed_quote: Arc::new(consumed_quote),
                    received: Arc::new(received),
                    quote_id: Arc::new(String::new()),
                    usd_value: Arc::new(BigDecimal::from(0)),
                    entry_index: entry_index as u64,
                    coords: coords.clone(),
                }
            },
        )
        .collect())
}

/// Explode Claim parallel arrays into per-entry items, SKIPPING zero amounts
/// (zero = on-chain skipped item: ineligible / already claimed / unfunded).
/// `entry_index` preserves the ORIGINAL array position so the on-chain layout
/// stays reconstructable. Errors on any length mismatch.
pub fn explode_claim(
    holder: &str,
    source_tokens: Vec<String>,
    dividend_tokens: Vec<String>,
    amounts: Vec<BigDecimal>,
    coords: LogCoords,
) -> anyhow::Result<Vec<DividendClaim>> {
    let n = source_tokens.len();
    if dividend_tokens.len() != n || amounts.len() != n {
        anyhow::bail!(
            "Claim array length mismatch: sources={} dividends={} amounts={}",
            n,
            dividend_tokens.len(),
            amounts.len()
        );
    }

    let holder = Arc::new(holder.to_string());
    Ok(source_tokens
        .into_iter()
        .zip(dividend_tokens)
        .zip(amounts)
        .enumerate()
        .filter(|(_, ((_, _), amount))| amount > &BigDecimal::from(0))
        .map(
            |(entry_index, ((source_token, dividend_token), amount))| DividendClaim {
                holder: holder.clone(),
                source_token: Arc::new(source_token),
                dividend_token: Arc::new(dividend_token),
                amount: Arc::new(amount),
                usd_value: Arc::new(BigDecimal::from(0)),
                entry_index: entry_index as u64,
                coords: coords.clone(),
            },
        )
        .collect())
}

pub fn compose_dividend_claim_usd(
    amount: &BigDecimal,
    decimals_factor: &BigDecimal,
    quote_usd: Option<&BigDecimal>,
    whitelist_usd: Option<&BigDecimal>,
    chain: Option<(&BigDecimal, &BigDecimal)>,
) -> Option<BigDecimal> {
    let unit_usd = if let Some(quote_usd) = quote_usd {
        quote_usd.clone()
    } else if let Some(whitelist_usd) = whitelist_usd {
        whitelist_usd.clone()
    } else if let Some((quote_per_token, quote_usd)) = chain {
        quote_per_token * quote_usd
    } else {
        return None;
    };

    Some((amount / decimals_factor) * unit_usd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn coords() -> LogCoords {
        LogCoords {
            transaction_hash: Arc::new("0xtx".to_string()),
            block_number: 100,
            block_timestamp: 1_700_000_000,
            log_index: 1,
            transaction_index: 0,
        }
    }

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn explode_setup_produces_entry_per_token() {
        let entries = explode_setup(
            "0xSOURCE",
            vec!["0xA".into(), "0xB".into()],
            vec![6000, 4000],
            bd("1000"),
            coords(),
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].ratio, 6000);
        assert_eq!(entries[1].entry_index, 1);
        assert_eq!(*entries[1].dividend_token, "0xB");
    }

    #[test]
    fn explode_setup_rejects_length_mismatch() {
        let res = explode_setup(
            "0xS",
            vec!["0xA".into()],
            vec![6000, 4000],
            bd("0"),
            coords(),
        );
        assert!(res.is_err());
    }

    #[test]
    fn explode_conversion_rejects_length_mismatch() {
        let res = explode_conversion(
            vec!["0xS".into()],
            vec!["0xA".into(), "0xB".into()],
            vec![bd("1")],
            vec![bd("2")],
            coords(),
        );
        assert!(res.is_err());
    }

    #[test]
    fn explode_conversion_maps_fields_per_order() {
        let entries = explode_conversion(
            vec!["0xS1".into(), "0xS2".into()],
            vec!["0xA".into(), "0xB".into()],
            vec![bd("400"), bd("500")],
            vec![bd("111"), bd("222")],
            coords(),
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        // Parallel arrays must map per-order, never transposed across entries.
        assert_eq!(*entries[0].source_token, "0xS1");
        assert_eq!(*entries[0].dividend_token, "0xA");
        assert_eq!(*entries[0].consumed_quote, bd("400"));
        assert_eq!(*entries[0].received, bd("111"));
        assert_eq!(entries[0].entry_index, 0);
        assert_eq!(*entries[1].source_token, "0xS2");
        assert_eq!(*entries[1].dividend_token, "0xB");
        assert_eq!(*entries[1].consumed_quote, bd("500"));
        assert_eq!(*entries[1].received, bd("222"));
        assert_eq!(entries[1].entry_index, 1);
        // quote_id / usd_value stay placeholders until stream.rs enrichment.
        assert_eq!(*entries[0].quote_id, "");
        assert_eq!(*entries[0].usd_value, bd("0"));
    }

    #[test]
    fn explode_deposit_maps_pending_and_entry_index_per_slice() {
        let entries = explode_deposit(
            "0xSOURCE",
            vec!["0xQUOTE".into(), "0xTOKEN".into()],
            vec![bd("100"), bd("200")],
            vec![false, true],
            coords(),
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(*entries[0].source_token, "0xSOURCE");
        assert_eq!(*entries[0].dividend_token, "0xQUOTE");
        assert_eq!(*entries[0].amount, bd("100"));
        assert!(!entries[0].pending);
        assert_eq!(entries[0].entry_index, 0);
        assert_eq!(*entries[1].source_token, "0xSOURCE");
        assert_eq!(*entries[1].dividend_token, "0xTOKEN");
        assert_eq!(*entries[1].amount, bd("200"));
        assert!(entries[1].pending);
        assert_eq!(entries[1].entry_index, 1);
        assert_eq!(*entries[0].quote_id, "");
        assert_eq!(*entries[0].usd_value, bd("0"));
    }

    #[test]
    fn explode_deposit_rejects_length_mismatch() {
        let res = explode_deposit(
            "0xSOURCE",
            vec!["0xA".into(), "0xB".into()],
            vec![bd("100")],
            vec![false, true],
            coords(),
        );

        assert!(res.is_err());
    }

    #[test]
    fn explode_deposit_skips_zero_slices_and_keeps_original_entry_index() {
        let entries = explode_deposit(
            "0xSOURCE",
            vec!["0xA".into(), "0xB".into(), "0xC".into()],
            vec![bd("100"), bd("0"), bd("300")],
            vec![false, true, true],
            coords(),
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_index, 0);
        assert_eq!(entries[1].entry_index, 2);
        assert_eq!(*entries[1].dividend_token, "0xC");
        assert_eq!(*entries[1].amount, bd("300"));
        assert!(entries[1].pending);
    }

    #[test]
    fn explode_claim_all_zero_returns_empty() {
        let claims = explode_claim(
            "0xH",
            vec!["0xS1".into(), "0xS2".into()],
            vec!["0xA".into(), "0xB".into()],
            vec![bd("0"), bd("0")],
            coords(),
        )
        .unwrap();
        assert!(claims.is_empty(), "all-zero claim log produces no entries");
    }

    #[test]
    fn explode_claim_skips_zero_amounts_and_keeps_original_entry_index() {
        let claims = explode_claim(
            "0xHOLDER",
            vec!["0xS1".into(), "0xS2".into(), "0xS3".into()],
            vec!["0xA".into(), "0xB".into(), "0xC".into()],
            vec![bd("100"), bd("0"), bd("300")],
            coords(),
        )
        .unwrap();
        assert_eq!(claims.len(), 2, "zero entry must be skipped");
        assert_eq!(claims[0].entry_index, 0);
        assert_eq!(
            claims[1].entry_index, 2,
            "original array position preserved"
        );
        assert_eq!(*claims[1].amount, bd("300"));
    }

    #[test]
    fn explode_claim_rejects_length_mismatch() {
        let res = explode_claim(
            "0xH",
            vec!["0xS".into()],
            vec!["0xA".into()],
            vec![bd("1"), bd("2")],
            coords(),
        );
        assert!(res.is_err());
    }

    // ---- compose_dividend_claim_usd: claim USD source priority + math ----
    // Priority: quote > whitelist(DefiLlama) > chain(quote_per_token × quote_usd).
    // All paths: usd = (amount / decimals_factor) × unit_usd. None when no source.

    /// Normalized equality so scale differences from division don't fail the test
    /// (bigdecimal `==` is scale-sensitive: 1.0 != 1.00).
    fn usd_eq(got: Option<BigDecimal>, want: &str) {
        let got = got.expect("expected Some(usd)");
        assert_eq!(got.normalized(), bd(want).normalized(), "got {got}");
    }

    #[test]
    fn compose_uses_quote_usd_when_token_is_quote() {
        // amount 2e24 / 1e18 = 2e6 tokens; × quote_usd 3 = 6e6.
        // whitelist + chain are present but quote must win.
        usd_eq(
            compose_dividend_claim_usd(
                &bd("2000000000000000000000000"),
                &bd("1000000000000000000"),
                Some(&bd("3")),
                Some(&bd("999")),
                Some((&bd("0.02"), &bd("2.5"))),
            ),
            "6000000",
        );
    }

    #[test]
    fn compose_falls_back_to_whitelist_when_no_quote() {
        // amount 1e18 / 1e18 = 1 token; × whitelist usd/token 4.5 = 4.5.
        // chain present but whitelist must win.
        usd_eq(
            compose_dividend_claim_usd(
                &bd("1000000000000000000"),
                &bd("1000000000000000000"),
                None,
                Some(&bd("4.5")),
                Some((&bd("0.02"), &bd("2.5"))),
            ),
            "4.5",
        );
    }

    #[test]
    fn compose_falls_back_to_chain_when_no_quote_or_whitelist() {
        // amount 1e18 / 1e18 = 1 token; × (qpt 0.02 × qusd 2.5) = 0.05.
        usd_eq(
            compose_dividend_claim_usd(
                &bd("1000000000000000000"),
                &bd("1000000000000000000"),
                None,
                None,
                Some((&bd("0.02"), &bd("2.5"))),
            ),
            "0.05",
        );
    }

    #[test]
    fn compose_chain_applies_decimals_and_both_factors() {
        // amount 5e24 / 1e18 = 5e6 tokens; × (qpt 0.001 × qusd 20 = 0.02) = 100000.
        usd_eq(
            compose_dividend_claim_usd(
                &bd("5000000000000000000000000"),
                &bd("1000000000000000000"),
                None,
                None,
                Some((&bd("0.001"), &bd("20"))),
            ),
            "100000",
        );
    }

    #[test]
    fn compose_returns_none_when_no_source() {
        assert_eq!(
            compose_dividend_claim_usd(
                &bd("1000000000000000000"),
                &bd("1000000000000000000"),
                None,
                None,
                None,
            ),
            None
        );
    }
}
