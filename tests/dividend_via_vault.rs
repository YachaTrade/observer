//! Integration test for folding DividendVault indexing into the Vault stream
//! (refactor/dividend-into-v2vault).
//!
//! These assert that dividend events, wrapped as `VaultEvent::Dividend(..)`,
//! flow through the Vault receive path (`vault::receive::process_events`)
//! and preserve the two behaviours that the standalone Dividend receive
//! guaranteed:
//!   1. Setup seeds `v2_dividend_vault_stats` rows.
//!   2. The 2-phase ordering resolves a Claim's `merkle_root` against a
//!      MerkleRoot inserted IN THE SAME BATCH (phase 1 roots commit before
//!      phase 2 claims), using the (block, tx_index, log_index) tuple.
//!
//! Pre-implementation these fail to compile: `VaultEvent::Dividend` does not
//! exist yet and `vault::receive::process_events` is private. That is the
//! intended RED state — the migration must add the wrapper variant and route
//! dividend events through the vault receive.

mod common;

use std::sync::Arc;

use anyhow::Result;
use bigdecimal::BigDecimal;
use common::setup_test_db;
use std::str::FromStr;

use observer::db::postgres::PostgresDatabase;
use observer::event::vault::receive::process_events;
use observer::types::dividend::{
    DividendClaim, DividendEvent, DividendMerkleRoot, DividendSetupEntry, LogCoords,
};
use observer::types::vault::VaultEvent;

const SOURCE: &str = "0x1111111111111111111111111111111111111111";
const DIV_QUOTE: &str = "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A";
const HOLDER: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ROOT1: &str = "0x0101010101010101010101010101010101010101010101010101010101010101";

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

/// LogCoords at a fixed block with caller-chosen ordering tie-breakers.
fn coords(tx_hash: &str, block: u64, tx_index: u64, log_index: u64) -> LogCoords {
    LogCoords {
        transaction_hash: Arc::new(tx_hash.to_string()),
        block_number: block,
        block_timestamp: 1_700_000_000 + log_index,
        log_index,
        transaction_index: tx_index,
    }
}

fn div(e: DividendEvent) -> VaultEvent {
    VaultEvent::Dividend(e)
}

#[tokio::test]
async fn dividend_events_route_through_vault_receive() -> Result<()> {
    let db = setup_test_db().await?;
    let database = Arc::new(PostgresDatabase {
        pool: db.pool.clone(),
    });

    // One batch carrying Setup + MerkleRoot + Claim, interleaved as they would
    // arrive on the unified vault stream. The Claim sits at (100, 0, 3); ROOT1
    // at (100, 0, 1) is the at-or-before root in the SAME batch, so a correct
    // 2-phase insert (roots first) must resolve it.
    let events = vec![
        div(DividendEvent::Setup(DividendSetupEntry {
            source_token: Arc::new(SOURCE.to_string()),
            dividend_token: Arc::new(DIV_QUOTE.to_string()),
            ratio: 6000,
            min_balance: Arc::new(bd("1000")),
            entry_index: 0,
            coords: coords("0xtx_setup", 100, 0, 0),
        })),
        div(DividendEvent::MerkleRoot(DividendMerkleRoot {
            merkle_root: Arc::new(ROOT1.to_string()),
            coords: coords("0xtx_root", 100, 0, 1),
        })),
        div(DividendEvent::Claim(DividendClaim {
            holder: Arc::new(HOLDER.to_string()),
            source_token: Arc::new(SOURCE.to_string()),
            dividend_token: Arc::new(DIV_QUOTE.to_string()),
            amount: Arc::new(bd("100")),
            usd_value: Arc::new(bd("0.5")),
            entry_index: 0,
            coords: coords("0xtx_claim", 100, 0, 3),
        })),
    ];

    process_events(events, database).await?;

    // 1. Setup seeded a stats row for (SOURCE, DIV_QUOTE).
    let (stats_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM v2_dividend_vault_stats WHERE source_token = $1 AND dividend_token = $2",
    )
    .bind(SOURCE)
    .bind(DIV_QUOTE)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        stats_count, 1,
        "Setup routed through Vault must seed a v2_dividend_vault_stats row"
    );

    // 2. The Claim resolved its merkle_root against the same-batch root —
    //    proving phase-1 roots committed before phase-2 claims via the vault path.
    let (root,): (Option<String>,) =
        sqlx::query_as("SELECT merkle_root FROM v2_dividend_claims WHERE holder = $1")
            .bind(HOLDER)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        root.as_deref(),
        Some(ROOT1),
        "Claim must resolve the same-batch MerkleRoot (2-phase ordering preserved through Vault receive)"
    );

    Ok(())
}
