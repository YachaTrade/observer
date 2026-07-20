//! VaultRegistry contracts for type mapping and replay-safe metadata updates.
//! DividendVault uses ordinal 5, so its registrations must be indexed rather
//! than dropped.
//! registrations are indexed instead of dropped.
//!
//! Contract enum (IVaultRegistry.sol, append-only): Custom=0, Burn=1, LP=2,
//! Creator=3, Gift=4, Dividend=5. Before this fix `from_u8(5)` errored, so the
//! Register-event parse failed and the DividendVault registration row + metadata
//! were never persisted.

use observer::db::postgres::controller::v2::vault_registry::UPSERT_VAULT_METADATA_SQL;
use observer::types::v2::vault_registry::RegisteredVaultType;

#[test]
fn from_u8_maps_dividend_variant() {
    assert_eq!(
        RegisteredVaultType::from_u8(5).unwrap(),
        RegisteredVaultType::Dividend,
        "VaultType ordinal 5 must map to Dividend (contract append-only)"
    );
}

#[test]
fn dividend_as_str_is_screaming_snake() {
    assert_eq!(
        RegisteredVaultType::Dividend.as_str(),
        "DIVIDEND",
        "as_str follows the existing convention (CUSTOM/BURN/LP/CREATOR_FEE/GIFT)"
    );
}

#[test]
fn existing_variants_unchanged() {
    assert_eq!(
        RegisteredVaultType::from_u8(0).unwrap(),
        RegisteredVaultType::Custom
    );
    assert_eq!(
        RegisteredVaultType::from_u8(4).unwrap(),
        RegisteredVaultType::Gift
    );
}

#[test]
fn from_u8_still_rejects_truly_unknown() {
    assert!(
        RegisteredVaultType::from_u8(6).is_err(),
        "6 is not a contract VaultType — catch-all must still error"
    );
}

#[test]
fn metadata_register_replay_preserves_a_newer_update_timestamp() {
    assert!(
        UPSERT_VAULT_METADATA_SQL
            .contains("updated_at = GREATEST(v2_vault_metadata.updated_at, EXCLUDED.updated_at)"),
        "replaying Register must not regress a later Deactivate timestamp"
    );
}

#[test]
fn metadata_replay_does_not_erase_successful_enrichment() {
    assert!(
        UPSERT_VAULT_METADATA_SQL.contains("CASE")
            && UPSERT_VAULT_METADATA_SQL
                .contains("v2_vault_metadata.metadata_uri = EXCLUDED.metadata_uri"),
        "same-URI replay without metadata must preserve successful enrichment"
    );
}
