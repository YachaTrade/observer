#[test]
fn main_wires_the_selected_implementations_to_generic_events() {
    let main = include_str!("../src/main.rs");
    let normalized: String = main
        .chars()
        .filter(|character| !character.is_whitespace() && *character != ',')
        .collect();

    for mapping in [
        "event_curve::CurveEventHandler>(EventType::Curve)",
        "event_dex::DexEventHandler>(EventType::Dex)",
        "event_lp_manager::LpManagerEventHandler>(EventType::LpManager)",
        "event_token::TokenEventHandler>(EventType::Token)",
        "event_price::PriceEventHandler>(EventType::Price)",
        "event_vault::VaultEventHandler>(EventType::Vault)",
        "event_vault_registry::VaultRegistryEventHandler>(EventType::VaultRegistry)",
    ] {
        assert!(normalized.contains(mapping), "missing mapping: {mapping}");
    }

    assert_eq!(
        normalized
            .matches("set.spawn(event_run_event_handler::<")
            .count(),
        7
    );
    assert!(!normalized.contains("PriceUsd"));
    assert!(!normalized.contains("EventType::V2"));
    assert!(!normalized.contains("EventType::Reward"));
    assert!(!normalized.contains("EventType::Creator"));
    assert!(!normalized.contains("EventType::Distributor"));
}

#[test]
fn price_usd_is_defined_but_not_spawned() {
    let main = include_str!("../src/main.rs");
    let common = include_str!("../src/event/common/mod.rs");
    let sync = include_str!("../src/sync/mod.rs");

    assert!(common.contains("pub mod price_usd;"));
    assert!(sync.contains("PriceUsd"));
    assert!(sync.contains("EventType::PriceUsd => \"price_usd\""));
    assert!(!main.contains("PriceUsdEventHandler"));
    assert!(!main.contains("EventType::PriceUsd"));
}

#[test]
fn token_stream_filters_balances_by_token_table_membership() {
    let stream = include_str!("../src/event/common/token/stream.rs");

    assert!(
        stream.contains("cache_manager.token_exists(&token_addr_str).await"),
        "token Transfer filtering must use token-table membership"
    );
    assert!(
        !stream.contains("check_white_list_token"),
        "token Transfer filtering must not use the removed whitelist cache"
    );
}

#[test]
fn configuration_uses_only_generic_giwa_names() {
    let config = include_str!("../src/config.rs");

    for required in [
        "\"BONDING_CURVE\"",
        "\"DEX_FACTORY\"",
        "\"YACHA_ROUTER\"",
        "YACHA_ROUTER_ADDRESS",
        "\"LP_MANAGER\"",
        "\"BURN_VAULT\"",
        "\"LP_VAULT\"",
        "\"CREATOR_FEE_VAULT\"",
        "\"GIFT_VAULT\"",
        "\"DIVIDEND_VAULT\"",
        "\"VAULT_REGISTRY\"",
        "\"DEPLOY_FE_AMOUNT\"",
        "\"GRADUATE_FEE_AMOUNT\"",
        "\"BONDING_CURVE_FEE_RATE\"",
        "\"DEX_ROUTER_FEE_RATE\"",
    ] {
        assert!(config.contains(required), "missing {required}");
    }

    for forbidden in [
        "\"DEX_ROUTER\"",
        "DEX_ROUTER_ADDRESS",
        "\"CREATE_FEE_AMOUNT\"",
        "V1_BONDING_CURVE",
        "V1_DEX_FACTORY",
        "V1_DEX_ROUTER",
        "V1_LP_MANAGER",
        "V1_CREATE_FEE_AMOUNT",
        "V1_GRADUATE_FEE_AMOUNT",
        "V1_BONDING_CURVE_FEE_RATE",
        "V1_DEX_ROUTER_FEE_RATE",
        "V2_BONDING_CURVE",
        "V2_FEE_",
        "V2_LP_MANAGER",
        "V2_BURN_VAULT",
        "V2_LP_VAULT",
        "V2_CREATOR_FEE_VAULT",
        "V2_GIFT_VAULT",
        "V2_DIVIDEND_VAULT",
        "V2_VAULT_REGISTRY",
        "V2_NAD_FUN_FACTORY",
    ] {
        assert!(!config.contains(forbidden), "stale {forbidden}");
    }
}

#[test]
fn active_streams_use_deployed_yacha_router_and_lp_manager_events() {
    let dex = include_str!("../src/event/dex/stream.rs");
    let lp_manager = include_str!("../src/event/lp_manager/stream.rs");

    for required in [
        "abi/YachaRouter.json",
        "YachaRouter::RouterBuy",
        "YachaRouter::RouterSell",
    ] {
        assert!(
            dex.contains(required),
            "missing deployed router ABI: {required}"
        );
    }

    for forbidden in ["abi/GiwaRouter.json", "GiwaRouter::Buy", "GiwaRouter::Sell"] {
        assert!(!dex.contains(forbidden), "stale router ABI: {forbidden}");
    }

    for required in [
        "abi/LPManager.json",
        "LPManager::Allocate",
        "LPManager::Collect",
    ] {
        assert!(
            lp_manager.contains(required),
            "missing deployed LPManager ABI: {required}"
        );
    }

    for forbidden in [
        "abi/ILpManager.json",
        "LpManagerAllocate",
        "LpManagerCollect",
        ".config().call()",
    ] {
        assert!(
            !lp_manager.contains(forbidden),
            "stale LPManager integration: {forbidden}"
        );
    }
}

#[test]
fn lp_manager_collect_contract_is_quote_only() {
    let stream = include_str!("../src/event/lp_manager/stream.rs");
    let types = include_str!("../src/types/lp_manager.rs");
    let abi: serde_json::Value =
        serde_json::from_str(include_str!("../abi/LPManager.json")).expect("valid LPManager ABI");
    let collect_type = types
        .split("pub struct Collect")
        .nth(1)
        .expect("Collect type")
        .split("pub enum LpManagerEvent")
        .next()
        .expect("Collect fields");
    let collect_event = abi
        .as_array()
        .expect("ABI array")
        .iter()
        .find(|item| item["type"] == "event" && item["name"] == "Collect")
        .expect("Collect ABI event");
    let input_names: Vec<&str> = collect_event["inputs"]
        .as_array()
        .expect("Collect inputs")
        .iter()
        .map(|input| input["name"].as_str().expect("input name"))
        .collect();

    assert!(stream.contains("\"Collect(address,address,uint256,uint256)\""));
    assert!(!stream.contains("LegacyLPManagerEvents"));
    assert!(!collect_type.contains("token_amount"));
    assert_eq!(input_names, ["token", "pool", "quoteAmount", "timestamp"]);
}

#[test]
fn lp_collect_persistence_uses_quote_only_columns() {
    let controller = include_str!("../src/db/postgres/controller/lp.rs");
    let collect_sql = controller
        .split("pub const HANDLE_LP_COLLECT_SQL")
        .nth(1)
        .expect("single collect SQL")
        .split("pub const BATCH_HANDLE_LP_ALLOCATE_SQL")
        .next()
        .expect("single collect SQL body");
    let batch_collect_sql = controller
        .split("pub const BATCH_HANDLE_LP_COLLECT_SQL")
        .nth(1)
        .expect("batch collect SQL")
        .split("pub struct LpController")
        .next()
        .expect("batch collect SQL body");

    for forbidden in ["token_amount", "c_amount", "ft_amount", "ct_amount"] {
        assert!(
            !collect_sql.contains(forbidden),
            "single collect SQL still references {forbidden}"
        );
        assert!(
            !batch_collect_sql.contains(forbidden),
            "batch collect SQL still references {forbidden}"
        );
    }
}

#[test]
fn lp_collect_migrations_define_quote_only_schema() {
    let init = include_str!("../migrations/0001_init.sql");
    let collect_table = init
        .split("CREATE TABLE IF NOT EXISTS lp_collect_history")
        .nth(1)
        .expect("lp_collect_history table")
        .split("CREATE OR REPLACE FUNCTION update_lp_collect_status_from_collect")
        .next()
        .expect("lp_collect_history definition");

    for forbidden in ["token_amount", "c_amount", "ft_amount", "ct_amount"] {
        assert!(
            !collect_table.contains(forbidden),
            "fresh lp_collect_history still defines {forbidden}"
        );
    }

    let migration_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/0002_lp_collect_quote_only.sql");
    let migration = std::fs::read_to_string(&migration_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", migration_path.display()));
    for retired in ["token_amount", "c_amount", "ft_amount", "ct_amount"] {
        assert!(
            migration.contains(&format!("DROP COLUMN IF EXISTS {retired}")),
            "existing-database migration does not drop {retired}"
        );
    }
}

#[test]
fn giwa_event_types_include_vault_streams() {
    let sync = include_str!("../src/sync/mod.rs");

    for required in [
        "EventType::Vault => \"vault\"",
        "EventType::VaultRegistry => \"vault_registry\"",
    ] {
        assert!(
            sync.contains(required),
            "missing generic vault stream: {required}"
        );
    }

    for forbidden in [
        "V2Vault",
        "v2_vault",
        "V2VaultRegistry",
        "v2_vault_registry",
    ] {
        assert!(
            !sync.contains(forbidden),
            "stale vault stream name: {forbidden}"
        );
    }
}

#[test]
fn curve_receiver_waits_for_its_dependency_before_processing_events() {
    let receiver = include_str!("../src/event/curve/receive.rs");
    let normalized: String = receiver
        .chars()
        .filter(|character| !character.is_whitespace() && *character != ',')
        .collect();

    let dependency_check = normalized
        .find("RECEIVE_MANAGER.check_last_processed_block(to_blockevent_type).await;")
        .expect("active Curve receiver must wait for its Price dependency");
    let event_grouping = normalized
        .find("letevents_by_token=group_events_by_token(events);")
        .expect("active Curve receiver must group events before processing");

    assert!(
        dependency_check < event_grouping,
        "Curve dependency wait must happen before event grouping/processing"
    );
}

#[test]
fn vault_receiver_waits_for_curve_before_processing_events() {
    let receiver = include_str!("../src/event/vault/receive.rs");
    let normalized: String = receiver
        .chars()
        .filter(|character| !character.is_whitespace() && *character != ',')
        .collect();

    let dependency_check = normalized
        .find("RECEIVE_MANAGER.check_last_processed_block(to_blockevent_type).await;")
        .expect("Vault receiver must wait for Curve before processing");
    let processing = normalized
        .find("process_events(eventsdb).await")
        .expect("Vault receiver must process non-empty batches");

    assert!(
        dependency_check < processing,
        "Vault dependency wait must happen before event processing"
    );
}

#[test]
fn vault_receivers_do_not_advance_after_persistence_failure() {
    for (name, receiver) in [
        ("Vault", include_str!("../src/event/vault/receive.rs")),
        (
            "VaultRegistry",
            include_str!("../src/event/vault_registry/receive.rs"),
        ),
    ] {
        assert!(
            receiver.contains("let _ = ack.send(Err(") && receiver.contains("return Err(error);"),
            "{name} receiver must reject persistence failure to the stream"
        );
        assert!(
            !receiver.contains("if let Err(e) = process_events(events, db).await"),
            "{name} receiver must not swallow persistence failure"
        );
    }
}

#[test]
fn vault_streams_do_not_send_partial_parse_batches() {
    for (name, stream) in [
        ("Vault", include_str!("../src/event/vault/stream.rs")),
        (
            "VaultRegistry",
            include_str!("../src/event/vault_registry/stream.rs"),
        ),
    ] {
        assert!(
            stream.contains("let mut batch_failed = false;"),
            "{name} stream must track parse failures"
        );
        assert!(
            stream.contains("if batch_failed"),
            "{name} stream must reject a partial batch"
        );
    }
}

#[test]
fn vault_historical_contract_reads_use_the_event_block() {
    let vault = include_str!("../src/event/vault/mod.rs");
    let registry = include_str!("../src/event/vault_registry/stream.rs");

    assert!(
        vault.contains("fetch_expiry_duration_secs(block_number: u64)")
            && vault.contains("call_contract_at_block"),
        "GiftVault expiryDuration must be read at the setup block"
    );
    assert!(
        registry.contains("call_contract_at_block"),
        "VaultRegistry metadataURI must be read at the register block"
    );
}

#[test]
fn vault_streams_require_canonical_log_coordinates_and_bounded_ranges() {
    for (name, stream) in [
        ("Vault", include_str!("../src/event/vault/stream.rs")),
        (
            "VaultRegistry",
            include_str!("../src/event/vault_registry/stream.rs"),
        ),
    ] {
        assert!(
            !stream.contains("unwrap_or(u64::MAX)"),
            "{name} stream must reject missing log coordinates"
        );
        assert!(
            stream.contains("(block_batch_size / 2).max(1)"),
            "{name} stream must keep RPC ranges above zero"
        );
    }
}

#[test]
fn provider_block_timestamps_never_fall_back_to_wall_clock_time() {
    let rpc = include_str!("../src/client/api.rs");
    assert!(
        !rpc.contains("chrono::Utc::now().timestamp()"),
        "missing indexed blocks must error instead of fabricating a timestamp"
    );
}

#[test]
fn vault_enrichment_fails_closed_instead_of_persisting_guesses() {
    let receiver = include_str!("../src/event/vault/receive.rs");
    let registry = include_str!("../src/event/vault_registry/stream.rs");

    assert!(
        !receiver.contains("EXPIRY_DURATION_FALLBACK_SECS"),
        "Gift expiry RPC failures must not persist a guessed duration"
    );

    let uri_call = registry
        .find("call_contract_at_block(IVaultMetadata::metadataURICall")
        .expect("registry must read metadataURI at the event block");
    let cache_lookup = registry
        .find("controller.fetch_cached_metadata(vault_id)")
        .expect("registry metadata cache lookup missing");
    assert!(
        uri_call < cache_lookup,
        "registry must verify the event-block URI before using cached metadata"
    );
}

#[test]
fn curve_runtime_observability_uses_generic_names() {
    let stream = include_str!("../src/event/curve/stream.rs");
    let receiver = include_str!("../src/event/curve/receive.rs");

    assert!(
        stream.contains("CurveEventChannel::new(\"curve_events\")"),
        "active Curve channel must use the generic monitored name"
    );

    for forbidden in [
        "v2_curve_events",
        "[V2_CURVE]",
        "V2_CURVE_DBG",
        "V2 Curve",
        "v2 curve",
    ] {
        assert!(
            !stream.contains(forbidden),
            "stale Curve stream observability string: {forbidden}"
        );
        assert!(
            !receiver.contains(forbidden),
            "stale Curve receiver observability string: {forbidden}"
        );
    }
}

#[test]
fn active_token_stream_does_not_claim_removed_dex_registration() {
    let token_stream = include_str!("../src/event/common/token/stream.rs");

    assert!(!token_stream.contains("registered by V2Dex stream"));
}
