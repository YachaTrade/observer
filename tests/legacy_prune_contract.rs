const MIGRATION: &str = include_str!("../migrations/0001_init.sql");
const README: &str = include_str!("../README.md");
const AGENTS: &str = include_str!("../AGENTS.md");
const ACTIVE_EVENT_DOCS: &str = concat!(
    include_str!("../docs/event-indexing.md"),
    include_str!("../docs/event/common/price.md"),
    include_str!("../docs/event/common/token.md"),
    include_str!("../docs/event/curve.md"),
    include_str!("../docs/event/dex.md"),
    include_str!("../docs/event/dividend.md"),
    include_str!("../docs/event/lp-manager.md"),
    include_str!("../docs/event/vault.md"),
    include_str!("../docs/event/vault_registry.md"),
);

#[test]
fn price_usd_schema_is_present() {
    assert!(MIGRATION.contains("CREATE TABLE IF NOT EXISTS price_usd"));
    assert!(MIGRATION.contains("PRIMARY KEY (token_id, block_number)"));
    assert!(MIGRATION.contains("price_usd_source_id VARCHAR(42)"));
}

#[test]
fn price_usd_uses_quote_token_targets() {
    let stream = include_str!("../src/event/common/price_usd/stream.rs");
    assert!(stream.contains("FROM quote_token"));
    assert!(stream.contains("price_usd_source_id IS NOT NULL"));
    assert!(!stream.contains("whitelist_token"));
}

#[test]
fn price_usd_receive_failure_does_not_advance() {
    let receiver = include_str!("../src/event/common/price_usd/receive.rs");
    let error_pos = receiver.find("batch_insert_price_usd").unwrap();
    let advance_pos = receiver.find("set_last_processed_block").unwrap();
    assert!(error_pos < advance_pos);
    assert!(receiver.contains("let _ = ack.send(Err"));
    assert!(receiver.contains("let _ = ack.send(Ok(()))"));
}

#[test]
fn price_usd_target_query_failure_aborts_the_range() {
    let stream = include_str!("../src/event/common/price_usd/stream.rs");
    assert!(stream.contains("let targets = load_price_usd_targets().await?;"));
}

#[test]
fn price_usd_timestamp_failure_aborts_the_range() {
    let stream = include_str!("../src/event/common/price_usd/stream.rs");
    assert!(stream.contains("collect_block_timestamps(from_block, to_block"));
    assert!(stream.contains("get_block_timestamp(client, block_number)"));
}

#[test]
fn price_usd_waits_for_receiver_ack_before_stream_checkpoint() {
    let module = include_str!("../src/event/common/price_usd/mod.rs");
    let stream = include_str!("../src/event/common/price_usd/stream.rs");
    assert!(module.contains("AcknowledgedEventBatch<PriceUsdRow>"));
    assert!(module.contains("AcknowledgedEventChannel<PriceUsdRow>"));

    let send_pos = stream.find("channel.send(events").unwrap();
    let checkpoint_pos = stream.find("set_event_block_processed_block").unwrap();
    assert!(send_pos < checkpoint_pos);
    assert!(stream.contains("channel.send(events, to_block, latest_block).await?;"));
}

#[test]
fn price_usd_cache_lookup_is_latest_at_or_before_block() {
    let cache = include_str!("../src/db/cache/mod.rs");
    assert!(cache.contains("SELECT price FROM price_usd"));
    assert!(cache.contains("WHERE token_id = $1 AND block_number <= $2"));
    assert!(cache.contains("ORDER BY block_number DESC"));
    assert!(cache.contains("pub async fn get_price_usd_before"));
}

#[test]
fn set_fee_protocol_has_no_indexing_surface() {
    let dex = include_str!("../src/types/dex.rs");
    let fee = include_str!("../src/db/postgres/controller/fee.rs");
    let docs = include_str!("../docs/event-indexing.md");
    assert!(!dex.contains("struct SetFeeProtocol"));
    assert!(!fee.contains("set_fee_protocol"));
    assert!(!MIGRATION.contains("set_fee_history"));
    assert!(!docs.contains("SetFeeProtocol"));
}

#[test]
fn v2_pair_share_indexing_is_absent() {
    let token_stream = include_str!("../src/event/common/token/stream.rs");
    let token_types = include_str!("../src/types/token.rs");
    assert!(!token_stream.contains("parse_lp_position_log"));
    assert!(!token_types.contains("LpPositionHistoryEvent"));
    for object in [
        "lp_event_type",
        "lp_position_history",
        "lp_position_cost_basis",
        "pool_fee_hourly",
        "pool_apr",
    ] {
        assert!(!MIGRATION.contains(object), "stale schema object: {object}");
    }
}

#[test]
fn legacy_curve_module_is_absent() {
    let types = include_str!("../src/types/mod.rs");
    let dex_stream = include_str!("../src/event/dex/stream.rs");
    assert!(!types.contains("legacy_curve"));
    assert!(!dex_stream.contains("legacy_curve"));
    assert!(!dex_stream.contains("MarketType::DEX"));
}

#[test]
fn cache_has_only_canonical_market_and_quote_apis() {
    let cache = include_str!("../src/db/cache/mod.rs");
    let redis = include_str!("../src/db/redis/mod.rs");
    assert!(!cache.contains("V2_DEX"));
    assert!(!cache.contains("pub async fn get_price("));
    assert!(!cache.contains("pub async fn insert_price_batch("));
    assert!(!redis.contains("token_curve_v2:"));
    assert!(!redis.contains("token_dev_v2:"));
}

#[test]
fn pool_pair_lookup_prefers_pool_without_a_market_row() {
    let cache = include_str!("../src/db/cache/mod.rs");
    let start = cache.find("pub async fn get_pool_pair").unwrap();
    let end = start + cache[start..].find("// Price 캐시 관련 메서드들").unwrap();
    let lookup = &cache[start..end];

    let pool_query = lookup
        .find("SELECT token0, token1 FROM pool WHERE pool_id = $1")
        .expect("get_pool_pair must query the canonical pool row independently");
    let market_fallback = lookup
        .find("SELECT token_id, quote_id FROM market WHERE pool_id = $1 AND market_type = 'DEX'")
        .expect("get_pool_pair must retain the DEX market graduation fallback");

    assert!(pool_query < market_fallback);
    assert!(!lookup.contains("JOIN pool"));
}

#[test]
fn active_surfaces_have_no_legacy_version_labels() {
    let source_surfaces = concat!(
        include_str!("../src/db/postgres/controller/mod.rs"),
        include_str!("../src/db/postgres/controller/dividend.rs"),
        include_str!("../src/db/postgres/controller/market.rs"),
        include_str!("../src/db/postgres/controller/sniping.rs"),
        include_str!("../src/db/postgres/controller/token.rs"),
        include_str!("../src/db/postgres/controller/vault.rs"),
        include_str!("../src/db/postgres/controller/vault_registry.rs"),
        include_str!("../src/db/redis/mod.rs"),
        include_str!("../src/event/common/price/provider/mod.rs"),
        include_str!("../src/event/curve/receive.rs"),
        include_str!("../src/types/fee.rs"),
        include_str!("common/mod.rs"),
    )
    .to_ascii_lowercase();

    for stale_label in ["v1", "v2", "legacy", "nadfun", "nad.fun", "monad"] {
        assert!(
            !source_surfaces.contains(stale_label),
            "stale source label: {stale_label}"
        );
    }

    let migration = MIGRATION.to_ascii_lowercase();
    for stale_label in ["v1", "v2", "legacy", "nadfun", "nad.fun", "monad"] {
        assert!(
            !migration.contains(stale_label),
            "stale migration label: {stale_label}"
        );
    }

    let active_docs = ACTIVE_EVENT_DOCS.to_ascii_lowercase();
    for stale_label in ["v1", "v2_", "nadfun", "nad.fun", "monad"] {
        assert!(
            !active_docs.contains(stale_label),
            "stale active-document label: {stale_label}"
        );
    }
}

#[test]
fn active_docs_reflect_current_runtime() {
    assert!(README.starts_with("# GIWA Observer"));

    assert!(AGENTS.contains("seven handlers"));
    for handler in [
        "`curve`",
        "`dex`",
        "`lp_manager`",
        "`token`",
        "`price`",
        "`vault`",
        "`vault_registry`",
    ] {
        assert!(
            AGENTS.contains(handler),
            "missing active handler: {handler}"
        );
    }
    assert!(AGENTS.contains("PriceUsd is dormant"));
    for stale_claim in ["NADFUN", "UNISWAPV3", "token.version", "token.chain"] {
        assert!(
            !AGENTS.contains(stale_claim),
            "stale AGENTS claim: {stale_claim}"
        );
    }

    let dex = include_str!("../docs/event/dex.md");
    for active_event in ["Swap", "Mint", "Burn"] {
        assert!(
            dex.contains(active_event),
            "missing active DEX event: {active_event}"
        );
    }
    for stale_claim in [
        "SetFeeProtocol",
        "LpPositionHistory",
        "lp_position_history",
        "lp_position_cost_basis",
        "V2 LP",
    ] {
        assert!(
            !ACTIVE_EVENT_DOCS.contains(stale_claim),
            "stale active-document claim: {stale_claim}"
        );
    }

    let active_overviews = concat!(
        include_str!("../README.md"),
        include_str!("../docs/event-indexing.md"),
        include_str!("../docs/event/curve.md"),
        include_str!("../docs/event/dex.md"),
        include_str!("../docs/event/lp-manager.md"),
    );
    for stale_claim in [
        "Nad.fun Observer",
        "Implementation provenance",
        "implementation versions",
        "v1 LPManager ABI",
        "v2 BondingCurve ABI",
        "v2 vault ABIs",
        "Existing MON rows",
        "historical row",
    ] {
        assert!(
            !active_overviews.contains(stale_claim),
            "stale runtime overview: {stale_claim}"
        );
    }

    let metadata = concat!(
        include_str!("../src/utils/metadata.rs"),
        include_str!("../src/utils/vault_metadata.rs"),
    );
    assert!(metadata.contains("GIWA-Observer/1.0"));
    assert!(!metadata.contains("Nad-Observer/1.0"));

    let vault_types = concat!(
        include_str!("../src/types/vault.rs"),
        include_str!("../src/types/vault_registry.rs"),
    );
    assert!(!vault_types.contains("nadfun-contract-v2"));
}
