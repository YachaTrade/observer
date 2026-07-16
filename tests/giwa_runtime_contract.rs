#[test]
fn main_wires_the_selected_implementations_to_generic_events() {
    let main = include_str!("../src/main.rs");
    let normalized: String = main
        .chars()
        .filter(|character| !character.is_whitespace() && *character != ',')
        .collect();

    for mapping in [
        "event_v2_curve::V2CurveEventHandler>(EventType::Curve)",
        "event_dex::DexEventHandler>(EventType::Dex)",
        "event_lp_manager::LpManagerEventHandler>(EventType::LpManager)",
        "event_token::TokenEventHandler>(EventType::Token)",
        "event_price::PriceEventHandler>(EventType::Price)",
        "event_price_usd::PriceUsdEventHandler>(EventType::PriceUsd)",
    ] {
        assert!(normalized.contains(mapping), "missing mapping: {mapping}");
    }

    assert_eq!(
        normalized
            .matches("set.spawn(event_run_event_handler::<")
            .count(),
        6
    );
    assert!(!normalized.contains("EventType::V2"));
    assert!(!normalized.contains("EventType::Reward"));
    assert!(!normalized.contains("EventType::Creator"));
    assert!(!normalized.contains("EventType::Distributor"));
}

#[test]
fn configuration_uses_only_generic_giwa_names() {
    let config = include_str!("../src/config.rs");

    for required in [
        "\"BONDING_CURVE\"",
        "\"DEX_FACTORY\"",
        "\"DEX_ROUTER\"",
        "\"LP_MANAGER\"",
        "\"CREATE_FEE_AMOUNT\"",
        "\"GRADUATE_FEE_AMOUNT\"",
        "\"BONDING_CURVE_FEE_RATE\"",
        "\"DEX_ROUTER_FEE_RATE\"",
    ] {
        assert!(config.contains(required), "missing {required}");
    }

    for forbidden in [
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
        "V2_NAD_FUN_FACTORY",
    ] {
        assert!(!config.contains(forbidden), "stale {forbidden}");
    }
}

#[test]
fn curve_receiver_waits_for_its_dependency_before_processing_events() {
    let receiver = include_str!("../src/event/v2/curve/receive.rs");
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
fn curve_runtime_observability_uses_generic_names() {
    let stream = include_str!("../src/event/v2/curve/stream.rs");
    let receiver = include_str!("../src/event/v2/curve/receive.rs");

    assert!(
        stream.contains("V2CurveEventChannel::new(\"curve_events\")"),
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
fn active_token_stream_does_not_claim_removed_v2_dex_registration() {
    let token_stream = include_str!("../src/event/common/token/stream.rs");

    assert!(!token_stream.contains("registered by V2Dex stream"));
}
