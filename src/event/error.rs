/// Module-specific error filtering for event stream parsing
///
/// Each module has its own method to check if an error should be skipped (not logged).
/// This allows fine-grained control over which errors are intentional and which are actual bugs.
pub struct SkippableError;

impl SkippableError {
    /// Token module: Errors from token transfer events
    ///
    /// Intentional errors from parse_log:
    /// - "Not a white list token" - Token not in whitelist
    /// - "Unknown event type" - Event signature not recognized
    /// - "from and to are same" - Self-transfer
    ///
    /// Additional filters:
    /// - "Duplicate" - Duplicate event
    /// - "Mint event skip" - Mint events are intentionally skipped
    /// - "from and to are both None" - Invalid transfer
    pub fn should_skip_token(error_msg: &str) -> bool {
        error_msg.starts_with("Unknown event type")
            || error_msg.starts_with("Not a white list token")
            || error_msg.starts_with("from and to are same")
    }

    /// Curve module: Errors from bonding curve events
    ///
    /// Intentional errors from parse_log:
    /// - "Not a white list token" - Token not in whitelist
    /// - "Unknown event type" - Event signature not recognized
    /// - "Lock event not implemented" - Lock events not yet implemented
    /// - "Fail to fetch token metadata" - Metadata fetch failed (non-critical)
    ///
    /// Additional filters:
    /// - "Duplicate" - Duplicate event
    /// - "Invalid" - Invalid data
    /// - "Client error" - RPC client error
    pub fn should_skip_curve(error_msg: &str) -> bool {
        error_msg.starts_with("Unknown event type")
            || error_msg.starts_with("Not a white list token")
            || error_msg.starts_with("Lock event not implemented")
    }

    /// DEX module: Errors from DEX swap events
    ///
    /// Intentional errors from parse_log:
    /// - "Not a DexRouter address" - Not a recognized DEX router
    /// - "Not a white list dex address" - DEX not in whitelist
    /// - "Unknown event type" - Event signature not recognized
    /// - "DEX pair not found" - Pair address not found
    ///
    /// Additional filters:
    /// - "Duplicate" - Duplicate event
    pub fn should_skip_dex(error_msg: &str) -> bool {
        error_msg.starts_with("Unknown event type")
            || error_msg.starts_with("Not a DexRouter address")
            || error_msg.starts_with("DEX pair not found")
            || error_msg.starts_with("Not a white list dex address")
    }

    /// Reward module: Errors from reward pool events
    ///
    /// Intentional errors from parse_log:
    /// - "Unknown event type" - Event signature not recognized
    ///
    /// Additional filters:
    /// - "Duplicate" - Duplicate event
    /// - "Not a factory address" - Not a recognized factory
    /// - "Not a white list curve" - Curve not in whitelist
    pub fn should_skip_reward(error_msg: &str) -> bool {
        error_msg.starts_with("Unknown event type")
    }

    /// LP Manager module: Errors from LP manager events
    ///
    /// Intentional errors from parse_log:
    /// - "Unknown event type" - Event signature not recognized
    ///
    /// Additional filters:
    /// - "Not a lp manager" - Not a recognized LP manager
    pub fn should_skip_lp_manager(error_msg: &str) -> bool {
        error_msg.starts_with("Unknown event type")
    }

    /// Creator Treasury module: Errors from creator treasury events
    ///
    /// Intentional errors from parse_log:
    /// - "Unknown event type" - Event signature not recognized
    ///
    /// Additional filters:
    /// - "Duplicate" - Duplicate event
    /// - "Not a factory address" - Not a recognized factory
    /// - "Not a white list curve" - Curve not in whitelist
    pub fn should_skip_creator(error_msg: &str) -> bool {
        error_msg.starts_with("Duplicate")
            || error_msg.starts_with("Unknown event type")
            || error_msg.starts_with("Not a factory address")
            || error_msg.starts_with("Not a white list curve")
    }
}
