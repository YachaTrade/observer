pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use alloy::{primitives::Address, sol};
use anyhow::{Context, Result};

use crate::{
    client::RpcClient,
    config::GIFT_VAULT_ADDRESS,
    event::core::{AcknowledgedEventBatch, AcknowledgedEventChannel},
    sync::EventType,
    types::v2::vault::V2VaultEvent,
};

use crate::event::handler::{EventHandler, run_event_handler};
pub type VaultEventBatch = AcknowledgedEventBatch<V2VaultEvent>;
pub type VaultEventChannel = AcknowledgedEventChannel<V2VaultEvent>;

pub struct VaultEventHandler;

impl EventHandler for VaultEventHandler {
    type Event = Vec<V2VaultEvent>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<VaultEventHandler>(event_type).await
}

// ===========================================================================
// GiftVault.expiryDuration() — fetched on-demand per SETUP event.
//
// Source of truth: the GiftVault contract itself. The indexer used to read
// the duration from a GIFT_EXPIRY_DURATION env var, which silently drifted
// whenever the contract owner called setExpiryDuration(). We now call the
// contract per SETUP so the stamp on v2_gifts.expires_at always reflects
// the on-chain value in effect at the time the gift was created — no env
// coordination, no startup cache that goes stale mid-stream.
//
// Cost: +1 RPC roundtrip per GiftVault.Setup event. SETUP frequency is
// low (gift creation is a deliberate user action, not high-volume swap
// flow) so this is acceptable. If SETUP volume ever grows enough to
// matter, swap the on-demand fetch for OnceLock + ExpiryUpdate-event-
// driven cache refresh (we already index v2_gift_expiry_updates).
// ===========================================================================

sol! {
    #[allow(missing_docs)]
    interface IGiftVault {
        function expiryDuration() external view returns (uint256);
    }
}

/// Call `GiftVault.expiryDuration()` on the configured contract and
/// return the result in seconds. Errors include: GIFT_VAULT unset,
/// address parse failure, RPC timeout/error, return value overflow.
///
/// Used by the SETUP event handler to compute `expires_at` =
/// block_timestamp + this value. Errors are propagated so an unavailable
/// archival read cannot persist a guessed expiry.
pub async fn fetch_expiry_duration_secs(block_number: u64) -> Result<i64> {
    let addr_str = GIFT_VAULT_ADDRESS.as_str();
    if addr_str.is_empty() {
        anyhow::bail!("GIFT_VAULT unset");
    }
    let addr: Address = addr_str.parse().context("GIFT_VAULT parse")?;
    let rpc = RpcClient::instance()?;
    let value = rpc
        .call_contract_at_block(IGiftVault::expiryDurationCall {}, addr, block_number)
        .await
        .context("GiftVault.expiryDuration() RPC")?;
    let secs: i64 = value
        .try_into()
        .map_err(|_| anyhow::anyhow!("GiftVault.expiryDuration() overflows i64: {value}"))?;
    Ok(secs)
}

pub(crate) fn compute_gift_expires_at(block_timestamp: u64, duration_secs: i64) -> Result<i64> {
    let block_timestamp =
        i64::try_from(block_timestamp).context("GiftVault SETUP block timestamp overflows i64")?;
    block_timestamp
        .checked_add(duration_secs)
        .context("GiftVault SETUP expires_at overflows i64")
}

#[cfg(test)]
mod tests {
    use super::compute_gift_expires_at;

    #[test]
    fn gift_expiry_rejects_timestamp_overflow() {
        assert!(compute_gift_expires_at(u64::MAX, 0).is_err());
    }

    #[test]
    fn gift_expiry_rejects_duration_addition_overflow() {
        assert!(compute_gift_expires_at(i64::MAX as u64, 1).is_err());
    }
}
