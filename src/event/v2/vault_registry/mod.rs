pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::{
    event::core::{AcknowledgedEventBatch, AcknowledgedEventChannel},
    sync::EventType,
    types::v2::vault_registry::V2VaultRegistryEvent,
};

use crate::event::handler::{EventHandler, run_event_handler};

pub type VaultRegistryEventBatch = AcknowledgedEventBatch<V2VaultRegistryEvent>;
pub type VaultRegistryEventChannel = AcknowledgedEventChannel<V2VaultRegistryEvent>;

pub struct VaultRegistryEventHandler;

impl EventHandler for VaultRegistryEventHandler {
    type Event = Vec<V2VaultRegistryEvent>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<VaultRegistryEventHandler>(event_type).await
}
