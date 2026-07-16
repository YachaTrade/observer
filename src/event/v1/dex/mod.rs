pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::{
    event::core::{EventBatch, EventChannel},
    sync::EventType,
    types::v1::dex::DexEvent,
};

use crate::event::handler::{EventHandler, run_event_handler};
pub type DexEventBatch = EventBatch<DexEvent>;
pub type DexEventChannel = EventChannel<DexEvent>;

pub struct DexEventHandler;

impl EventHandler for DexEventHandler {
    type Event = Vec<DexEvent>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<DexEventHandler>(event_type).await
}
