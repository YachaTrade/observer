pub mod receive;
pub mod stream;

use anyhow::Result;

use std::{future::Future, pin::Pin};

use crate::event::handler::{EventHandler, run_event_handler};
use crate::{
    event::core::{EventBatch, EventChannel},
    sync::EventType,
    types::lp_manager::LpManagerEvent,
};

pub type LpManagerEventBatch = EventBatch<LpManagerEvent>;
pub type LpManagerEventChannel = EventChannel<LpManagerEvent>;
pub struct LpManagerEventHandler;

impl EventHandler for LpManagerEventHandler {
    type Event = Vec<LpManagerEvent>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<LpManagerEventHandler>(event_type).await
}
