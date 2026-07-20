pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::{
    event::core::{EventBatch, EventChannel},
    sync::EventType,
    types::curve::CurveEvent,
};

use crate::event::handler::{EventHandler, run_event_handler};
pub type CurveEventBatch = EventBatch<CurveEvent>;
pub type CurveEventChannel = EventChannel<CurveEvent>;

pub struct CurveEventHandler;

impl EventHandler for CurveEventHandler {
    type Event = Vec<CurveEvent>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<CurveEventHandler>(event_type).await
}
