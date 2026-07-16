pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::{
    event::core::{EventBatch, EventChannel},
    sync::EventType,
    types::v2::curve::V2CurveEvent,
};

use crate::event::handler::{EventHandler, run_event_handler};
pub type V2CurveEventBatch = EventBatch<V2CurveEvent>;
pub type V2CurveEventChannel = EventChannel<V2CurveEvent>;

pub struct V2CurveEventHandler;

impl EventHandler for V2CurveEventHandler {
    type Event = Vec<V2CurveEvent>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<V2CurveEventHandler>(event_type).await
}
