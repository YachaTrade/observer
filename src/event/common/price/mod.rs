pub mod provider;
pub mod receive;
pub mod sampler;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::{
    event::core::{EventBatch, EventChannel},
    sync::EventType,
    types::price::UpdatePrice,
};

use crate::event::handler::{EventHandler, run_event_handler};
pub type PriceEventBatch = EventBatch<UpdatePrice>;
pub type PriceEventChannel = EventChannel<UpdatePrice>;

pub struct PriceEventHandler;

impl EventHandler for PriceEventHandler {
    type Event = Vec<UpdatePrice>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<PriceEventHandler>(event_type).await
}
