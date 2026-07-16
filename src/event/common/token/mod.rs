pub mod lp_position;
pub mod receive;
pub mod stream;

use anyhow::Result;

use std::{future::Future, pin::Pin};

use crate::{
    event::core::{EventBatch, EventChannel},
    sync::EventType,
    types::token::TokenEvent,
};

use crate::event::handler::{EventHandler, run_event_handler};
pub type TokenEventBatch = EventBatch<TokenEvent>;
pub type TokenEventChannel = EventChannel<TokenEvent>;

pub struct TokenEventHandler;

impl EventHandler for TokenEventHandler {
    type Event = Vec<TokenEvent>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream::stream_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<TokenEventHandler>(event_type).await
}
