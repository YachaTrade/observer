pub mod provider;
pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::{sync::EventType, types::price::UpdatePrice};

use crate::event::handler::{EventHandler, run_event_handler};
pub type PriceEventChannel = crate::event::core::AcknowledgedEventChannel<UpdatePrice>;
pub type PriceEventBatch = crate::event::core::AcknowledgedEventBatch<UpdatePrice>;

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

#[cfg(test)]
mod tests {
    use crate::{event::core::AcknowledgedEventChannel, types::price::UpdatePrice};

    #[tokio::test]
    async fn acknowledged_price_channel_propagates_receive_failure() {
        let (channel, mut receiver) = AcknowledgedEventChannel::new("price_ack_failure");
        let receive = tokio::spawn(async move {
            let batch: crate::event::core::AcknowledgedEventBatch<UpdatePrice> =
                receiver.recv().await.unwrap();
            batch
                .ack
                .send(Err("price persistence failed".to_string()))
                .unwrap();
        });

        let error = channel.send(vec![], 10, 11).await.unwrap_err();
        assert!(error.to_string().contains("price persistence failed"));
        receive.await.unwrap();
    }
}
