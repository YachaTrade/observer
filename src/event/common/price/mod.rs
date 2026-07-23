pub mod provider;
pub mod receive;
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[tokio::test]
    async fn price_send_does_not_wait_for_receiver_acknowledgement() {
        let (channel, mut receiver) = super::PriceEventChannel::new("price_no_ack_gate");

        tokio::time::timeout(Duration::from_millis(50), channel.send(vec![], 10, 11))
            .await
            .expect("Price send must not wait for receiver persistence")
            .expect("Price batch must be enqueued");

        let batch = receiver.recv().await.expect("Price batch must be received");
        assert_eq!(batch.to_block, 10);
        assert_eq!(batch.latest_block, 11);
    }

    #[tokio::test]
    async fn price_send_reports_a_closed_receiver_for_supervised_restart() {
        let (channel, receiver) = super::PriceEventChannel::new("price_closed_receiver");
        drop(receiver);

        let error = channel
            .send(vec![], 10, 11)
            .await
            .expect_err("closed receiver must be reported to the stream supervisor");

        assert!(error.to_string().contains("channel closed"));
    }
}
