pub mod provider;
pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::{
    event::core::{AcknowledgedEventBatch, AcknowledgedEventChannel},
    sync::EventType,
    types::price::UpdatePrice,
};

use crate::event::handler::{EventHandler, run_event_handler};
pub type PriceEventBatch = AcknowledgedEventBatch<UpdatePrice>;
pub type PriceEventChannel = AcknowledgedEventChannel<UpdatePrice>;

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
    async fn price_send_waits_for_receiver_persistence() {
        let (channel, mut receiver) = super::PriceEventChannel::new("price_ack_gate");
        let receive = tokio::spawn(async move {
            let batch = receiver.recv().await.expect("Price batch must be received");
            assert_eq!(batch.to_block, 10);
            assert_eq!(batch.latest_block, 11);
            tokio::time::sleep(Duration::from_millis(25)).await;
            batch.ack.send(Ok(())).unwrap();
        });

        channel.send(vec![], 10, 11).await.unwrap();
        receive.await.unwrap();
    }

    #[tokio::test]
    async fn price_send_propagates_receiver_persistence_failure() {
        let (channel, mut receiver) = super::PriceEventChannel::new("price_ack_failure");
        let receive = tokio::spawn(async move {
            let batch = receiver.recv().await.unwrap();
            batch
                .ack
                .send(Err("database write failed".to_string()))
                .unwrap();
        });

        let error = channel.send(vec![], 10, 11).await.unwrap_err();
        assert!(error.to_string().contains("database write failed"));
        receive.await.unwrap();
    }
}
