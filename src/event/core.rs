use std::time::Duration;

use tokio::sync::mpsc::error::SendError;
use tracing::warn;

use crate::metrics::{MonitoredReceiver, MonitoredSender, monitored_channel};

/// Default buffer size applied to event channels when no explicit capacity is requested.
pub const DEFAULT_CHANNEL_BUFFER: usize = 1_000;

/// Batch of events accompanied by block metadata shared across pipelines.
#[derive(Debug, Clone)]
pub struct EventBatch<E> {
    pub events: Vec<E>,
    pub to_block: u64,
    pub latest_block: u64,
}

/// Generic channel wrapper that provides bounded buffering with monitoring hooks.
pub struct EventChannel<E> {
    pub sender: MonitoredSender<EventBatch<E>>,
    buffer_size: usize,
    name: &'static str,
}

impl<E> EventChannel<E> {
    pub fn new(name: &'static str) -> (Self, MonitoredReceiver<EventBatch<E>>) {
        Self::with_capacity(name, DEFAULT_CHANNEL_BUFFER)
    }

    pub fn with_capacity(
        name: &'static str,
        buffer_size: usize,
    ) -> (Self, MonitoredReceiver<EventBatch<E>>) {
        let (sender, receiver) = monitored_channel(name, buffer_size);
        (
            Self {
                sender,
                buffer_size,
                name,
            },
            receiver,
        )
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn capacity(&self) -> usize {
        self.sender.capacity()
    }

    pub async fn send(
        &self,
        events: Vec<E>,
        to_block: u64,
        latest_block: u64,
    ) -> Result<(), SendError<EventBatch<E>>> {
        if self.sender.capacity() == 0 {
            warn!(
                "{} channel is full, waiting for space to become available...",
                self.name
            );

            while self.sender.capacity() == 0 {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }

        self.sender
            .send(EventBatch {
                events,
                to_block,
                latest_block,
            })
            .await
    }
}
