use std::time::Instant;

use anyhow::Result;
use serde_json::to_value;

use crate::{
    db::postgres::{
        PostgresDatabase,
        controller::v2::{
            V2VaultRegistryController, VaultActiveData, VaultMetadataData, VaultRegistryEventData,
        },
    },
    sync::{EventType, receive::RECEIVE_MANAGER},
    types::v2::vault_registry::V2VaultRegistryEvent,
};

use super::VaultRegistryEventBatch;
use crate::metrics::MonitoredReceiver;
use tracing::{instrument, warn};

#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<VaultRegistryEventBatch>,
    event_type: EventType,
) -> Result<()> {
    let mut total_events = 0;
    while let Some(batch) = receiver.recv().await {
        let db = PostgresDatabase::instance()?;
        let VaultRegistryEventBatch {
            events,
            to_block,
            latest_block,
            ack,
        } = batch;

        let time = Instant::now();
        let event_count = events.len();
        total_events += event_count;

        if !events.is_empty()
            && let Err(error) = process_events(events, db).await
        {
            let _ = ack.send(Err(format!("{error:#}")));
            return Err(error);
        }

        let elapsed_ms = time.elapsed().as_millis();
        warn!(
            "📊 {:?} Receiver: Events: {} | Total Events: {} | Process time: {}ms | To Block: {} | Latest Block: {}",
            event_type, event_count, total_events, elapsed_ms, to_block, latest_block,
        );
        RECEIVE_MANAGER
            .set_last_processed_block(event_type, to_block, latest_block)
            .await;
        let _ = ack.send(Ok(()));
    }

    Ok(())
}

async fn process_events(
    events: Vec<V2VaultRegistryEvent>,
    db: std::sync::Arc<PostgresDatabase>,
) -> Result<()> {
    let controller = V2VaultRegistryController::new(db);

    let mut registry_batch: Vec<VaultRegistryEventData> = Vec::new();
    let mut metadata_batch: Vec<VaultMetadataData> = Vec::new();
    let mut active_batch: Vec<VaultActiveData> = Vec::new();

    for event in events {
        match event {
            V2VaultRegistryEvent::Register(e) => {
                registry_batch.push(VaultRegistryEventData {
                    vault_id: (*e.vault).clone(),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                });

                let metadata_json = e.metadata.as_ref().and_then(|m| to_value(m).ok());
                let metadata_fetched_at = if metadata_json.is_some() {
                    Some(e.block_timestamp as i64)
                } else {
                    None
                };

                metadata_batch.push(VaultMetadataData {
                    vault_id: (*e.vault).clone(),
                    name: (*e.name).clone(),
                    creator: (*e.creator).clone(),
                    vault_type: e.vault_type.as_str().to_string(),
                    metadata_uri: e.metadata_uri.as_ref().map(|u| (**u).clone()),
                    metadata: metadata_json,
                    metadata_fetched_at,
                    registered_at: e.block_timestamp as i64,
                });
            }
            V2VaultRegistryEvent::Deactivate(e) => {
                active_batch.push(VaultActiveData {
                    vault_id: (*e.vault).clone(),
                    active: e.active,
                    updated_at: e.block_timestamp as i64,
                });
            }
        }
    }

    // Order matters: insert Register event rows + upsert metadata FIRST,
    // then apply any Deactivate updates. Within a single batch the stream
    // stage already sorted events by (block, tx_index, log_index), but
    // Register/Deactivate may target the same vault across batches; the
    // SQL guards (registered_at / updated_at comparisons) keep us safe
    // across batches regardless.
    let (r1, r2) = tokio::join!(
        controller.batch_insert_registry_events(&registry_batch),
        controller.upsert_vault_metadata_batch(&metadata_batch),
    );
    r1?;
    r2?;
    controller.update_vault_active_batch(&active_batch).await?;

    Ok(())
}
