use std::{sync::Arc, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;
use tokio::{sync::Semaphore, task::JoinSet, time::Instant};
use tracing::{error, instrument, warn};

use crate::{
    client::RpcClient,
    config::{BLOCK_BATCH_SIZE, VAULT_REGISTRY_ADDRESS},
    db::postgres::{PostgresDatabase, controller::VaultRegistryController},
    event::get_block_timestamp,
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::vault_registry::{
        RegisteredVaultType, VaultDeactivate, VaultRegister, VaultRegistryEvent,
    },
    utils::vault_metadata::fetch_vault_metadata,
};

use super::VaultRegistryEventChannel;

const MAX_CONCURRENT_LOG_TASKS: usize = 16;
const MAX_LOG_RETRIES: u32 = 5;

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    VaultRegistry,
    "abi/VaultRegistry.json"
}

sol! {
    #[allow(missing_docs)]
    interface IVaultMetadata {
        function metadataURI() external view returns (string);
    }
}

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    if VAULT_REGISTRY_ADDRESS.is_empty() {
        warn!("[VAULT_REGISTRY] address not configured, skipping");
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }

    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let mut total_events = 0;
    let mut consecutive_log_failures = 0_u32;
    let (channel, receiver) = VaultRegistryEventChannel::new("vault_registry_events");

    tokio::spawn(async move {
        if let Err(e) = super::receive::receive_events(receiver, event_type).await {
            error!("Failed to receive vault registry events: {}", e);
        }
    });

    let client = RpcClient::instance()?;
    let address = VAULT_REGISTRY_ADDRESS.parse::<Address>().unwrap();

    loop {
        let latest_block = client.get_cached_latest_block();
        let time = Instant::now();
        let BlockRange {
            from_block,
            to_block,
        } = STREAM_MANAGER
            .get_next_block_range(event_type, block_batch_size, latest_block)
            .await;

        if from_block > to_block {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let filter = Filter::new()
            .from_block(BlockNumberOrTag::Number(from_block))
            .to_block(BlockNumberOrTag::Number(to_block))
            .address(address)
            .events(vec![
                VaultRegistry::Register::SIGNATURE,
                VaultRegistry::Deactivate::SIGNATURE,
            ]);

        let logs = match client.get_logs(filter).await {
            Ok(logs) => logs,
            Err(e) => {
                consecutive_log_failures += 1;
                if consecutive_log_failures >= MAX_LOG_RETRIES {
                    return Err(anyhow::anyhow!(
                        "vault registry get_logs failed {MAX_LOG_RETRIES} consecutive times: {e}"
                    ));
                }
                block_batch_size = (block_batch_size / 2).max(1);
                error!("[VAULT_REGISTRY] Failed to get logs: {}", e);
                let backoff_ms = 250 * (1_u64 << (consecutive_log_failures - 1));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }
        };
        consecutive_log_failures = 0;

        let logs_count = logs.len();
        let mut events: Vec<VaultRegistryEvent> = Vec::new();
        let mut batch_failed = false;

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_LOG_TASKS));
        for log in logs {
            let permit = semaphore.clone().acquire_owned().await?;
            join_set.spawn(async move {
                let _permit = permit;
                let client = RpcClient::instance().unwrap();
                let result = parse_log(log.clone(), client).await;
                (log, result)
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((log, parse_result)) => match parse_result {
                    Ok(event) => events.push(event),
                    Err(e) => {
                        batch_failed = true;
                        error!(
                            error = %e,
                            log = ?log,
                            "[VAULT_REGISTRY] Failed to parse log"
                        );
                    }
                },
                Err(join_err) => {
                    batch_failed = true;
                    error!("[VAULT_REGISTRY] Task join error: {}", join_err);
                }
            }
        }

        if batch_failed {
            return Err(anyhow::anyhow!(
                "vault registry rejected partial block range {from_block}-{to_block}"
            ));
        }

        events.sort_by(|a, b| {
            (a.block_number(), a.transaction_index(), a.log_index()).cmp(&(
                b.block_number(),
                b.transaction_index(),
                b.log_index(),
            ))
        });

        let events_count = events.len();
        total_events += events_count;
        let elapsed_ms = time.elapsed().as_millis();

        channel.send(events, to_block, to_block).await?;

        warn!(
            "📊 {:?} STREAM: Blocks: from={} to={} | Logs: {} | Events: {} | Total Events: {} | Process time: {}ms",
            event_type, from_block, to_block, logs_count, events_count, total_events, elapsed_ms
        );

        block_batch_size = *BLOCK_BATCH_SIZE;

        STREAM_MANAGER
            .set_event_block_processed_block(event_type, to_block)
            .await;
    }
}

async fn parse_log(log: Log, client: &RpcClient) -> Result<VaultRegistryEvent> {
    let transaction_hash = log
        .transaction_hash
        .ok_or(anyhow::anyhow!("No transaction hash"))?
        .to_string();

    let block_number = log
        .block_number
        .ok_or_else(|| anyhow::anyhow!("No block number"))?;

    let block_timestamp = match log.block_timestamp {
        Some(timestamp) => timestamp,
        None => get_block_timestamp(client, block_number).await?,
    };

    let log_index = log
        .log_index
        .ok_or_else(|| anyhow::anyhow!("No log index"))?;
    let transaction_index = log
        .transaction_index
        .ok_or_else(|| anyhow::anyhow!("No transaction index"))?;

    match log.topic0() {
        Some(&VaultRegistry::Register::SIGNATURE_HASH) => {
            let VaultRegistry::Register {
                vault,
                name,
                creator,
                vaultType,
            } = log.log_decode()?.inner.data;

            let vault_addr = vault;
            let vault_str = vault.to_string();
            let vault_type = RegisteredVaultType::from_u8(vaultType)?;

            // Cache-first (same pattern as fetch_token_metadata): reuse
            // metadata already stored in v2_vault_metadata to skip the
            // eth_call + HTTP round-trip on reorg / sync replays.
            let (metadata_uri, metadata) =
                fetch_vault_details(client, vault_addr, &vault_str, block_number).await?;

            Ok(VaultRegistryEvent::Register(VaultRegister {
                vault: Arc::new(vault_str),
                name: Arc::new(name),
                creator: Arc::new(creator.to_string()),
                vault_type,
                metadata_uri: Some(Arc::new(metadata_uri)),
                metadata,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            }))
        }

        Some(&VaultRegistry::Deactivate::SIGNATURE_HASH) => {
            let VaultRegistry::Deactivate { vault, active } = log.log_decode()?.inner.data;

            Ok(VaultRegistryEvent::Deactivate(VaultDeactivate {
                vault: Arc::new(vault.to_string()),
                active,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            }))
        }

        _ => Err(anyhow::anyhow!("Unknown vault registry event type")),
    }
}

/// Resolve vault metadata with DB-first caching (mirrors
/// `fetch_token_metadata`'s token_metadata-table lookup).
///
///   1. Cache hit in v2_vault_metadata → return it, skip eth_call + HTTP.
///   2. Cache miss → eth_call metadataURI(), then HTTP fetch the JSON.
///
/// URI is returned even when the HTTP parse fails so the DB row still
/// carries `metadata_uri` for later backfill.
async fn fetch_vault_details(
    client: &RpcClient,
    vault: Address,
    vault_id: &str,
    block_number: u64,
) -> Result<(String, Option<crate::types::vault_registry::VaultMetadata>)> {
    // 1. Resolve the canonical URI at the Register event block.
    let uri: String = client
        .call_contract_at_block(IVaultMetadata::metadataURICall {}, vault, block_number)
        .await?;

    // 2. Reuse cached JSON only when it belongs to the same canonical URI.
    if let Ok(db) = PostgresDatabase::instance() {
        let controller = VaultRegistryController::new(db);
        match controller.fetch_cached_metadata(vault_id).await {
            Ok(Some((cached_uri, md))) if cached_uri == uri => {
                tracing::info!(
                    "[VAULT_REGISTRY] metadata cache hit for {} (uri={})",
                    vault_id,
                    uri
                );
                return Ok((uri, Some(md)));
            }
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "[VAULT_REGISTRY] cache lookup failed for {}: {:#}",
                    vault_id, e
                );
            }
        }
    }

    // 3. Cache miss or changed URI — fetch the allowlisted off-chain JSON.
    let metadata = match fetch_vault_metadata(&uri).await {
        Ok(md) => Some(md),
        Err(e) => {
            warn!(
                "[VAULT_REGISTRY] HTTP fetch failed for vault {} uri {}: {:#}",
                vault, uri, e
            );
            None
        }
    };

    Ok((uri, metadata))
}
