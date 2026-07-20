use std::collections::HashSet;
use std::time::Instant;

use anyhow::Result;
use bigdecimal::BigDecimal;

use crate::{
    db::postgres::{
        PostgresDatabase,
        controller::{
            account::AccountController,
            v2::{
                CreatorFeeClaimData, CreatorUpdateData, DividendClaimData, DividendConversionData,
                DividendDepositData, DividendMerkleRootData, DividendSetupData, GiftData,
                GiftExpiryUpdateData, V2DividendController, V2VaultController, VaultBurnData,
                VaultLpInjectData,
            },
        },
    },
    sync::{EventType, receive::RECEIVE_MANAGER},
    types::v2::{
        dividend::V2DividendEvent,
        vault::{V2VaultEvent, VaultType},
    },
};

use super::{VaultEventBatch, compute_gift_expires_at, fetch_expiry_duration_secs};
use crate::metrics::MonitoredReceiver;
use tracing::{instrument, warn};

#[instrument(skip(receiver))]
pub async fn receive_events(
    mut receiver: MonitoredReceiver<VaultEventBatch>,
    event_type: EventType,
) -> Result<()> {
    let mut total_events = 0;
    while let Some(batch) = receiver.recv().await {
        let db = PostgresDatabase::instance()?;
        let VaultEventBatch {
            events,
            to_block,
            latest_block,
            ack,
        } = batch;

        RECEIVE_MANAGER
            .check_last_processed_block(to_block, event_type)
            .await;

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

pub async fn process_events(
    events: Vec<V2VaultEvent>,
    db: std::sync::Arc<PostgresDatabase>,
) -> Result<()> {
    let controller = V2VaultController::new(db.clone());
    let dividend_controller = V2DividendController::new(db.clone());
    let account_controller = AccountController::new(db);

    let mut burn_batch = Vec::new();
    let mut lp_inject_batch = Vec::new();
    let mut creator_claim_batch = Vec::new();
    let mut gift_batch = Vec::new();
    let mut creator_update_batch: Vec<CreatorUpdateData> = Vec::new();
    let mut gift_expiry_batch: Vec<GiftExpiryUpdateData> = Vec::new();
    let mut setup_batch: Vec<DividendSetupData> = Vec::new();
    let mut deposit_batch: Vec<DividendDepositData> = Vec::new();
    let mut conversion_batch: Vec<DividendConversionData> = Vec::new();
    let mut root_batch: Vec<DividendMerkleRootData> = Vec::new();
    let mut claim_batch: Vec<DividendClaimData> = Vec::new();
    // Wallet addresses that need an `account` row created if missing.
    // gift-bot receivers and newly-bound creators often arrive here without
    // any prior trade history, so the curve/dex upsert paths haven't seen
    // them. Collect them here and upsert in one batch.
    let mut account_ids: HashSet<String> = HashSet::new();

    for event in events {
        match event {
            V2VaultEvent::Burn(e) => {
                let vault_type_str = match e.vault_type {
                    VaultType::Burn => "BURN",
                    VaultType::Gift => "GIFT",
                    _ => "BURN",
                };
                burn_batch.push(VaultBurnData {
                    vault_type: vault_type_str.to_string(),
                    token_id: (*e.token).clone(),
                    quote_in: (*e.quote_in).clone(),
                    token_burned: (*e.token_burned).clone(),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: Some((*e.quote_id).clone()),
                    usd_value: (*e.usd_value).clone(),
                });
            }
            V2VaultEvent::LpInject(e) => {
                lp_inject_batch.push(VaultLpInjectData {
                    token_id: (*e.token).clone(),
                    quote_used: (*e.quote_used).clone(),
                    token_used: (*e.token_used).clone(),
                    lp_burned: (*e.lp_burned).clone(),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: Some((*e.quote_id).clone()),
                    usd_value: (*e.usd_value).clone(),
                });
            }
            V2VaultEvent::CreatorDeposit(e) => {
                creator_claim_batch.push(CreatorFeeClaimData {
                    event_type: "DEPOSIT".to_string(),
                    token_id: (*e.token).clone(),
                    creator: None,
                    amount: (*e.amount).clone(),
                    new_balance: Some((*e.new_balance).clone()),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: Some((*e.quote_id).clone()),
                    usd_value: (*e.usd_value).clone(),
                });
            }
            V2VaultEvent::CreatorClaim(e) => {
                account_ids.insert((*e.creator).clone());
                creator_claim_batch.push(CreatorFeeClaimData {
                    event_type: "CLAIM".to_string(),
                    token_id: (*e.token).clone(),
                    creator: Some((*e.creator).clone()),
                    amount: (*e.amount).clone(),
                    new_balance: None,
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: Some((*e.quote_id).clone()),
                    usd_value: (*e.usd_value).clone(),
                });
            }
            V2VaultEvent::GiftVaultSetup(e) => {
                // Per-SETUP RPC: read the current GiftVault.expiryDuration()
                // from the contract so each gift's expires_at reflects the
                // on-chain duration in effect at SETUP time (not a stale
                // cached value if the contract owner has called
                // setExpiryDuration() since process start).
                let duration_secs = fetch_expiry_duration_secs(e.block_number).await?;
                let expires_at = compute_gift_expires_at(e.block_timestamp, duration_secs)?;
                gift_batch.push(GiftData {
                    event_type: "SETUP".to_string(),
                    token_id: (*e.token).clone(),
                    platform: Some(e.platform.as_str().to_string()),
                    platform_id: Some((*e.platform_id).clone()),
                    receiver: None,
                    amount: None,
                    new_balance: None,
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: None,
                    usd_value: BigDecimal::from(0),
                    expires_at,
                });
            }
            V2VaultEvent::GiftDeposit(e) => {
                gift_batch.push(GiftData {
                    event_type: "DEPOSIT".to_string(),
                    token_id: (*e.token).clone(),
                    platform: None,
                    platform_id: None,
                    receiver: None,
                    amount: Some((*e.amount).clone()),
                    new_balance: Some((*e.new_balance).clone()),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: Some((*e.quote_id).clone()),
                    usd_value: (*e.usd_value).clone(),
                    expires_at: 0,
                });
            }
            V2VaultEvent::GiftClaim(e) => {
                account_ids.insert((*e.receiver).clone());
                gift_batch.push(GiftData {
                    event_type: "CLAIM".to_string(),
                    token_id: (*e.token).clone(),
                    platform: None,
                    platform_id: None,
                    receiver: Some((*e.receiver).clone()),
                    amount: Some((*e.amount).clone()),
                    new_balance: None,
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: Some((*e.quote_id).clone()),
                    usd_value: (*e.usd_value).clone(),
                    expires_at: 0,
                });
            }
            V2VaultEvent::GiftExpire(e) => {
                gift_batch.push(GiftData {
                    event_type: "EXPIRE".to_string(),
                    token_id: (*e.token).clone(),
                    platform: None,
                    platform_id: None,
                    receiver: None,
                    amount: Some((*e.amount).clone()),
                    new_balance: None,
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: Some((*e.quote_id).clone()),
                    usd_value: (*e.usd_value).clone(),
                    expires_at: 0,
                });
            }
            V2VaultEvent::CreatorVaultSetup(e) => {
                account_ids.insert((*e.creator).clone());
                creator_update_batch.push(CreatorUpdateData {
                    event_type: "SETUP".to_string(),
                    token_id: (*e.token).clone(),
                    old_creator: None,
                    new_creator: (*e.creator).clone(),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                });
            }
            V2VaultEvent::CreatorUpdate(e) => {
                account_ids.insert((*e.new_creator).clone());
                creator_update_batch.push(CreatorUpdateData {
                    event_type: "UPDATE".to_string(),
                    token_id: (*e.token).clone(),
                    old_creator: Some((*e.old_creator).clone()),
                    new_creator: (*e.new_creator).clone(),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                });
            }
            V2VaultEvent::GiftReceiverSet(e) => {
                account_ids.insert((*e.receiver).clone());
                gift_batch.push(GiftData {
                    event_type: "RECEIVER_SET".to_string(),
                    token_id: (*e.token).clone(),
                    platform: None,
                    platform_id: None,
                    receiver: Some((*e.receiver).clone()),
                    amount: None,
                    new_balance: None,
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                    quote_id: None,
                    usd_value: BigDecimal::from(0),
                    // Receiver bound → gift no longer expires.
                    expires_at: 0,
                });
            }
            V2VaultEvent::GiftExpiryUpdate(e) => {
                gift_expiry_batch.push(GiftExpiryUpdateData {
                    old_duration: (*e.old_duration).clone(),
                    new_duration: (*e.new_duration).clone(),
                    transaction_hash: (*e.transaction_hash).clone(),
                    block_number: e.block_number as i64,
                    created_at: e.block_timestamp as i64,
                    log_index: e.log_index as i32,
                    tx_index: e.transaction_index as i32,
                });
            }
            V2VaultEvent::Dividend(de) => match de {
                V2DividendEvent::Setup(e) => {
                    setup_batch.push(DividendSetupData {
                        source_token: (*e.source_token).clone(),
                        dividend_token: (*e.dividend_token).clone(),
                        ratio: e.ratio,
                        min_balance: (*e.min_balance).clone(),
                        entry_index: e.entry_index as i32,
                        transaction_hash: (*e.coords.transaction_hash).clone(),
                        block_number: e.coords.block_number as i64,
                        created_at: e.coords.block_timestamp as i64,
                        log_index: e.coords.log_index as i32,
                        tx_index: e.coords.transaction_index as i32,
                    });
                }
                V2DividendEvent::Deposit(e) => {
                    deposit_batch.push(DividendDepositData {
                        source_token: (*e.source_token).clone(),
                        dividend_token: (*e.dividend_token).clone(),
                        amount: (*e.amount).clone(),
                        pending: e.pending,
                        entry_index: e.entry_index as i32,
                        transaction_hash: (*e.coords.transaction_hash).clone(),
                        block_number: e.coords.block_number as i64,
                        created_at: e.coords.block_timestamp as i64,
                        log_index: e.coords.log_index as i32,
                        tx_index: e.coords.transaction_index as i32,
                        quote_id: Some((*e.quote_id).clone()),
                        usd_value: (*e.usd_value).clone(),
                    });
                }
                V2DividendEvent::Conversion(e) => {
                    conversion_batch.push(DividendConversionData {
                        source_token: (*e.source_token).clone(),
                        dividend_token: (*e.dividend_token).clone(),
                        consumed_quote: (*e.consumed_quote).clone(),
                        received: (*e.received).clone(),
                        entry_index: e.entry_index as i32,
                        transaction_hash: (*e.coords.transaction_hash).clone(),
                        block_number: e.coords.block_number as i64,
                        created_at: e.coords.block_timestamp as i64,
                        log_index: e.coords.log_index as i32,
                        tx_index: e.coords.transaction_index as i32,
                        quote_id: Some((*e.quote_id).clone()),
                        usd_value: (*e.usd_value).clone(),
                    });
                }
                V2DividendEvent::MerkleRoot(e) => {
                    root_batch.push(DividendMerkleRootData {
                        merkle_root: (*e.merkle_root).clone(),
                        transaction_hash: (*e.coords.transaction_hash).clone(),
                        block_number: e.coords.block_number as i64,
                        created_at: e.coords.block_timestamp as i64,
                        log_index: e.coords.log_index as i32,
                        tx_index: e.coords.transaction_index as i32,
                    });
                }
                V2DividendEvent::Claim(e) => {
                    claim_batch.push(DividendClaimData {
                        holder: (*e.holder).clone(),
                        source_token: (*e.source_token).clone(),
                        dividend_token: (*e.dividend_token).clone(),
                        amount: (*e.amount).clone(),
                        entry_index: e.entry_index as i32,
                        transaction_hash: (*e.coords.transaction_hash).clone(),
                        block_number: e.coords.block_number as i64,
                        created_at: e.coords.block_timestamp as i64,
                        log_index: e.coords.log_index as i32,
                        tx_index: e.coords.transaction_index as i32,
                        usd_value: (*e.usd_value).clone(),
                    });
                }
            },
        }
    }

    // Upsert account rows for any newly-seen receivers / creators before
    // the event batches land, so downstream consumers reading
    // v2_gift_vault_stats.receiver or v2_creator_fee_vault_stats can JOIN
    // against `account` without missing rows.
    if !account_ids.is_empty() {
        let account_list: Vec<String> = account_ids.into_iter().collect();
        account_controller
            .batch_upsert_accounts(&account_list)
            .await?;
    }

    let (r1, r2, r3, r4, r5, r6) = tokio::join!(
        controller.batch_insert_vault_burns(&burn_batch),
        controller.batch_insert_vault_lp_injections(&lp_inject_batch),
        controller.batch_insert_creator_fee_claims(&creator_claim_batch),
        controller.batch_insert_gifts(&gift_batch),
        controller.batch_insert_creator_updates(&creator_update_batch),
        controller.batch_insert_gift_expiry_updates(&gift_expiry_batch),
    );

    r1?;
    r2?;
    r3?;
    r4?;
    r5?;
    r6?;

    // Phase 1: configs + period markers. Ordering lets claim merkle_root
    // subqueries see same-batch roots when the root insert succeeds.
    let (r1, r2) = tokio::join!(
        dividend_controller.batch_insert_dividend_setups(&setup_batch),
        dividend_controller.batch_insert_dividend_merkle_roots(&root_batch),
    );
    r1?;
    r2?;

    // Phase 2: value-moving events.
    let (r3, r4, r5) = tokio::join!(
        dividend_controller.batch_insert_dividend_deposits(&deposit_batch),
        dividend_controller.batch_insert_dividend_conversions(&conversion_batch),
        dividend_controller.batch_insert_dividend_claims(&claim_batch),
    );
    r3?;
    r4?;
    r5?;

    Ok(())
}
