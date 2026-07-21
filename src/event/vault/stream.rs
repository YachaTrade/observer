use std::{sync::Arc, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;
use bigdecimal::BigDecimal;
use tokio::{sync::Semaphore, task::JoinSet, time::Instant};

use tracing::{error, instrument, warn};

use crate::{
    client::RpcClient,
    config::{
        BLOCK_BATCH_SIZE, BURN_VAULT_ADDRESS, CREATOR_FEE_VAULT_ADDRESS, DIVIDEND_VAULT_ADDRESS,
        GIFT_VAULT_ADDRESS, LP_VAULT_ADDRESS,
    },
    db::cache::CacheManager,
    event::get_block_timestamp,
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::dividend::{
        DividendEvent, DividendMerkleRoot, LogCoords, compose_dividend_claim_usd, explode_claim,
        explode_conversion, explode_deposit, explode_setup,
    },
    types::vault::{
        CreatorClaim, CreatorDeposit, CreatorUpdate, CreatorVaultSetup, GiftClaim, GiftDeposit,
        GiftExpire, GiftExpiryUpdate, GiftPlatform, GiftReceiverSet, GiftVaultSetup, LpInject,
        VaultBurn, VaultEvent, VaultType,
    },
    utils::to_big_decimal,
};

use super::super::usd_enrich::enrich_usd;

use super::VaultEventChannel;

const MAX_CONCURRENT_LOG_TASKS: usize = 16;
const MAX_LOG_RETRIES: u32 = 5;

sol! {
    #[allow(missing_docs, clippy::too_many_arguments)]
    #[sol(rpc)]
    BurnVault,
    "abi/BurnVault.json"
}

sol! {
    #[allow(missing_docs, clippy::too_many_arguments)]
    #[sol(rpc)]
    LPVault,
    "abi/LPVault.json"
}

sol! {
    #[allow(missing_docs, clippy::too_many_arguments)]
    #[sol(rpc)]
    CreatorFeeVault,
    "abi/CreatorFeeVault.json"
}

sol! {
    #[allow(missing_docs, clippy::too_many_arguments)]
    #[sol(rpc)]
    GiftVault,
    "abi/GiftVault.json"
}

sol! {
    #[allow(missing_docs, clippy::too_many_arguments)]
    #[sol(rpc)]
    DividendVault,
    "abi/DividendVault.json"
}

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    let mut addresses: Vec<Address> = Vec::new();
    if !BURN_VAULT_ADDRESS.is_empty() {
        addresses.push(BURN_VAULT_ADDRESS.parse::<Address>().unwrap());
    }
    if !LP_VAULT_ADDRESS.is_empty() {
        addresses.push(LP_VAULT_ADDRESS.parse::<Address>().unwrap());
    }
    if !CREATOR_FEE_VAULT_ADDRESS.is_empty() {
        addresses.push(CREATOR_FEE_VAULT_ADDRESS.parse::<Address>().unwrap());
    }
    if !GIFT_VAULT_ADDRESS.is_empty() {
        addresses.push(GIFT_VAULT_ADDRESS.parse::<Address>().unwrap());
    }
    if !DIVIDEND_VAULT_ADDRESS.is_empty() {
        addresses.push(DIVIDEND_VAULT_ADDRESS.parse::<Address>().unwrap());
    } else {
        warn!("[VAULT] No dividend vault address configured, skipping dividend vault logs");
    }

    if addresses.is_empty() {
        warn!("[VAULT] No vault addresses configured, skipping");
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }

    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let mut total_events = 0;
    let mut consecutive_log_failures = 0_u32;
    let (channel, receiver) = VaultEventChannel::new("vault_events");

    tokio::spawn(async move {
        if let Err(e) = super::receive::receive_events(receiver, event_type).await {
            error!("Failed to receive vault events: {}", e);
        }
    });

    let client = RpcClient::instance()?;

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
            .address(addresses.clone())
            .events(vec![
                BurnVault::Burn::SIGNATURE,
                LPVault::AddLiquidity::SIGNATURE,
                CreatorFeeVault::Deposit::SIGNATURE,
                CreatorFeeVault::Claim::SIGNATURE,
                CreatorFeeVault::VaultSetup::SIGNATURE,
                CreatorFeeVault::CreatorUpdate::SIGNATURE,
                GiftVault::VaultSetup::SIGNATURE,
                GiftVault::Claim::SIGNATURE,
                GiftVault::Expire::SIGNATURE,
                GiftVault::ReceiverSet::SIGNATURE,
                GiftVault::ExpiryUpdate::SIGNATURE,
                DividendVault::DividendSetup::SIGNATURE,
                DividendVault::Deposit::SIGNATURE,
                DividendVault::Converted::SIGNATURE,
                DividendVault::SetMerkleRoot::SIGNATURE,
                DividendVault::Claim::SIGNATURE,
            ]);

        let logs = match client.get_logs(filter).await {
            Ok(logs) => logs,
            Err(e) => {
                consecutive_log_failures += 1;
                if consecutive_log_failures >= MAX_LOG_RETRIES {
                    return Err(anyhow::anyhow!(
                        "vault get_logs failed {MAX_LOG_RETRIES} consecutive times: {e}"
                    ));
                }
                block_batch_size = (block_batch_size / 2).max(1);
                error!("[VAULT] Failed to get logs: {}", e);
                let backoff_ms = 250 * (1_u64 << (consecutive_log_failures - 1));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }
        };
        consecutive_log_failures = 0;

        let logs_count = logs.len();
        let mut events: Vec<VaultEvent> = Vec::new();
        let mut batch_failed = false;

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_LOG_TASKS));
        for log in logs {
            let cache_manager = match CacheManager::instance() {
                Ok(cm) => cm,
                Err(e) => {
                    error!("Failed to get CacheManager instance: {}", e);
                    batch_failed = true;
                    continue;
                }
            };

            let permit = semaphore.clone().acquire_owned().await?;
            join_set.spawn(async move {
                let _permit = permit;
                let result = parse_log(log.clone(), client, cache_manager).await;
                (log, result)
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((log, parse_result)) => match parse_result {
                    Ok(parsed) => {
                        events.extend(parsed);
                    }
                    Err(e) => {
                        batch_failed = true;
                        error!(
                            error = %e,
                            log = ?log,
                            "Failed to parse vault log"
                        );
                    }
                },
                Err(join_err) => {
                    batch_failed = true;
                    error!("Task join error: {}", join_err);
                }
            }
        }

        if batch_failed {
            return Err(anyhow::anyhow!(
                "vault rejected partial block range {from_block}-{to_block}"
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

fn determine_vault_type(log_address: &str) -> VaultType {
    if log_address == *BURN_VAULT_ADDRESS {
        VaultType::Burn
    } else if log_address == *LP_VAULT_ADDRESS {
        VaultType::Lp
    } else if log_address == *CREATOR_FEE_VAULT_ADDRESS {
        VaultType::CreatorFee
    } else {
        VaultType::Gift
    }
}

/// USD value of a dividend-token amount using quote, whitelist, then chain
/// sources. Missing all sources returns 0 with the existing WARN.
async fn dividend_token_usd(
    cache: &CacheManager,
    dividend_token: &str,
    amount: &BigDecimal,
    block_num: i64,
    block_timestamp: i64,
) -> BigDecimal {
    let decimals = cache.get_token_decimals_factor(dividend_token).await;
    let quote_usd = cache.get_quote_usd_price(dividend_token, block_num).await;
    let whitelist_usd = cache.get_price_usd_before(dividend_token, block_num).await;
    let ph = cache
        .get_token_quote_price_history_before(dividend_token, block_timestamp)
        .await;
    let quote_id = cache
        .get_token_quote_id(dividend_token)
        .await
        .ok()
        .flatten();
    let quote_usd_of_quote = match quote_id {
        Some(quote_id) => cache.get_quote_usd_price(&quote_id, block_num).await,
        None => None,
    };
    let chain_ref = match (&ph, &quote_usd_of_quote) {
        (Some(ph_val), Some(quote_usd_of_quote)) => Some((ph_val, &**quote_usd_of_quote)),
        _ => None,
    };

    match compose_dividend_claim_usd(
        amount,
        &decimals,
        quote_usd.as_deref(),
        whitelist_usd.as_ref(),
        chain_ref,
    ) {
        Some(v) => v,
        None => {
            warn!(
                "[DIVIDEND] No USD price for dividend token={} block={} -- usd_value=0",
                dividend_token, block_num
            );
            BigDecimal::from(0)
        }
    }
}

/// One log may explode into multiple events (DividendVault array events).
async fn parse_log(
    log: Log,
    client: &RpcClient,
    cache: Arc<CacheManager>,
) -> Result<Vec<VaultEvent>> {
    let log_address = log.address().to_string();
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
    let coords = LogCoords {
        transaction_hash: Arc::new(transaction_hash.clone()),
        block_number,
        block_timestamp,
        log_index,
        transaction_index,
    };

    match log.topic0() {
        // BurnVault.Burn and GiftVault.Burn share the same signature
        Some(&BurnVault::Burn::SIGNATURE_HASH) => {
            let BurnVault::Burn {
                token,
                pair,
                quoteIn,
                tokenBurned,
            } = log.log_decode()?.inner.data;

            let token_str = token.to_string();
            let quote_in_arc = Arc::new(to_big_decimal(quoteIn));
            let (quote_id, usd_value) =
                enrich_usd(&cache, &token_str, &quote_in_arc, block_number as i64).await;

            Ok(vec![VaultEvent::Burn(VaultBurn {
                vault_type: determine_vault_type(&log_address),
                token: Arc::new(token_str),
                pair: Arc::new(pair.to_string()),
                quote_in: quote_in_arc,
                token_burned: Arc::new(to_big_decimal(tokenBurned)),
                quote_id,
                usd_value,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&LPVault::AddLiquidity::SIGNATURE_HASH) => {
            let LPVault::AddLiquidity {
                token,
                pair,
                quoteUsed,
                tokenUsed,
                lpBurned,
            } = log.log_decode()?.inner.data;

            let token_str = token.to_string();
            let quote_used_arc = Arc::new(to_big_decimal(quoteUsed));
            let (quote_id, usd_value) =
                enrich_usd(&cache, &token_str, &quote_used_arc, block_number as i64).await;

            Ok(vec![VaultEvent::LpInject(LpInject {
                token: Arc::new(token_str),
                pair: Arc::new(pair.to_string()),
                quote_used: quote_used_arc,
                token_used: Arc::new(to_big_decimal(tokenUsed)),
                lp_burned: Arc::new(to_big_decimal(lpBurned)),
                quote_id,
                usd_value,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&CreatorFeeVault::Deposit::SIGNATURE_HASH) => {
            let vault_type = determine_vault_type(&log_address);
            match vault_type {
                VaultType::Gift => {
                    // GiftVault.Deposit has same signature as CreatorFeeVault.Deposit
                    let GiftVault::Deposit {
                        token,
                        amount,
                        newBalance,
                    } = log.log_decode()?.inner.data;

                    let token_str = token.to_string();
                    let amount_arc = Arc::new(to_big_decimal(amount));
                    let (quote_id, usd_value) =
                        enrich_usd(&cache, &token_str, &amount_arc, block_number as i64).await;

                    Ok(vec![VaultEvent::GiftDeposit(GiftDeposit {
                        token: Arc::new(token_str),
                        amount: amount_arc,
                        new_balance: Arc::new(to_big_decimal(newBalance)),
                        quote_id,
                        usd_value,
                        transaction_hash: Arc::new(transaction_hash),
                        block_number,
                        block_timestamp,
                        log_index,
                        transaction_index,
                    })])
                }
                _ => {
                    let CreatorFeeVault::Deposit {
                        token,
                        amount,
                        newBalance,
                    } = log.log_decode()?.inner.data;

                    let token_str = token.to_string();
                    let amount_arc = Arc::new(to_big_decimal(amount));
                    let (quote_id, usd_value) =
                        enrich_usd(&cache, &token_str, &amount_arc, block_number as i64).await;

                    Ok(vec![VaultEvent::CreatorDeposit(CreatorDeposit {
                        token: Arc::new(token_str),
                        amount: amount_arc,
                        new_balance: Arc::new(to_big_decimal(newBalance)),
                        quote_id,
                        usd_value,
                        transaction_hash: Arc::new(transaction_hash),
                        block_number,
                        block_timestamp,
                        log_index,
                        transaction_index,
                    })])
                }
            }
        }

        Some(&CreatorFeeVault::Claim::SIGNATURE_HASH) => {
            let vault_type = determine_vault_type(&log_address);
            match vault_type {
                VaultType::Gift => {
                    let GiftVault::Claim {
                        token,
                        receiver,
                        amount,
                    } = log.log_decode()?.inner.data;

                    let token_str = token.to_string();
                    let amount_arc = Arc::new(to_big_decimal(amount));
                    let (quote_id, usd_value) =
                        enrich_usd(&cache, &token_str, &amount_arc, block_number as i64).await;

                    Ok(vec![VaultEvent::GiftClaim(GiftClaim {
                        token: Arc::new(token_str),
                        receiver: Arc::new(receiver.to_string()),
                        amount: amount_arc,
                        quote_id,
                        usd_value,
                        transaction_hash: Arc::new(transaction_hash),
                        block_number,
                        block_timestamp,
                        log_index,
                        transaction_index,
                    })])
                }
                _ => {
                    let CreatorFeeVault::Claim {
                        token,
                        creator,
                        amount,
                    } = log.log_decode()?.inner.data;

                    let token_str = token.to_string();
                    let amount_arc = Arc::new(to_big_decimal(amount));
                    let (quote_id, usd_value) =
                        enrich_usd(&cache, &token_str, &amount_arc, block_number as i64).await;

                    Ok(vec![VaultEvent::CreatorClaim(CreatorClaim {
                        token: Arc::new(token_str),
                        creator: Arc::new(creator.to_string()),
                        amount: amount_arc,
                        quote_id,
                        usd_value,
                        transaction_hash: Arc::new(transaction_hash),
                        block_number,
                        block_timestamp,
                        log_index,
                        transaction_index,
                    })])
                }
            }
        }

        Some(&GiftVault::VaultSetup::SIGNATURE_HASH) => {
            let GiftVault::VaultSetup {
                token,
                platform,
                id,
            } = log.log_decode()?.inner.data;

            Ok(vec![VaultEvent::GiftVaultSetup(GiftVaultSetup {
                token: Arc::new(token.to_string()),
                platform: GiftPlatform::from_u8(platform)?,
                platform_id: Arc::new(id),
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&GiftVault::Expire::SIGNATURE_HASH) => {
            let GiftVault::Expire { token, amount } = log.log_decode()?.inner.data;

            let token_str = token.to_string();
            let amount_arc = Arc::new(to_big_decimal(amount));
            let (quote_id, usd_value) =
                enrich_usd(&cache, &token_str, &amount_arc, block_number as i64).await;

            Ok(vec![VaultEvent::GiftExpire(GiftExpire {
                token: Arc::new(token_str),
                amount: amount_arc,
                quote_id,
                usd_value,
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&CreatorFeeVault::VaultSetup::SIGNATURE_HASH) => {
            let CreatorFeeVault::VaultSetup { token, creator } = log.log_decode()?.inner.data;

            Ok(vec![VaultEvent::CreatorVaultSetup(CreatorVaultSetup {
                token: Arc::new(token.to_string()),
                creator: Arc::new(creator.to_string()),
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&CreatorFeeVault::CreatorUpdate::SIGNATURE_HASH) => {
            let CreatorFeeVault::CreatorUpdate {
                token,
                oldCreator,
                newCreator,
            } = log.log_decode()?.inner.data;

            Ok(vec![VaultEvent::CreatorUpdate(CreatorUpdate {
                token: Arc::new(token.to_string()),
                old_creator: Arc::new(oldCreator.to_string()),
                new_creator: Arc::new(newCreator.to_string()),
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&GiftVault::ReceiverSet::SIGNATURE_HASH) => {
            let GiftVault::ReceiverSet { token, receiver } = log.log_decode()?.inner.data;

            Ok(vec![VaultEvent::GiftReceiverSet(GiftReceiverSet {
                token: Arc::new(token.to_string()),
                receiver: Arc::new(receiver.to_string()),
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&GiftVault::ExpiryUpdate::SIGNATURE_HASH) => {
            let GiftVault::ExpiryUpdate {
                oldDuration,
                newDuration,
            } = log.log_decode()?.inner.data;

            Ok(vec![VaultEvent::GiftExpiryUpdate(GiftExpiryUpdate {
                old_duration: Arc::new(to_big_decimal(oldDuration)),
                new_duration: Arc::new(to_big_decimal(newDuration)),
                transaction_hash: Arc::new(transaction_hash),
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            })])
        }

        Some(&DividendVault::DividendSetup::SIGNATURE_HASH) => {
            let DividendVault::DividendSetup {
                sourceToken,
                dividendTokens,
                ratios,
                minBalance,
            } = log.log_decode()?.inner.data;

            let entries = explode_setup(
                &sourceToken.to_string(),
                dividendTokens.iter().map(|a| a.to_string()).collect(),
                ratios,
                to_big_decimal(minBalance),
                coords,
            )?;
            Ok(entries
                .into_iter()
                .map(DividendEvent::Setup)
                .map(VaultEvent::Dividend)
                .collect())
        }

        Some(&DividendVault::Deposit::SIGNATURE_HASH) => {
            let DividendVault::Deposit {
                sourceToken,
                dividendTokens,
                slices,
                pending,
            } = log.log_decode()?.inner.data;

            let mut deposits = explode_deposit(
                &sourceToken.to_string(),
                dividendTokens.iter().map(|a| a.to_string()).collect(),
                slices.into_iter().map(to_big_decimal).collect(),
                pending,
                coords,
            )?;
            // Deposit slices are quote-denominated regardless of pending state.
            for deposit in &mut deposits {
                let (quote_id, usd_value) = enrich_usd(
                    &cache,
                    &deposit.source_token,
                    &deposit.amount,
                    block_number as i64,
                )
                .await;
                deposit.quote_id = quote_id;
                deposit.usd_value = usd_value;
            }
            Ok(deposits
                .into_iter()
                .map(DividendEvent::Deposit)
                .map(VaultEvent::Dividend)
                .collect())
        }

        Some(&DividendVault::Converted::SIGNATURE_HASH) => {
            let DividendVault::Converted {
                sourceTokens,
                dividendTokens,
                consumedQuote,
                received,
            } = log.log_decode()?.inner.data;

            let mut conversions = explode_conversion(
                sourceTokens.iter().map(|a| a.to_string()).collect(),
                dividendTokens.iter().map(|a| a.to_string()).collect(),
                consumedQuote.into_iter().map(to_big_decimal).collect(),
                received.into_iter().map(to_big_decimal).collect(),
                coords,
            )?;
            // consumed_quote is quote-denominated -> same enrich path as deposits.
            for conv in &mut conversions {
                let (quote_id, usd_value) = enrich_usd(
                    &cache,
                    &conv.source_token,
                    &conv.consumed_quote,
                    block_number as i64,
                )
                .await;
                conv.quote_id = quote_id;
                conv.usd_value = usd_value;
            }
            Ok(conversions
                .into_iter()
                .map(DividendEvent::Conversion)
                .map(VaultEvent::Dividend)
                .collect())
        }

        Some(&DividendVault::SetMerkleRoot::SIGNATURE_HASH) => {
            let DividendVault::SetMerkleRoot { merkleRoot } = log.log_decode()?.inner.data;

            Ok(vec![VaultEvent::Dividend(DividendEvent::MerkleRoot(
                DividendMerkleRoot {
                    merkle_root: Arc::new(merkleRoot.to_string()),
                    coords,
                },
            ))])
        }

        Some(&DividendVault::Claim::SIGNATURE_HASH) => {
            let DividendVault::Claim {
                holder,
                sourceTokens,
                dividendTokens,
                amounts,
            } = log.log_decode()?.inner.data;

            let mut claims = explode_claim(
                &holder.to_string(),
                sourceTokens.iter().map(|a| a.to_string()).collect(),
                dividendTokens.iter().map(|a| a.to_string()).collect(),
                amounts.into_iter().map(to_big_decimal).collect(),
                coords,
            )?;
            for claim in &mut claims {
                claim.usd_value = Arc::new(
                    dividend_token_usd(
                        &cache,
                        &claim.dividend_token,
                        &claim.amount,
                        block_number as i64,
                        block_timestamp as i64,
                    )
                    .await,
                );
            }
            Ok(claims
                .into_iter()
                .map(DividendEvent::Claim)
                .map(VaultEvent::Dividend)
                .collect())
        }

        _ => Err(anyhow::anyhow!("Unknown vault event type")),
    }
}
