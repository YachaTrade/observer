use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock},
    time::Duration,
};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;
use bigdecimal::BigDecimal;
use tokio::{task::JoinSet, time::Instant};
use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::{
        BLOCK_BATCH_SIZE, BONDING_CURVE_ADDRESS, DEX_FACTORY_ADDRESS, DEX_ROUTER_ADDRESS,
        LP_MANAGER_ADDRESS, WNATIVE_ADDRESS, get_quote_decimals, quote_configs,
    },
    db::cache::CacheManager,
    event::{
        common::token::{TokenEventChannel, receive::receive_events},
        get_block_timestamp,
    },
    sync::{BlockRange, EventType, stream::STREAM_MANAGER},
    types::token::{PositionHistoryEvent, TokenBalance, TokenBurn, TokenEvent, TransferType},
    utils::to_big_decimal,
};

// ============================================================================
// Constants & Static
// ============================================================================

const LOG_FETCH_RETRY_DELAY_MS: u64 = 500;

static ZERO_ADDRESS: LazyLock<Address> = LazyLock::new(|| Address::ZERO);

/// WMON 주소 (Address로 파싱해서 캐시)
static WMON_ADDRESS: LazyLock<Address> =
    LazyLock::new(|| WNATIVE_ADDRESS.parse().expect("Invalid WMON address"));

/// Non-WMON quote addresses parsed once at first access.
/// Depends on `init_quote_configs_from_db` having been called during startup.
static NON_WMON_QUOTE_ADDRESSES: LazyLock<Vec<Address>> = LazyLock::new(|| {
    quote_configs()
        .iter()
        .filter(|q| q.address != *WNATIVE_ADDRESS)
        .filter_map(|q| q.address.parse::<Address>().ok())
        .collect()
});

/// 시스템 주소 HashSet (정적 주소들)
static SYSTEM_ADDRESSES: LazyLock<HashSet<Address>> = LazyLock::new(|| {
    let mut set = HashSet::with_capacity(5);
    set.insert(Address::ZERO);
    for address in [
        &*BONDING_CURVE_ADDRESS,
        &*DEX_FACTORY_ADDRESS,
        &*DEX_ROUTER_ADDRESS,
        &*LP_MANAGER_ADDRESS,
    ] {
        set.insert(address.parse().expect("active GIWA address must be valid"));
    }
    set
});

// ============================================================================
// Parsed Log Types
// ============================================================================

/// 파싱된 로그 - Address 직접 저장 (String 변환 오버헤드 제거)
#[derive(Debug, Clone)]
enum ParsedLog {
    /// 토큰 전송
    Transfer {
        token: Address,
        from: Address,
        to: Address,
        amount: BigDecimal,
        tx_hash: Arc<String>,
        block_number: u64,
        block_timestamp: u64,
        tx_index: u64,
        log_index: u64,
    },
    /// WMON Deposit (quote_out) - tx_sender는 나중에 매칭
    Deposit {
        amount: BigDecimal,
        tx_hash: Arc<String>,
    },
    /// WMON Withdrawal (quote_in) - tx_sender는 나중에 매칭
    Withdrawal {
        amount: BigDecimal,
        tx_hash: Arc<String>,
    },
    /// Non-WMON quote ERC-20 Transfer. Direction resolved against tx_sender later.
    QuoteTransfer {
        quote_id: Address,
        from: Address,
        to: Address,
        amount: BigDecimal,
        tx_hash: Arc<String>,
    },
}

sol! {
    #[allow(missing_docs, clippy::too_many_arguments)]
    #[sol(rpc)]
    IToken,
    "abi/IToken.json"
}

// WMON (Wrapped Native) Deposit/Withdrawal events
sol! {
    #[allow(missing_docs)]
    event Deposit(address indexed dst, uint wad);
    event Withdrawal(address indexed src, uint wad);
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[instrument(skip(event_type))]
pub async fn stream_events(event_type: EventType) -> Result<()> {
    info!("Starting token transfer event streaming");

    let (channel, receiver) = TokenEventChannel::new("token_events");
    spawn_receiver(receiver, event_type);

    let mut block_batch_size = *BLOCK_BATCH_SIZE;
    let mut total_events = 0;

    loop {
        let client = RpcClient::instance()?;
        let cache_manager = CacheManager::instance()?;
        let time = Instant::now();

        // 1. 블록 범위 조회
        let BlockRange {
            from_block,
            to_block,
        } = STREAM_MANAGER
            .get_next_block_range(
                event_type,
                block_batch_size,
                client.get_cached_latest_block(),
            )
            .await;

        if from_block > to_block {
            continue;
        }

        // 2. 로그 fetch (Transfer + Deposit + Withdrawal + QuoteTransfer)
        let logs = match fetch_logs(client, from_block, to_block).await {
            Ok(logs) => logs,
            Err(e) => {
                error!(
                    "[TOKEN] Failed to get logs: {} | blocks {}-{} | batch {}",
                    e, from_block, to_block, block_batch_size
                );
                block_batch_size /= 2;
                tokio::time::sleep(Duration::from_millis(LOG_FETCH_RETRY_DELAY_MS)).await;
                continue;
            }
        };
        let logs_count = logs.len();

        // 3. 로그 파싱 (병렬) - Transfer, Deposit, Withdrawal 모두
        let (parsed_logs, token_events) =
            parse_logs_parallel(logs, client, cache_manager.clone()).await;

        // 4. TokenEvent 분리 (Balance, Burn)
        let (balances, burns) = separate_token_events(token_events);
        let filtered_balances = filter_latest_balances(balances);

        // 5. PositionHistory 생성 (ParsedLog 기반)
        let position_histories =
            build_position_histories(&parsed_logs, cache_manager.clone()).await;

        // 6. 최종 이벤트 구성 및 전송
        let final_events = build_final_events(filtered_balances, burns, position_histories);
        let events_count = final_events.len();
        total_events += events_count;

        if let Err(e) = channel.send(final_events, to_block, to_block).await {
            error!("[TOKEN] Failed to send events: {}", e);
            continue;
        }

        // 7. 로깅 및 상태 업데이트
        warn!(
            "📊 {:?} Stream: Blocks {}-{} | Logs {} | Events {} | Total {} | {}ms",
            event_type,
            from_block,
            to_block,
            logs_count,
            events_count,
            total_events,
            time.elapsed().as_millis()
        );

        block_batch_size = *BLOCK_BATCH_SIZE;
        STREAM_MANAGER
            .set_event_block_processed_block(event_type, to_block)
            .await;
    }
}

// ============================================================================
// Stream Helper Functions
// ============================================================================

fn spawn_receiver(
    receiver: crate::metrics::MonitoredReceiver<crate::event::core::EventBatch<TokenEvent>>,
    event_type: EventType,
) {
    tokio::spawn(async move {
        if let Err(e) = receive_events(receiver, event_type).await {
            error!("[TOKEN] Failed to receive events: {}", e);
        }
    });
}

async fn fetch_logs(client: &RpcClient, from_block: u64, to_block: u64) -> Result<Vec<Log>> {
    let from = BlockNumberOrTag::Number(from_block);
    let to = BlockNumberOrTag::Number(to_block);

    // Primary filter: token Transfer + WMON Deposit/Withdrawal (all contracts)
    let primary_filter = Filter::new().from_block(from).to_block(to).events([
        IToken::Transfer::SIGNATURE,
        Deposit::SIGNATURE,
        Withdrawal::SIGNATURE,
    ]);

    // Non-WMON quote Transfer filter (only when there are non-WMON quotes configured)
    let non_wmon_addrs = NON_WMON_QUOTE_ADDRESSES.clone();

    if non_wmon_addrs.is_empty() {
        return client.get_logs(primary_filter).await;
    }

    let quote_transfer_filter = Filter::new()
        .address(non_wmon_addrs)
        .from_block(from)
        .to_block(to)
        .event(IToken::Transfer::SIGNATURE);

    // Fetch both in parallel
    let (primary_result, quote_result) = tokio::join!(
        client.get_logs(primary_filter),
        client.get_logs(quote_transfer_filter),
    );

    let mut logs = primary_result?;
    logs.extend(quote_result?);
    Ok(logs)
}

/// 로그 병렬 파싱 → (ParsedLog, TokenEvent) 반환
async fn parse_logs_parallel(
    logs: Vec<Log>,
    _client: &RpcClient,
    cache_manager: Arc<CacheManager>,
) -> (Vec<ParsedLog>, Vec<TokenEvent>) {
    if logs.is_empty() {
        return (Vec::new(), Vec::new());
    }

    // 로그 병렬 파싱 (Deposit/Withdrawal/Transfer 모두)
    let mut join_set = JoinSet::new();

    for log in logs {
        let cm = cache_manager.clone();
        join_set.spawn(async move {
            let client = match RpcClient::instance() {
                Ok(c) => c,
                Err(_) => return (None, Vec::new()),
            };
            parse_log(log, client, cm).await
        });
    }

    let mut parsed_logs = Vec::new();
    let mut token_events = Vec::new();

    while let Some(Ok((parsed, events))) = join_set.join_next().await {
        if let Some(p) = parsed {
            parsed_logs.push(p);
        }
        token_events.extend(events);
    }

    // TokenEvent 정렬
    token_events.sort_by_key(|e| (e.block_number(), e.transaction_index(), e.log_index()));

    (parsed_logs, token_events)
}

/// 로그 파싱 → (ParsedLog, TokenEvent)
async fn parse_log(
    log: Log,
    client: &RpcClient,
    cache_manager: Arc<CacheManager>,
) -> (Option<ParsedLog>, Vec<TokenEvent>) {
    let token_address = Address::from(log.address().0);

    // WMON의 Deposit/Withdrawal 처리
    if token_address == *WMON_ADDRESS {
        return parse_wmon_log(&log);
    }

    // Non-WMON quote Transfer: parse directly without whitelist check
    if NON_WMON_QUOTE_ADDRESSES.contains(&token_address) {
        if log.topic0() == Some(&IToken::Transfer::SIGNATURE_HASH) {
            return (parse_quote_transfer_log(&log), Vec::new());
        }
        return (None, Vec::new());
    }

    // Only index balances for contracts registered in the token table.
    let token_addr_str = token_address.to_string();
    let is_indexed_token = match cache_manager.token_exists(&token_addr_str).await {
        Ok(v) => v,
        Err(_) => return (None, Vec::new()),
    };

    if !is_indexed_token {
        return (None, Vec::new());
    }

    // Transfer 이벤트만 처리
    if log.topic0() != Some(&IToken::Transfer::SIGNATURE_HASH) {
        return (None, Vec::new());
    }

    parse_transfer_log(log, client, cache_manager).await
}

/// Split the flat `TokenEvent` stream into per-kind buckets the caller can
/// route to different storage layers.
///
/// ⚠️ Every `TokenEvent` variant that should be persisted by the stream layer
/// must be handled here. The `_ => {}` arm silently drops unknown variants
/// (`Transfer`, `PositionHistory` are emitted directly by `build_final_events`,
/// not by `parse_log`, so they currently shouldn't appear here — but when
/// adding new variants, add the arm here AND update `build_final_events`.
fn separate_token_events(events: Vec<TokenEvent>) -> (Vec<TokenBalance>, Vec<TokenBurn>) {
    let mut balances = Vec::new();
    let mut burns = Vec::new();

    for event in events {
        match event {
            TokenEvent::Balance(b) => balances.push(b),
            TokenEvent::Burn(b) => burns.push(b),
            _ => {}
        }
    }

    (balances, burns)
}

fn filter_latest_balances(balances: Vec<TokenBalance>) -> Vec<TokenBalance> {
    let mut seen: HashSet<(Arc<String>, Arc<String>)> = HashSet::new();
    let mut filtered = Vec::new();

    // 역순 순회 (최신값 우선)
    for balance in balances.into_iter().rev() {
        let key = (balance.account_id.clone(), balance.token.clone());
        if seen.insert(key) {
            filtered.push(balance);
        }
    }

    filtered
}

fn build_final_events(
    balances: Vec<TokenBalance>,
    burns: Vec<TokenBurn>,
    positions: Vec<PositionHistoryEvent>,
) -> Vec<TokenEvent> {
    let mut events = Vec::with_capacity(balances.len() + burns.len() + positions.len());

    events.extend(balances.into_iter().map(TokenEvent::Balance));
    events.extend(burns.into_iter().map(TokenEvent::Burn));
    events.extend(positions.into_iter().map(TokenEvent::PositionHistory));
    events.sort_by_key(|event| {
        (
            event.block_number(),
            event.transaction_index(),
            event.log_index(),
        )
    });

    events
}

// ============================================================================
// Log Parsing
// ============================================================================

/// 로그 메타데이터 (Address 직접 저장)
struct LogMeta {
    token_address: Address,
    transaction_hash: String,
    block_number: u64,
    block_timestamp: u64,
    log_index: u64,
    tx_index: u64,
}

impl LogMeta {
    async fn from_log(log: &Log, client: &RpcClient) -> Result<Self> {
        let transaction_hash = log
            .transaction_hash
            .ok_or_else(|| anyhow::anyhow!("No transaction hash"))?
            .to_string();

        let block_number = log
            .block_number
            .ok_or_else(|| anyhow::anyhow!("Missing block number"))?;

        let block_timestamp = match log.block_timestamp {
            Some(ts) => ts,
            None => get_block_timestamp(client, block_number).await?,
        };

        Ok(Self {
            token_address: Address::from(log.address().0),
            transaction_hash,
            block_number,
            block_timestamp,
            log_index: log.log_index.unwrap_or(u64::MAX),
            tx_index: log.transaction_index.unwrap_or(u64::MAX),
        })
    }
}

/// WMON Deposit/Withdrawal 파싱 (tx_hash만 저장, sender는 나중에 매칭)
fn parse_wmon_log(log: &Log) -> (Option<ParsedLog>, Vec<TokenEvent>) {
    let tx_hash = match log.transaction_hash {
        Some(h) => Arc::new(h.to_string()),
        None => return (None, Vec::new()),
    };

    let topic0 = match log.topic0() {
        Some(t) => t,
        None => return (None, Vec::new()),
    };

    // Deposit 이벤트 (유저가 MON을 보내서 WMON을 받음 → quote_out)
    if *topic0 == Deposit::SIGNATURE_HASH
        && let Ok(decoded) = log.log_decode::<Deposit>()
    {
        let Deposit { wad, .. } = decoded.inner.data;
        let amount = to_big_decimal(wad);
        return (Some(ParsedLog::Deposit { amount, tx_hash }), Vec::new());
    }

    // Withdrawal 이벤트 (유저가 WMON을 태워서 MON을 받음 → quote_in)
    if *topic0 == Withdrawal::SIGNATURE_HASH
        && let Ok(decoded) = log.log_decode::<Withdrawal>()
    {
        let Withdrawal { wad, .. } = decoded.inner.data;
        let amount = to_big_decimal(wad);
        return (Some(ParsedLog::Withdrawal { amount, tx_hash }), Vec::new());
    }

    // Transfer 등 다른 이벤트는 무시 (WMON도 ERC20이라 Transfer 이벤트 있음)
    (None, Vec::new())
}

/// Non-WMON quote Transfer parsing. Captures amount and from/to for later
/// tx_sender-based direction inference.
fn parse_quote_transfer_log(log: &Log) -> Option<ParsedLog> {
    let tx_hash = Arc::new(log.transaction_hash?.to_string());
    let quote_id = log.address();

    let decoded = log.log_decode::<IToken::Transfer>().ok()?;
    let IToken::Transfer { from, to, value } = decoded.inner.data;

    if from == to {
        return None;
    }

    Some(ParsedLog::QuoteTransfer {
        quote_id,
        from,
        to,
        amount: to_big_decimal(value),
        tx_hash,
    })
}

/// Transfer 로그 파싱
async fn parse_transfer_log(
    log: Log,
    client: &RpcClient,
    cache_manager: Arc<CacheManager>,
) -> (Option<ParsedLog>, Vec<TokenEvent>) {
    let meta = match LogMeta::from_log(&log, client).await {
        Ok(m) => m,
        Err(_) => return (None, Vec::new()),
    };

    let decoded = match log.log_decode::<IToken::Transfer>() {
        Ok(d) => d,
        Err(_) => return (None, Vec::new()),
    };
    let IToken::Transfer { from, to, value } = decoded.inner.data;

    // from == to 체크
    if from == to {
        warn!(
            "Same from/to address: {:?} tx: {}",
            from, meta.transaction_hash
        );
        return (None, Vec::new());
    }

    let amount = to_big_decimal(value);
    let tx_hash = Arc::new(meta.transaction_hash.clone());
    let mut events = Vec::new();

    // ParsedLog::Transfer 생성 (Address 직접 저장)
    let parsed_log = ParsedLog::Transfer {
        token: meta.token_address,
        from,
        to,
        amount: amount.clone(),
        tx_hash: tx_hash.clone(),
        block_number: meta.block_number,
        block_timestamp: meta.block_timestamp,
        tx_index: meta.tx_index,
        log_index: meta.log_index,
    };

    // 1. Burn 이벤트 (to가 zero address)
    if to == *ZERO_ADDRESS
        && let Ok(burn) = TokenBurn::new(
            Arc::new(from.to_string()),
            Arc::new(meta.token_address.to_string()),
            Arc::new(amount.clone()),
            meta.block_timestamp,
            meta.block_number,
            tx_hash.clone(),
            meta.log_index,
            meta.tx_index,
        )
    {
        events.push(TokenEvent::Burn(burn));
    }

    // 2. Balance 이벤트 (시스템 주소 제외) - 시스템 주소 체크 + Balance 조회 모두 병렬
    let token_addr_str = meta.token_address.to_string();

    let (from_result, to_result) = tokio::join!(
        async {
            if is_system_address(&cache_manager, from, &token_addr_str).await {
                None
            } else {
                try_get_balance(client, &meta, from).await
            }
        },
        async {
            if is_system_address(&cache_manager, to, &token_addr_str).await {
                None
            } else {
                try_get_balance(client, &meta, to).await
            }
        }
    );

    if let Some(balance) = from_result {
        events.push(balance);
    }
    if let Some(balance) = to_result {
        events.push(balance);
    }

    (Some(parsed_log), events)
}

/// Balance 조회 시도 (실패 시 None)
async fn try_get_balance(
    client: &RpcClient,
    meta: &LogMeta,
    address: Address,
) -> Option<TokenEvent> {
    match get_balance_at_block(client, meta.token_address, address, meta.block_number).await {
        Ok(balance) => TokenBalance::new(
            Arc::new(address.to_string()),
            Arc::new(meta.token_address.to_string()),
            Arc::new(balance),
            meta.block_timestamp,
            meta.block_number,
            Arc::new(meta.transaction_hash.clone()),
            meta.log_index,
            meta.tx_index,
        )
        .ok()
        .map(TokenEvent::Balance),
        Err(e) => {
            warn!("Failed to get balance for {}: {}", address, e);
            None
        }
    }
}

// ============================================================================
// Balance & Address Utilities
// ============================================================================

async fn get_balance_at_block(
    client: &RpcClient,
    token_address: Address,
    account_address: Address,
    block_number: u64,
) -> Result<BigDecimal> {
    let call = IToken::balanceOfCall {
        account: account_address,
    };
    let balance = client
        .call_contract_at_block(call, token_address, block_number)
        .await?;

    Ok(to_big_decimal(balance))
}

/// 시스템 주소 체크 (HashSet 활용)
async fn is_system_address(
    _cache_manager: &CacheManager,
    address: Address,
    _token_id: &str,
) -> bool {
    // 정적 시스템 주소 (O(1) lookup)
    SYSTEM_ADDRESSES.contains(&address)
}

// ============================================================================
// Position History Builder
// ============================================================================

/// Per-tx, per-quote flows keyed by quote contract address.
/// quote_address -> (quote_in, quote_out)
type TxQuoteFlows = std::collections::HashMap<Address, (BigDecimal, BigDecimal)>;

/// ParsedLog → PositionHistory 변환
async fn build_position_histories(
    parsed_logs: &[ParsedLog],
    cache_manager: Arc<CacheManager>,
) -> Vec<PositionHistoryEvent> {
    if parsed_logs.is_empty() {
        return Vec::new();
    }

    // 1. tx_hash별로 그룹핑
    let mut tx_groups: HashMap<&str, Vec<&ParsedLog>> = HashMap::new();
    for log in parsed_logs {
        let tx_hash = match log {
            ParsedLog::Transfer { tx_hash, .. } => tx_hash.as_str(),
            ParsedLog::Deposit { tx_hash, .. } => tx_hash.as_str(),
            ParsedLog::Withdrawal { tx_hash, .. } => tx_hash.as_str(),
            ParsedLog::QuoteTransfer { tx_hash, .. } => tx_hash.as_str(),
        };
        tx_groups.entry(tx_hash).or_default().push(log);
    }

    // 2. Quote 이벤트가 있는 tx_hash 수집 (WMON Deposit/Withdrawal + non-WMON QuoteTransfer)
    let quote_tx_hashes: HashSet<&str> = parsed_logs
        .iter()
        .filter(|log| {
            matches!(
                log,
                ParsedLog::Deposit { .. }
                    | ParsedLog::Withdrawal { .. }
                    | ParsedLog::QuoteTransfer { .. }
            )
        })
        .map(|log| match log {
            ParsedLog::Deposit { tx_hash, .. } => tx_hash.as_str(),
            ParsedLog::Withdrawal { tx_hash, .. } => tx_hash.as_str(),
            ParsedLog::QuoteTransfer { tx_hash, .. } => tx_hash.as_str(),
            _ => unreachable!(),
        })
        .collect();

    // 3. tx_sender 조회 (quote 이벤트 있는 tx만, Redis 캐시 활용)
    let tx_senders = fetch_tx_senders_for_hashes(&quote_tx_hashes, cache_manager.clone()).await;

    // 4. Quote flows 구축 (tx_hash + quote_address 기준)
    let quote_flows = build_quote_flows(parsed_logs, &tx_senders);

    // 5. Transfer만 추출해서 PositionHistory 생성
    let mut positions = Vec::new();

    // Per-block-range caches to avoid redundant Redis/DB hits within this batch.
    //
    // Memory upper bound is cheap:
    //   token_quote_cache -> unique tokens seen in this batch (tens to low thousands)
    //   quote_price_cache -> unique quotes × unique blocks in this batch
    // Both are dropped when build_position_histories returns.
    //
    // `token_quote_cache` stores Arc<String> so cache hits are refcount bumps
    // rather than full String clones. `quote_price_cache`'s key also uses the
    // same Arc so building the (quote_id, block) key is likewise refcount-only.
    //
    // Note: Err results from `get_token_quote_id` are cached as WMON fallback.
    // In practice this only fires on fatal PG pool failure (a Redis/DB query
    // error is internally downgraded to `Ok(None)` by CacheManager), so the
    // cached fallback is not a transient-error poisoning risk.
    let mut token_quote_cache: HashMap<Address, Arc<String>> = HashMap::new();
    let mut quote_price_cache: HashMap<(Arc<String>, i64), Option<Arc<BigDecimal>>> =
        HashMap::new();

    for (tx_hash, logs) in &tx_groups {
        // Transfer만 필터
        let transfers: Vec<_> = logs
            .iter()
            .filter(|log| matches!(log, ParsedLog::Transfer { .. }))
            .collect();

        if transfers.is_empty() {
            continue;
        }

        // 해당 tx의 quote flows 조회
        let tx_quote_flows = quote_flows.get(*tx_hash);

        for transfer in transfers {
            if let ParsedLog::Transfer {
                token,
                from,
                to,
                amount,
                tx_hash,
                block_number,
                block_timestamp,
                tx_index,
                log_index,
            } = transfer
            {
                // Resolve the token's quote_id (default to WMON if unknown/unregistered).
                // Cached per token Address to avoid redundant Redis/DB hits across transfers.
                // Stored as Arc<String> so both this cache and the price cache key below
                // share the underlying allocation -- cache hits are refcount bumps only.
                let quote_id_arc: Arc<String> = match token_quote_cache.get(token) {
                    Some(q) => Arc::clone(q),
                    None => {
                        let token_str = token.to_string();
                        let resolved: String = match cache_manager
                            .get_token_quote_id(&token_str)
                            .await
                        {
                            Ok(Some(q)) => q,
                            Ok(None) => WNATIVE_ADDRESS.clone(),
                            Err(e) => {
                                warn!(
                                    "[TOKEN] get_token_quote_id failed for {}: {} - falling back to WMON",
                                    token_str, e
                                );
                                WNATIVE_ADDRESS.clone()
                            }
                        };
                        let arc = Arc::new(resolved);
                        token_quote_cache.insert(*token, Arc::clone(&arc));
                        arc
                    }
                };

                let quote_addr: Address = quote_id_arc.parse().unwrap_or_else(|_| *WMON_ADDRESS);

                // Look up the (quote_in, quote_out) for this specific quote in this tx.
                let this_tx_quote_flow: Option<(BigDecimal, BigDecimal)> = tx_quote_flows
                    .and_then(|flows| flows.get(&quote_addr))
                    .cloned();

                // Prefetch USD price for this (quote_id, block_num) if not cached.
                // Cached per (quote_id, block_num) to avoid the full fallback chain
                // on every transfer within the same block. Key reuses the same Arc
                // (refcount bump only, no String clone).
                let price_key = (Arc::clone(&quote_id_arc), *block_number as i64);
                let quote_price: Option<Arc<BigDecimal>> = match quote_price_cache.get(&price_key) {
                    Some(p) => p.clone(),
                    None => {
                        let p = cache_manager
                            .get_quote_usd_price(quote_id_arc.as_str(), *block_number as i64)
                            .await;
                        quote_price_cache.insert(price_key, p.clone());
                        p
                    }
                };

                // EOA 체크 (병렬)
                let from_str = from.to_string();
                let to_str = to.to_string();
                let (from_is_eoa, to_is_eoa) = tokio::join!(
                    async {
                        *from != *ZERO_ADDRESS
                            && cache_manager.check_is_eoa(&from_str).await.unwrap_or(false)
                    },
                    async {
                        *to != *ZERO_ADDRESS
                            && cache_manager.check_is_eoa(&to_str).await.unwrap_or(false)
                    }
                );

                // tx_sender와 매칭되는 주소에만 quote flow 적용
                let tx_sender = tx_senders.get(tx_hash.as_str());

                // EOA → EOA Transfer 판별 (해당 quote flow 없음)
                let is_eoa_to_eoa_transfer =
                    from_is_eoa && to_is_eoa && this_tx_quote_flow.is_none();

                // from의 token_out
                if from_is_eoa {
                    let (quote_in, quote_out) = match tx_sender == Some(from) {
                        true => this_tx_quote_flow.clone().unwrap_or_default(),
                        false => (BigDecimal::from(0), BigDecimal::from(0)),
                    };

                    let has_quote_in = quote_in > BigDecimal::from(0);
                    let has_quote_out = quote_out > BigDecimal::from(0);

                    // transfer_type 결정 (from: 토큰 보내는 쪽)
                    let transfer_type = match (is_eoa_to_eoa_transfer, has_quote_in, has_quote_out)
                    {
                        (true, _, _) => TransferType::TransferOut,
                        (false, true, _) => TransferType::Sell, // 토큰 팔고 quote 받음
                        (false, _, true) => TransferType::LpAdd, // 토큰도 주고 quote도 줌
                        _ => TransferType::Other,
                    };

                    // sender_address: from은 보내는 쪽이므로 항상 None
                    positions.push(create_position_history(
                        *token,
                        *from,
                        tx_hash,
                        *block_number,
                        *block_timestamp,
                        *tx_index,
                        *log_index,
                        quote_in,
                        quote_out,
                        BigDecimal::from(0),
                        amount.clone(),
                        quote_id_arc.as_str(),
                        &quote_price,
                        transfer_type,
                        None,
                    ));
                }

                // to의 token_in
                if to_is_eoa {
                    let (quote_in, quote_out) = match tx_sender == Some(to) {
                        true => this_tx_quote_flow.clone().unwrap_or_default(),
                        false => (BigDecimal::from(0), BigDecimal::from(0)),
                    };

                    let has_quote_in = quote_in > BigDecimal::from(0);
                    let has_quote_out = quote_out > BigDecimal::from(0);

                    // transfer_type 결정 (to: 토큰 받는 쪽)
                    let transfer_type = match (
                        is_eoa_to_eoa_transfer,
                        has_quote_out,
                        has_quote_in,
                        from_is_eoa,
                    ) {
                        (true, _, _, _) => TransferType::TransferIn,
                        (false, true, _, _) => TransferType::Buy, // quote 주고 토큰 받음
                        (false, _, true, _) => TransferType::LpRemove, // 토큰도 받고 quote도 받음
                        (false, _, _, false) => TransferType::Airdrop, // Contract → EOA, no quote
                        _ => TransferType::Other,
                    };

                    // sender_address (EOA→EOA일 때만)
                    let sender_address = match is_eoa_to_eoa_transfer {
                        true => Some(Arc::new(from_str.clone())),
                        false => None,
                    };

                    positions.push(create_position_history(
                        *token,
                        *to,
                        tx_hash,
                        *block_number,
                        *block_timestamp,
                        *tx_index,
                        *log_index,
                        quote_in,
                        quote_out,
                        amount.clone(),
                        BigDecimal::from(0),
                        quote_id_arc.as_str(),
                        &quote_price,
                        transfer_type,
                        sender_address,
                    ));
                }
            }
        }
    }

    positions
}

/// tx_hash들의 sender 조회 (병렬, Redis 캐시 활용)
async fn fetch_tx_senders_for_hashes(
    tx_hashes: &HashSet<&str>,
    cache_manager: Arc<CacheManager>,
) -> HashMap<String, Address> {
    if tx_hashes.is_empty() {
        return HashMap::new();
    }

    let mut join_set = JoinSet::new();

    for hash_str in tx_hashes.iter() {
        let hash_string = hash_str.to_string();
        let cm = cache_manager.clone();
        join_set.spawn(async move {
            // CacheManager를 통해 조회 (Redis 캐시 → RPC → Redis 저장)
            match cm.get_tx_sender(&hash_string).await {
                Ok(Some(sender)) => Some((hash_string, sender)),
                Ok(None) => {
                    warn!("[TOKEN] TX not found: {}", hash_string);
                    None
                }
                Err(e) => {
                    warn!("[TOKEN] Failed to get tx_sender for {}: {}", hash_string, e);
                    None
                }
            }
        });
    }

    let mut senders = HashMap::new();
    while let Some(Ok(Some((hash, sender)))) = join_set.join_next().await {
        senders.insert(hash, sender);
    }

    if !senders.is_empty() {
        info!(
            "[TOKEN] Fetched {} tx_senders for quote matching",
            senders.len()
        );
    }
    senders
}

/// Build per-tx, per-quote flow maps from parsed logs.
///
/// WMON flows come from Deposit/Withdrawal events, summed at the tx level
/// (the `dst`/`src` field is ignored — flows are attributed to the tx_sender later).
///
/// Non-WMON quote flows come from ERC-20 Transfer events: flows are only
/// recorded if `from` or `to` matches the tx_sender (direction follows).
fn build_quote_flows(
    parsed_logs: &[ParsedLog],
    tx_senders: &HashMap<String, Address>,
) -> HashMap<String, TxQuoteFlows> {
    let mut flows: HashMap<String, TxQuoteFlows> = HashMap::new();
    let wmon_addr = *WMON_ADDRESS;

    for log in parsed_logs {
        match log {
            ParsedLog::Deposit { amount, tx_hash } => {
                let entry = flows.entry(tx_hash.to_string()).or_default();
                let slot = entry.entry(wmon_addr).or_default();
                slot.1 += amount; // quote_out
            }
            ParsedLog::Withdrawal { amount, tx_hash } => {
                let entry = flows.entry(tx_hash.to_string()).or_default();
                let slot = entry.entry(wmon_addr).or_default();
                slot.0 += amount; // quote_in
            }
            ParsedLog::QuoteTransfer {
                quote_id,
                from,
                to,
                amount,
                tx_hash,
            } => {
                let sender = match tx_senders.get(tx_hash.as_str()) {
                    Some(s) => s,
                    None => continue,
                };

                let entry = flows.entry(tx_hash.to_string()).or_default();
                let slot = entry.entry(*quote_id).or_default();
                if from == sender {
                    slot.1 += amount; // quote_out
                }
                if to == sender {
                    slot.0 += amount; // quote_in
                }
            }
            ParsedLog::Transfer { .. } => {}
        }
    }

    flows
}

/// PositionHistory 생성 헬퍼 (quote-aware)
///
/// `quote_price` is prefetched by the caller (per (quote_id, block) cache in
/// `build_position_histories`) so this function stays synchronous.
#[inline]
#[allow(clippy::too_many_arguments)]
fn create_position_history(
    token: Address,
    account: Address,
    tx_hash: &Arc<String>,
    block_number: u64,
    block_timestamp: u64,
    tx_index: u64,
    log_index: u64,
    quote_in: BigDecimal,
    quote_out: BigDecimal,
    token_in: BigDecimal,
    token_out: BigDecimal,
    quote_id: &str,
    quote_price: &Option<Arc<BigDecimal>>,
    transfer_type: TransferType,
    sender_address: Option<Arc<String>>,
) -> PositionHistoryEvent {
    let quote_decimals = get_quote_decimals(quote_id);

    let (usd_in, usd_out) = match quote_price {
        Some(price) => (
            (&quote_in / quote_decimals) * &**price,
            (&quote_out / quote_decimals) * &**price,
        ),
        None => (BigDecimal::from(0), BigDecimal::from(0)),
    };

    PositionHistoryEvent {
        account_id: Arc::new(account.to_string()),
        token_id: Arc::new(token.to_string()),
        quote_in: Arc::new(quote_in),
        quote_out: Arc::new(quote_out),
        usd_in: Arc::new(usd_in),
        usd_out: Arc::new(usd_out),
        token_in: Arc::new(token_in),
        token_out: Arc::new(token_out),
        transaction_hash: tx_hash.clone(),
        block_number,
        block_timestamp,
        tx_index,
        log_index,
        transfer_type,
        sender_address,
    }
}
