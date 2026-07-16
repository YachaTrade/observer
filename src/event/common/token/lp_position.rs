//! V2 NadFunPair LP `Transfer` log → 1 or 2 `LpPositionHistoryEvent` rows.
//!
//! Decoding rules (see spec `docs/superpowers/specs/2026-05-14-v2-lp-position-design.md`):
//!
//! | chain event                  | rows emitted                                           |
//! |------------------------------|--------------------------------------------------------|
//! | `from = 0x0` (mint)          | 1 row: `{account=to,   event="mint",         lp_in=v}` |
//! | `to   = 0x0` (burn)          | 1 row: `{account=from, event="burn",         lp_out=v}`|
//! | holder → holder              | 2 rows: transfer_out (sender) + transfer_in (recipient)|
//!
//! Cost basis (`token0_in/out`, `token1_in/out`) is filled by the BEFORE INSERT
//! trigger `update_lp_position_on_history`, not by this parser.

use std::sync::Arc;

use alloy::rpc::types::Log;
use alloy::sol;
use bigdecimal::BigDecimal;
use tracing::{error, info};

use crate::types::token::LpPositionHistoryEvent;
use crate::utils::to_big_decimal;

sol! {
    #[sol(rpc)]
    interface IV2PairTransfer {
        event Transfer(address indexed from, address indexed to, uint256 value);
    }
}

const ZERO_ADDR: &str = "0x0000000000000000000000000000000000000000";

/// Decode a V2 NadFunPair `Transfer` log into 1 or 2 `LpPositionHistoryEvent`
/// rows. Returns an empty vec on decode failure, missing `transaction_hash`,
/// or self-transfer (`from == to`).
///
/// The same `log_index` is used for both rows in a holder→holder transfer —
/// the primary key `(account_id, pool_id, transaction_hash, tx_index, log_index)`
/// disambiguates by `account_id`.
pub fn parse_lp_position_log(
    log: &Log,
    block_number: u64,
    block_timestamp: u64,
    transaction_index: u64,
    log_index: u64,
) -> Vec<LpPositionHistoryEvent> {
    let pool_addr = log.address().to_string();
    let tx_hash_opt = log.transaction_hash.map(|h| h.to_string());
    let tx_hash_str = tx_hash_opt.as_deref().unwrap_or("<no-tx>");

    let decoded = match log.log_decode::<IV2PairTransfer::Transfer>() {
        Ok(d) => d,
        Err(e) => {
            error!(
                "[LP] decode_failed pool={} tx={} log_index={} err={:?}",
                pool_addr, tx_hash_str, log_index, e
            );
            return Vec::new();
        }
    };
    let IV2PairTransfer::Transfer { from, to, value } = decoded.inner.data;
    if from == to {
        error!(
            "[LP] self_transfer_dropped pool={} tx={} log_index={} addr={}",
            pool_addr, tx_hash_str, log_index, from
        );
        return Vec::new();
    }

    let tx_hash = match log.transaction_hash {
        Some(h) => Arc::new(h.to_string()),
        None => {
            error!(
                "[LP] missing_tx_hash pool={} block={} log_index={} from={} to={} value={}",
                pool_addr, block_number, log_index, from, to, value
            );
            return Vec::new();
        }
    };
    let pool_id = Arc::new(log.address().to_string());
    let amount = Arc::new(to_big_decimal(value));
    let zero = Arc::new(BigDecimal::from(0));

    let from_str = from.to_string();
    let to_str = to.to_string();
    let from_arc = Arc::new(from_str.clone());
    let to_arc = Arc::new(to_str.clone());

    if from_str.eq_ignore_ascii_case(ZERO_ADDR) {
        // MINT — 1 row
        info!(
            "[LP] mint pool={} tx={} log_index={} to={} value={}",
            pool_addr, tx_hash_str, log_index, to_str, value
        );
        vec![LpPositionHistoryEvent {
            account_id: to_arc,
            pool_id,
            lp_in: amount,
            lp_out: zero,
            event_type: "mint",
            counterparty: None,
            block_number,
            block_timestamp,
            transaction_hash: tx_hash,
            transaction_index,
            log_index,
        }]
    } else if to_str.eq_ignore_ascii_case(ZERO_ADDR) {
        // BURN — 1 row
        info!(
            "[LP] burn pool={} tx={} log_index={} from={} value={}",
            pool_addr, tx_hash_str, log_index, from_str, value
        );
        vec![LpPositionHistoryEvent {
            account_id: from_arc,
            pool_id,
            lp_in: zero,
            lp_out: amount,
            event_type: "burn",
            counterparty: None,
            block_number,
            block_timestamp,
            transaction_hash: tx_hash,
            transaction_index,
            log_index,
        }]
    } else {
        // HOLDER → HOLDER — 2 rows. Share Arcs where possible.
        info!(
            "[LP] transfer pool={} tx={} log_index={} from={} to={} value={}",
            pool_addr, tx_hash_str, log_index, from_str, to_str, value
        );
        vec![
            LpPositionHistoryEvent {
                account_id: from_arc.clone(),
                pool_id: pool_id.clone(),
                lp_in: zero.clone(),
                lp_out: amount.clone(),
                event_type: "transfer_out",
                counterparty: Some(to_arc.clone()),
                block_number,
                block_timestamp,
                transaction_hash: tx_hash.clone(),
                transaction_index,
                log_index,
            },
            LpPositionHistoryEvent {
                account_id: to_arc,
                pool_id,
                lp_in: amount,
                lp_out: zero,
                event_type: "transfer_in",
                counterparty: Some(from_arc),
                block_number,
                block_timestamp,
                transaction_hash: tx_hash,
                transaction_index,
                // SAME log_index — PK is (account, pool, tx, tx_idx, log_idx)
                // so the two rows disambiguate by account_id.
                log_index,
            },
        ]
    }
}
