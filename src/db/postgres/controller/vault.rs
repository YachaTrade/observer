use std::sync::Arc;

use anyhow::Result;
use bigdecimal::BigDecimal;

use crate::{db::postgres::PostgresDatabase, measure_postgres};

use super::retry_query;

// ==================== SQL Constants ====================

pub const INSERT_VAULT_BURNS_SQL: &str = r#"
INSERT INTO v2_vault_burns (vault_type, token_id, quote_in, token_burned, transaction_hash, block_number, created_at, log_index, tx_index, quote_id, usd_value)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::numeric[], $4::numeric[], $5::text[], $6::bigint[], $7::bigint[], $8::int[], $9::int[], $10::text[], $11::numeric[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub const INSERT_VAULT_LP_INJECTIONS_SQL: &str = r#"
INSERT INTO v2_vault_lp_injections (token_id, quote_used, token_used, lp_burned, transaction_hash, block_number, created_at, log_index, tx_index, quote_id, usd_value)
SELECT * FROM UNNEST($1::text[], $2::numeric[], $3::numeric[], $4::numeric[], $5::text[], $6::bigint[], $7::bigint[], $8::int[], $9::int[], $10::text[], $11::numeric[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub const INSERT_CREATOR_FEE_CLAIMS_SQL: &str = r#"
INSERT INTO v2_creator_fee_claims (event_type, token_id, creator, amount, new_balance, transaction_hash, block_number, created_at, log_index, tx_index, quote_id, usd_value)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::numeric[], $5::numeric[], $6::text[], $7::bigint[], $8::bigint[], $9::int[], $10::int[], $11::text[], $12::numeric[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub const INSERT_GIFTS_SQL: &str = r#"
INSERT INTO v2_gifts (event_type, token_id, platform, platform_id, receiver, amount, new_balance, transaction_hash, block_number, created_at, log_index, tx_index, quote_id, usd_value, expires_at)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::numeric[], $7::numeric[], $8::text[], $9::bigint[], $10::bigint[], $11::int[], $12::int[], $13::text[], $14::numeric[], $15::bigint[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub const INSERT_CREATOR_UPDATES_SQL: &str = r#"
INSERT INTO v2_creator_updates (event_type, token_id, old_creator, new_creator, transaction_hash, block_number, created_at, log_index, tx_index)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[], $8::int[], $9::int[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

pub const INSERT_GIFT_EXPIRY_UPDATES_SQL: &str = r#"
INSERT INTO v2_gift_expiry_updates (old_duration, new_duration, transaction_hash, block_number, created_at, log_index, tx_index)
SELECT * FROM UNNEST($1::numeric[], $2::numeric[], $3::text[], $4::bigint[], $5::bigint[], $6::int[], $7::int[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

// ==================== Controller ====================

pub struct VaultController {
    pub db: Arc<PostgresDatabase>,
}

impl VaultController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        VaultController { db }
    }

    pub async fn batch_insert_vault_burns(&self, data: &[VaultBurnData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let vault_types: Vec<&str> = data.iter().map(|d| d.vault_type.as_str()).collect();
        let token_ids: Vec<&str> = data.iter().map(|d| d.token_id.as_str()).collect();
        let quote_ins: Vec<&BigDecimal> = data.iter().map(|d| &d.quote_in).collect();
        let token_burneds: Vec<&BigDecimal> = data.iter().map(|d| &d.token_burned).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let quote_ids: Vec<Option<&str>> = data.iter().map(|d| d.quote_id.as_deref()).collect();
        let usd_values: Vec<&BigDecimal> = data.iter().map(|d| &d.usd_value).collect();

        retry_query("vault_burns", || async {
            measure_postgres!("v2_batch_insert_vault_burns", {
                sqlx::query(INSERT_VAULT_BURNS_SQL)
                    .bind(&vault_types)
                    .bind(&token_ids)
                    .bind(&quote_ins)
                    .bind(&token_burneds)
                    .bind(&tx_hashes)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .bind(&quote_ids)
                    .bind(&usd_values)
                    .execute(&self.db.pool)
                    .await
            })
        })
        .await
    }

    pub async fn batch_insert_vault_lp_injections(&self, data: &[VaultLpInjectData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let token_ids: Vec<&str> = data.iter().map(|d| d.token_id.as_str()).collect();
        let quote_useds: Vec<&BigDecimal> = data.iter().map(|d| &d.quote_used).collect();
        let token_useds: Vec<&BigDecimal> = data.iter().map(|d| &d.token_used).collect();
        let lp_burneds: Vec<&BigDecimal> = data.iter().map(|d| &d.lp_burned).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let quote_ids: Vec<Option<&str>> = data.iter().map(|d| d.quote_id.as_deref()).collect();
        let usd_values: Vec<&BigDecimal> = data.iter().map(|d| &d.usd_value).collect();

        retry_query("vault_lp_inject", || async {
            measure_postgres!("v2_batch_insert_vault_lp_injections", {
                sqlx::query(INSERT_VAULT_LP_INJECTIONS_SQL)
                    .bind(&token_ids)
                    .bind(&quote_useds)
                    .bind(&token_useds)
                    .bind(&lp_burneds)
                    .bind(&tx_hashes)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .bind(&quote_ids)
                    .bind(&usd_values)
                    .execute(&self.db.pool)
                    .await
            })
        })
        .await
    }

    pub async fn batch_insert_creator_fee_claims(
        &self,
        data: &[CreatorFeeClaimData],
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let event_types: Vec<&str> = data.iter().map(|d| d.event_type.as_str()).collect();
        let token_ids: Vec<&str> = data.iter().map(|d| d.token_id.as_str()).collect();
        let creators: Vec<Option<&str>> = data.iter().map(|d| d.creator.as_deref()).collect();
        let amounts: Vec<&BigDecimal> = data.iter().map(|d| &d.amount).collect();
        let new_balances: Vec<Option<&BigDecimal>> =
            data.iter().map(|d| d.new_balance.as_ref()).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let quote_ids: Vec<Option<&str>> = data.iter().map(|d| d.quote_id.as_deref()).collect();
        let usd_values: Vec<&BigDecimal> = data.iter().map(|d| &d.usd_value).collect();

        retry_query("creator_fee_claims", || async {
            measure_postgres!("v2_batch_insert_creator_fee_claims", {
                sqlx::query(INSERT_CREATOR_FEE_CLAIMS_SQL)
                    .bind(&event_types)
                    .bind(&token_ids)
                    .bind(&creators)
                    .bind(&amounts)
                    .bind(&new_balances)
                    .bind(&tx_hashes)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .bind(&quote_ids)
                    .bind(&usd_values)
                    .execute(&self.db.pool)
                    .await
            })
        })
        .await
    }

    pub async fn batch_insert_gifts(&self, data: &[GiftData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let event_types: Vec<&str> = data.iter().map(|d| d.event_type.as_str()).collect();
        let token_ids: Vec<&str> = data.iter().map(|d| d.token_id.as_str()).collect();
        let platforms: Vec<Option<&str>> = data.iter().map(|d| d.platform.as_deref()).collect();
        let platform_ids: Vec<Option<&str>> =
            data.iter().map(|d| d.platform_id.as_deref()).collect();
        let receivers: Vec<Option<&str>> = data.iter().map(|d| d.receiver.as_deref()).collect();
        let amounts: Vec<Option<&BigDecimal>> = data.iter().map(|d| d.amount.as_ref()).collect();
        let new_balances: Vec<Option<&BigDecimal>> =
            data.iter().map(|d| d.new_balance.as_ref()).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let quote_ids: Vec<Option<&str>> = data.iter().map(|d| d.quote_id.as_deref()).collect();
        let usd_values: Vec<&BigDecimal> = data.iter().map(|d| &d.usd_value).collect();
        let expires_ats: Vec<i64> = data.iter().map(|d| d.expires_at).collect();

        retry_query("gifts", || async {
            measure_postgres!("v2_batch_insert_gifts", {
                sqlx::query(INSERT_GIFTS_SQL)
                    .bind(&event_types)
                    .bind(&token_ids)
                    .bind(&platforms)
                    .bind(&platform_ids)
                    .bind(&receivers)
                    .bind(&amounts)
                    .bind(&new_balances)
                    .bind(&tx_hashes)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .bind(&quote_ids)
                    .bind(&usd_values)
                    .bind(&expires_ats)
                    .execute(&self.db.pool)
                    .await
            })
        })
        .await
    }

    pub async fn batch_insert_creator_updates(&self, data: &[CreatorUpdateData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let event_types: Vec<&str> = data.iter().map(|d| d.event_type.as_str()).collect();
        let token_ids: Vec<&str> = data.iter().map(|d| d.token_id.as_str()).collect();
        let old_creators: Vec<Option<&str>> =
            data.iter().map(|d| d.old_creator.as_deref()).collect();
        let new_creators: Vec<&str> = data.iter().map(|d| d.new_creator.as_str()).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();

        retry_query("creator_updates", || async {
            measure_postgres!("v2_batch_insert_creator_updates", {
                sqlx::query(INSERT_CREATOR_UPDATES_SQL)
                    .bind(&event_types)
                    .bind(&token_ids)
                    .bind(&old_creators)
                    .bind(&new_creators)
                    .bind(&tx_hashes)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .execute(&self.db.pool)
                    .await
            })
        })
        .await
    }

    pub async fn batch_insert_gift_expiry_updates(
        &self,
        data: &[GiftExpiryUpdateData],
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let old_durations: Vec<&BigDecimal> = data.iter().map(|d| &d.old_duration).collect();
        let new_durations: Vec<&BigDecimal> = data.iter().map(|d| &d.new_duration).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();

        retry_query("gift_expiry_updates", || async {
            measure_postgres!("v2_batch_insert_gift_expiry_updates", {
                sqlx::query(INSERT_GIFT_EXPIRY_UPDATES_SQL)
                    .bind(&old_durations)
                    .bind(&new_durations)
                    .bind(&tx_hashes)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .execute(&self.db.pool)
                    .await
            })
        })
        .await
    }
}

// ==================== Data Structs ====================

pub struct VaultBurnData {
    pub vault_type: String,
    pub token_id: String,
    pub quote_in: BigDecimal,
    pub token_burned: BigDecimal,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
    pub quote_id: Option<String>,
    pub usd_value: BigDecimal,
}

pub struct VaultLpInjectData {
    pub token_id: String,
    pub quote_used: BigDecimal,
    pub token_used: BigDecimal,
    pub lp_burned: BigDecimal,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
    pub quote_id: Option<String>,
    pub usd_value: BigDecimal,
}

pub struct CreatorFeeClaimData {
    pub event_type: String,
    pub token_id: String,
    pub creator: Option<String>,
    pub amount: BigDecimal,
    pub new_balance: Option<BigDecimal>,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
    pub quote_id: Option<String>,
    pub usd_value: BigDecimal,
}

pub struct GiftData {
    pub event_type: String,
    pub token_id: String,
    // GiftVault.Platform enum name (e.g. "GITHUB", "X"); Some only for SETUP rows.
    pub platform: Option<String>,
    pub platform_id: Option<String>,
    pub receiver: Option<String>,
    pub amount: Option<BigDecimal>,
    pub new_balance: Option<BigDecimal>,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
    pub quote_id: Option<String>,
    pub usd_value: BigDecimal,
    // Gift expiry epoch in seconds. Meaningful on SETUP rows
    // (= block_timestamp + GiftVault.expiryDuration()) and RECEIVER_SET rows
    // (= 0, expiry cleared once a receiver is bound). 0 placeholder on
    // other event types — the trigger only reads expires_at for SETUP.
    pub expires_at: i64,
}

pub struct CreatorUpdateData {
    // 'SETUP' (initial bind, old_creator = None) or 'UPDATE' (subsequent change).
    pub event_type: String,
    pub token_id: String,
    pub old_creator: Option<String>,
    pub new_creator: String,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
}

pub struct GiftExpiryUpdateData {
    pub old_duration: BigDecimal,
    pub new_duration: BigDecimal,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
}
