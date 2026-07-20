use std::sync::Arc;

use anyhow::Result;
use bigdecimal::BigDecimal;

use crate::{db::postgres::PostgresDatabase, measure_postgres};

use super::retry_query;

// ==================== SQL Constants ====================

pub const INSERT_DIVIDEND_SETUPS_SQL: &str = r#"
INSERT INTO v2_dividend_setups (source_token, dividend_token, ratio, min_balance, entry_index, transaction_hash, block_number, created_at, log_index, tx_index)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::int[], $4::numeric[], $5::int[], $6::text[], $7::bigint[], $8::bigint[], $9::int[], $10::int[])
ON CONFLICT (transaction_hash, tx_index, log_index, entry_index) DO NOTHING
"#;

pub const INSERT_DIVIDEND_DEPOSITS_SQL: &str = r#"
INSERT INTO v2_dividend_deposits (source_token, dividend_token, amount, pending, entry_index, transaction_hash, block_number, created_at, log_index, tx_index, quote_id, usd_value)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::numeric[], $4::bool[], $5::int[], $6::text[], $7::bigint[], $8::bigint[], $9::int[], $10::int[], $11::text[], $12::numeric[])
ON CONFLICT (transaction_hash, tx_index, log_index, entry_index) DO NOTHING
"#;

pub const INSERT_DIVIDEND_CONVERSIONS_SQL: &str = r#"
INSERT INTO v2_dividend_conversions (source_token, dividend_token, consumed_quote, received, entry_index, transaction_hash, block_number, created_at, log_index, tx_index, quote_id, usd_value)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::numeric[], $4::numeric[], $5::int[], $6::text[], $7::bigint[], $8::bigint[], $9::int[], $10::int[], $11::text[], $12::numeric[])
ON CONFLICT (transaction_hash, tx_index, log_index, entry_index) DO NOTHING
"#;

pub const INSERT_DIVIDEND_MERKLE_ROOTS_SQL: &str = r#"
INSERT INTO v2_dividend_merkle_roots (merkle_root, transaction_hash, block_number, created_at, log_index, tx_index)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::bigint[], $5::int[], $6::int[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

// merkle_root is resolved per row: latest SetMerkleRoot at or before the
// claim's (block, tx, log) coordinates. Requires merkle root rows from the
// same batch to be inserted BEFORE claims (receive.rs sequences this).
pub const INSERT_DIVIDEND_CLAIMS_SQL: &str = r#"
INSERT INTO v2_dividend_claims
    (holder, source_token, dividend_token, amount, merkle_root, entry_index,
     transaction_hash, block_number, created_at, log_index, tx_index, usd_value)
SELECT u.holder, u.source_token, u.dividend_token, u.amount,
       (SELECT m.merkle_root
          FROM v2_dividend_merkle_roots m
         WHERE (m.block_number, m.tx_index, m.log_index)
            <= (u.block_number, u.tx_index, u.log_index)
         ORDER BY m.block_number DESC, m.tx_index DESC, m.log_index DESC
         LIMIT 1),
       u.entry_index, u.transaction_hash, u.block_number, u.created_at,
       u.log_index, u.tx_index, u.usd_value
FROM UNNEST($1::text[], $2::text[], $3::text[], $4::numeric[], $5::int[],
            $6::text[], $7::bigint[], $8::bigint[], $9::int[], $10::int[], $11::numeric[])
     AS u(holder, source_token, dividend_token, amount, entry_index,
          transaction_hash, block_number, created_at, log_index, tx_index, usd_value)
ON CONFLICT (transaction_hash, tx_index, log_index, entry_index) DO NOTHING
"#;

// ==================== Data Structs ====================

pub struct DividendSetupData {
    pub source_token: String,
    pub dividend_token: String,
    pub ratio: i32,
    pub min_balance: BigDecimal,
    pub entry_index: i32,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
}

pub struct DividendDepositData {
    pub source_token: String,
    pub dividend_token: String,
    pub amount: BigDecimal,
    pub pending: bool,
    pub entry_index: i32,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
    pub quote_id: Option<String>,
    pub usd_value: BigDecimal,
}

pub struct DividendConversionData {
    pub source_token: String,
    pub dividend_token: String,
    pub consumed_quote: BigDecimal,
    pub received: BigDecimal,
    pub entry_index: i32,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
    pub quote_id: Option<String>,
    pub usd_value: BigDecimal,
}

pub struct DividendMerkleRootData {
    pub merkle_root: String,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
}

pub struct DividendClaimData {
    pub holder: String,
    pub source_token: String,
    pub dividend_token: String,
    pub amount: BigDecimal,
    pub entry_index: i32,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
    pub usd_value: BigDecimal,
}

// ==================== Controller ====================

pub struct V2DividendController {
    pub db: Arc<PostgresDatabase>,
}

impl V2DividendController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        V2DividendController { db }
    }

    pub async fn batch_insert_dividend_setups(&self, data: &[DividendSetupData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let source_tokens: Vec<&str> = data.iter().map(|d| d.source_token.as_str()).collect();
        let dividend_tokens: Vec<&str> = data.iter().map(|d| d.dividend_token.as_str()).collect();
        let ratios: Vec<i32> = data.iter().map(|d| d.ratio).collect();
        let min_balances: Vec<&BigDecimal> = data.iter().map(|d| &d.min_balance).collect();
        let entry_indices: Vec<i32> = data.iter().map(|d| d.entry_index).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();

        retry_query("dividend_setups", || async {
            measure_postgres!("v2_batch_insert_dividend_setups", {
                sqlx::query(INSERT_DIVIDEND_SETUPS_SQL)
                    .bind(&source_tokens)
                    .bind(&dividend_tokens)
                    .bind(&ratios)
                    .bind(&min_balances)
                    .bind(&entry_indices)
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

    pub async fn batch_insert_dividend_deposits(&self, data: &[DividendDepositData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let source_tokens: Vec<&str> = data.iter().map(|d| d.source_token.as_str()).collect();
        let dividend_tokens: Vec<&str> = data.iter().map(|d| d.dividend_token.as_str()).collect();
        let amounts: Vec<&BigDecimal> = data.iter().map(|d| &d.amount).collect();
        let pendings: Vec<bool> = data.iter().map(|d| d.pending).collect();
        let entry_indices: Vec<i32> = data.iter().map(|d| d.entry_index).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let quote_ids: Vec<Option<&str>> = data.iter().map(|d| d.quote_id.as_deref()).collect();
        let usd_values: Vec<&BigDecimal> = data.iter().map(|d| &d.usd_value).collect();

        retry_query("dividend_deposits", || async {
            measure_postgres!("v2_batch_insert_dividend_deposits", {
                sqlx::query(INSERT_DIVIDEND_DEPOSITS_SQL)
                    .bind(&source_tokens)
                    .bind(&dividend_tokens)
                    .bind(&amounts)
                    .bind(&pendings)
                    .bind(&entry_indices)
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

    pub async fn batch_insert_dividend_conversions(
        &self,
        data: &[DividendConversionData],
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let source_tokens: Vec<&str> = data.iter().map(|d| d.source_token.as_str()).collect();
        let dividend_tokens: Vec<&str> = data.iter().map(|d| d.dividend_token.as_str()).collect();
        let consumed_quotes: Vec<&BigDecimal> = data.iter().map(|d| &d.consumed_quote).collect();
        let receiveds: Vec<&BigDecimal> = data.iter().map(|d| &d.received).collect();
        let entry_indices: Vec<i32> = data.iter().map(|d| d.entry_index).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let quote_ids: Vec<Option<&str>> = data.iter().map(|d| d.quote_id.as_deref()).collect();
        let usd_values: Vec<&BigDecimal> = data.iter().map(|d| &d.usd_value).collect();

        retry_query("dividend_conversions", || async {
            measure_postgres!("v2_batch_insert_dividend_conversions", {
                sqlx::query(INSERT_DIVIDEND_CONVERSIONS_SQL)
                    .bind(&source_tokens)
                    .bind(&dividend_tokens)
                    .bind(&consumed_quotes)
                    .bind(&receiveds)
                    .bind(&entry_indices)
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

    pub async fn batch_insert_dividend_merkle_roots(
        &self,
        data: &[DividendMerkleRootData],
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let merkle_roots: Vec<&str> = data.iter().map(|d| d.merkle_root.as_str()).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();

        retry_query("dividend_merkle_roots", || async {
            measure_postgres!("v2_batch_insert_dividend_merkle_roots", {
                sqlx::query(INSERT_DIVIDEND_MERKLE_ROOTS_SQL)
                    .bind(&merkle_roots)
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

    pub async fn batch_insert_dividend_claims(&self, data: &[DividendClaimData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let holders: Vec<&str> = data.iter().map(|d| d.holder.as_str()).collect();
        let source_tokens: Vec<&str> = data.iter().map(|d| d.source_token.as_str()).collect();
        let dividend_tokens: Vec<&str> = data.iter().map(|d| d.dividend_token.as_str()).collect();
        let amounts: Vec<&BigDecimal> = data.iter().map(|d| &d.amount).collect();
        let entry_indices: Vec<i32> = data.iter().map(|d| d.entry_index).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();
        let usd_values: Vec<&BigDecimal> = data.iter().map(|d| &d.usd_value).collect();

        retry_query("dividend_claims", || async {
            measure_postgres!("v2_batch_insert_dividend_claims", {
                sqlx::query(INSERT_DIVIDEND_CLAIMS_SQL)
                    .bind(&holders)
                    .bind(&source_tokens)
                    .bind(&dividend_tokens)
                    .bind(&amounts)
                    .bind(&entry_indices)
                    .bind(&tx_hashes)
                    .bind(&block_numbers)
                    .bind(&created_ats)
                    .bind(&log_indices)
                    .bind(&tx_indices)
                    .bind(&usd_values)
                    .execute(&self.db.pool)
                    .await
            })
        })
        .await
    }
}
