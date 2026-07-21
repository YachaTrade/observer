use std::sync::Arc;

use anyhow::Result;
use bigdecimal::BigDecimal;

use crate::{db::postgres::PostgresDatabase, measure_postgres};

use super::retry_query;

// ==================== SQL Constants ====================

pub const INSERT_SNIPING_PENALTIES_SQL: &str = r#"
INSERT INTO sniping_history (token_id, buyer, sniping_fee, penalty_bps, transaction_hash, block_number, created_at, log_index, tx_index)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::numeric[], $4::numeric[], $5::text[], $6::bigint[], $7::bigint[], $8::int[], $9::int[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

// ==================== Controller ====================

pub struct SnipingController {
    pub db: Arc<PostgresDatabase>,
}

impl SnipingController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        SnipingController { db }
    }

    pub async fn batch_insert_sniping_penalties(&self, data: &[SnipingPenaltyData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let token_ids: Vec<&str> = data.iter().map(|d| d.token_id.as_str()).collect();
        let buyers: Vec<&str> = data.iter().map(|d| d.buyer.as_str()).collect();
        let sniping_fees: Vec<&BigDecimal> = data.iter().map(|d| &d.sniping_fee).collect();
        let penalty_bps_list: Vec<&BigDecimal> = data.iter().map(|d| &d.penalty_bps).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();

        retry_query("sniping_penalties", || async {
            measure_postgres!("v2_batch_insert_sniping_penalties", {
                sqlx::query(INSERT_SNIPING_PENALTIES_SQL)
                    .bind(&token_ids)
                    .bind(&buyers)
                    .bind(&sniping_fees)
                    .bind(&penalty_bps_list)
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

pub struct SnipingPenaltyData {
    pub token_id: String,
    pub buyer: String,
    pub sniping_fee: BigDecimal,
    pub penalty_bps: BigDecimal,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
}
