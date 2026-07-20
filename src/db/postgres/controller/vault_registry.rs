use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::Value as JsonValue;

use crate::{
    db::postgres::PostgresDatabase, measure_postgres, types::vault_registry::VaultMetadata,
};

use super::retry_query;

// ==================== SQL Constants ====================

pub const INSERT_VAULT_REGISTRY_SQL: &str = r#"
INSERT INTO v2_vault_registry (vault_id, transaction_hash, block_number, created_at, log_index, tx_index)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::bigint[], $5::int[], $6::int[])
ON CONFLICT (transaction_hash, tx_index, log_index) DO NOTHING
"#;

// Upsert metadata row on Register. The ON CONFLICT guard prevents a stale
// replay of an older Register from overwriting a newer one.
pub const UPSERT_VAULT_METADATA_SQL: &str = r#"
INSERT INTO v2_vault_metadata (
    vault_id, name, creator, vault_type, active,
    metadata_uri, metadata, metadata_fetched_at,
    registered_at, updated_at
)
VALUES ($1, $2, $3, $4, TRUE, $5, $6, $7, $8, $8)
ON CONFLICT (vault_id) DO UPDATE SET
    name = EXCLUDED.name,
    creator = EXCLUDED.creator,
    vault_type = EXCLUDED.vault_type,
    metadata_uri = EXCLUDED.metadata_uri,
    metadata = CASE
        WHEN EXCLUDED.metadata IS NOT NULL THEN EXCLUDED.metadata
        WHEN v2_vault_metadata.metadata_uri = EXCLUDED.metadata_uri THEN v2_vault_metadata.metadata
        ELSE NULL
    END,
    metadata_fetched_at = CASE
        WHEN EXCLUDED.metadata IS NOT NULL THEN EXCLUDED.metadata_fetched_at
        WHEN v2_vault_metadata.metadata_uri = EXCLUDED.metadata_uri THEN v2_vault_metadata.metadata_fetched_at
        ELSE NULL
    END,
    registered_at = EXCLUDED.registered_at,
    updated_at = GREATEST(v2_vault_metadata.updated_at, EXCLUDED.updated_at)
WHERE v2_vault_metadata.registered_at <= EXCLUDED.registered_at
"#;

// Update `active` on Deactivate. Guarded by block order so reorg replay of
// an older event can't undo a newer state change.
pub const UPDATE_VAULT_ACTIVE_SQL: &str = r#"
UPDATE v2_vault_metadata
   SET active = $2, updated_at = $3
 WHERE vault_id = $1
   AND updated_at <= $3
"#;

// ==================== Controller ====================

pub struct VaultRegistryController {
    pub db: Arc<PostgresDatabase>,
}

impl VaultRegistryController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        VaultRegistryController { db }
    }

    pub async fn batch_insert_registry_events(
        &self,
        data: &[VaultRegistryEventData],
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let vault_ids: Vec<&str> = data.iter().map(|d| d.vault_id.as_str()).collect();
        let tx_hashes: Vec<&str> = data.iter().map(|d| d.transaction_hash.as_str()).collect();
        let block_numbers: Vec<i64> = data.iter().map(|d| d.block_number).collect();
        let created_ats: Vec<i64> = data.iter().map(|d| d.created_at).collect();
        let log_indices: Vec<i32> = data.iter().map(|d| d.log_index).collect();
        let tx_indices: Vec<i32> = data.iter().map(|d| d.tx_index).collect();

        retry_query("vault_registry_events", || async {
            measure_postgres!("v2_batch_insert_vault_registry_events", {
                sqlx::query(INSERT_VAULT_REGISTRY_SQL)
                    .bind(&vault_ids)
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

    /// Upserts metadata rows one-by-one. The ON CONFLICT guard needs per-row
    /// parameters and we don't expect high volume (Register is admin-driven),
    /// so a sequential loop is clearer than the UNNEST trick.
    pub async fn upsert_vault_metadata_batch(&self, data: &[VaultMetadataData]) -> Result<()> {
        for row in data {
            retry_query("vault_metadata", || async {
                measure_postgres!("v2_upsert_vault_metadata", {
                    sqlx::query(UPSERT_VAULT_METADATA_SQL)
                        .bind(&row.vault_id)
                        .bind(&row.name)
                        .bind(&row.creator)
                        .bind(&row.vault_type)
                        .bind(row.metadata_uri.as_deref())
                        .bind(row.metadata.as_ref())
                        .bind(row.metadata_fetched_at)
                        .bind(row.registered_at)
                        .execute(&self.db.pool)
                        .await
                })
            })
            .await?;
        }
        Ok(())
    }

    pub async fn update_vault_active_batch(&self, data: &[VaultActiveData]) -> Result<()> {
        for row in data {
            retry_query("vault_metadata_active", || async {
                measure_postgres!("v2_update_vault_active", {
                    sqlx::query(UPDATE_VAULT_ACTIVE_SQL)
                        .bind(&row.vault_id)
                        .bind(row.active)
                        .bind(row.updated_at)
                        .execute(&self.db.pool)
                        .await
                })
            })
            .await?;
        }
        Ok(())
    }

    /// Mirror of `TokenController::fetch_metadata` for vault registry.
    /// Returns the (uri, metadata) pair if this vault has already been
    /// indexed with non-null metadata. Lets the stream phase skip both
    /// the eth_call to `metadataURI()` and the HTTP fetch on replay.
    pub async fn fetch_cached_metadata(
        &self,
        vault_id: &str,
    ) -> Result<Option<(String, VaultMetadata)>> {
        // Uses runtime `sqlx::query` (not `query!`) because this crate builds
        // with `SQLX_OFFLINE=true` and the schema file isn't part of the
        // prepared query cache.
        let row: Option<(Option<String>, Option<JsonValue>)> = sqlx::query_as(
            r#"
            SELECT metadata_uri, metadata
            FROM v2_vault_metadata
            WHERE vault_id = $1
              AND metadata_uri IS NOT NULL
              AND metadata IS NOT NULL
            "#,
        )
        .bind(vault_id)
        .fetch_optional(&self.db.pool)
        .await
        .context("query v2_vault_metadata for cached metadata")?;

        let Some((uri_opt, json_opt)) = row else {
            return Ok(None);
        };
        let Some(uri) = uri_opt else { return Ok(None) };
        let Some(json) = json_opt else {
            return Ok(None);
        };

        match serde_json::from_value::<VaultMetadata>(json) {
            Ok(md) => Ok(Some((uri, md))),
            Err(e) => {
                // Corrupt row — treat as cache miss so we re-fetch.
                tracing::warn!(
                    "v2_vault_metadata JSONB for {} failed to deserialize: {:#}",
                    vault_id,
                    e
                );
                Ok(None)
            }
        }
    }
}

// ==================== Data Structs ====================

pub struct VaultRegistryEventData {
    pub vault_id: String,
    pub transaction_hash: String,
    pub block_number: i64,
    pub created_at: i64,
    pub log_index: i32,
    pub tx_index: i32,
}

pub struct VaultMetadataData {
    pub vault_id: String,
    pub name: String,
    pub creator: String,
    pub vault_type: String,
    pub metadata_uri: Option<String>,
    pub metadata: Option<JsonValue>,
    pub metadata_fetched_at: Option<i64>,
    pub registered_at: i64,
}

pub struct VaultActiveData {
    pub vault_id: String,
    pub active: bool,
    pub updated_at: i64,
}
