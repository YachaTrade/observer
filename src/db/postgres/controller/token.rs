use std::{sync::Arc, time::Duration};

use crate::{
    config::DEFAULT_DELAY,
    db::postgres::PostgresDatabase,
    measure_postgres,
};

use anyhow::{Context, Result, anyhow};
use tokio::time::sleep;
use tracing::{error, info, warn};

/// SQL for batch inserting tokens, markets, and price_history rows via a
/// CTE chain. Params $1-$24 are UNNEST arrays, $25 is the WNATIVE quote_id
/// used to look up the latest native USD price.
pub const BATCH_INSERT_TOKENS_AND_MARKETS_SQL: &str = r#"
                    WITH data AS (
                        SELECT * FROM UNNEST(
                            $1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                            $6::text[], $7::text[], $8::text[], $9::text[], $10::boolean[],
                            $11::boolean[], $12::numeric[], $13::bigint[], $14::numeric[], $15::text[],
                            $16::bigint[], $17::bigint[], $18::text[], $19::int[], $20::int[],
                            $21::numeric[], $22::numeric[], $23::text[], $24::text[]
                        ) AS t(
                            token_id, name, symbol, creator, description,
                            twitter, telegram, website, image_uri, is_nsfw,
                            is_graduated, total_supply, created_at, price, market_type,
                            latest_trade_at, block_number, transaction_hash, log_index, tx_index,
                            reserve_quote, reserve_token, version, quote_id
                        )
                    ),
                    insert_token AS (
                        INSERT INTO token (
                            token_id, name, symbol, creator, description, twitter, telegram, website,
                            image_uri, is_nsfw, is_graduated, total_supply, created_at,
                            transaction_hash, version, chain
                        )
                        SELECT
                            token_id, name, symbol, creator, description, twitter, telegram, website,
                            image_uri, is_nsfw, is_graduated, total_supply, created_at,
                            transaction_hash, version, 'GIWA'
                        FROM data
                        ON CONFLICT (token_id) DO NOTHING
                    ),
                    latest_native_price AS (
                        SELECT COALESCE(
                            (SELECT p.price FROM price p WHERE p.quote_id = $25 ORDER BY p.block_number DESC LIMIT 1),
                            0
                        ) AS native_usd
                    ),
                    insert_market AS (
                        INSERT INTO market (
                            market_type, token_id, quote_id, price, ath_price, ath_price_quote, reserve_quote, reserve_token, latest_trade_at, created_at
                        )
                        SELECT
                            d.market_type, d.token_id, d.quote_id, d.price,
                            d.price * lnp.native_usd,
                            d.price,
                            d.reserve_quote, d.reserve_token, d.latest_trade_at, d.created_at
                        FROM data d, latest_native_price lnp
                        ON CONFLICT (token_id) DO NOTHING
                    )
                    INSERT INTO price_history (
                        token_id, price, volume, created_at, block_number,
                        transaction_hash, log_index, tx_index
                    )
                    SELECT
                        token_id, price, 0, created_at, block_number,
                        transaction_hash, log_index, tx_index
                    FROM data
                    ON CONFLICT (token_id, block_number, transaction_hash, tx_index, log_index) DO NOTHING
                    "#;

/// v1/v2 공용 token+market batch insert 데이터
pub struct TokenBatchData {
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub creator: String,
    pub description: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub website: Option<String>,
    pub image_uri: String,
    pub is_nsfw: bool,
    pub version: String,
    pub market_type: String, // "CURVE" or "DEX"
    pub quote_id: String, // quote token address (default: WMON)
    pub virtual_native: String,
    pub virtual_token: String,
    pub block_number: i64,
    pub block_timestamp: i64,
    pub transaction_hash: String,
    pub log_index: i32,
    pub tx_index: i32,
}

pub struct TokenController {
    pub db: Arc<PostgresDatabase>,
}

impl TokenController {
    pub fn new(db: Arc<PostgresDatabase>) -> Self {
        TokenController { db }
    }

    /// Fetch metadata from token_metadata table
    pub async fn fetch_metadata(
        &self,
        metadata_url: &str,
    ) -> Result<crate::types::v1::curve::TokenMetadata> {
        let row = sqlx::query!(
            r#"
            SELECT name, symbol, description, image_url, website, twitter, telegram, is_nsfw
            FROM token_metadata
            WHERE metadata_url = $1
            "#,
            metadata_url
        )
        .fetch_optional(&self.db.pool)
        .await
        .context("Failed to query token_metadata")?;

        match row {
            Some(r) => {
                info!("✅ Metadata found in DB: {}", metadata_url);
                Ok(crate::types::v1::curve::TokenMetadata {
                    description: r.description,
                    twitter: r.twitter,
                    telegram: r.telegram,
                    website: r.website,
                    image_uri: r.image_url.unwrap_or_default(),
                    is_nsfw: r.is_nsfw,
                })
            }
            None => {
                info!(
                    "❌ Metadata not found in DB, will fetch from URL: {}",
                    metadata_url
                );
                Err(anyhow!("Metadata not found in DB"))
            }
        }
    }

    /// Delete metadata from token_metadata table after successful token insert
    pub async fn delete_metadata(&self, metadata_url: &str) -> Result<()> {
        sqlx::query!(
            r#"
            DELETE FROM token_metadata
            WHERE metadata_url = $1
            "#,
            metadata_url
        )
        .execute(&self.db.pool)
        .await
        .context("Failed to delete token_metadata")?;

        info!("🗑️ Deleted metadata: {}", metadata_url);
        Ok(())
    }

    /// Batch delete metadata from token_metadata table
    pub async fn batch_delete_metadata(&self, metadata_urls: &[String]) -> Result<()> {
        if metadata_urls.is_empty() {
            return Ok(());
        }

        sqlx::query!(
            r#"
            DELETE FROM token_metadata
            WHERE metadata_url = ANY($1)
            "#,
            metadata_urls
        )
        .execute(&self.db.pool)
        .await
        .context("Failed to batch delete token_metadata")?;

        info!("🗑️ Batch deleted {} metadata entries", metadata_urls.len());
        Ok(())
    }

    pub async fn batch_insert_tokens_and_markets(
        &self,
        batch: &[TokenBatchData],
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let max_attempts = 10;
        let base_delay = Duration::from_millis(*DEFAULT_DELAY);
        let mut attempt = 0;

        let total_supply = bigdecimal::BigDecimal::from(1_000_000_000_000_000_000_000_000_000u128);

        let mut token_ids = Vec::new();
        let mut names = Vec::new();
        let mut symbols = Vec::new();
        let mut creators = Vec::new();
        let mut descriptions = Vec::new();
        let mut twitters = Vec::new();
        let mut telegrams = Vec::new();
        let mut websites = Vec::new();
        let mut image_uris = Vec::new();
        let mut is_nsfws = Vec::new();
        let mut is_graduateds = Vec::new();
        let mut total_supplies = Vec::new();
        let mut created_ats = Vec::new();
        let mut prices = Vec::new();
        let mut market_types = Vec::new();
        let mut latest_trade_ats = Vec::new();
        let mut block_numbers = Vec::new();
        let mut transaction_hashes = Vec::new();
        let mut log_indices = Vec::new();
        let mut tx_indices = Vec::new();
        let mut reserve_quotes = Vec::new();
        let mut reserve_tokens = Vec::new();
        let mut versions = Vec::new();
        let mut quote_ids = Vec::new();

        for data in batch {
            use bigdecimal::{BigDecimal, RoundingMode};
            use std::str::FromStr;

            let vn = BigDecimal::from_str(&data.virtual_native).unwrap_or_default();
            let vt = BigDecimal::from_str(&data.virtual_token).unwrap_or_default();
            let price = if vt > BigDecimal::from(0) {
                (&vn / &vt).with_scale_round(10, RoundingMode::Up)
            } else {
                BigDecimal::from(0)
            };

            token_ids.push(data.token_id.clone());
            names.push(data.name.clone());
            symbols.push(data.symbol.clone());
            creators.push(data.creator.clone());
            descriptions.push(data.description.clone());
            twitters.push(data.twitter.clone());
            telegrams.push(data.telegram.clone());
            websites.push(data.website.clone());
            image_uris.push(data.image_uri.clone());
            is_nsfws.push(data.is_nsfw);
            is_graduateds.push(false);
            total_supplies.push(total_supply.clone());
            created_ats.push(data.block_timestamp);
            prices.push(price);
            market_types.push(data.market_type.clone());
            latest_trade_ats.push(data.block_timestamp);
            block_numbers.push(data.block_number);
            transaction_hashes.push(data.transaction_hash.clone());
            log_indices.push(data.log_index);
            tx_indices.push(data.tx_index);
            reserve_quotes.push(data.virtual_native.clone());
            reserve_tokens.push(data.virtual_token.clone());
            versions.push(data.version.clone());
            quote_ids.push(data.quote_id.clone());
        }

        loop {
            attempt += 1;
            let query_result = measure_postgres!("token_batch_insert_tokens_and_markets", {
                sqlx::query(BATCH_INSERT_TOKENS_AND_MARKETS_SQL)
                .bind(&token_ids)           // $1
                .bind(&names)               // $2
                .bind(&symbols)             // $3
                .bind(&creators)            // $4
                .bind(&descriptions)        // $5
                .bind(&twitters)            // $6
                .bind(&telegrams)           // $7
                .bind(&websites)            // $8
                .bind(&image_uris)          // $9
                .bind(&is_nsfws)            // $10
                .bind(&is_graduateds)       // $11
                .bind(&total_supplies)      // $12
                .bind(&created_ats)         // $13
                .bind(&prices)              // $14
                .bind(&market_types)        // $15
                .bind(&latest_trade_ats)    // $16
                .bind(&block_numbers)       // $17
                .bind(&transaction_hashes)  // $18
                .bind(&log_indices)         // $19
                .bind(&tx_indices)          // $20
                .bind(&reserve_quotes)     // $21
                .bind(&reserve_tokens)      // $22
                .bind(&versions)            // $23
                .bind(&quote_ids)           // $24
                .bind(&*crate::config::WNATIVE_ADDRESS)  // $25 (WMON for latest_native_price)
                .execute(&self.db.pool)
                .await
            });

            match query_result {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if attempt >= max_attempts {
                        let err_msg = format!(
                            "Failed to batch insert {} tokens and markets after {} attempts: {}",
                            batch.len(),
                            attempt,
                            err
                        );
                        error!("[TOKEN] {}", err_msg);
                        return Err(anyhow!(err_msg));
                    }

                    let current_delay = if err.to_string().contains("deadlock") {
                        base_delay.mul_f32(2.0_f32.powi(attempt - 1))
                    } else {
                        base_delay.mul_f32(1.5_f32.powi(attempt - 1))
                    };
                    warn!(
                        "[TOKEN] Error batch inserting {} tokens and markets: {}. Backing off for {}ms before retry",
                        batch.len(),
                        err,
                        current_delay.as_millis()
                    );
                    sleep(current_delay).await;
                    continue;
                }
            }
        }
    }
}
