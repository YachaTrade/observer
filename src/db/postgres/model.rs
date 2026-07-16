use std::str::FromStr;

use bigdecimal::RoundingMode;

use sqlx::types::BigDecimal;

use crate::types::v1::curve::{CreateCurve, TokenMetadata};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Token {
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
    pub is_graduated: bool,
    pub total_supply: BigDecimal,
    pub created_at: i64,
    pub create_transaction_hash: String,
}
impl From<CreateCurve> for Token {
    fn from(create_curve: CreateCurve) -> Self {
        let TokenMetadata {
            description,
            twitter,
            telegram,
            website,
            image_uri,
            is_nsfw,
        } = create_curve.token_metadata;
        let total_supply = BigDecimal::from(1_000_000_000_000_000_000_000_000_000u128); //10억 ** 18
        Token {
            token_id: (*create_curve.token).clone(),
            name: (*create_curve.name).clone(),
            symbol: (*create_curve.symbol).clone(),
            creator: (*create_curve.creator).clone(),
            description,
            twitter,
            telegram,
            website,
            image_uri,
            is_nsfw,
            is_graduated: false,
            total_supply,
            created_at: create_curve.block_timestamp as i64,
            create_transaction_hash: (*create_curve.transaction_hash).clone(),
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Market {
    pub market_type: String,
    pub token_id: String,
    pub virtual_native: Option<BigDecimal>,
    pub virtual_token: Option<BigDecimal>,
    pub reserve_token: BigDecimal,
    pub reserve_quote: BigDecimal,
    pub price: BigDecimal,
    pub latest_trade_at: i64,
    pub created_at: i64,
}

impl From<CreateCurve> for Market {
    fn from(create_curve: CreateCurve) -> Self {
        let virtual_native =
            BigDecimal::from_str(create_curve.virtual_native.to_string().as_str()).unwrap();
        let virtual_token =
            BigDecimal::from_str(create_curve.virtual_token.to_string().as_str()).unwrap();

        let price =
            (virtual_native.clone() / virtual_token.clone()).with_scale_round(10, RoundingMode::Up);

        let latest_trade_at = create_curve.block_timestamp as i64;
        let reserve_token = BigDecimal::from(1_000_000_000_000_000_000_000_000_000u128);
        let reserve_quote = BigDecimal::from(0u128);

        let market_type = "CURVE".to_string();
        Self {
            market_type,
            token_id: (*create_curve.token).clone(),
            virtual_native: Some(virtual_native),
            virtual_token: Some(virtual_token),
            reserve_token,
            reserve_quote,
            latest_trade_at,
            price,
            created_at: create_curve.block_timestamp as i64,
        }
    }
}
