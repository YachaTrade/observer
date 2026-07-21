use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::time::sleep;
use tracing::warn;

pub mod account;
pub mod balance;
pub mod burn;
pub mod chart;
pub mod dividend;
pub mod fee;
pub mod lp;
pub mod lp_position;
pub mod market;
pub mod mint;
pub mod pool;
pub mod position;
pub mod price;
pub mod sniping;
pub mod swap;
pub mod token;
pub mod vault;
pub mod vault_registry;

pub use dividend::*;
pub use sniping::*;
pub use vault::*;
pub use vault_registry::*;

/// Shared retry helper for controller SQL operations. Retries up to 10 times
/// with exponential backoff; deadlock errors get a steeper backoff curve.
pub(crate) async fn retry_query<F, Fut, E>(name: &str, f: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<sqlx::postgres::PgQueryResult, E>>,
    E: std::fmt::Display,
{
    let max_attempts = 10;
    let base_delay = Duration::from_millis(100);
    let mut attempt = 0;

    loop {
        attempt += 1;
        match f().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if attempt >= max_attempts {
                    return Err(anyhow!(
                        "[V2] Failed to insert {} after {} attempts: {}",
                        name,
                        attempt,
                        e
                    ));
                }
                let delay = if e.to_string().to_lowercase().contains("deadlock") {
                    base_delay.mul_f32(2.0_f32.powi(attempt - 1))
                } else {
                    base_delay.mul_f32(1.5_f32.powi(attempt - 1))
                };
                warn!(
                    "[V2] {} insert failed on attempt {}: {}. Retrying in {}ms",
                    name,
                    attempt,
                    e,
                    delay.as_millis()
                );
                sleep(delay).await;
            }
        }
    }
}
