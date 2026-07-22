use std::{
    error::Error,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::types::vault_registry::VaultMetadata;

const REQUEST_TIMEOUT_SECS: u64 = 10;
const ALLOWED_HOST: &str = "storage.nadapp.net";
const MAX_METADATA_BYTES: usize = 1024 * 1024;
const MAX_RETRIES: u32 = 5;

fn normalize_vault_metadata_url(uri: &str) -> Result<reqwest::Url> {
    let raw = if !uri.starts_with("http://") && !uri.starts_with("https://") {
        format!("https://{uri}")
    } else {
        uri.to_string()
    };
    let url = reqwest::Url::parse(&raw).context("invalid vault metadata URL")?;
    let allowed = url.scheme() == "https"
        && url.host_str() == Some(ALLOWED_HOST)
        && url.port_or_known_default() == Some(443)
        && url.username().is_empty()
        && url.password().is_none()
        && url.path().ends_with(".json")
        && url.query().is_none()
        && url.fragment().is_none();
    if !allowed {
        return Err(anyhow!("Invalid vault metadata URI: {url}"));
    }
    Ok(url)
}

/// Fetch and parse off-chain vault metadata JSON.
///
/// Mirrors [`crate::utils::metadata::fetch_token_metadata`]:
///   - Same URL allowlist (`storage.nadapp.net` / `*.json`).
///   - Loop-based retry (up to `MAX_RETRIES`) on transport errors.
///   - 404 short-circuits without retry.
///
/// DB caching is handled upstream in
/// [`VaultRegistryController::fetch_cached_metadata`], same as
/// `TokenController::fetch_metadata` serves tokens.
pub async fn fetch_vault_metadata(uri: &str) -> Result<VaultMetadata> {
    let start = Instant::now();

    let url = normalize_vault_metadata_url(uri)?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("HTTP 클라이언트 생성 실패")?;

    match do_fetch(&client, &url).await {
        Ok(metadata) => {
            info!(
                "🕒 fetch_vault_metadata 성공: {}ms (URL: {})",
                start.elapsed().as_millis(),
                url
            );
            Ok(metadata)
        }
        Err(err) => {
            error!(
                "🕒 fetch_vault_metadata 실패: {}ms (URL: {}) err={:#}",
                start.elapsed().as_millis(),
                url,
                err
            );
            Err(err)
        }
    }
}

async fn do_fetch(client: &reqwest::Client, url: &reqwest::Url) -> Result<VaultMetadata> {
    let mut retries: u32 = 0;
    loop {
        let response = match client
            .get(url.clone())
            .header("Accept", "application/json")
            .header("User-Agent", "GIWA-Observer/1.0")
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                sleep(Duration::from_millis(300)).await;
                warn!(
                    "fetch_vault_metadata transport error for {}: {:?}",
                    url,
                    err.source()
                );
                retries += 1;
                if retries >= MAX_RETRIES {
                    return Err(anyhow!(
                        "fetch_vault_metadata failed after {} retries: {}",
                        retries,
                        err
                    ));
                }
                continue;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let msg = format!("HTTP {} for vault metadata at {}", status, url);
            if status.as_u16() == 404 || status.is_redirection() {
                error!("{}", msg);
                return Err(anyhow!(msg));
            }
            retries += 1;
            if retries >= MAX_RETRIES {
                return Err(anyhow!(msg));
            }
            sleep(Duration::from_millis(300)).await;
            continue;
        }

        if response
            .content_length()
            .is_some_and(|length| length > MAX_METADATA_BYTES as u64)
        {
            return Err(anyhow!("vault metadata body too large at {url}"));
        }

        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .with_context(|| format!("reading body from {}", url))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_METADATA_BYTES {
                return Err(anyhow!("vault metadata body too large at {url}"));
            }
            bytes.extend_from_slice(&chunk);
        }

        if bytes.is_empty() {
            return Err(anyhow!("empty body from {}", url));
        }

        return serde_json::from_slice::<VaultMetadata>(&bytes)
            .with_context(|| format!("parsing vault metadata JSON from {}", url));
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_vault_metadata_url;

    #[test]
    fn vault_metadata_url_requires_the_exact_https_host_and_json_path() {
        assert!(normalize_vault_metadata_url("storage.nadapp.net/vault.json").is_ok());
        assert!(normalize_vault_metadata_url("https://storage.nadapp.net/vault.json").is_ok());

        for rejected in [
            "http://storage.nadapp.net/vault.json",
            "https://storage.nadapp.net.evil.example/vault.json",
            "https://storage.nadapp.net/vault.txt",
            "https://user@storage.nadapp.net/vault.json",
            "https://storage.nadapp.net:8443/vault.json",
        ] {
            assert!(
                normalize_vault_metadata_url(rejected).is_err(),
                "accepted unsafe metadata URL: {rejected}"
            );
        }
    }
}
