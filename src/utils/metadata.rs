use anyhow::{Context, Result, anyhow};
use std::{
    error::Error,
    time::{Duration, Instant},
};
use tokio::time::{self, sleep};
use tracing::{error, info, warn};

use crate::{
    db::postgres::{PostgresDatabase, controller::token::TokenController},
    types::metadata::TokenMetadata,
};

const REQUEST_TIMEOUT_SECS: u64 = 10;
const ALLOWED_BASE_URL: &str = "https://storage.yacha.trade/";

fn normalize_token_metadata_url(token_uri: &str) -> Result<reqwest::Url> {
    let raw = if !token_uri.starts_with("http://") && !token_uri.starts_with("https://") {
        format!("https://{token_uri}")
    } else {
        token_uri.to_string()
    };

    let url = reqwest::Url::parse(&raw).context("invalid token metadata URL")?;
    let allowed = url.as_str().starts_with(ALLOWED_BASE_URL)
        && url.scheme() == "https"
        && url.host_str() == Some("storage.yacha.trade")
        && url.port_or_known_default() == Some(443)
        && url.username().is_empty()
        && url.password().is_none()
        && url.path().ends_with(".json")
        && url.query().is_none()
        && url.fragment().is_none();
    if !allowed {
        return Err(anyhow!("Invalid token URI: {url}"));
    }

    Ok(url)
}

fn build_metadata_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("HTTP 클라이언트 생성 실패")
}

pub async fn fetch_token_metadata(token_uri: &str) -> Result<TokenMetadata> {
    let start_time = Instant::now();

    let url = normalize_token_metadata_url(token_uri)
        .inspect_err(|error| error!("{}", error))?
        .to_string();

    // 1. DB에서 먼저 조회 시도
    let db = PostgresDatabase::instance()?;
    let token_controller = TokenController::new(db);

    if let Ok(metadata) = token_controller.fetch_metadata(&url).await {
        let elapsed = start_time.elapsed();
        info!(
            "🕒 fetch_metadata from DB 성공: {}ms (URL: {})",
            elapsed.as_millis(),
            url
        );
        return Ok(metadata);
    }

    // 2. DB에 없으면 HTTP로 조회

    let client = build_metadata_client()?;

    // 첫 번째 시도
    match fetch_metadata(&client, &url).await {
        Ok(metadata) => {
            let elapsed = start_time.elapsed();
            info!(
                "🕒 fetch_metadata 성공: {}ms (URL: {})",
                elapsed.as_millis(),
                url
            );
            Ok(metadata)
        }
        Err(err) => {
            // 타임아웃 오류일 경우만 재시도
            if err.to_string().to_lowercase().contains("timeout") {
                let timeout_elapsed = start_time.elapsed();
                warn!(
                    "🕒 fetch_metadata 타임아웃 발생: {}ms, 재시도 중: {}",
                    timeout_elapsed.as_millis(),
                    url
                );
                time::sleep(Duration::from_millis(500)).await;

                match fetch_metadata(&client, &url).await {
                    Ok(metadata) => {
                        let elapsed = start_time.elapsed();
                        info!(
                            "🕒 fetch_metadata 재시도 성공: {}ms (URL: {})",
                            elapsed.as_millis(),
                            url
                        );
                        Ok(metadata)
                    }
                    Err(retry_err) => {
                        let elapsed = start_time.elapsed();
                        error!(
                            "🕒 fetch_metadata 재시도 실패: {}ms (URL: {})",
                            elapsed.as_millis(),
                            url
                        );
                        Err(retry_err)
                    }
                }
            } else {
                let elapsed = start_time.elapsed();
                error!(
                    "🕒 fetch_metadata 실패: {}ms (URL: {})",
                    elapsed.as_millis(),
                    url
                );
                Err(err)
            }
        }
    }
}

async fn fetch_metadata(client: &reqwest::Client, url: &str) -> Result<TokenMetadata> {
    let mut metadata = TokenMetadata::default();
    let max_retries = 5;
    let mut retries = 0;
    loop {
        let response = match client
            .get(url)
            .header("Accept", "application/json")
            .header("User-Agent", "GIWA-Observer/1.0")
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                sleep(Duration::from_millis(300)).await;
                warn!(
                    "Failed to fetch metadata: error sending request for url ({}) err :{:?}",
                    url,
                    err.source()
                );
                retries += 1;
                if retries >= max_retries {
                    return Err(anyhow!(
                        "Failed to fetch metadata after {} retries, err : {}",
                        retries,
                        err
                    ));
                }
                continue;
            }
        };

        if !response.status().is_success() {
            let err_msg = format!("Invalid HTTP status: {}: {}", response.status(), url);
            if response.status().as_u16() == 404 || response.status().is_redirection() {
                error!("{}", err_msg);
                return Err(anyhow!(err_msg));
            }

            continue;
        }

        let text = response
            .text()
            .await
            .context(format!("응답 바디 읽기 실패: {}", url))?;

        if text.trim().is_empty() {
            let err_msg = format!("빈 응답: {}", url);
            error!("{}", err_msg);
            return Err(anyhow!(err_msg));
        }

        let json = serde_json::from_str::<serde_json::Value>(&text)
            .context(format!("Invalid Json: {}", url))?;

        let image_uri = json
            .get("image_uri")
            .and_then(|v| v.as_str())
            .or_else(|| json.get("image").and_then(|v| v.as_str()))
            .ok_or_else(|| anyhow!("메타데이터에서 이미지 URI를 찾을 수 없음"))?;

        if image_uri.is_empty() {
            let err_msg = "메타데이터에서 이미지 URI가 비어 있음".to_string();
            error!("{}", err_msg);
            return Err(anyhow!(err_msg));
        }

        metadata.image_uri = image_uri.to_string();

        for (field, target) in [
            ("description", &mut metadata.description),
            ("website", &mut metadata.website),
            ("twitter", &mut metadata.twitter),
            ("telegram", &mut metadata.telegram),
        ] {
            if let Some(value) = json.get(field).and_then(|v| v.as_str()) {
                *target = Some(value.to_string());
            }
        }
        break;
    }

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::{build_metadata_client, normalize_token_metadata_url};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn token_metadata_url_accepts_yacha_json() {
        let json = normalize_token_metadata_url(
            "https://storage.yacha.trade/metadata/b52e9013-2654-445c-86bd-1b1e1e0776a8.json",
        )
        .expect("Yacha metadata URL with .json should be accepted");
        assert_eq!(
            json.as_str(),
            "https://storage.yacha.trade/metadata/b52e9013-2654-445c-86bd-1b1e1e0776a8.json"
        );
    }

    #[test]
    fn token_metadata_url_rejects_legacy_or_unsafe_locations() {
        for rejected in [
            "https://storage.nadapp.net/metadata/token.json",
            "http://storage.yacha.trade/metadata/token.json",
            "https://storage.yacha.trade.evil.example/metadata/token.json",
            "https://user@storage.yacha.trade/metadata/token.json",
            "https://storage.yacha.trade:8443/metadata/token.json",
            "https://storage.yacha.trade/metadata/token?format=.json",
            "https://storage.yacha.trade/metadata/not-json#ignored.json",
        ] {
            assert!(
                normalize_token_metadata_url(rejected).is_err(),
                "accepted unsafe metadata URL: {rejected}"
            );
        }
    }

    #[tokio::test]
    async fn metadata_client_does_not_follow_redirects() {
        let target = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind redirect target");
        let target_address = target.local_addr().expect("read redirect target address");
        tokio::spawn(async move {
            let (mut stream, _) = target.accept().await.expect("accept redirected request");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .await
                .expect("write redirect target response");
        });

        let redirect = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind redirect server");
        let redirect_address = redirect.local_addr().expect("read redirect server address");
        tokio::spawn(async move {
            let (mut stream, _) = redirect.accept().await.expect("accept initial request");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await;
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{target_address}/metadata.json\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write redirect response");
        });

        let response = build_metadata_client()
            .expect("build metadata client")
            .get(format!("http://{redirect_address}/metadata.json"))
            .send()
            .await
            .expect("send metadata request");

        assert!(
            response.status().is_redirection(),
            "metadata client followed redirect to {}",
            response.url()
        );
    }
}
