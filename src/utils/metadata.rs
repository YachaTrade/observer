use anyhow::{Context, Result, anyhow};
use std::{
    error::Error,
    time::{Duration, Instant},
};
use tokio::time::{self, sleep};
use tracing::{error, info, warn};

use crate::{
    db::postgres::{PostgresDatabase, controller::token::TokenController},
    types::v1::curve::TokenMetadata,
};

const REQUEST_TIMEOUT_SECS: u64 = 10;

pub async fn fetch_token_metadata(token_uri: &str) -> Result<TokenMetadata> {
    let start_time = Instant::now();

    let url = if !token_uri.starts_with("http://") && !token_uri.starts_with("https://") {
        format!("https://{}", token_uri)
    } else {
        token_uri.to_string()
    };

    // URI 검증

    if !(url.starts_with("https://storage.nadapp.net/") && url.ends_with(".json")) {
        error!("Invalid token URI: {}", url);
        return Err(anyhow!("Invalid token URI: {}", url));
    }

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

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()
        .context("HTTP 클라이언트 생성 실패")?;

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
            .header("User-Agent", "Nad-Observer/1.0") // User-Agent 추가
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
            if response.status().as_u16() == 404 {
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
