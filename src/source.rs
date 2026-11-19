use std::{fs, time::Duration};

use chrono::{DateTime, Utc};

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

pub fn read_file(path: &str) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("cannot read {path}: {e}"))
}

pub fn build_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| format!("cannot create HTTP client: {e}"))
}

/// Fetches the page and returns its body together with the time the full
/// response body was received.
pub async fn fetch_url(
    client: &reqwest::Client,
    url: &str,
) -> Result<(String, DateTime<Utc>), String> {
    let response = client.get(url).send().await.map_err(|e| {
        if e.is_timeout() {
            format!(
                "request to {url} timed out after {}s",
                HTTP_TIMEOUT.as_secs()
            )
        } else {
            format!("request to {url} failed: {e}")
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("request to {url} returned HTTP status {status}"));
    }
    let body = response.text().await.map_err(|e| {
        if e.is_timeout() {
            format!(
                "reading response from {url} timed out after {}s",
                HTTP_TIMEOUT.as_secs()
            )
        } else {
            format!("cannot read response body from {url}: {e}")
        }
    })?;
    Ok((body, Utc::now()))
}
