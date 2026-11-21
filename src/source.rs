use std::{fs, time::Duration};

use chrono::{DateTime, Utc};

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// `reqwest` errors print only their top level ("error sending request for url (...)"); this
/// appends the underlying causes, e.g. "connection refused" or "dns error".
fn describe(e: &reqwest::Error) -> String {
    let mut text = e.to_string();
    let mut cause = std::error::Error::source(e);
    while let Some(c) = cause {
        let part = c.to_string();
        if !text.contains(&part) {
            text = format!("{text}: {part}");
        }
        cause = c.source();
    }
    text
}

/// A failed fetch. `status` is set when the server answered with a non-2xx HTTP status, so callers
/// can react to e.g. 401/403/429 differently from a connection error or a timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchError {
    pub status: Option<u16>,
    pub message: String,
}

impl FetchError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            status: None,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<FetchError> for String {
    fn from(e: FetchError) -> Self {
        e.message
    }
}

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
    fetch_url_with_status(client, url)
        .await
        .map(|(body, observed_at, _)| (body, observed_at))
        .map_err(String::from)
}

/// Like `fetch_url`, also returning the HTTP status code (always a 2xx one on success).
pub async fn fetch_url_with_status(
    client: &reqwest::Client,
    url: &str,
) -> Result<(String, DateTime<Utc>, u16), FetchError> {
    let response = client.get(url).send().await.map_err(|e| {
        FetchError::other(if e.is_timeout() {
            format!(
                "request to {url} timed out after {}s",
                HTTP_TIMEOUT.as_secs()
            )
        } else {
            format!("request to {url} failed: {}", describe(&e))
        })
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(FetchError {
            status: Some(status.as_u16()),
            message: format!("request to {url} returned HTTP status {status}"),
        });
    }
    let body = response.text().await.map_err(|e| {
        FetchError::other(if e.is_timeout() {
            format!(
                "reading response from {url} timed out after {}s",
                HTTP_TIMEOUT.as_secs()
            )
        } else {
            format!("cannot read response body from {url}: {}", describe(&e))
        })
    })?;
    Ok((body, Utc::now(), status.as_u16()))
}
