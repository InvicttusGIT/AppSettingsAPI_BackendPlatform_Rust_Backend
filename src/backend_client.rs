use std::time::Duration;

use reqwest::{header::HeaderMap, StatusCode};
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("not found")]
    NotFound,
    #[error("backend returned {status}: {body}")]
    Http { status: u16, body: String },
    #[error(transparent)]
    Request(#[from] reqwest::Error),
}

#[derive(Clone)]
pub struct BackendClient {
    base_url: String,
    client: reqwest::Client,
}

impl BackendClient {
    pub fn new(base_url: String, timeout: Duration) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(2000)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .connect_timeout(Duration::from_secs(3))
            .timeout(timeout)
            .build()?;
        Ok(Self { base_url, client })
    }

    pub async fn get_app_settings<T: DeserializeOwned>(
        &self,
        app_device_id: &str,
        version_number: Option<&str>,
        country_code: Option<&str>,
        secret_key: &str,
        inbound_headers: &HeaderMap,
    ) -> Result<T, BackendError> {
        let mut req = self
            .client
            .get(format!("{}/api/v1/app-settings", self.base_url))
            .query(&[("app_device_id", app_device_id)])
            .header("X-Secret-Key", secret_key);

        if let Some(v) = version_number {
            if !v.trim().is_empty() {
                req = req.query(&[("version_number", v)]);
            }
        }
        if let Some(c) = country_code {
            if !c.trim().is_empty() {
                req = req.query(&[("country_code", c)]);
            }
        }

        req = forward_ip_headers(req, inbound_headers);
        let resp = req.send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err(BackendError::NotFound);
        }
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(BackendError::Http { status, body });
        }
        Ok(resp.json::<T>().await?)
    }
}

fn forward_ip_headers(req: reqwest::RequestBuilder, src: &HeaderMap) -> reqwest::RequestBuilder {
    let mut out = req;
    for key in ["Forwarded", "X-Forwarded-For", "X-Real-IP"] {
        if let Some(v) = src.get(key) {
            out = out.header(key, v.clone());
        }
    }
    out
}

