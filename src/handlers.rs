use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use std::net::SocketAddr;
use std::time::Instant;
use tracing::info;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::service::{AppSettingsService, ServiceError};

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<AppSettingsService>,
}

#[derive(Debug, Deserialize)]
pub struct AppSettingsQuery {
    pub app_device_id: String,
    pub version_number: Option<String>,
    pub country_code: Option<String>,
    pub country: Option<String>,
    pub r#async: Option<String>,
}

pub async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

pub async fn appsettings_metrics(State(state): State<AppState>) -> impl IntoResponse {
    match state.service.metrics_snapshot_json() {
        Ok(bytes) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            bytes,
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":"failed to build metrics snapshot"})),
        )
            .into_response(),
    }
}

pub async fn get_app_settings(
    State(state): State<AppState>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<AppSettingsQuery>,
) -> Response {
    let t0 = Instant::now();
    let sampled = state.service.should_log_start();
    if sampled {
        info!(
            "appsettings.request.start method=GET path=/api/v1/appsetting-api/app-settings app_device_id={} version={} country_code={} async={}",
            q.app_device_id,
            q.version_number.as_deref().unwrap_or(""),
            q.country_code
                .as_deref()
                .or(q.country.as_deref())
                .unwrap_or(""),
            q.r#async.as_deref().unwrap_or("")
        );
    }

    if q.app_device_id.trim().is_empty() {
        let resp = (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "app_device_id is required (UUID format from apprepository_app_devices table)" })),
        ).into_response();
        let total_ms = t0.elapsed().as_millis();
        if state.service.should_log_end(sampled, total_ms) {
            info!(
                "appsettings.request.end status=400 total_ms={} reason=missing_app_device_id",
                total_ms
            );
        }
        return resp;
    }
    if Uuid::parse_str(&q.app_device_id).is_err() {
        let resp = (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "app_device_id must be a valid UUID format" })),
        ).into_response();
        let total_ms = t0.elapsed().as_millis();
        if state.service.should_log_end(sampled, total_ms) {
            info!(
                "appsettings.request.end status=400 total_ms={} reason=invalid_uuid",
                total_ms
            );
        }
        return resp;
    }

    let Some(secret_key) = headers.get("X-Secret-Key").and_then(|v| v.to_str().ok()) else {
        let resp = (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "X-Secret-Key header is required" })),
        ).into_response();
        let total_ms = t0.elapsed().as_millis();
        if state.service.should_log_end(sampled, total_ms) {
            info!(
                "appsettings.request.end status=401 total_ms={} reason=missing_secret_key",
                total_ms
            );
        }
        return resp;
    };

    let async_mode = q
        .r#async
        .as_deref()
        .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    let explicit_country = q
        .country_code
        .as_deref()
        .or(q.country.as_deref());

    if async_mode {
        match state
            .service
            .get_async(
                &q.app_device_id,
                q.version_number.as_deref(),
                explicit_country,
                &headers,
                Some(remote_addr.ip().to_string()),
                secret_key,
            )
            .await
        {
            Ok((Some(resp), true)) => {
                let out = (
                    StatusCode::OK,
                    [("X-Cache-Mode", "async-cache")],
                    Json(json!(resp)),
                )
                    .into_response();
                let total_ms = t0.elapsed().as_millis();
                if state.service.should_log_end(sampled, total_ms) {
                    info!(
                        "appsettings.request.end status=200 total_ms={} mode=async-cache",
                        total_ms
                    );
                }
                return out;
            }
            Ok((None, false)) => {
                // Cold miss behavior parity with latest Go path: sync-on-cold-miss.
            }
            Err(e) => return map_err(e),
            _ => {}
        }
    }

    let response = match state.service.get(
        &q.app_device_id,
        q.version_number.as_deref(),
        explicit_country,
        &headers,
        Some(remote_addr.ip().to_string()),
        secret_key,
    ).await {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))),
        Err(ServiceError::NotFound) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(ServiceError::VersionNotFound(v)) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("app settings not found for version_number={v}") })),
        ),
        Err(ServiceError::Upstream) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": "failed to fetch app settings from backend" })),
        ),
        Err(ServiceError::Internal) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "internal server error" })),
        ),
    }.into_response();

    let total_ms = t0.elapsed().as_millis();
    if state.service.should_log_end(sampled, total_ms) {
        info!(
            "appsettings.request.end status={} total_ms={} mode=sync",
            response.status().as_u16(),
            total_ms
        );
    }
    response
}

fn map_err(e: ServiceError) -> axum::response::Response {
    match e {
        ServiceError::NotFound => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response(),
        ServiceError::VersionNotFound(v) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("app settings not found for version_number={v}") })),
        )
            .into_response(),
        ServiceError::Upstream => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": "failed to fetch app settings from backend" })),
        )
            .into_response(),
        ServiceError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "internal server error" })),
        )
            .into_response(),
    }
}

