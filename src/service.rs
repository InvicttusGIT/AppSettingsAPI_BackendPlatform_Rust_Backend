use std::{net::IpAddr, str::FromStr, sync::Arc, time::{Duration, Instant}};

use axum::http::HeaderMap;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::info;

use crate::{
    backend_client::{BackendClient, BackendError},
    cache::{CacheState, TwoLevelCache},
    geoip::GeoIpService,
    metrics::AppSettingsMetrics,
    models::AppSettingsResponse,
};

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("not found")]
    NotFound,
    #[error("app settings not found for version_number={0}")]
    VersionNotFound(String),
    #[error("upstream error")]
    Upstream,
    #[error("internal error")]
    Internal,
}

#[derive(Clone)]
pub struct AppSettingsService {
    cache: Arc<TwoLevelCache>,
    backend: BackendClient,
    backend_sem: Arc<Semaphore>,
    fallback_country_code: String,
    geoip: Option<Arc<dyn GeoIpService>>,
    metrics: AppSettingsMetrics,
}

impl AppSettingsService {
    pub fn new(
        cache: Arc<TwoLevelCache>,
        backend: BackendClient,
        backend_max_inflight: usize,
        fallback_country_code: String,
        geoip: Option<Arc<dyn GeoIpService>>,
        metrics: AppSettingsMetrics,
    ) -> Self {
        Self {
            cache,
            backend,
            backend_sem: Arc::new(Semaphore::new(backend_max_inflight.max(1))),
            fallback_country_code,
            geoip,
            metrics,
        }
    }

    pub async fn get(
        &self,
        app_device_id: &str,
        version_number: Option<&str>,
        explicit_country_code: Option<&str>,
        headers: &HeaderMap,
        remote_ip: Option<String>,
        secret_key: &str,
    ) -> Result<AppSettingsResponse, ServiceError> {
        let t0 = Instant::now();
        let emit_table = |outcome: &str, country: &Option<String>, cache_state: CacheState, cache_ms: u128, backend_ms: u128, total_ms: u128| {
            info!(
                "\n+----------------------+--------------------------------------+\n\
| FIELD                | VALUE                                |\n\
+----------------------+--------------------------------------+\n\
| country              | {:<36} |\n\
| cache_state          | {:<36} |\n\
| cache_ms             | {:<36} |\n\
| backend_ms           | {:<36} |\n\
| total_ms             | {:<36} |\n\
| outcome              | {:<36} |\n\
+----------------------+--------------------------------------+",
                country.as_deref().unwrap_or(""),
                format!("{:?}", cache_state),
                format!("{} ms", cache_ms),
                format!("{} ms", backend_ms),
                format!("{} ms", total_ms),
                outcome
            );
        };
        info!(
            "appsettings.step=start op=get app_device_id={} version={}",
            app_device_id,
            version_number.unwrap_or("")
        );

        let t_country = Instant::now();
        let country = resolve_country(
            explicit_country_code,
            headers,
            remote_ip,
            &self.fallback_country_code,
            self.geoip.as_ref(),
        );
        let country_dur = t_country.elapsed();
        info!(
            "appsettings.step=country_resolve op=get duration_ms={} country={}",
            dur_ms(country_dur),
            country.as_deref().unwrap_or("")
        );

        let key = TwoLevelCache::key(app_device_id, version_number, country.as_deref());
        let t_cache = Instant::now();
        let cached = self.cache.get(&key).await.map_err(|_| ServiceError::Internal)?;
        let cache_dur = t_cache.elapsed();
        info!(
            "appsettings.step=cache_get op=get duration_ms={} state={:?} not_found={}",
            dur_ms(cache_dur),
            cached.state,
            cached.not_found
        );

        if cached.not_found && cached.state != CacheState::Miss {
            self.metrics.observe(metric_bucket(version_number, cached.state), t0.elapsed(), cache_dur, std::time::Duration::ZERO);
            emit_table(
                "not_found",
                &country,
                cached.state,
                dur_ms(cache_dur),
                0,
                dur_ms(t0.elapsed()),
            );
            info!(
                "appsettings.step=end op=get outcome=not_found total_ms={}",
                dur_ms(t0.elapsed())
            );
            return match version_number {
                Some(v) => Err(ServiceError::VersionNotFound(v.to_string())),
                None => Err(ServiceError::NotFound),
            };
        }
        if let Some(v) = cached.value {
            if matches!(cached.state, CacheState::Fresh | CacheState::Stale) {
                self.metrics.observe(metric_bucket(version_number, cached.state), t0.elapsed(), cache_dur, std::time::Duration::ZERO);
                emit_table(
                    "cache_hit",
                    &country,
                    cached.state,
                    dur_ms(cache_dur),
                    0,
                    dur_ms(t0.elapsed()),
                );
                info!(
                    "appsettings.step=end op=get outcome=cache_hit state={:?} total_ms={}",
                    cached.state,
                    dur_ms(t0.elapsed())
                );
                return Ok(v);
            }
        }

        let t_backend = Instant::now();
        let _permit = self.backend_sem.acquire().await.map_err(|_| ServiceError::Internal)?;
        let fetched = self
            .backend
            .get_app_settings::<AppSettingsResponse>(
                app_device_id,
                version_number,
                country.as_deref(),
                secret_key,
                headers,
            )
            .await;
        let backend_dur = t_backend.elapsed();
        info!(
            "appsettings.step=backend_call op=get duration_ms={}",
            dur_ms(backend_dur)
        );

        match fetched {
            Ok(v) => {
                let t_set = Instant::now();
                let _ = self.cache.set_value(&key, v.clone()).await;
                let set_dur = t_set.elapsed();
                self.metrics.observe(metric_bucket(version_number, CacheState::Miss), t0.elapsed(), cache_dur, backend_dur);
                emit_table(
                    "backend_success",
                    &country,
                    CacheState::Miss,
                    dur_ms(cache_dur),
                    dur_ms(backend_dur),
                    dur_ms(t0.elapsed()),
                );
                info!(
                    "appsettings.step=cache_set op=get duration_ms={} kind=value",
                    dur_ms(set_dur)
                );
                info!(
                    "appsettings.step=end op=get outcome=backend_success total_ms={}",
                    dur_ms(t0.elapsed())
                );
                Ok(v)
            }
            Err(BackendError::NotFound) => {
                let t_set = Instant::now();
                let _ = self.cache.set_not_found(&key).await;
                let set_dur = t_set.elapsed();
                self.metrics.observe(metric_bucket(version_number, CacheState::Miss), t0.elapsed(), cache_dur, backend_dur);
                emit_table(
                    "backend_not_found",
                    &country,
                    CacheState::Miss,
                    dur_ms(cache_dur),
                    dur_ms(backend_dur),
                    dur_ms(t0.elapsed()),
                );
                info!(
                    "appsettings.step=cache_set op=get duration_ms={} kind=not_found",
                    dur_ms(set_dur)
                );
                info!(
                    "appsettings.step=end op=get outcome=backend_not_found total_ms={}",
                    dur_ms(t0.elapsed())
                );
                match version_number {
                    Some(v) => Err(ServiceError::VersionNotFound(v.to_string())),
                    None => Err(ServiceError::NotFound),
                }
            }
            Err(BackendError::Http { status: 400, .. }) => {
                if let Some(fallback) = fallback_for_invalid_country(&country, &self.fallback_country_code) {
                    let fallback_key = TwoLevelCache::key(app_device_id, version_number, Some(&fallback));
                    let t_retry = Instant::now();
                    let retry = self
                        .backend
                        .get_app_settings::<AppSettingsResponse>(
                            app_device_id,
                            version_number,
                            Some(&fallback),
                            secret_key,
                            headers,
                        )
                        .await
                        .map_err(|_| ServiceError::Upstream)?;
                    let retry_dur = t_retry.elapsed();
                    info!(
                        "appsettings.step=backend_retry_fallback op=get duration_ms={} fallback_country={}",
                        dur_ms(retry_dur),
                        fallback
                    );
                    let t_set = Instant::now();
                    let _ = self.cache.set_value(&fallback_key, retry.clone()).await;
                    let set_dur = t_set.elapsed();
                    self.metrics.observe(metric_bucket(version_number, CacheState::Miss), t0.elapsed(), cache_dur, backend_dur + retry_dur);
                    emit_table(
                        "fallback_success",
                        &country,
                        CacheState::Miss,
                        dur_ms(cache_dur),
                        dur_ms(backend_dur + retry_dur),
                        dur_ms(t0.elapsed()),
                    );
                    info!(
                        "appsettings.step=cache_set op=get duration_ms={} kind=value_fallback",
                        dur_ms(set_dur)
                    );
                    info!(
                        "appsettings.step=end op=get outcome=fallback_success total_ms={}",
                        dur_ms(t0.elapsed())
                    );
                    Ok(retry)
                } else {
                    self.metrics.observe(metric_bucket(version_number, CacheState::Miss), t0.elapsed(), cache_dur, backend_dur);
                    emit_table(
                        "backend_400_no_fallback",
                        &country,
                        CacheState::Miss,
                        dur_ms(cache_dur),
                        dur_ms(backend_dur),
                        dur_ms(t0.elapsed()),
                    );
                    info!(
                        "appsettings.step=end op=get outcome=backend_400_no_fallback total_ms={}",
                        dur_ms(t0.elapsed())
                    );
                    Err(ServiceError::Upstream)
                }
            }
            Err(_) => {
                self.metrics.observe(metric_bucket(version_number, CacheState::Miss), t0.elapsed(), cache_dur, backend_dur);
                emit_table(
                    "backend_error",
                    &country,
                    CacheState::Miss,
                    dur_ms(cache_dur),
                    dur_ms(backend_dur),
                    dur_ms(t0.elapsed()),
                );
                info!(
                    "appsettings.step=end op=get outcome=backend_error total_ms={}",
                    dur_ms(t0.elapsed())
                );
                Err(ServiceError::Upstream)
            }
        }
    }

    pub async fn get_async(
        &self,
        app_device_id: &str,
        version_number: Option<&str>,
        explicit_country_code: Option<&str>,
        headers: &HeaderMap,
        remote_ip: Option<String>,
        _secret_key: &str,
    ) -> Result<(Option<AppSettingsResponse>, bool), ServiceError> {
        let country = resolve_country(
            explicit_country_code,
            headers,
            remote_ip,
            &self.fallback_country_code,
            self.geoip.as_ref(),
        );
        let key = TwoLevelCache::key(app_device_id, version_number, country.as_deref());
        let cached = self.cache.get(&key).await.map_err(|_| ServiceError::Internal)?;
        if cached.not_found && cached.state != CacheState::Miss {
            return match version_number {
                Some(v) => Err(ServiceError::VersionNotFound(v.to_string())),
                None => Err(ServiceError::NotFound),
            };
        }
        if let Some(v) = cached.value {
            if matches!(cached.state, CacheState::Fresh | CacheState::Stale) {
                return Ok((Some(v), true));
            }
        }
        Ok((None, false))
    }

    pub fn metrics_snapshot_json(&self) -> anyhow::Result<Vec<u8>> {
        self.metrics.snapshot_json()
    }
}

fn dur_ms(d: Duration) -> u128 {
    d.as_millis()
}

fn resolve_country(
    explicit_country_code: Option<&str>,
    headers: &HeaderMap,
    remote_ip: Option<String>,
    fallback: &str,
    geoip: Option<&Arc<dyn GeoIpService>>,
) -> Option<String> {
    if let Some(cc) = explicit_country_code {
        if !cc.trim().is_empty() {
            return Some(cc.trim().to_uppercase());
        }
    }
    let ip = extract_client_ip(headers, remote_ip);
    if let Some(ip_str) = ip {
        if !is_private_or_loopback(&ip_str) {
            if let Some(g) = geoip {
                if let Ok(cc) = g.lookup_country_code(&ip_str) {
                    if !cc.trim().is_empty() {
                        return Some(cc.trim().to_uppercase());
                    }
                }
            }
        } else if !fallback.is_empty() {
            return Some(fallback.to_string());
        }
    }
    None
}

fn extract_client_ip(headers: &HeaderMap, remote_ip: Option<String>) -> Option<String> {
    let forwarded = headers.get("Forwarded").and_then(|v| v.to_str().ok()).unwrap_or_default();
    if let Some(idx) = forwarded.to_lowercase().find("for=") {
        let mut s = forwarded[idx + 4..].split(';').next().unwrap_or_default().trim().to_string();
        s = s.trim_matches('"').to_string();
        if !s.is_empty() {
            return Some(s);
        }
    }
    let xff = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !xff.trim().is_empty() {
        return Some(xff.split(',').next().unwrap_or_default().trim().to_string());
    }
    let xri = headers
        .get("X-Real-IP")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if xri.is_some() {
        return xri.map(strip_ip_port);
    }
    remote_ip.map(strip_ip_port).filter(|s| !s.is_empty())
}

fn strip_ip_port(ip: String) -> String {
    let s = ip.trim().trim_matches('"').to_string();
    if let Ok(sa) = s.parse::<std::net::SocketAddr>() {
        return sa.ip().to_string();
    }
    if s.starts_with('[') && s.ends_with(']') {
        return s.trim_matches('[').trim_matches(']').to_string();
    }
    if let Some((host, _)) = s.rsplit_once(':') {
        if host.parse::<std::net::IpAddr>().is_ok() {
            return host.to_string();
        }
    }
    s
}

fn is_private_or_loopback(ip_str: &str) -> bool {
    let clean = ip_str.trim().trim_matches('[').trim_matches(']');
    let Ok(ip) = IpAddr::from_str(clean) else {
        return true;
    };
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unicast_link_local(),
    }
}

fn fallback_for_invalid_country(country: &Option<String>, fallback: &str) -> Option<String> {
    if fallback.trim().is_empty() {
        return None;
    }
    if country.as_deref().unwrap_or_default().eq_ignore_ascii_case(fallback) {
        return None;
    }
    Some(fallback.to_string())
}

fn metric_bucket(version_number: Option<&str>, state: CacheState) -> &'static str {
    match (version_number.is_some(), state) {
        (false, CacheState::Fresh) => "by_appid:hit:fresh",
        (false, CacheState::Stale) => "by_appid:hit:stale",
        (false, CacheState::Miss) => "by_appid:miss:backend",
        (true, CacheState::Fresh) => "by_version:hit:fresh",
        (true, CacheState::Stale) => "by_version:hit:stale",
        (true, CacheState::Miss) => "by_version:miss:backend",
    }
}

