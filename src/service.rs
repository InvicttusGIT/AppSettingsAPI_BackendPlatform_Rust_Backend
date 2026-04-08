use std::{
    net::IpAddr,
    str::FromStr,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use axum::http::HeaderMap;
use dashmap::DashMap;
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Duration as TokioDuration, timeout};
use tracing::{info, warn};

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
    in_flight: Arc<DashMap<String, Arc<Notify>>>,
    log_slow_ms: u128,
    log_sample_rate: u64,
    log_table_enabled: bool,
    log_counter: Arc<AtomicU64>,
    debug_enabled: bool,
    wait_warn_ms: u64,
}

impl AppSettingsService {
    pub fn new(
        cache: Arc<TwoLevelCache>,
        backend: BackendClient,
        backend_max_inflight: usize,
        fallback_country_code: String,
        geoip: Option<Arc<dyn GeoIpService>>,
        metrics: AppSettingsMetrics,
        log_slow_ms: u128,
        log_sample_rate: u64,
        log_table_enabled: bool,
        debug_enabled: bool,
        wait_warn_ms: u64,
    ) -> Self {
        Self {
            cache,
            backend,
            backend_sem: Arc::new(Semaphore::new(backend_max_inflight.max(1))),
            fallback_country_code,
            geoip,
            metrics,
            in_flight: Arc::new(DashMap::new()),
            log_slow_ms,
            log_sample_rate: log_sample_rate.max(1),
            log_table_enabled,
            log_counter: Arc::new(AtomicU64::new(0)),
            debug_enabled,
            wait_warn_ms: wait_warn_ms.max(1),
        }
    }

    #[inline]
    pub fn should_log_start(&self) -> bool {
        if self.log_sample_rate <= 1 {
            return true;
        }
        let n = self.log_counter.fetch_add(1, Ordering::Relaxed);
        n % self.log_sample_rate == 0
    }

    #[inline]
    pub fn should_log_end(&self, sampled: bool, total_ms: u128) -> bool {
        sampled || total_ms >= self.log_slow_ms
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
        let sampled = self.should_log_start();
        let log_table_enabled = self.log_table_enabled;

        let t_country = Instant::now();
        let country = resolve_country(
            explicit_country_code,
            headers,
            remote_ip,
            &self.fallback_country_code,
            self.geoip.as_ref(),
        )
        .await;
        let country_dur = t_country.elapsed();

        if sampled {
            info!(
                "appsettings.step=country_resolve op=get duration_ms={} country={}",
                dur_ms(country_dur),
                country.as_deref().unwrap_or("")
            );
        }

        let key = TwoLevelCache::key(app_device_id, version_number, country.as_deref());

        loop {
            let t_cache = Instant::now();
            let cached = self.cache.get(&key).await.map_err(|_| ServiceError::Internal)?;
            let cache_dur = t_cache.elapsed();

            if sampled {
                info!(
                    "appsettings.step=cache_get op=get duration_ms={} state={:?} not_found={}",
                    dur_ms(cache_dur),
                    cached.state,
                    cached.not_found
                );
            }

            let total_ms = dur_ms(t0.elapsed());
            let log_end = self.should_log_end(sampled, total_ms);

            if cached.not_found && cached.state != CacheState::Miss {
                self.metrics.observe(
                    metric_bucket(version_number, cached.state),
                    t0.elapsed(),
                    cache_dur,
                    std::time::Duration::ZERO,
                );
                if log_table_enabled && log_end {
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
                        format!("{:?}", cached.state),
                        format!("{} ms", dur_ms(cache_dur)),
                        "0 ms".to_string(),
                        format!("{} ms", total_ms),
                        "not_found"
                    );
                }
                if log_end {
                    info!("appsettings.step=end op=get outcome=not_found total_ms={}", total_ms);
                }
                return match version_number {
                    Some(v) => Err(ServiceError::VersionNotFound(v.to_string())),
                    None => Err(ServiceError::NotFound),
                };
            }

            if let Some(v) = cached.value {
                if matches!(cached.state, CacheState::Fresh | CacheState::Stale) {
                    self.metrics.observe(
                        metric_bucket(version_number, cached.state),
                        t0.elapsed(),
                        cache_dur,
                        std::time::Duration::ZERO,
                    );
                    if log_table_enabled && log_end {
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
                            format!("{:?}", cached.state),
                            format!("{} ms", dur_ms(cache_dur)),
                            "0 ms".to_string(),
                            format!("{} ms", total_ms),
                            "cache_hit"
                        );
                    }
                    if log_end {
                        info!(
                            "appsettings.step=end op=get outcome=cache_hit state={:?} total_ms={}",
                            cached.state, total_ms
                        );
                    }

                    // True SWR: if stale, serve immediately and refresh in background.
                    if cached.state == CacheState::Stale {
                        // Only one in-flight refresh per key.
                        use dashmap::mapref::entry::Entry;
                        let refresh_key = key.clone();
                        match self.in_flight.entry(refresh_key.clone()) {
                            Entry::Vacant(e) => {
                                let notify = Arc::new(Notify::new());
                                e.insert(notify.clone());

                                let svc = self.clone();
                                let app_device_id_owned = app_device_id.to_string();
                                let version_owned = version_number.map(|s| s.to_string());
                                let country_owned = country.clone();
                                let headers_owned = headers.clone();
                                let secret_key_owned = secret_key.to_string();

                                tokio::spawn(async move {
                                    let res = svc
                                        .fetch_backend_and_update_cache(
                                            Instant::now(),
                                            &refresh_key,
                                            &app_device_id_owned,
                                            version_owned.as_deref(),
                                            country_owned.as_deref(),
                                            &headers_owned,
                                            &secret_key_owned,
                                            std::time::Duration::ZERO,
                                            false,
                                        )
                                        .await;

                                    svc.in_flight.remove(&refresh_key);
                                    notify.notify_waiters();
                                    if let Err(e) = res {
                                        warn!(
                                            "appsettings.step=background_refresh_done key={} status=error err={}",
                                            refresh_key, e
                                        );
                                    } else if svc.debug_enabled {
                                        info!(
                                            "appsettings.step=background_refresh_done key={} status=ok",
                                            refresh_key
                                        );
                                    }
                                });
                            }
                            Entry::Occupied(_) => {}
                        }
                    }

                    return Ok(v);
                }
            }

            // Cache miss: single-flight backend fetch (avoid stampede).
            use dashmap::mapref::entry::Entry;
            match self.in_flight.entry(key.clone()) {
                Entry::Occupied(o) => {
                    let notify = o.get().clone();
                    drop(o);
                    let mut waited = 0u64;
                    loop {
                        let slice = TokioDuration::from_millis(self.wait_warn_ms);
                        match timeout(slice, notify.notified()).await {
                            Ok(_) => break,
                            Err(_) => {
                                waited = waited.saturating_add(self.wait_warn_ms);
                                warn!(
                                    "appsettings.step=singleflight_wait key={} waited_ms={} inflight_keys={}",
                                    key,
                                    waited,
                                    self.in_flight.len()
                                );
                            }
                        }
                    }
                    continue; // Re-check cache after the fetcher updates it.
                }
                Entry::Vacant(vacant) => {
                    let notify = Arc::new(Notify::new());
                    vacant.insert(notify.clone());

                    let res = self
                        .fetch_backend_and_update_cache(
                            t0,
                            &key,
                            app_device_id,
                            version_number,
                            country.as_deref(),
                            headers,
                            secret_key,
                            cache_dur,
                            sampled,
                        )
                        .await;

                    self.in_flight.remove(&key);
                    notify.notify_waiters();
                    return res;
                }
            }
        }
    }

    async fn fetch_backend_and_update_cache(
        &self,
        t0: Instant,
        key: &str,
        app_device_id: &str,
        version_number: Option<&str>,
        country: Option<&str>,
        headers: &HeaderMap,
        secret_key: &str,
        cache_dur: Duration,
        sampled: bool,
    ) -> Result<AppSettingsResponse, ServiceError> {
        let log_slow_ms = self.log_slow_ms;
        let log_table_enabled = self.log_table_enabled;

        let emit_table = |outcome: &str, cache_state: CacheState, cache_ms: u128, backend_ms: u128, total_ms: u128, country: Option<&str>| {
            if !log_table_enabled {
                return;
            }
            if !sampled && total_ms < log_slow_ms {
                return;
            }
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
                country.unwrap_or(""),
                format!("{:?}", cache_state),
                format!("{} ms", cache_ms),
                format!("{} ms", backend_ms),
                format!("{} ms", total_ms),
                outcome
            );
        };

        let t_backend = Instant::now();
        if sampled {
            info!("appsettings.step=backend_call op=get duration_ms=..");
        }
        let t_sem = Instant::now();
        let _permit = self
            .backend_sem
            .acquire()
            .await
            .map_err(|_| ServiceError::Internal)?;
        let sem_wait_ms = dur_ms(t_sem.elapsed()) as u64;
        if sem_wait_ms >= self.wait_warn_ms {
            warn!(
                "appsettings.step=backend_semaphore_wait wait_ms={} available_permits={} key={}",
                sem_wait_ms,
                self.backend_sem.available_permits(),
                key
            );
        } else if self.debug_enabled {
            info!(
                "appsettings.step=backend_semaphore_wait wait_ms={} available_permits={} key={}",
                sem_wait_ms,
                self.backend_sem.available_permits(),
                key
            );
        }

        let fetched = self
            .backend
            .get_app_settings::<AppSettingsResponse>(
                app_device_id,
                version_number,
                country,
                secret_key,
                headers,
            )
            .await;

        let backend_dur = t_backend.elapsed();
        let total_ms = dur_ms(t0.elapsed());
        let backend_ms = dur_ms(backend_dur) as u64;
        if backend_ms >= self.wait_warn_ms {
            warn!(
                "appsettings.step=backend_call wait_ms={} key={} country={}",
                backend_ms,
                key,
                country.unwrap_or("")
            );
        }
        if sampled {
            info!(
                "appsettings.step=backend_call op=get duration_ms={}",
                dur_ms(backend_dur)
            );
        }

        match fetched {
            Ok(v) => {
                let _ = self.cache.set_value(key, v.clone()).await;
                self.metrics.observe(
                    metric_bucket(version_number, CacheState::Miss),
                    t0.elapsed(),
                    cache_dur,
                    backend_dur,
                );
                emit_table(
                    "backend_success",
                    CacheState::Miss,
                    dur_ms(cache_dur),
                    dur_ms(backend_dur),
                    total_ms,
                    country,
                );
                Ok(v)
            }
            Err(BackendError::NotFound) => {
                let _ = self.cache.set_not_found(key).await;
                self.metrics.observe(
                    metric_bucket(version_number, CacheState::Miss),
                    t0.elapsed(),
                    cache_dur,
                    backend_dur,
                );
                emit_table(
                    "backend_not_found",
                    CacheState::Miss,
                    dur_ms(cache_dur),
                    dur_ms(backend_dur),
                    total_ms,
                    country,
                );
                match version_number {
                    Some(v) => Err(ServiceError::VersionNotFound(v.to_string())),
                    None => Err(ServiceError::NotFound),
                }
            }
            Err(BackendError::Http { status: 400, .. }) => {
                if let Some(fallback) = fallback_for_invalid_country(country, &self.fallback_country_code)
                {
                    let fallback_key =
                        TwoLevelCache::key(app_device_id, version_number, Some(&fallback));
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

                    // Cache aliasing: set both original (invalid) key and fallback key.
                    let _ = self.cache.set_value(&fallback_key, retry.clone()).await;
                    let _ = self.cache.set_value(key, retry.clone()).await;

                    self.metrics.observe(
                        metric_bucket(version_number, CacheState::Miss),
                        t0.elapsed(),
                        cache_dur,
                        backend_dur + retry_dur,
                    );
                    emit_table(
                        "fallback_success",
                        CacheState::Miss,
                        dur_ms(cache_dur),
                        dur_ms(backend_dur + retry_dur),
                        total_ms,
                        country,
                    );
                    Ok(retry)
                } else {
                    self.metrics.observe(
                        metric_bucket(version_number, CacheState::Miss),
                        t0.elapsed(),
                        cache_dur,
                        backend_dur,
                    );
                    emit_table(
                        "backend_400_no_fallback",
                        CacheState::Miss,
                        dur_ms(cache_dur),
                        dur_ms(backend_dur),
                        total_ms,
                        country,
                    );
                    Err(ServiceError::Upstream)
                }
            }
            Err(_) => {
                self.metrics.observe(
                    metric_bucket(version_number, CacheState::Miss),
                    t0.elapsed(),
                    cache_dur,
                    backend_dur,
                );
                emit_table(
                    "backend_error",
                    CacheState::Miss,
                    dur_ms(cache_dur),
                    dur_ms(backend_dur),
                    total_ms,
                    country,
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
        )
        .await;
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

async fn resolve_country(
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
                // GeoIP lookup can be CPU-heavy; offload so we don't block Tokio worker threads.
                let g = Arc::clone(g);
                let ip_for_lookup = ip_str.clone();
                if let Ok(res) = tokio::task::spawn_blocking(move || g.lookup_country_code(&ip_for_lookup)).await
                {
                    if let Ok(cc) = res {
                        if !cc.trim().is_empty() {
                            return Some(cc.trim().to_uppercase());
                        }
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

fn fallback_for_invalid_country(country: Option<&str>, fallback: &str) -> Option<String> {
    if fallback.trim().is_empty() {
        return None;
    }
    if country.unwrap_or_default().eq_ignore_ascii_case(fallback) {
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

