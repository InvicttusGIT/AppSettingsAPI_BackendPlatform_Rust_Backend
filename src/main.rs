mod backend_client;
mod cache;
mod config;
mod handlers;
mod geoip;
mod metrics;
mod models;
mod service;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{routing::get, Router};
use dotenvy::dotenv;
use handlers::{appsettings_metrics, get_app_settings, health, AppState};
use tracing::info;

use crate::{
    backend_client::BackendClient,
    cache::TwoLevelCache,
    config::AppConfig,
    geoip::{CachedGeoIpService, GeoIpService, MaxMindGeoIpService},
    metrics::AppSettingsMetrics,
    service::AppSettingsService,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info,hyper=warn,tower_http=warn".to_string()))
        .with_target(false)
        .compact()
        .init();

    let cfg = AppConfig::from_env()?;
    info!("Starting BackendPlatform_Rust_API env={} addr={}", cfg.env, cfg.listen_addr);

    let mut redis_cfg = deadpool_redis::Config::from_url(cfg.redis_url.clone());
    redis_cfg.pool = Some(deadpool::managed::PoolConfig {
        max_size: cfg.redis_pool_size,
        ..Default::default()
    });
    let redis_pool = redis_cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
    let backend_client = BackendClient::new(cfg.backend_base_url.clone(), cfg.request_timeout)?;
    let metrics = AppSettingsMetrics::new(cfg.metrics_sample_rate);

    let geoip: Option<Arc<dyn GeoIpService>> = if cfg.geoip_db_path.trim().is_empty() {
        None
    } else {
        match MaxMindGeoIpService::new(&cfg.geoip_db_path) {
            Ok(base) => {
                let base: Arc<dyn GeoIpService> = Arc::new(base);
                if cfg.geoip_cache_enabled {
                    Some(Arc::new(CachedGeoIpService::new(
                        base,
                        cfg.geoip_cache_ttl,
                        cfg.geoip_cache_max_keys,
                    )))
                } else {
                    Some(base)
                }
            }
            Err(err) => {
                info!("GeoIP disabled: {}", err);
                None
            }
        }
    };
    let effective_ttl = if cfg.mem_cache_ttl > std::time::Duration::ZERO {
        cfg.mem_cache_ttl
    } else {
        cfg.cache_ttl
    };
    let effective_stale_window = if cfg.mem_cache_stale_window > std::time::Duration::ZERO {
        cfg.mem_cache_stale_window
    } else {
        cfg.cache_stale_window
    };

    let cache = Arc::new(TwoLevelCache::new(
        redis_pool,
        cfg.mem_cache_enabled,
        cfg.mem_cache_max_keys,
        effective_ttl,
        effective_stale_window,
        cfg.negative_cache_ttl.max(cfg.mem_cache_negative_ttl),
    ));
    let service = Arc::new(AppSettingsService::new(
        cache,
        backend_client,
        cfg.backend_max_inflight,
        cfg.fallback_country_code,
        geoip,
        metrics,
        cfg.log_slow_ms,
        cfg.log_sample_rate,
        cfg.log_table_enabled,
    ));

    let app_state = AppState { service };
    let app = Router::new()
        .route("/health", get(health))
        .route("/internal/metrics/appsettings", get(appsettings_metrics))
        .route("/api/v1/appsetting-api/app-settings", get(get_app_settings))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

