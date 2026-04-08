use std::{env, net::SocketAddr, time::Duration};

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub env: String,
    pub listen_addr: SocketAddr,
    pub backend_base_url: String,
    pub request_timeout: Duration,
    pub fallback_country_code: String,
    pub geoip_db_path: String,
    pub geoip_cache_enabled: bool,
    pub geoip_cache_ttl: Duration,
    pub geoip_cache_max_keys: usize,
    pub cache_provider: String,
    pub cache_url: String,
    pub cache_pool_size: usize,
    pub cache_ttl: Duration,
    pub cache_stale_window: Duration,
    pub negative_cache_ttl: Duration,
    pub mem_cache_enabled: bool,
    pub mem_cache_ttl: Duration,
    pub mem_cache_stale_window: Duration,
    pub mem_cache_negative_ttl: Duration,
    pub mem_cache_max_keys: usize,
    pub mem_cache_evict_scan: usize,
    pub mem_cache_hard_max_multiplier: usize,
    pub backend_max_inflight: usize,
    pub log_slow_ms: u128,
    pub log_sample_rate: u64,
    pub log_table_enabled: bool,
    pub metrics_sample_rate: u64,
    pub load_debug_enabled: bool,
    pub wait_warn_ms: u64,
    pub cache_wait_warn_ms: u64,
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let port = get_u16("APP_PORT", 8081);
        let cache_provider = get("CACHE_PROVIDER", "redis").trim().to_lowercase();
        let cache_url = build_cache_url_from_env(&cache_provider);
        let cache_pool_size = match cache_provider.as_str() {
            "keydb" => get_usize("KEYDB_POOL_SIZE", get_usize("REDIS_POOL_SIZE", 256)),
            _ => get_usize("REDIS_POOL_SIZE", 256),
        };

        Ok(Self {
            env: get("APP_ENV", "development"),
            listen_addr: SocketAddr::from(([0, 0, 0, 0], port)),
            backend_base_url: get("BACKEND_BASE_URL", "http://localhost:8080"),
            request_timeout: get_duration("REQUEST_TIMEOUT", Duration::from_secs(15)),
            fallback_country_code: get("FALLBACK_COUNTRY_CODE", "").to_uppercase(),
            geoip_db_path: get("GEOIP_DB_PATH", ""),
            geoip_cache_enabled: get_bool("GEOIP_CACHE_ENABLED", true),
            geoip_cache_ttl: get_duration("GEOIP_CACHE_TTL", Duration::from_secs(300)),
            geoip_cache_max_keys: get_usize("GEOIP_CACHE_MAX_KEYS", 100_000),
            cache_provider,
            cache_url,
            cache_pool_size,
            cache_ttl: get_duration("CACHE_TTL", Duration::from_secs(300)),
            cache_stale_window: get_duration("CACHE_STALE_WINDOW", Duration::from_secs(60)),
            negative_cache_ttl: get_duration("NEGATIVE_CACHE_TTL", Duration::from_secs(30)),
            mem_cache_enabled: get_bool("MEM_CACHE_ENABLED", true),
            mem_cache_ttl: get_duration("MEM_CACHE_TTL", Duration::from_secs(30)),
            mem_cache_stale_window: get_duration("MEM_CACHE_STALE_WINDOW", Duration::from_secs(30)),
            mem_cache_negative_ttl: get_duration("MEM_CACHE_NEGATIVE_TTL", Duration::from_secs(5)),
            mem_cache_max_keys: get_usize("MEM_CACHE_MAX_KEYS", 200_000),
            mem_cache_evict_scan: get_usize("MEM_CACHE_EVICT_SCAN", 2048),
            mem_cache_hard_max_multiplier: get_usize("MEM_CACHE_HARD_MAX_MULTIPLIER", 2).max(1),
            backend_max_inflight: get_usize("BACKEND_MAX_INFLIGHT", 200),
            log_slow_ms: get_usize("LOG_SLOW_MS", 50) as u128,
            log_sample_rate: get_usize("LOG_SAMPLE_RATE", 1000) as u64,
            log_table_enabled: get_bool("LOG_TABLE_ENABLED", false),
            metrics_sample_rate: get_usize("METRICS_SAMPLE_RATE", 1000) as u64,
            load_debug_enabled: get_bool("LOAD_DEBUG_ENABLED", false),
            wait_warn_ms: get_usize("WAIT_WARN_MS", 2000) as u64,
            cache_wait_warn_ms: get_usize("CACHE_WAIT_WARN_MS", 1000) as u64,
        })
    }
}

fn build_cache_url_from_env(cache_provider: &str) -> String {
    if let Ok(url) = env::var("CACHE_URL") {
        if !url.trim().is_empty() {
            return url;
        }
    }
    match cache_provider {
        "keydb" => {
            let addr = get("KEYDB_ADDR", env::var("REDIS_ADDR").ok().as_deref().unwrap_or("localhost:6379"));
            let password = env::var("KEYDB_PASSWORD")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .or_else(|| env::var("REDIS_PASSWORD").ok().filter(|v| !v.trim().is_empty()))
                .unwrap_or_default();
            let db = env::var("KEYDB_DB")
                .ok()
                .and_then(|v| v.parse::<u8>().ok())
                .unwrap_or_else(|| get_u8("REDIS_DB", 0));
            if password.is_empty() {
                format!("redis://{addr}/{db}")
            } else {
                format!("redis://:{}@{addr}/{db}", password)
            }
        }
        _ => {
            let addr = get("REDIS_ADDR", "localhost:6379");
            let password = env::var("REDIS_PASSWORD").unwrap_or_default();
            let db = get_u8("REDIS_DB", 0);
            if password.is_empty() {
                format!("redis://{addr}/{db}")
            } else {
                format!("redis://:{}@{addr}/{db}", password)
            }
        }
    }
}

fn get(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn get_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes" | "y" | "on"),
        Err(_) => default,
    }
}

fn get_u16(key: &str, default: u16) -> u16 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn get_u8(key: &str, default: u8) -> u8 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn get_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn get_duration(key: &str, default: Duration) -> Duration {
    match env::var(key) {
        Ok(v) => parse_duration(&v).unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_duration(v: &str) -> Option<Duration> {
    let s = v.trim().to_lowercase();
    if let Some(ms) = s.strip_suffix("ms") {
        return ms.parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(sec) = s.strip_suffix('s') {
        return sec.parse::<u64>().ok().map(Duration::from_secs);
    }
    if let Some(min) = s.strip_suffix('m') {
        return min.parse::<u64>().ok().map(|x| Duration::from_secs(x * 60));
    }
    if let Some(hr) = s.strip_suffix('h') {
        return hr.parse::<u64>().ok().map(|x| Duration::from_secs(x * 3600));
    }
    None
}

