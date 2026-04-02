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
    pub redis_url: String,
    pub redis_pool_size: usize,
    pub cache_ttl: Duration,
    pub cache_stale_window: Duration,
    pub negative_cache_ttl: Duration,
    pub mem_cache_enabled: bool,
    pub mem_cache_ttl: Duration,
    pub mem_cache_stale_window: Duration,
    pub mem_cache_negative_ttl: Duration,
    pub mem_cache_max_keys: usize,
    pub backend_max_inflight: usize,
    pub log_slow_ms: u128,
    pub log_sample_rate: u64,
    pub log_table_enabled: bool,
    pub metrics_sample_rate: u64,
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let port = get_u16("APP_PORT", 8081);
        let redis_addr = get("REDIS_ADDR", "localhost:6379");
        let redis_password = env::var("REDIS_PASSWORD").unwrap_or_default();
        let redis_db = get_u8("REDIS_DB", 0);
        let redis_url = if redis_password.is_empty() {
            format!("redis://{redis_addr}/{redis_db}")
        } else {
            format!("redis://:{}@{redis_addr}/{redis_db}", redis_password)
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
            redis_url,
            redis_pool_size: get_usize("REDIS_POOL_SIZE", 256),
            cache_ttl: get_duration("CACHE_TTL", Duration::from_secs(300)),
            cache_stale_window: get_duration("CACHE_STALE_WINDOW", Duration::from_secs(60)),
            negative_cache_ttl: get_duration("NEGATIVE_CACHE_TTL", Duration::from_secs(30)),
            mem_cache_enabled: get_bool("MEM_CACHE_ENABLED", true),
            mem_cache_ttl: get_duration("MEM_CACHE_TTL", Duration::from_secs(30)),
            mem_cache_stale_window: get_duration("MEM_CACHE_STALE_WINDOW", Duration::from_secs(30)),
            mem_cache_negative_ttl: get_duration("MEM_CACHE_NEGATIVE_TTL", Duration::from_secs(5)),
            mem_cache_max_keys: get_usize("MEM_CACHE_MAX_KEYS", 200_000),
            backend_max_inflight: get_usize("BACKEND_MAX_INFLIGHT", 200),
            log_slow_ms: get_usize("LOG_SLOW_MS", 50) as u128,
            log_sample_rate: get_usize("LOG_SAMPLE_RATE", 1000) as u64,
            log_table_enabled: get_bool("LOG_TABLE_ENABLED", false),
            metrics_sample_rate: get_usize("METRICS_SAMPLE_RATE", 1000) as u64,
        })
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

