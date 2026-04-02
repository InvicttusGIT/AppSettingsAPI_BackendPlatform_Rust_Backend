use std::{collections::HashMap, net::IpAddr, sync::Arc, time::{Duration, Instant}};

use maxminddb::{geoip2, Reader};
use tokio::sync::RwLock;

pub trait GeoIpService: Send + Sync {
    fn lookup_country_code(&self, ip: &str) -> anyhow::Result<String>;
}

pub struct MaxMindGeoIpService {
    reader: Reader<Vec<u8>>,
}

impl MaxMindGeoIpService {
    pub fn new(db_path: &str) -> anyhow::Result<Self> {
        let reader = Reader::open_readfile(db_path)?;
        Ok(Self { reader })
    }
}

impl GeoIpService for MaxMindGeoIpService {
    fn lookup_country_code(&self, ip: &str) -> anyhow::Result<String> {
        let ip: IpAddr = ip.parse()?;
        let country: geoip2::Country = self.reader.lookup(ip)?;
        let cc = country
            .country
            .and_then(|c| c.iso_code)
            .map(|s| s.to_string())
            .unwrap_or_default();
        if cc.is_empty() {
            anyhow::bail!("no country iso code");
        }
        Ok(cc)
    }
}

#[derive(Clone)]
pub struct CachedGeoIpService {
    base: Arc<dyn GeoIpService>,
    ttl: Duration,
    max_keys: usize,
    entries: Arc<RwLock<HashMap<String, (String, Instant)>>>,
}

impl CachedGeoIpService {
    pub fn new(base: Arc<dyn GeoIpService>, ttl: Duration, max_keys: usize) -> Self {
        Self {
            base,
            ttl,
            max_keys,
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl GeoIpService for CachedGeoIpService {
    fn lookup_country_code(&self, ip: &str) -> anyhow::Result<String> {
        let ip = ip.trim().to_string();
        let now = Instant::now();
        if let Ok(g) = self.entries.try_read() {
            if let Some((cc, exp)) = g.get(&ip) {
                if *exp > now {
                    return Ok(cc.clone());
                }
            }
        }
        let cc = self.base.lookup_country_code(&ip)?;
        if let Ok(mut w) = self.entries.try_write() {
            if self.max_keys > 0 && w.len() >= self.max_keys {
                w.clear();
            }
            w.insert(ip, (cc.clone(), now + self.ttl));
        }
        Ok(cc)
    }
}

