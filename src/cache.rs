use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::models::AppSettingsResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    Miss,
    Fresh,
    Stale,
}

#[derive(Debug, Clone)]
pub struct CacheResult {
    pub value: Option<AppSettingsResponse>,
    pub state: CacheState,
    pub not_found: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEnvelope {
    v: u8,
    fresh_until_ms: i64,
    stale_until_ms: i64,
    #[serde(default)]
    not_found: bool,
    payload: Option<AppSettingsResponse>,
}

#[derive(Clone)]
pub struct TwoLevelCache {
    redis_pool: deadpool_redis::Pool,
    l1: DashMap<String, CacheEnvelope>,
    l1_enabled: bool,
    l1_max_keys: usize,
    l1_evict_scan: usize,
    l1_hard_max_multiplier: usize,
    ttl: Duration,
    stale_window: Duration,
    negative_ttl: Duration,
    debug_enabled: bool,
    wait_warn_ms: u64,
}

impl TwoLevelCache {
    pub fn new(
        redis_pool: deadpool_redis::Pool,
        l1_enabled: bool,
        l1_max_keys: usize,
        l1_evict_scan: usize,
        l1_hard_max_multiplier: usize,
        ttl: Duration,
        stale_window: Duration,
        negative_ttl: Duration,
        debug_enabled: bool,
        wait_warn_ms: u64,
    ) -> Self {
        Self {
            redis_pool,
            l1: DashMap::new(),
            l1_enabled,
            l1_max_keys,
            l1_evict_scan: l1_evict_scan.max(64),
            l1_hard_max_multiplier: l1_hard_max_multiplier.max(1),
            ttl,
            stale_window,
            negative_ttl,
            debug_enabled,
            wait_warn_ms: wait_warn_ms.max(1),
        }
    }

    pub fn key(app_device_id: &str, version_number: Option<&str>, country_code: Option<&str>) -> String {
        let cc = country_code.unwrap_or("").trim().to_uppercase();
        match (version_number, cc.is_empty()) {
            (Some(v), false) => format!("app_settings:by_app_device:{app_device_id}:version:{v}:country:{cc}"),
            (Some(v), true) => format!("app_settings:by_app_device:{app_device_id}:version:{v}"),
            (None, false) => format!("app_settings:by_app_device:{app_device_id}:country:{cc}"),
            (None, true) => format!("app_settings:by_app_device:{app_device_id}"),
        }
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<CacheResult> {
        let now = now_ms();
        if self.l1_enabled {
            if let Some(env) = self.l1.get(key).map(|v| v.clone()) {
                if now <= env.stale_until_ms {
                    let state = if now <= env.fresh_until_ms { CacheState::Fresh } else { CacheState::Stale };
                    return Ok(CacheResult {
                        value: env.payload,
                        state,
                        not_found: env.not_found,
                    });
                }
                // Expired entry: eagerly drop it.
                self.l1.remove(key);
            }
            self.l1_prune_if_needed(now);
        }

        let t_pool = std::time::Instant::now();
        let mut conn = self.redis_pool.get().await?;
        let pool_wait_ms = t_pool.elapsed().as_millis() as u64;
        if pool_wait_ms >= self.wait_warn_ms {
            warn!(
                "cache.step=pool_get wait_ms={} key={} l1_size={}",
                pool_wait_ms,
                key,
                self.l1.len()
            );
        } else if self.debug_enabled {
            info!("cache.step=pool_get wait_ms={} key={}", pool_wait_ms, key);
        }

        let t_get = std::time::Instant::now();
        let raw: Option<String> = conn.get(key).await?;
        let redis_get_ms = t_get.elapsed().as_millis() as u64;
        if redis_get_ms >= self.wait_warn_ms {
            warn!("cache.step=redis_get wait_ms={} key={}", redis_get_ms, key);
        } else if self.debug_enabled {
            info!("cache.step=redis_get wait_ms={} key={}", redis_get_ms, key);
        }
        let Some(raw) = raw else {
            return Ok(CacheResult { value: None, state: CacheState::Miss, not_found: false });
        };

        let env: CacheEnvelope = serde_json::from_str(&raw)?;
        if now > env.stale_until_ms {
            return Ok(CacheResult { value: None, state: CacheState::Miss, not_found: false });
        }

        if self.l1_enabled {
            self.l1_prune_if_needed(now);
            self.l1.insert(key.to_string(), env.clone());
        }

        Ok(CacheResult {
            value: env.payload,
            state: if now <= env.fresh_until_ms { CacheState::Fresh } else { CacheState::Stale },
            not_found: env.not_found,
        })
    }

    pub async fn set_value(&self, key: &str, value: AppSettingsResponse) -> anyhow::Result<()> {
        let env = mk_envelope(self.ttl, self.stale_window, Some(value), false);
        self.set_envelope(key, env).await
    }

    pub async fn set_not_found(&self, key: &str) -> anyhow::Result<()> {
        let env = mk_envelope(self.negative_ttl, self.stale_window, None, true);
        self.set_envelope(key, env).await
    }

    async fn set_envelope(&self, key: &str, env: CacheEnvelope) -> anyhow::Result<()> {
        let ttl_secs = ((env.stale_until_ms - now_ms()) / 1000).max(1) as u64;
        let payload = serde_json::to_string(&env)?;
        let t_pool = std::time::Instant::now();
        let mut conn = self.redis_pool.get().await?;
        let pool_wait_ms = t_pool.elapsed().as_millis() as u64;
        if pool_wait_ms >= self.wait_warn_ms {
            warn!(
                "cache.step=pool_get_for_set wait_ms={} key={} ttl_secs={}",
                pool_wait_ms,
                key,
                ttl_secs
            );
        } else if self.debug_enabled {
            info!(
                "cache.step=pool_get_for_set wait_ms={} key={} ttl_secs={}",
                pool_wait_ms,
                key,
                ttl_secs
            );
        }
        let t_set = std::time::Instant::now();
        let _: () = conn.set_ex(key, payload, ttl_secs).await?;
        let redis_set_ms = t_set.elapsed().as_millis() as u64;
        if redis_set_ms >= self.wait_warn_ms {
            warn!("cache.step=redis_set wait_ms={} key={}", redis_set_ms, key);
        } else if self.debug_enabled {
            info!("cache.step=redis_set wait_ms={} key={}", redis_set_ms, key);
        }
        if self.l1_enabled {
            self.l1_prune_if_needed(now_ms());
            self.l1.insert(key.to_string(), env);
        }
        Ok(())
    }

    fn l1_prune_if_needed(&self, now: i64) {
        if !self.l1_enabled || self.l1_max_keys == 0 {
            return;
        }
        let len = self.l1.len();
        if len <= self.l1_max_keys {
            return;
        }

        // Prefer removing expired entries first.
        let mut expired_keys = Vec::new();
        for r in self.l1.iter().take(self.l1_evict_scan) {
            if r.value().stale_until_ms < now {
                expired_keys.push(r.key().clone());
            }
        }
        let mut removed = 0usize;
        for key in expired_keys {
            if self.l1.remove(&key).is_some() {
                removed += 1;
            }
        }

        // Still too big: evict a small batch (best-effort) to get under max.
        if removed == 0 && self.l1.len() > self.l1_max_keys {
            let target = (self.l1.len() - self.l1_max_keys).min(self.l1_evict_scan);
            let keys: Vec<String> = self
                .l1
                .iter()
                .take(target)
                .map(|r| r.key().clone())
                .collect();
            for key in keys {
                let _ = self.l1.remove(&key);
            }
        }

        // Hard cap safety: avoid unbounded growth if eviction can't keep up.
        let hard_max = self.l1_max_keys.saturating_mul(self.l1_hard_max_multiplier.max(1));
        if hard_max > 0 && self.l1.len() > hard_max {
            self.l1.clear();
        }
    }
}

fn mk_envelope(
    ttl: Duration,
    stale_window: Duration,
    payload: Option<AppSettingsResponse>,
    not_found: bool,
) -> CacheEnvelope {
    let now = now_ms();
    let fresh = now + ttl.as_millis() as i64;
    let stale = fresh + stale_window.as_millis() as i64;
    CacheEnvelope {
        v: 1,
        fresh_until_ms: fresh,
        stale_until_ms: stale,
        not_found,
        payload,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

