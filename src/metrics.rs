use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};

use hdrhistogram::Histogram;
use serde::Serialize;

#[derive(Clone)]
pub struct AppSettingsMetrics {
    inner: Arc<Mutex<HashMap<String, Bucket>>>,
    sample_rate: u64,
    sample_counter: Arc<AtomicU64>,
}

struct Bucket {
    count: u64,
    total: Histogram<u64>,
    cache: Histogram<u64>,
    backend: Histogram<u64>,
}

#[derive(Serialize)]
struct Percentiles {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    mean_ms: f64,
}

#[derive(Serialize)]
struct Summary {
    count: u64,
    total: Percentiles,
    cache: Percentiles,
    backend: Percentiles,
}

#[derive(Serialize)]
pub struct Snapshot {
    generated_at: String,
    buckets: HashMap<String, Summary>,
    order: Vec<String>,
}

impl AppSettingsMetrics {
    pub fn new(sample_rate: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            sample_rate: sample_rate.max(1),
            sample_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn observe(&self, name: &str, total: Duration, cache: Duration, backend: Duration) {
        if name.is_empty() {
            return;
        }
        if self.sample_rate > 1 {
            let n = self.sample_counter.fetch_add(1, Ordering::Relaxed);
            if n % self.sample_rate != 0 {
                return;
            }
        }
        let mut m = match self.inner.lock() {
            Ok(v) => v,
            Err(_) => return,
        };
        let b = m.entry(name.to_string()).or_insert_with(|| Bucket {
            count: 0,
            total: Histogram::new_with_bounds(1_000, 60_000_000, 3).expect("hist"),
            cache: Histogram::new_with_bounds(1_000, 60_000_000, 3).expect("hist"),
            backend: Histogram::new_with_bounds(1_000, 60_000_000, 3).expect("hist"),
        });
        b.count += 1;
        let _ = b.total.record(clamp_us(total));
        let _ = b.cache.record(clamp_us(cache));
        let _ = b.backend.record(clamp_us(backend));
    }

    pub fn snapshot_json(&self) -> anyhow::Result<Vec<u8>> {
        let m = self.inner.lock().map_err(|_| anyhow::anyhow!("metrics lock poisoned"))?;
        let mut order: Vec<String> = m.keys().cloned().collect();
        order.sort();
        let mut buckets = HashMap::new();
        for k in &order {
            if let Some(v) = m.get(k) {
                buckets.insert(
                    k.clone(),
                    Summary {
                        count: v.count,
                        total: summarize(&v.total),
                        cache: summarize(&v.cache),
                        backend: summarize(&v.backend),
                    },
                );
            }
        }
        let snap = Snapshot {
            generated_at: format!("{:?}", SystemTime::now()),
            buckets,
            order,
        };
        Ok(serde_json::to_vec_pretty(&snap)?)
    }
}

fn summarize(h: &Histogram<u64>) -> Percentiles {
    let us = |v: u64| v as f64 / 1000.0;
    Percentiles {
        p50_ms: us(h.value_at_quantile(0.50)),
        p95_ms: us(h.value_at_quantile(0.95)),
        p99_ms: us(h.value_at_quantile(0.99)),
        max_ms: us(h.max()),
        mean_ms: us(h.mean() as u64),
    }
}

fn clamp_us(d: Duration) -> u64 {
    d.as_micros().clamp(1_000, 60_000_000) as u64
}

