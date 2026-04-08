use std::{
    collections::BTreeMap,
    env,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use reqwest::StatusCode;
use tokio::sync::Mutex;

fn get(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn get_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn get_duration(key: &str, default: Duration) -> Duration {
    env::var(key)
        .ok()
        .and_then(|v| parse_duration(&v))
        .unwrap_or(default)
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let target = get(
        "LOADTEST_URL",
        "http://127.0.0.1:8081/api/v1/appsetting-api/app-settings",
    );
    let app_device_id = get(
        "LOADTEST_APP_DEVICE_ID",
        "00000000-0000-0000-0000-000000000000",
    );
    let version_number = env::var("LOADTEST_VERSION_NUMBER")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let country_code = env::var("LOADTEST_COUNTRY_CODE")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let secret_key = get("LOADTEST_SECRET_KEY", "dev");

    let concurrency = get_usize("LOADTEST_CONCURRENCY", 200).max(1);
    let duration = get_duration("LOADTEST_DURATION", Duration::from_secs(20));
    let request_timeout = get_duration("LOADTEST_REQUEST_TIMEOUT", Duration::from_secs(10));

    let client = reqwest::Client::builder()
        .timeout(request_timeout)
        .pool_max_idle_per_host(concurrency)
        .build()?;

    let hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?)); // ms
    let status_counts = Arc::new(Mutex::new(BTreeMap::<u16, u64>::new()));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let client = client.clone();
        let target = target.clone();
        let app_device_id = app_device_id.clone();
        let version_number = version_number.clone();
        let country_code = country_code.clone();
        let secret_key = secret_key.clone();

        let hist = Arc::clone(&hist);
        let status_counts = Arc::clone(&status_counts);
        let ok = Arc::clone(&ok);
        let err = Arc::clone(&err);

        tasks.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                let mut req = client
                    .get(&target)
                    .header("X-Secret-Key", &secret_key)
                    .query(&[("app_device_id", app_device_id.as_str())]);
                if let Some(v) = version_number.as_deref() {
                    req = req.query(&[("version_number", v)]);
                }
                if let Some(cc) = country_code.as_deref() {
                    req = req.query(&[("country_code", cc)]);
                }

                let t = Instant::now();
                match req.send().await {
                    Ok(resp) => {
                        let status = resp.status();
                        let _ = resp.bytes().await; // fully consume body for keep-alive

                        let ms = t.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                        {
                            let mut h = hist.lock().await;
                            let _ = h.record(ms.max(1));
                        }
                        {
                            let mut m = status_counts.lock().await;
                            *m.entry(status.as_u16()).or_insert(0) += 1;
                        }
                        if status == StatusCode::OK {
                            ok.fetch_add(1, Ordering::Relaxed);
                        } else {
                            err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for t in tasks {
        let _ = t.await;
    }

    let elapsed = t0.elapsed().as_secs_f64().max(0.0001);
    let ok_n = ok.load(Ordering::Relaxed);
    let err_n = err.load(Ordering::Relaxed);
    let total = ok_n + err_n;

    let h = hist.lock().await;
    let p50 = h.value_at_quantile(0.50);
    let p95 = h.value_at_quantile(0.95);
    let p99 = h.value_at_quantile(0.99);
    let max = h.max();

    let m = status_counts.lock().await;
    println!("load-test results");
    println!("  url={}", target);
    println!("  concurrency={}", concurrency);
    println!("  duration_ms={}", duration.as_millis());
    println!("  total_requests={} ok={} err={}", total, ok_n, err_n);
    println!("  rps={:.1}", (total as f64) / elapsed);
    println!("  latency_ms p50={} p95={} p99={} max={}", p50, p95, p99, max);
    println!("  status_counts={:?}", *m);

    Ok(())
}

