use dotenvy::dotenv;
use redis::AsyncCommands;

fn get(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let cache_provider = get("CACHE_PROVIDER", "redis").trim().to_lowercase();
    let cache_url = if let Ok(url) = std::env::var("CACHE_URL") {
        if !url.trim().is_empty() {
            url
        } else {
            String::new()
        }
    } else {
        String::new()
    };
    let redis_url = if !cache_url.is_empty() {
        cache_url
    } else if cache_provider == "keydb" {
        let addr = get("KEYDB_ADDR", std::env::var("REDIS_ADDR").ok().as_deref().unwrap_or("localhost:6379"));
        let password = std::env::var("KEYDB_PASSWORD")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| std::env::var("REDIS_PASSWORD").ok().filter(|v| !v.trim().is_empty()))
            .unwrap_or_default();
        let db: u8 = std::env::var("KEYDB_DB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| std::env::var("REDIS_DB").ok().and_then(|v| v.parse().ok()).unwrap_or(0));
        if password.is_empty() {
            format!("redis://{addr}/{db}")
        } else {
            format!("redis://:{}@{addr}/{db}", password)
        }
    } else {
        let addr = get("REDIS_ADDR", "localhost:6379");
        let password = std::env::var("REDIS_PASSWORD").unwrap_or_default();
        let db: u8 = std::env::var("REDIS_DB").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
        if password.is_empty() {
            format!("redis://{addr}/{db}")
        } else {
            format!("redis://:{}@{addr}/{db}", password)
        }
    };

    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let mut cursor: u64 = 0;
    let mut deleted = 0usize;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("app_settings:*")
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await?;
        if !keys.is_empty() {
            let _: () = conn.del(keys.clone()).await?;
            deleted += keys.len();
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    println!("Flushed {} app_settings key(s) from cache backend ({})", deleted, cache_provider);
    Ok(())
}

