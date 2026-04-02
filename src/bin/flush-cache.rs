use dotenvy::dotenv;
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let redis_addr = std::env::var("REDIS_ADDR").unwrap_or_else(|_| "localhost:6379".to_string());
    let redis_password = std::env::var("REDIS_PASSWORD").unwrap_or_default();
    let redis_db: u8 = std::env::var("REDIS_DB").ok().and_then(|v| v.parse().ok()).unwrap_or(0);

    let redis_url = if redis_password.is_empty() {
        format!("redis://{redis_addr}/{redis_db}")
    } else {
        format!("redis://:{}@{redis_addr}/{redis_db}", redis_password)
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
    println!("Flushed {} app_settings key(s) from Redis (db={})", deleted, redis_db);
    Ok(())
}

