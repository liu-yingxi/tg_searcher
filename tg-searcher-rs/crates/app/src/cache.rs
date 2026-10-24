//! Cache abstraction used for pagination contexts, selected chats and usage
//! statistics. Redis is used in production; an in-memory implementation keeps
//! development and tests dependency-free.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use redis::AsyncCommands;
use tracing::{info, warn};

#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn set(&self, key: &str, value: &str, ttl: Option<Duration>) -> Result<()>;
    async fn delete(&self, keys: &[&str]) -> Result<()>;
    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()>;
    async fn set_card(&self, key: &str) -> Result<usize>;
    async fn expire(&self, key: &str, ttl: Duration) -> Result<()>;
    fn is_volatile(&self) -> bool;
}

#[derive(Debug)]
enum Entry {
    Value(String, Option<Instant>),
    Set(HashSet<String>, Option<Instant>),
}

impl Entry {
    fn expired(&self, now: Instant) -> bool {
        match self {
            Entry::Value(_, expiry) | Entry::Set(_, expiry) => {
                expiry.is_some_and(|expiry| expiry <= now)
            }
        }
    }
}

/// In-memory cache with per-key TTL.
#[derive(Default)]
pub struct MemoryCache {
    values: Mutex<HashMap<String, Entry>>,
}

impl MemoryCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn purge(&self, key: &str) {
        let now = Instant::now();
        let mut values = self.values.lock().expect("cache mutex poisoned");
        if values.get(key).is_some_and(|entry| entry.expired(now)) {
            values.remove(key);
        }
    }
}

#[async_trait]
impl Cache for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        self.purge(key);
        let values = self.values.lock().expect("cache mutex poisoned");
        Ok(match values.get(key) {
            Some(Entry::Value(value, _)) => Some(value.clone()),
            _ => None,
        })
    }

    async fn set(&self, key: &str, value: &str, ttl: Option<Duration>) -> Result<()> {
        let expiry = ttl.map(|ttl| Instant::now() + ttl);
        let mut values = self.values.lock().expect("cache mutex poisoned");
        values.insert(key.to_string(), Entry::Value(value.to_string(), expiry));
        Ok(())
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        let mut values = self.values.lock().expect("cache mutex poisoned");
        for key in keys {
            values.remove(*key);
        }
        Ok(())
    }

    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()> {
        self.purge(key);
        let mut values = self.values.lock().expect("cache mutex poisoned");
        match values.get_mut(key) {
            Some(Entry::Set(set, _)) => {
                for member in members {
                    set.insert((*member).to_string());
                }
            }
            _ => {
                let mut set = HashSet::new();
                for member in members {
                    set.insert((*member).to_string());
                }
                values.insert(key.to_string(), Entry::Set(set, None));
            }
        }
        Ok(())
    }

    async fn set_card(&self, key: &str) -> Result<usize> {
        self.purge(key);
        let values = self.values.lock().expect("cache mutex poisoned");
        Ok(match values.get(key) {
            Some(Entry::Set(set, _)) => set.len(),
            _ => 0,
        })
    }

    async fn expire(&self, key: &str, ttl: Duration) -> Result<()> {
        let mut values = self.values.lock().expect("cache mutex poisoned");
        let expiry = Instant::now() + ttl;
        match values.get_mut(key) {
            Some(Entry::Value(_, current)) | Some(Entry::Set(_, current)) => {
                *current = Some(expiry);
            }
            None => {}
        }
        Ok(())
    }

    fn is_volatile(&self) -> bool {
        true
    }
}

/// Redis-backed cache.
pub struct RedisCache {
    manager: tokio::sync::Mutex<redis::aio::ConnectionManager>,
}

impl RedisCache {
    pub async fn connect(addr: &str) -> Result<Self> {
        let url = if addr.contains("://") {
            addr.to_string()
        } else {
            format!("redis://{addr}")
        };
        let client =
            redis::Client::open(url.clone()).with_context(|| format!("invalid Redis URL {url}"))?;
        let manager = client
            .get_connection_manager()
            .await
            .with_context(|| format!("cannot connect to Redis at {addr}"))?;
        let cache = Self {
            manager: tokio::sync::Mutex::new(manager),
        };
        let pong: String = cache
            .manager
            .lock()
            .await
            .ping()
            .await
            .context("Redis ping failed")?;
        if !pong.eq_ignore_ascii_case("PONG") {
            anyhow::bail!("unexpected Redis ping response: {pong}");
        }
        info!(%addr, "connected to Redis");
        Ok(cache)
    }
}

#[async_trait]
impl Cache for RedisCache {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut connection = self.manager.lock().await;
        let value: Option<String> = connection.get(key).await?;
        Ok(value)
    }

    async fn set(&self, key: &str, value: &str, ttl: Option<Duration>) -> Result<()> {
        let mut connection = self.manager.lock().await;
        match ttl {
            Some(ttl) => {
                let seconds = ttl.as_secs().max(1);
                let _: () = connection.set_ex(key, value, seconds).await?;
            }
            None => {
                let _: () = connection.set(key, value).await?;
            }
        }
        Ok(())
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut connection = self.manager.lock().await;
        let _: () = connection.del(keys).await?;
        Ok(())
    }

    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()> {
        if members.is_empty() {
            return Ok(());
        }
        let mut connection = self.manager.lock().await;
        let _: () = connection.sadd(key, members).await?;
        Ok(())
    }

    async fn set_card(&self, key: &str) -> Result<usize> {
        let mut connection = self.manager.lock().await;
        let value: usize = connection.scard(key).await?;
        Ok(value)
    }

    async fn expire(&self, key: &str, ttl: Duration) -> Result<()> {
        let mut connection = self.manager.lock().await;
        let _: () = connection.expire(key, ttl.as_secs().max(1) as i64).await?;
        Ok(())
    }

    fn is_volatile(&self) -> bool {
        false
    }
}

/// Create the configured cache, falling back to memory if Redis is disabled or
/// unreachable. The returned boolean records the fallback (used by `/usage`).
pub async fn build_cache(no_redis: bool, redis: Option<&str>) -> (std::sync::Arc<dyn Cache>, bool) {
    if no_redis {
        info!("Redis disabled by configuration; using volatile memory cache");
        return (std::sync::Arc::new(MemoryCache::new()), true);
    }
    match redis {
        Some(addr) if !addr.trim().is_empty() => match RedisCache::connect(addr).await {
            Ok(cache) => (std::sync::Arc::new(cache), false),
            Err(e) => {
                warn!("Redis unavailable ({e}); falling back to volatile memory cache");
                (std::sync::Arc::new(MemoryCache::new()), true)
            }
        },
        _ => {
            warn!("no Redis address configured; using volatile memory cache");
            (std::sync::Arc::new(MemoryCache::new()), true)
        }
    }
}

/// Parse `host:port` (port defaults to 6379). Kept for config validation.
pub fn parse_redis_addr(value: &str) -> Option<(String, u16)> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    match value.rsplit_once(':') {
        Some((host, port)) => {
            let port = port.parse().ok()?;
            Some((host.to_string(), port))
        }
        None => Some((value.to_string(), 6379)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_cache_ttl_and_sets() {
        let cache = MemoryCache::new();
        cache
            .set("a", "1", Some(Duration::from_millis(20)))
            .await
            .unwrap();
        assert_eq!(cache.get("a").await.unwrap().as_deref(), Some("1"));
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(cache.get("a").await.unwrap().is_none());

        cache.set_add("s", &["u1", "u2", "u2"]).await.unwrap();
        assert_eq!(cache.set_card("s").await.unwrap(), 2);
        cache.set_add("s", &["u3"]).await.unwrap();
        assert_eq!(cache.set_card("s").await.unwrap(), 3);
        cache.delete(&["s"]).await.unwrap();
        assert_eq!(cache.set_card("s").await.unwrap(), 0);
    }

    #[test]
    fn redis_addr_parsing() {
        assert_eq!(parse_redis_addr("redis:6379"), Some(("redis".into(), 6379)));
        assert_eq!(
            parse_redis_addr("localhost"),
            Some(("localhost".into(), 6379))
        );
        assert_eq!(parse_redis_addr(""), None);
    }
}
