//! Token-bucket rate limiting keyed by caller, tenant, topic or method.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, ErrorCode, Result};
use crate::wire::Meta;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyDimension {
    Service,
    Method,
    CallerService,
    TenantId,
}

#[derive(Debug, Clone, Copy)]
pub struct RateLimitContext<'a> {
    pub topic: &'a str,
    pub method: &'a str,
    pub caller_service: Option<&'a str>,
    pub tenant_id: Option<&'a str>,
    pub meta: Option<&'a Meta>,
}

pub type KeyExtractor = Arc<dyn Fn(&RateLimitContext<'_>) -> Option<String> + Send + Sync>;

#[derive(Clone)]
#[non_exhaustive]
pub struct RateLimitConfig {
    pub capacity: f64,
    pub refill_per_sec: f64,
    pub key_by: Option<Vec<KeyDimension>>,
    pub key_extractor: Option<KeyExtractor>,
    pub max_entries: usize,
    pub idle_evict: Duration,
}

impl fmt::Debug for RateLimitConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimitConfig")
            .field("capacity", &self.capacity)
            .field("refill_per_sec", &self.refill_per_sec)
            .field("key_by", &self.key_by)
            .field("custom_key", &self.key_extractor.is_some())
            .field("max_entries", &self.max_entries)
            .field("idle_evict", &self.idle_evict)
            .finish()
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            capacity: 100.0,
            refill_per_sec: 50.0,
            key_by: None,
            key_extractor: None,
            max_entries: 100_000,
            idle_evict: Duration::from_secs(5 * 60),
        }
    }
}

impl RateLimitConfig {
    pub fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self {
            capacity,
            refill_per_sec,
            ..Self::default()
        }
    }

    pub fn key_by(mut self, dimensions: impl IntoIterator<Item = KeyDimension>) -> Self {
        self.key_by = Some(dimensions.into_iter().collect());
        self
    }

    pub fn key_extractor(
        mut self,
        f: impl Fn(&RateLimitContext<'_>) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.key_extractor = Some(Arc::new(f));
        self
    }

    pub fn max_entries(mut self, max: usize) -> Self {
        self.max_entries = max.max(1);
        self
    }

    pub fn idle_evict(mut self, after: Duration) -> Self {
        self.idle_evict = after;
        self
    }

    fn key_for(&self, ctx: &RateLimitContext<'_>) -> Option<String> {
        if let Some(extractor) = &self.key_extractor {
            return extractor(ctx);
        }
        if let Some(dimensions) = &self.key_by {
            let parts: Vec<&str> = dimensions
                .iter()
                .map(|d| match d {
                    KeyDimension::Service => ctx.topic,
                    KeyDimension::Method => ctx.method,
                    KeyDimension::CallerService => ctx.caller_service.unwrap_or("anon"),
                    KeyDimension::TenantId => ctx.tenant_id.unwrap_or("no-tenant"),
                })
                .collect();
            return Some(parts.join(":"));
        }
        Some(match (ctx.caller_service, ctx.tenant_id) {
            (Some(caller), _) => format!("{}:{}:{caller}", ctx.topic, ctx.method),
            (None, Some(tenant)) => format!("{}:{}:tenant:{tenant}", ctx.topic, ctx.method),
            (None, None) => format!("{}:{}", ctx.topic, ctx.method),
        })
    }
}

#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
    last_touched: Instant,
}

#[derive(Debug)]
pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: Mutex<HashMap<String, Bucket>>,
    checks: AtomicU64,
}

const SWEEP_EVERY: u64 = 4096;

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
            checks: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }

    fn sweep(&self, buckets: &mut HashMap<String, Bucket>, now: Instant) {
        let idle = self.config.idle_evict;
        buckets.retain(|_, b| now.duration_since(b.last_touched) <= idle);
    }

    pub fn check(&self, ctx: &RateLimitContext<'_>) -> Result<()> {
        let Some(key) = self.config.key_for(ctx) else {
            return Ok(());
        };
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap();
        if self
            .checks
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(SWEEP_EVERY)
        {
            self.sweep(&mut buckets, now);
        }
        if !buckets.contains_key(&key) && buckets.len() >= self.config.max_entries {
            self.sweep(&mut buckets, now);
        }
        let key = if !buckets.contains_key(&key) && buckets.len() >= self.config.max_entries {
            "<overflow>".to_owned()
        } else {
            key
        };
        let capacity = self.config.capacity;
        let refill = self.config.refill_per_sec;
        let bucket = buckets.entry(key).or_insert_with(|| Bucket {
            tokens: capacity,
            last_refill: now,
            last_touched: now,
        });
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            bucket.tokens = (bucket.tokens + elapsed * refill).min(capacity);
            bucket.last_refill = now;
        }
        bucket.last_touched = now;
        if bucket.tokens < 1.0 {
            let retry_after_ms = if refill > 0.0 {
                ((1.0 - bucket.tokens) / refill * 1000.0).ceil() as u64
            } else {
                self.config.idle_evict.as_millis() as u64
            };
            let mut err = Error::new(
                ErrorCode::RateLimited,
                format!("Rate limit exceeded for {}", ctx.method),
            )
            .with_detail("topic", ctx.topic)
            .with_detail("method", ctx.method)
            .with_detail("retryAfterMs", retry_after_ms)
            .with_retryable(true);
            if let Some(caller) = ctx.caller_service {
                err = err.with_detail("callerService", caller);
            }
            if let Some(tenant) = ctx.tenant_id {
                err = err.with_detail("tenantId", tenant);
            }
            return Err(err);
        }
        bucket.tokens -= 1.0;
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<(String, f64)> {
        self.buckets
            .lock()
            .unwrap()
            .iter()
            .map(|(k, b)| (k.clone(), b.tokens.floor()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(caller: Option<&'a str>) -> RateLimitContext<'a> {
        RateLimitContext {
            topic: "user-events",
            method: "user.get",
            caller_service: caller,
            tenant_id: None,
            meta: None,
        }
    }

    #[test]
    fn buckets_per_caller_and_retry_after_hint() {
        let limiter = RateLimiter::new(RateLimitConfig::new(2.0, 1.0));
        limiter.check(&ctx(Some("a"))).unwrap();
        limiter.check(&ctx(Some("a"))).unwrap();
        let err = limiter.check(&ctx(Some("a"))).unwrap_err();
        assert_eq!(err.code(), ErrorCode::RateLimited);
        assert!(err.retry_after().unwrap() > Duration::ZERO);
        limiter.check(&ctx(Some("b"))).unwrap();
        assert_eq!(limiter.snapshot().len(), 2);
    }

    #[test]
    fn custom_dimensions() {
        let cfg = RateLimitConfig::new(1.0, 0.0).key_by([KeyDimension::TenantId]);
        let limiter = RateLimiter::new(cfg);
        limiter.check(&ctx(Some("a"))).unwrap();
        assert!(limiter.check(&ctx(Some("b"))).is_err());
        let never = RateLimiter::new(RateLimitConfig::new(1.0, 0.0).key_extractor(|_| None));
        never.check(&ctx(None)).unwrap();
        never.check(&ctx(None)).unwrap();
    }
}
