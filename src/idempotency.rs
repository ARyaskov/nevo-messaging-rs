//! Idempotency: a client-side response cache and the server-side
//! claim-before-execute runtime with an optional distributed store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::sync::watch;

use crate::codec::Codec;
use crate::error::Result;
use crate::lru::TtlLru;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct IdempotencyConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub ttl: Duration,
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10_000,
            ttl: Duration::from_secs(5 * 60),
        }
    }
}

impl IdempotencyConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    pub fn max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }
}

pub fn client_key(service: &str, method: &str, idempotency_key: &str) -> String {
    format!(
        "{}:{service}|{}:{method}|{idempotency_key}",
        service.len(),
        method.len()
    )
}

/// Scoped by caller, tenant and method so one key can never surface another method's response.
pub fn server_key(
    caller: Option<&str>,
    tenant: Option<&str>,
    method: &str,
    supplied_key: Option<&str>,
    envelope_uuid: &str,
) -> Option<String> {
    let base = supplied_key
        .filter(|k| !k.is_empty())
        .or(Some(envelope_uuid).filter(|u| !u.is_empty()))?;
    let caller = caller.unwrap_or("");
    let tenant = tenant.unwrap_or("");
    Some(format!(
        "{}:{caller}|{}:{tenant}|{}:{method}|{base}",
        caller.len(),
        tenant.len(),
        method.len()
    ))
}

/// A cached response envelope plus the fingerprint of the payload it answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyEntry {
    pub codec: Codec,
    pub payload: Bytes,
    pub fingerprint: Option<String>,
}

/// In-process cache of response envelopes keyed by [`client_key`].
#[derive(Debug)]
pub struct ResponseCache {
    enabled: bool,
    inner: Mutex<TtlLru<IdempotencyEntry>>,
}

impl ResponseCache {
    pub fn new(config: &IdempotencyConfig) -> Self {
        Self {
            enabled: config.enabled,
            inner: Mutex::new(TtlLru::new(config.max_entries, config.ttl)),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get(&self, key: &str) -> Option<IdempotencyEntry> {
        if !self.enabled {
            return None;
        }
        self.inner.lock().unwrap().get(key).cloned()
    }

    pub fn insert(&self, key: &str, entry: IdempotencyEntry) {
        if self.enabled {
            self.inner.lock().unwrap().insert(key, entry);
        }
    }

    pub fn remove(&self, key: &str) -> bool {
        self.inner.lock().unwrap().remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// This caller reserved the key and must execute the handler.
    Acquired,
    /// Another caller holds the key; `existing` is set once its result landed.
    Lost { existing: Option<IdempotencyEntry> },
}

/// A shared backend (Redis, etcd, a database) for cross-replica deduplication.
pub trait IdempotencyStore: Send + Sync + 'static {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<IdempotencyEntry>>>;

    fn set<'a>(
        &'a self,
        key: &'a str,
        entry: IdempotencyEntry,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<()>>;

    /// Atomically reserves `key`; stores without an atomic primitive fall back to a racy read.
    fn claim<'a>(&'a self, key: &'a str, ttl: Duration) -> BoxFuture<'a, Result<Claim>> {
        let _ = ttl;
        Box::pin(async move {
            Ok(match self.get(key).await? {
                Some(existing) => Claim::Lost {
                    existing: Some(existing),
                },
                None => Claim::Acquired,
            })
        })
    }

    /// Polls for a finished result until `timeout` elapses.
    fn await_result<'a>(
        &'a self,
        key: &'a str,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<IdempotencyEntry>>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if let Some(entry) = self.get(key).await? {
                    return Ok(Some(entry));
                }
                if tokio::time::Instant::now() >= deadline {
                    return Ok(None);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<()>> {
        let _ = key;
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Begin {
    Hit(IdempotencyEntry),
    Conflict,
    Execute,
}

#[derive(Debug, Clone)]
enum Settled {
    Done(IdempotencyEntry),
    Released,
}

type Lease = watch::Sender<Option<Settled>>;

/// L1 cache + in-process single-flight + optional distributed claim.
pub struct TwoTierIdempotency {
    l1: Option<Mutex<TtlLru<IdempotencyEntry>>>,
    distributed: Option<Arc<dyn IdempotencyStore>>,
    ttl: Duration,
    await_timeout: Duration,
    leases: Mutex<HashMap<String, Lease>>,
}

impl std::fmt::Debug for TwoTierIdempotency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoTierIdempotency")
            .field("l1", &self.l1.is_some())
            .field("distributed", &self.distributed.is_some())
            .field("ttl", &self.ttl)
            .finish()
    }
}

fn conflicts(stored: Option<&str>, incoming: Option<&str>) -> bool {
    matches!((stored, incoming), (Some(a), Some(b)) if a != b)
}

fn settle(entry: IdempotencyEntry, fingerprint: Option<&str>) -> Begin {
    if conflicts(entry.fingerprint.as_deref(), fingerprint) {
        Begin::Conflict
    } else {
        Begin::Hit(entry)
    }
}

impl TwoTierIdempotency {
    pub fn new(config: &IdempotencyConfig, distributed: Option<Arc<dyn IdempotencyStore>>) -> Self {
        Self {
            l1: config
                .enabled
                .then(|| Mutex::new(TtlLru::new(config.max_entries, config.ttl))),
            distributed,
            ttl: config.ttl,
            await_timeout: Duration::from_secs(5),
            leases: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.l1.is_some() || self.distributed.is_some()
    }

    fn l1_get(&self, key: &str) -> Option<IdempotencyEntry> {
        self.l1.as_ref()?.lock().unwrap().get(key).cloned()
    }

    fn l1_set(&self, key: &str, entry: &IdempotencyEntry) {
        if let Some(l1) = &self.l1 {
            l1.lock().unwrap().insert(key, entry.clone());
        }
    }

    fn try_lead(&self, key: &str) -> Option<watch::Receiver<Option<Settled>>> {
        let mut leases = self.leases.lock().unwrap();
        if let Some(lease) = leases.get(key) {
            return Some(lease.subscribe());
        }
        let (tx, _rx) = watch::channel(None);
        leases.insert(key.to_owned(), tx);
        None
    }

    fn settle_lease(&self, key: &str, outcome: Settled) {
        if let Some(lease) = self.leases.lock().unwrap().remove(key) {
            lease.send_replace(Some(outcome));
        }
    }

    /// On `Execute` the caller must later [`commit`](Self::commit) or [`release`](Self::release).
    pub async fn begin(&self, key: &str, fingerprint: Option<&str>) -> Result<Begin> {
        if key.is_empty() || !self.is_enabled() {
            return Ok(Begin::Execute);
        }
        if let Some(entry) = self.l1_get(key) {
            return Ok(settle(entry, fingerprint));
        }
        loop {
            let Some(mut rx) = self.try_lead(key) else {
                break;
            };
            let outcome = loop {
                let current = rx.borrow().clone();
                if let Some(outcome) = current {
                    break Some(outcome);
                }
                if rx.changed().await.is_err() {
                    break None;
                }
            };
            match outcome {
                Some(Settled::Done(entry)) => return Ok(settle(entry, fingerprint)),
                Some(Settled::Released) | None => continue,
            }
        }

        if let Some(store) = &self.distributed {
            let claimed = async {
                match store.claim(key, self.ttl).await? {
                    Claim::Acquired => Ok(None),
                    Claim::Lost {
                        existing: Some(existing),
                    } => Ok(Some(existing)),
                    Claim::Lost { existing: None } => {
                        store.await_result(key, self.await_timeout).await
                    }
                }
            }
            .await;
            match claimed {
                Ok(Some(existing)) => {
                    self.l1_set(key, &existing);
                    self.settle_lease(key, Settled::Done(existing.clone()));
                    return Ok(settle(existing, fingerprint));
                }
                Ok(None) => {}
                Err(err) => {
                    self.settle_lease(key, Settled::Released);
                    return Err(err);
                }
            }
        }
        Ok(Begin::Execute)
    }

    pub async fn commit(&self, key: &str, entry: IdempotencyEntry) {
        if key.is_empty() {
            return;
        }
        self.l1_set(key, &entry);
        self.settle_lease(key, Settled::Done(entry.clone()));
        if let Some(store) = &self.distributed
            && let Err(err) = store.set(key, entry, self.ttl).await
        {
            tracing::warn!(
                event = "idem.commit.write.failed",
                error = %err,
                "distributed idempotency write failed; result kept in L1 only"
            );
        }
    }

    pub async fn release(&self, key: &str) {
        if key.is_empty() {
            return;
        }
        self.settle_lease(key, Settled::Released);
        if let Some(store) = &self.distributed
            && let Err(err) = store.delete(key).await
        {
            tracing::debug!(event = "idem.release.failed", error = %err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(tag: &str) -> IdempotencyEntry {
        IdempotencyEntry {
            codec: Codec::Json,
            payload: Bytes::from(tag.to_owned()),
            fingerprint: Some("fp".into()),
        }
    }

    #[tokio::test]
    async fn single_flight_collapses_concurrent_callers() {
        let idem = Arc::new(TwoTierIdempotency::new(&IdempotencyConfig::new(), None));
        let leader = idem.begin("k", Some("fp")).await.unwrap();
        assert_eq!(leader, Begin::Execute);
        let follower = {
            let idem = Arc::clone(&idem);
            tokio::spawn(async move { idem.begin("k", Some("fp")).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        idem.commit("k", entry("v1")).await;
        assert_eq!(follower.await.unwrap(), Begin::Hit(entry("v1")));
        assert_eq!(
            idem.begin("k", Some("other")).await.unwrap(),
            Begin::Conflict
        );
        assert_eq!(
            idem.begin("k", None).await.unwrap(),
            Begin::Hit(entry("v1"))
        );
    }

    #[tokio::test]
    async fn release_lets_a_follower_take_over() {
        let idem = Arc::new(TwoTierIdempotency::new(&IdempotencyConfig::new(), None));
        assert_eq!(idem.begin("k", None).await.unwrap(), Begin::Execute);
        let follower = {
            let idem = Arc::clone(&idem);
            tokio::spawn(async move { idem.begin("k", None).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        idem.release("k").await;
        assert_eq!(follower.await.unwrap(), Begin::Execute);
    }

    #[test]
    fn keys() {
        assert_eq!(client_key("svc", "m", "k"), "3:svc|1:m|k");
        assert_eq!(
            server_key(None, Some("t"), "m", None, "u").unwrap(),
            "0:|1:t|1:m|u"
        );
        assert_eq!(server_key(None, None, "m", Some(""), "").as_deref(), None);
    }
}
