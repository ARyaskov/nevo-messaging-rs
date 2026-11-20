//! Idempotency: a client-side response cache and the server-side
//! claim-before-execute runtime with an optional distributed store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::sync::watch;

use crate::codec::Codec;
use crate::error::{Error, ErrorCode, Result};
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

/// Scoped like [`server_key`] from the caller's side: target service, tenant,
/// the versioned method (so a version change never replays another version's
/// response) and the idempotency key.
pub fn client_key(
    service: &str,
    tenant: Option<&str>,
    method: &str,
    idempotency_key: &str,
) -> String {
    let tenant = tenant.unwrap_or("");
    format!(
        "{}:{service}|{}:{tenant}|{}:{method}|{idempotency_key}",
        service.len(),
        tenant.len(),
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

    /// Atomically reserve the key for this owner until the lease expires.
    fn claim<'a>(
        &'a self,
        key: &'a str,
        owner: uuid::Uuid,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Claim>>;

    /// Replace a live reservation with its result only if its owner matches.
    fn set<'a>(
        &'a self,
        key: &'a str,
        owner: uuid::Uuid,
        entry: IdempotencyEntry,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool>>;

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

    /// Remove a live reservation only if its owner matches.
    fn delete<'a>(&'a self, key: &'a str, owner: uuid::Uuid) -> BoxFuture<'a, Result<bool>>;
}

#[derive(Debug)]
pub enum Begin<'a> {
    Hit(IdempotencyEntry),
    Conflict,
    Execute(Option<ExecutionLease<'a>>),
}

#[derive(Debug, Clone)]
enum Settled {
    Done(IdempotencyEntry),
    Released,
}

type Lease = (uuid::Uuid, watch::Sender<Option<Settled>>);

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

fn settle(entry: IdempotencyEntry, fingerprint: Option<&str>) -> Begin<'static> {
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

    fn try_lead(&self, key: &str, owner: uuid::Uuid) -> Option<watch::Receiver<Option<Settled>>> {
        let mut leases = self.leases.lock().unwrap();
        if let Some((_, lease)) = leases.get(key) {
            return Some(lease.subscribe());
        }
        let (tx, _rx) = watch::channel(None);
        leases.insert(key.to_owned(), (owner, tx));
        None
    }

    fn settle_lease(&self, key: &str, owner: uuid::Uuid, outcome: Settled) {
        let mut leases = self.leases.lock().unwrap();
        if let std::collections::hash_map::Entry::Occupied(slot) = leases.entry(key.to_owned())
            && slot.get().0 == owner
        {
            slot.remove().1.send_replace(Some(outcome));
        }
    }

    /// Reserve execution. Dropping the returned lease wakes local waiters.
    pub async fn begin<'a>(&'a self, key: &'a str, fingerprint: Option<&str>) -> Result<Begin<'a>> {
        if key.is_empty() || !self.is_enabled() {
            return Ok(Begin::Execute(None));
        }
        if let Some(entry) = self.l1_get(key) {
            return Ok(settle(entry, fingerprint));
        }
        let owner = uuid::Uuid::now_v7();
        loop {
            let Some(mut rx) = self.try_lead(key, owner) else {
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

        let lease = ExecutionLease {
            runtime: self,
            key,
            owner,
            settled: false,
        };
        if let Some(entry) = self.l1_get(key) {
            lease.settle(Settled::Done(entry.clone()));
            return Ok(settle(entry, fingerprint));
        }
        if let Some(store) = &self.distributed {
            let claimed = async {
                match store.claim(key, owner, self.ttl).await? {
                    Claim::Acquired => Ok(None),
                    Claim::Lost {
                        existing: Some(existing),
                    } => Ok(Some(existing)),
                    Claim::Lost { existing: None } => {
                        let entry = store.await_result(key, self.await_timeout).await?;
                        entry.map(Some).ok_or_else(|| {
                            Error::new(ErrorCode::Timeout, "Idempotency owner is still pending")
                                .with_retryable(true)
                        })
                    }
                }
            }
            .await;
            match claimed {
                Ok(Some(existing)) => {
                    self.l1_set(key, &existing);
                    lease.settle(Settled::Done(existing.clone()));
                    return Ok(settle(existing, fingerprint));
                }
                Ok(None) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(Begin::Execute(Some(lease)))
    }
}

/// Ownership of an in-flight operation. The distributed claim expires if an
/// aborted task cannot release it through the asynchronous store API.
#[derive(Debug)]
pub struct ExecutionLease<'a> {
    runtime: &'a TwoTierIdempotency,
    key: &'a str,
    owner: uuid::Uuid,
    settled: bool,
}

impl ExecutionLease<'_> {
    fn settle(mut self, result: Settled) {
        self.runtime.settle_lease(self.key, self.owner, result);
        self.settled = true;
    }

    pub async fn commit(self, entry: IdempotencyEntry) {
        if let Some(store) = &self.runtime.distributed {
            match store
                .set(self.key, self.owner, entry.clone(), self.runtime.ttl)
                .await
            {
                Ok(true) => {}
                outcome => {
                    tracing::warn!(event = "idem.commit.write.failed", ?outcome);
                    return;
                }
            }
        }
        self.runtime.l1_set(self.key, &entry);
        self.settle(Settled::Done(entry));
    }

    pub async fn release(self) {
        if let Some(store) = &self.runtime.distributed
            && let Err(err) = store.delete(self.key, self.owner).await
        {
            tracing::debug!(event = "idem.release.failed", error = %err);
        }
        self.settle(Settled::Released);
    }
}

impl Drop for ExecutionLease<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.runtime
                .settle_lease(self.key, self.owner, Settled::Released);
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

    struct PendingStore;

    impl IdempotencyStore for PendingStore {
        fn get<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<Option<IdempotencyEntry>>> {
            Box::pin(async { Ok(None) })
        }

        fn set<'a>(
            &'a self,
            _key: &'a str,
            _owner: uuid::Uuid,
            _entry: IdempotencyEntry,
            _ttl: Duration,
        ) -> BoxFuture<'a, Result<bool>> {
            Box::pin(async { panic!("a losing claimant must never commit") })
        }

        fn claim<'a>(
            &'a self,
            _key: &'a str,
            _owner: uuid::Uuid,
            _ttl: Duration,
        ) -> BoxFuture<'a, Result<Claim>> {
            Box::pin(async { Ok(Claim::Lost { existing: None }) })
        }

        fn delete<'a>(&'a self, _key: &'a str, _owner: uuid::Uuid) -> BoxFuture<'a, Result<bool>> {
            Box::pin(async { panic!("a losing claimant must never delete") })
        }

        fn await_result<'a>(
            &'a self,
            _key: &'a str,
            _timeout: Duration,
        ) -> BoxFuture<'a, Result<Option<IdempotencyEntry>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[tokio::test]
    async fn pending_distributed_owner_never_grants_execution() {
        let idem =
            TwoTierIdempotency::new(&IdempotencyConfig::default(), Some(Arc::new(PendingStore)));
        for _ in 0..2 {
            let error = idem.begin("pending", None).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::Timeout);
        }
    }

    #[tokio::test]
    async fn single_flight_collapses_concurrent_callers() {
        let idem = Arc::new(TwoTierIdempotency::new(&IdempotencyConfig::new(), None));
        let Begin::Execute(Some(leader)) = idem.begin("k", Some("fp")).await.unwrap() else {
            panic!("first call must own the key");
        };
        let follower = {
            let idem = Arc::clone(&idem);
            tokio::spawn(async move {
                assert!(
                    matches!(idem.begin("k", Some("fp")).await.unwrap(), Begin::Hit(hit) if hit == entry("v1"))
                );
            })
        };
        leader.commit(entry("v1")).await;
        follower.await.unwrap();
        assert!(matches!(
            idem.begin("k", Some("other")).await.unwrap(),
            Begin::Conflict
        ));
    }

    #[tokio::test]
    async fn dropping_the_leader_wakes_a_waiting_follower() {
        let idem = TwoTierIdempotency::new(&IdempotencyConfig::new(), None);
        let leader = idem.begin("k", None).await.unwrap();
        assert!(matches!(leader, Begin::Execute(Some(_))));
        let mut follower = std::pin::pin!(idem.begin("k", None));
        assert!(futures::poll!(follower.as_mut()).is_pending());
        drop(leader);
        let result = tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Begin::Execute(Some(_))));
    }

    #[test]
    fn keys() {
        assert_eq!(
            client_key("svc", Some("t1"), "m@v1", "k"),
            "3:svc|2:t1|4:m@v1|k"
        );
        assert_eq!(client_key("svc", None, "m@v1", "k"), "3:svc|0:|4:m@v1|k");
        // Tenant and version changes must never surface another entry's response.
        assert_ne!(
            client_key("svc", Some("t1"), "m@v1", "k"),
            client_key("svc", Some("t2"), "m@v1", "k")
        );
        assert_ne!(
            client_key("svc", Some("t1"), "m@v1", "k"),
            client_key("svc", Some("t1"), "m@v2", "k")
        );
        assert_eq!(
            server_key(None, Some("t"), "m", None, "u").unwrap(),
            "0:|1:t|1:m|u"
        );
        assert_eq!(server_key(None, None, "m", Some(""), "").as_deref(), None);
    }
}
