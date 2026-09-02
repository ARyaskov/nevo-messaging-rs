//! Liveness and readiness probes exposed as the built-in `nevo.health`,
//! `nevo.live` and `nevo.ready` signals.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::time::unix_millis;

pub const HEALTH_METHOD: &str = "nevo.health";
pub const LIVENESS_METHOD: &str = "nevo.live";
pub const READINESS_METHOD: &str = "nevo.ready";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthKind {
    Liveness,
    Readiness,
    Both,
}

impl HealthKind {
    fn selected_by(self, filter: Option<HealthKind>) -> bool {
        match filter {
            None | Some(HealthKind::Both) => true,
            Some(wanted) => self == HealthKind::Both || self == wanted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Ok,
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckResult {
    pub status: CheckStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl CheckResult {
    pub fn ok() -> Self {
        Self {
            status: CheckStatus::Ok,
            message: None,
        }
    }

    pub fn ok_with(message: impl Into<String>) -> Self {
        Self {
            status: CheckStatus::Ok,
            message: Some(message.into()),
        }
    }

    pub fn down(message: impl Into<String>) -> Self {
        Self {
            status: CheckStatus::Down,
            message: Some(message.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckReport {
    #[serde(flatten)]
    pub result: CheckResult,
    pub kind: HealthKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OverallStatus {
    Ok,
    Degraded,
    Down,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthStatus {
    pub status: OverallStatus,
    pub service: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub uptime_ms: u64,
    pub ts: u64,
    #[serde(default)]
    pub checks: BTreeMap<String, CheckReport>,
}

type CheckFn = Arc<dyn Fn() -> BoxFuture<'static, CheckResult> + Send + Sync>;

struct Registered {
    run: CheckFn,
    kind: HealthKind,
    timeout: Option<Duration>,
    cache_for: Option<Duration>,
    cached: Option<(CheckResult, Instant)>,
}

struct Inner {
    service: String,
    instance_id: Option<String>,
    version: Option<String>,
    started: Instant,
    default_timeout: Duration,
    default_cache: Duration,
    checks: Mutex<BTreeMap<String, Registered>>,
}

/// Registry of named checks; cheap to clone and share.
#[derive(Clone)]
pub struct HealthRegistry {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for HealthRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let checks = self.inner.checks.lock().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("HealthRegistry")
            .field("service", &self.inner.service)
            .field("checks", &checks)
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CheckOptions {
    pub timeout: Option<Duration>,
    pub cache_for: Option<Duration>,
}

impl HealthRegistry {
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                service: service.into(),
                instance_id: None,
                version: None,
                started: Instant::now(),
                default_timeout: Duration::from_secs(3),
                default_cache: Duration::from_secs(1),
                checks: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub fn builder(service: impl Into<String>) -> HealthRegistryBuilder {
        HealthRegistryBuilder {
            service: service.into(),
            instance_id: None,
            version: None,
            timeout: Duration::from_secs(3),
            cache: Duration::from_secs(1),
        }
    }

    pub fn service(&self) -> &str {
        &self.inner.service
    }

    pub fn register<F, Fut>(&self, name: impl Into<String>, kind: HealthKind, check: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CheckResult> + Send + 'static,
    {
        self.register_with(name, kind, CheckOptions::default(), check);
    }

    pub fn register_with<F, Fut>(
        &self,
        name: impl Into<String>,
        kind: HealthKind,
        options: CheckOptions,
        check: F,
    ) where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CheckResult> + Send + 'static,
    {
        let run: CheckFn = Arc::new(move || Box::pin(check()));
        self.inner.checks.lock().unwrap().insert(
            name.into(),
            Registered {
                run,
                kind,
                timeout: options.timeout,
                cache_for: options.cache_for,
                cached: None,
            },
        );
    }

    pub fn unregister(&self, name: &str) -> bool {
        self.inner.checks.lock().unwrap().remove(name).is_some()
    }

    pub async fn liveness(&self) -> HealthStatus {
        self.report(Some(HealthKind::Liveness)).await
    }

    pub async fn readiness(&self) -> HealthStatus {
        self.report(Some(HealthKind::Readiness)).await
    }

    pub async fn report(&self, filter: Option<HealthKind>) -> HealthStatus {
        let now = Instant::now();
        let mut pending = Vec::new();
        let mut checks = BTreeMap::new();
        {
            let registry = self.inner.checks.lock().unwrap();
            for (name, entry) in registry.iter() {
                if !entry.kind.selected_by(filter) {
                    continue;
                }
                let cache_for = entry.cache_for.unwrap_or(self.inner.default_cache);
                if let Some((result, at)) = &entry.cached
                    && now.duration_since(*at) < cache_for
                {
                    checks.insert(
                        name.clone(),
                        CheckReport {
                            result: result.clone(),
                            kind: entry.kind,
                        },
                    );
                    continue;
                }
                let timeout = entry.timeout.unwrap_or(self.inner.default_timeout);
                pending.push((name.clone(), entry.kind, timeout, (entry.run)()));
            }
        }

        let results = futures::future::join_all(pending.into_iter().map(
            |(name, kind, timeout, fut)| async move {
                let result = if timeout.is_zero() {
                    fut.await
                } else {
                    match tokio::time::timeout(timeout, fut).await {
                        Ok(result) => result,
                        Err(_) => CheckResult::down(format!(
                            "Check {name:?} timed out after {}ms",
                            timeout.as_millis()
                        )),
                    }
                };
                (name, kind, result)
            },
        ))
        .await;

        {
            let mut registry = self.inner.checks.lock().unwrap();
            for (name, kind, result) in results {
                if let Some(entry) = registry.get_mut(&name) {
                    entry.cached = Some((result.clone(), Instant::now()));
                }
                checks.insert(name, CheckReport { result, kind });
            }
        }

        let down = checks
            .values()
            .filter(|c| c.result.status == CheckStatus::Down)
            .count();
        let status = if down == 0 {
            OverallStatus::Ok
        } else if down == checks.len() {
            OverallStatus::Down
        } else {
            OverallStatus::Degraded
        };
        HealthStatus {
            status,
            service: self.inner.service.clone(),
            instance_id: self.inner.instance_id.clone(),
            version: self.inner.version.clone(),
            uptime_ms: self.inner.started.elapsed().as_millis() as u64,
            ts: unix_millis(),
            checks,
        }
    }
}

#[derive(Debug)]
pub struct HealthRegistryBuilder {
    service: String,
    instance_id: Option<String>,
    version: Option<String>,
    timeout: Duration,
    cache: Duration,
}

impl HealthRegistryBuilder {
    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn cache(mut self, cache: Duration) -> Self {
        self.cache = cache;
        self
    }

    pub fn build(self) -> HealthRegistry {
        HealthRegistry {
            inner: Arc::new(Inner {
                service: self.service,
                instance_id: self.instance_id,
                version: self.version,
                started: Instant::now(),
                default_timeout: self.timeout,
                default_cache: self.cache,
                checks: Mutex::new(BTreeMap::new()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn aggregates_and_filters_by_kind() {
        let reg = HealthRegistry::builder("svc")
            .version("1.0")
            .cache(Duration::ZERO)
            .build();
        reg.register("db", HealthKind::Readiness, || async { CheckResult::ok() });
        reg.register("mem", HealthKind::Liveness, || async {
            CheckResult::down("high")
        });
        let all = reg.report(None).await;
        assert_eq!(all.status, OverallStatus::Degraded);
        assert_eq!(all.checks.len(), 2);
        let live = reg.liveness().await;
        assert_eq!(live.status, OverallStatus::Down);
        assert_eq!(live.checks["mem"].kind, HealthKind::Liveness);
        let ready = reg.readiness().await;
        assert_eq!(ready.status, OverallStatus::Ok);
        let json = serde_json::to_value(&ready).unwrap();
        assert_eq!(json["checks"]["db"]["status"], "ok");
        assert_eq!(json["version"], "1.0");
    }

    #[tokio::test]
    async fn slow_checks_time_out() {
        let reg = HealthRegistry::builder("svc")
            .timeout(Duration::from_millis(10))
            .build();
        reg.register("slow", HealthKind::Both, || async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            CheckResult::ok()
        });
        let report = reg.report(None).await;
        assert_eq!(report.checks["slow"].result.status, CheckStatus::Down);
    }
}
