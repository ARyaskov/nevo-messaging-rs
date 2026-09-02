//! Caller-based access control with optional token verification.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::lru::TtlLru;
use crate::wire::Meta;

pub type Claims = Map<String, Value>;

/// Verifies a bearer token and returns its claims, or `None` when it is invalid.
pub trait TokenVerifier: Send + Sync + 'static {
    fn verify<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Result<Option<Claims>>>;
}

impl<F, Fut> TokenVerifier for F
where
    F: Fn(&str) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<Claims>>> + Send + 'static,
{
    fn verify<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Result<Option<Claims>>> {
        Box::pin(self(token))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct AccessRule {
    pub topic: Option<String>,
    pub method: Option<String>,
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

impl AccessRule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn allow(mut self, callers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allow = callers.into_iter().map(Into::into).collect();
        self
    }

    pub fn deny(mut self, callers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.deny = callers.into_iter().map(Into::into).collect();
        self
    }

    fn topic_filter(&self) -> Option<&str> {
        self.topic.as_deref().filter(|t| *t != "*")
    }

    fn method_filter(&self) -> Option<&str> {
        self.method.as_deref().filter(|m| *m != "*")
    }
}

fn list_has(list: &[String], caller: Option<&str>) -> bool {
    if list.is_empty() {
        return false;
    }
    if list.iter().any(|v| v == "*") {
        return true;
    }
    caller.is_some_and(|c| list.iter().any(|v| v == c))
}

const JWT_CACHE_MAX: usize = 1024;
const JWT_CACHE_TTL: Duration = Duration::from_secs(60);

struct VerifiedToken {
    claims: Claims,
    expires_at: Instant,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct AccessControl {
    pub rules: Vec<AccessRule>,
    pub allow_all_by_default: Option<bool>,
    pub log_denied: bool,
    verifier: Option<Arc<dyn TokenVerifier>>,
    token_cache: Arc<Mutex<TtlLru<VerifiedToken>>>,
    warned_token_without_verifier: Arc<AtomicBool>,
}

impl fmt::Debug for AccessControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessControl")
            .field("rules", &self.rules)
            .field("allow_all_by_default", &self.allow_all_by_default)
            .field("log_denied", &self.log_denied)
            .field("verifier", &self.verifier.is_some())
            .finish()
    }
}

impl Default for AccessControl {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            allow_all_by_default: None,
            log_denied: true,
            verifier: None,
            token_cache: Arc::new(Mutex::new(TtlLru::new(JWT_CACHE_MAX, JWT_CACHE_TTL))),
            warned_token_without_verifier: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl AccessControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rule(mut self, rule: AccessRule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn rules(mut self, rules: impl IntoIterator<Item = AccessRule>) -> Self {
        self.rules.extend(rules);
        self
    }

    /// Without this, a config that has rules denies anything they don't match.
    pub fn allow_all_by_default(mut self, allow: bool) -> Self {
        self.allow_all_by_default = Some(allow);
        self
    }

    pub fn log_denied(mut self, log: bool) -> Self {
        self.log_denied = log;
        self
    }

    /// Identity then comes only from the verified token, never from `meta.service`.
    pub fn verifier(mut self, verifier: impl TokenVerifier) -> Self {
        self.verifier = Some(Arc::new(verifier));
        self
    }

    pub fn has_verifier(&self) -> bool {
        self.verifier.is_some()
    }

    fn effective_default(&self) -> bool {
        self.allow_all_by_default.unwrap_or(self.rules.is_empty())
    }

    /// Deny is authoritative; built-in `nevo.*` methods stay reachable under the deny-default.
    pub fn is_allowed(&self, topic: &str, method: &str, caller: Option<&str>) -> bool {
        let mut candidates: Vec<&AccessRule> = Vec::new();
        for pass in 0..4 {
            for rule in &self.rules {
                let topic_match = rule.topic_filter().is_some_and(|t| t == topic);
                let method_match = rule.method_filter().is_some_and(|m| m == method);
                let selected = match pass {
                    0 => {
                        rule.topic_filter().is_some()
                            && rule.method_filter().is_some()
                            && topic_match
                            && method_match
                    }
                    1 => {
                        rule.topic_filter().is_some()
                            && rule.method_filter().is_none()
                            && topic_match
                    }
                    2 => {
                        rule.topic_filter().is_none()
                            && rule.method_filter().is_some()
                            && method_match
                    }
                    _ => rule.topic_filter().is_none() && rule.method_filter().is_none(),
                };
                if selected {
                    candidates.push(rule);
                }
            }
        }
        if candidates.is_empty() {
            return self.effective_default() || method.starts_with("nevo.");
        }
        if candidates.iter().any(|r| list_has(&r.deny, caller)) {
            return false;
        }
        for rule in candidates {
            if rule.allow.is_empty() {
                return true;
            }
            if list_has(&rule.allow, caller) {
                return true;
            }
        }
        false
    }

    /// Resolves the caller's identity from the envelope metadata.
    pub async fn extract_caller(&self, meta: Option<&Meta>) -> Result<Option<String>> {
        let Some(verifier) = &self.verifier else {
            if meta.and_then(Meta::auth_token).is_some()
                && !self
                    .warned_token_without_verifier
                    .swap(true, Ordering::Relaxed)
            {
                tracing::warn!(
                    event = "acl.token_without_verifier",
                    "meta.auth.token is present but no verifier is configured; the token is ignored for caller identity"
                );
            }
            return Ok(meta.and_then(|m| m.service.clone()));
        };
        let Some(token) = meta.and_then(Meta::auth_token) else {
            return Ok(None);
        };
        let Some(claims) = self.verify_cached(verifier.as_ref(), token).await? else {
            return Ok(None);
        };
        let identity = ["service", "serviceName", "svc", "sub"]
            .iter()
            .find_map(|k| claims.get(*k).and_then(Value::as_str))
            .map(str::to_owned);
        if let (Some(identity), Some(claimed)) =
            (&identity, meta.and_then(|m| m.service.as_deref()))
            && identity != claimed
        {
            return Err(Error::unauthorized(
                "meta.service does not match the verified token identity",
            ));
        }
        Ok(identity)
    }

    async fn verify_cached(
        &self,
        verifier: &dyn TokenVerifier,
        token: &str,
    ) -> Result<Option<Claims>> {
        let digest = Sha256::digest(token.as_bytes());
        let key: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        let now = Instant::now();
        if let Some(cached) = self.token_cache.lock().unwrap().get(&key)
            && now < cached.expires_at
        {
            return Ok(Some(cached.claims.clone()));
        }
        let Some(claims) = verifier.verify(token).await? else {
            return Ok(None);
        };
        let mut expires_at = now + JWT_CACHE_TTL;
        if let Some(exp) = claims.get("exp").and_then(Value::as_f64) {
            let exp_ms = exp * 1000.0;
            let now_ms = crate::time::unix_millis() as f64;
            if exp_ms < now_ms + JWT_CACHE_TTL.as_millis() as f64 {
                let remaining = (exp_ms - now_ms).max(0.0);
                expires_at = now + Duration::from_millis(remaining as u64);
            }
        }
        if expires_at > now {
            self.token_cache.lock().unwrap().insert(
                key,
                VerifiedToken {
                    claims: claims.clone(),
                    expires_at,
                },
            );
        }
        Ok(Some(claims))
    }

    pub fn log_denial(&self, topic: &str, method: &str, service: &str, caller: Option<&str>) {
        if self.log_denied {
            tracing::warn!(
                event = "acl.denied",
                topic,
                method,
                service,
                caller = caller.unwrap_or(""),
                "access denied"
            );
        }
    }

    pub fn denied_error(method: &str, service: &str, caller: Option<&str>) -> Error {
        let mut err = Error::unauthorized("Access denied")
            .with_service(service)
            .with_detail("method", method)
            .with_detail("serviceName", service);
        if let Some(caller) = caller {
            err = err.with_detail("callerService", caller);
        }
        err
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn deny_default_once_rules_exist() {
        let acl = AccessControl::new()
            .rule(
                AccessRule::new()
                    .topic("user-events")
                    .method("*")
                    .allow(["frontend", "coordinator"]),
            )
            .rule(
                AccessRule::new()
                    .topic("user-events")
                    .method("user.delete")
                    .deny(["frontend"]),
            );
        assert!(acl.is_allowed("user-events", "user.get", Some("frontend")));
        assert!(!acl.is_allowed("user-events", "user.delete", Some("frontend")));
        assert!(acl.is_allowed("user-events", "user.delete", Some("coordinator")));
        assert!(!acl.is_allowed("other-events", "x", Some("frontend")));
        assert!(acl.is_allowed("other-events", "nevo.contract", None));
        assert!(AccessControl::new().is_allowed("t", "m", None));
        assert!(!acl.is_allowed("user-events", "user.get", None));
    }

    #[test]
    fn deny_beats_allow() {
        let acl = AccessControl::new()
            .rule(AccessRule::new().allow(["*"]).deny(["evil"]))
            .allow_all_by_default(true);
        assert!(acl.is_allowed("t", "m", Some("good")));
        assert!(!acl.is_allowed("t", "m", Some("evil")));
    }

    #[tokio::test]
    async fn caller_identity_from_meta_or_token() {
        let mut meta = Meta::new();
        meta.service = Some("frontend".into());
        let plain = AccessControl::new();
        assert_eq!(
            plain.extract_caller(Some(&meta)).await.unwrap().as_deref(),
            Some("frontend")
        );

        let verified = AccessControl::new().verifier(|token: &str| {
            let ok = token == "good";
            async move {
                Ok(ok.then(|| {
                    let mut claims = Claims::new();
                    claims.insert("service".into(), Value::String("frontend".into()));
                    claims
                }))
            }
        });
        assert_eq!(verified.extract_caller(Some(&meta)).await.unwrap(), None);
        meta.auth = Some(crate::wire::Auth::token("good"));
        assert_eq!(
            verified
                .extract_caller(Some(&meta))
                .await
                .unwrap()
                .as_deref(),
            Some("frontend")
        );
        meta.service = Some("spoof".into());
        assert_eq!(
            verified
                .extract_caller(Some(&meta))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Unauthorized
        );
        meta.auth = Some(crate::wire::Auth::token("bad"));
        meta.service = None;
        assert_eq!(verified.extract_caller(Some(&meta)).await.unwrap(), None);
    }
}
