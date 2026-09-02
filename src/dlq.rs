//! Dead-letter routing for envelopes that could not be processed.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::redact::redact_meta;
use crate::time::unix_millis;
use crate::wire::Meta;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DlqError {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl From<&Error> for DlqError {
    fn from(err: &Error) -> Self {
        Self {
            code: Some(err.code().as_u16()),
            message: Some(err.message().to_owned()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DlqEntry {
    pub id: String,
    pub topic: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DlqError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
    #[serde(skip)]
    pub raw_payload: Option<Bytes>,
    pub ts: u64,
    #[serde(default)]
    pub attempts: u32,
}

impl DlqEntry {
    pub fn new(topic: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            topic: topic.into(),
            reason: reason.into(),
            error: None,
            meta: None,
            raw_payload: None,
            ts: unix_millis(),
            attempts: 0,
        }
    }

    pub fn error(mut self, err: &Error) -> Self {
        self.error = Some(DlqError::from(err));
        self
    }

    pub fn meta(mut self, meta: Option<Meta>) -> Self {
        self.meta = meta;
        self
    }

    pub fn raw_payload(mut self, payload: Bytes) -> Self {
        self.raw_payload = Some(payload);
        self
    }
}

pub trait DlqSink: Send + Sync + 'static {
    fn route<'a>(&'a self, entry: &'a DlqEntry) -> BoxFuture<'a, Result<()>>;
}

/// Fans dead letters out to sinks, redacting secrets from metadata first.
#[derive(Clone)]
pub struct DlqRouter {
    enabled: bool,
    redact: bool,
    redact_keys: Vec<String>,
    sinks: Vec<Arc<dyn DlqSink>>,
}

impl std::fmt::Debug for DlqRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DlqRouter")
            .field("enabled", &self.enabled)
            .field("redact", &self.redact)
            .field("sinks", &self.sinks.len())
            .finish()
    }
}

impl Default for DlqRouter {
    fn default() -> Self {
        Self {
            enabled: true,
            redact: true,
            redact_keys: Vec::new(),
            sinks: Vec::new(),
        }
    }
}

impl DlqRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    pub fn sink(mut self, sink: impl DlqSink) -> Self {
        self.sinks.push(Arc::new(sink));
        self
    }

    pub fn shared_sink(mut self, sink: Arc<dyn DlqSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    pub fn redact(mut self, redact: bool) -> Self {
        self.redact = redact;
        self
    }

    pub fn redact_keys(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.redact_keys = keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.sinks.is_empty()
    }

    fn sanitise(&self, mut entry: DlqEntry) -> DlqEntry {
        if self.redact
            && let Some(meta) = &mut entry.meta
        {
            redact_meta(meta, &self.redact_keys);
        }
        entry
    }

    pub async fn route(&self, entry: DlqEntry) {
        if !self.is_enabled() {
            return;
        }
        let entry = self.sanitise(entry);
        for sink in &self.sinks {
            if let Err(err) = sink.route(&entry).await {
                tracing::error!(event = "dlq.sink_failed", error = %err, "dead-letter sink failed");
            }
        }
    }
}

/// A bounded in-memory dead-letter store, handy for tests and small services.
#[derive(Debug)]
pub struct InMemoryDlqStore {
    entries: Mutex<VecDeque<DlqEntry>>,
    max: usize,
}

impl InMemoryDlqStore {
    pub fn new(max: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            max: max.max(1),
        }
    }

    pub fn shared(max: usize) -> Arc<Self> {
        Arc::new(Self::new(max))
    }

    pub fn list(&self, limit: usize) -> Vec<DlqEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn oldest(&self, limit: usize) -> Vec<DlqEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn remove(&self, id: &str) -> bool {
        let mut entries = self.entries.lock().unwrap();
        let before = entries.len();
        entries.retain(|e| e.id != id);
        entries.len() != before
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }
}

impl DlqSink for InMemoryDlqStore {
    fn route<'a>(&'a self, entry: &'a DlqEntry) -> BoxFuture<'a, Result<()>> {
        let mut entries = self.entries.lock().unwrap();
        entries.push_back(entry.clone());
        while entries.len() > self.max {
            entries.pop_front();
        }
        Box::pin(async { Ok(()) })
    }
}

impl DlqSink for Arc<InMemoryDlqStore> {
    fn route<'a>(&'a self, entry: &'a DlqEntry) -> BoxFuture<'a, Result<()>> {
        InMemoryDlqStore::route(self, entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Auth;

    #[tokio::test]
    async fn routes_with_redaction_and_bounded_store() {
        let store = InMemoryDlqStore::shared(2);
        let router = DlqRouter::new().sink(Arc::clone(&store));
        for i in 0..3 {
            let mut meta = Meta::new();
            meta.auth = Some(Auth::token("secret"));
            meta.service = Some(format!("s{i}"));
            router
                .route(DlqEntry::new("topic", "handler-error").meta(Some(meta)))
                .await;
        }
        assert_eq!(store.len(), 2);
        let latest = &store.list(1)[0];
        assert_eq!(latest.meta.as_ref().unwrap().service.as_deref(), Some("s2"));
        assert_eq!(
            latest.meta.as_ref().unwrap().auth_token(),
            Some(crate::redact::REDACTED)
        );
        assert!(!DlqRouter::new().is_enabled());
    }
}
