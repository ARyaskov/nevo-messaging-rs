//! Heartbeat-based service discovery on the shared `__nevo.discovery` topic.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::time::unix_millis;

pub const DEFAULT_MAX_ENTRIES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Announcement {
    pub service_name: String,
    #[serde(default)]
    pub instance_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default)]
    pub transport: String,
    #[serde(default, deserialize_with = "lenient_u64")]
    pub ts: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Map<String, Value>>,
}

fn lenient_u64<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let raw = f64::deserialize(deserializer)?;
    Ok(if raw.is_finite() && raw >= 0.0 {
        raw.round() as u64
    } else {
        0
    })
}

impl Announcement {
    pub fn new(
        service_name: impl Into<String>,
        instance_id: impl Into<String>,
        transport: impl Into<String>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            instance_id: instance_id.into(),
            client_id: None,
            transport: transport.into(),
            ts: unix_millis(),
            host: None,
            port: None,
            version: None,
            capabilities: None,
            meta: None,
        }
    }

    fn effective_instance_id(&self) -> String {
        if !self.instance_id.is_empty() {
            self.instance_id.clone()
        } else if let Some(client) = &self.client_id {
            client.clone()
        } else {
            self.service_name.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryEntry {
    pub announcement: Announcement,
    pub last_seen: Instant,
}

impl DiscoveryEntry {
    pub fn service_name(&self) -> &str {
        &self.announcement.service_name
    }

    pub fn instance_id(&self) -> &str {
        &self.announcement.instance_id
    }

    pub fn age(&self) -> Duration {
        self.last_seen.elapsed()
    }
}

#[derive(Debug)]
struct Inner {
    entries: HashMap<String, DiscoveryEntry>,
    by_service: HashMap<String, HashSet<String>>,
    capacity_warned: bool,
}

/// Bounded registry of live instances learned from announcements.
#[derive(Debug)]
pub struct DiscoveryRegistry {
    inner: Mutex<Inner>,
    ttl: Duration,
    max_entries: usize,
}

fn entry_key(service: &str, instance: &str) -> String {
    format!("{service}::{instance}")
}

impl DiscoveryRegistry {
    pub fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, DEFAULT_MAX_ENTRIES)
    }

    pub fn with_capacity(ttl: Duration, max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                by_service: HashMap::new(),
                capacity_warned: false,
            }),
            ttl,
            max_entries: max_entries.max(1),
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn update(&self, mut announcement: Announcement) {
        if announcement.service_name.is_empty() {
            return;
        }
        announcement.instance_id = announcement.effective_instance_id();
        let key = entry_key(&announcement.service_name, &announcement.instance_id);
        let mut inner = self.inner.lock().unwrap();
        if !inner.entries.contains_key(&key) {
            self.evict_if_full(&mut inner);
        }
        inner
            .by_service
            .entry(announcement.service_name.clone())
            .or_default()
            .insert(key.clone());
        inner.entries.insert(
            key,
            DiscoveryEntry {
                announcement,
                last_seen: Instant::now(),
            },
        );
    }

    fn drop_entry(inner: &mut Inner, key: &str) {
        if let Some(entry) = inner.entries.remove(key) {
            let service = entry.announcement.service_name;
            if let Some(keys) = inner.by_service.get_mut(&service) {
                keys.remove(key);
                if keys.is_empty() {
                    inner.by_service.remove(&service);
                }
            }
        }
    }

    fn evict_if_full(&self, inner: &mut Inner) {
        if inner.entries.len() < self.max_entries {
            return;
        }
        let stale: Vec<String> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.age() > self.ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            Self::drop_entry(inner, &key);
            if inner.entries.len() < self.max_entries {
                return;
            }
        }
        while inner.entries.len() >= self.max_entries {
            let Some(oldest) = inner
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_seen)
                .map(|(k, _)| k.clone())
            else {
                return;
            };
            Self::drop_entry(inner, &oldest);
        }
        if !inner.capacity_warned {
            inner.capacity_warned = true;
            tracing::warn!(
                event = "discovery.capacity_reached",
                max_entries = self.max_entries,
                "DiscoveryRegistry is at capacity; live instances are being evicted"
            );
        }
    }

    pub fn prune(&self) {
        let mut inner = self.inner.lock().unwrap();
        let expired: Vec<String> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.age() > self.ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            Self::drop_entry(&mut inner, &key);
        }
    }

    pub fn list(&self) -> Vec<DiscoveryEntry> {
        self.inner
            .lock()
            .unwrap()
            .entries
            .values()
            .cloned()
            .collect()
    }

    pub fn list_service(&self, service: &str) -> Vec<DiscoveryEntry> {
        let inner = self.inner.lock().unwrap();
        inner
            .by_service
            .get(service)
            .map(|keys| {
                keys.iter()
                    .filter_map(|k| inner.entries.get(k).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn is_available(&self, service: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.by_service.get(service).is_some_and(|keys| {
            keys.iter()
                .filter_map(|k| inner.entries.get(k))
                .any(|e| e.age() <= self.ttl)
        })
    }

    pub fn instance_ids(&self, service: &str) -> Vec<String> {
        self.list_service(service)
            .into_iter()
            .map(|e| e.announcement.instance_id)
            .collect()
    }

    pub fn remove(&self, service: &str, instance_id: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let key = entry_key(service, instance_id);
        let present = inner.entries.contains_key(&key);
        if present {
            Self::drop_entry(&mut inner, &key);
        }
        present
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Opt-in heartbeat publishing and registry maintenance for a client.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DiscoveryConfig {
    pub heartbeat: Duration,
    pub ttl: Duration,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub version: Option<String>,
    pub capabilities: Vec<String>,
    pub max_entries: usize,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_secs(10),
            ttl: Duration::from_secs(30),
            host: None,
            port: None,
            version: None,
            capabilities: Vec::new(),
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }
}

impl DiscoveryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn heartbeat(mut self, every: Duration) -> Self {
        self.heartbeat = every;
        self
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn capabilities(mut self, caps: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.capabilities = caps.into_iter().map(Into::into).collect();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_tracks_instances_and_prunes() {
        let reg = DiscoveryRegistry::new(Duration::from_millis(20));
        reg.update(Announcement::new("user", "i1", "nats"));
        reg.update(Announcement::new("user", "i2", "nats"));
        reg.update(Announcement::new("", "x", "nats"));
        assert_eq!(reg.len(), 2);
        assert!(reg.is_available("user"));
        assert_eq!(reg.instance_ids("user").len(), 2);
        std::thread::sleep(Duration::from_millis(30));
        assert!(!reg.is_available("user"));
        reg.prune();
        assert!(reg.is_empty());
    }

    #[test]
    fn capacity_is_bounded() {
        let reg = DiscoveryRegistry::with_capacity(Duration::from_secs(60), 2);
        for i in 0..5 {
            reg.update(Announcement::new("svc", format!("i{i}"), "nats"));
        }
        assert_eq!(reg.len(), 2);
        assert!(reg.remove("svc", "i4"));
        assert!(!reg.remove("svc", "i0"));
    }

    #[test]
    fn announcement_wire_shape() {
        let json = serde_json::to_value(Announcement::new("user", "i1", "nats")).unwrap();
        assert_eq!(json["serviceName"], "user");
        assert_eq!(json["instanceId"], "i1");
        assert!(json["ts"].as_u64().unwrap() > 0);
        let back: Announcement =
            serde_json::from_str(r#"{"serviceName":"a","clientId":"c","ts":1.5e12}"#).unwrap();
        assert_eq!(back.ts, 1_500_000_000_000);
        let reg = DiscoveryRegistry::new(Duration::from_secs(1));
        reg.update(back);
        assert_eq!(reg.instance_ids("a"), vec!["c".to_owned()]);
    }
}
