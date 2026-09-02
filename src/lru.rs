use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Slot<V> {
    value: V,
    expires_at: Instant,
    touched: u64,
}

/// A bounded least-recently-used map whose entries also expire after a TTL.
#[derive(Debug)]
pub struct TtlLru<V> {
    map: HashMap<String, Slot<V>>,
    order: BTreeMap<u64, String>,
    seq: u64,
    max_entries: usize,
    ttl: Duration,
}

impl<V> TtlLru<V> {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            map: HashMap::new(),
            order: BTreeMap::new(),
            seq: 0,
            max_entries: max_entries.max(1),
            ttl,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn touch(&mut self, key: &str) -> Option<&V> {
        let seq = self.next_seq();
        let slot = self.map.get_mut(key)?;
        self.order.remove(&slot.touched);
        slot.touched = seq;
        self.order.insert(seq, key.to_owned());
        Some(&slot.value)
    }

    fn evict_expired(&mut self, key: &str, now: Instant) -> bool {
        let expired = self.map.get(key).is_some_and(|slot| now > slot.expires_at);
        if expired {
            self.remove(key);
        }
        expired
    }

    pub fn get(&mut self, key: &str) -> Option<&V> {
        if !self.map.contains_key(key) || self.evict_expired(key, Instant::now()) {
            return None;
        }
        self.touch(key)
    }

    pub fn contains(&mut self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn insert(&mut self, key: impl Into<String>, value: V) {
        let key = key.into();
        let now = Instant::now();
        let seq = self.next_seq();
        if let Some(slot) = self.map.get_mut(&key) {
            self.order.remove(&slot.touched);
            slot.value = value;
            slot.expires_at = now + self.ttl;
            slot.touched = seq;
            self.order.insert(seq, key);
            return;
        }
        self.map.insert(
            key.clone(),
            Slot {
                value,
                expires_at: now + self.ttl,
                touched: seq,
            },
        );
        self.order.insert(seq, key);
        while self.map.len() > self.max_entries {
            let Some((&oldest, _)) = self.order.iter().next() else {
                break;
            };
            if let Some(key) = self.order.remove(&oldest) {
                self.map.remove(&key);
            }
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<V> {
        let slot = self.map.remove(key)?;
        self.order.remove(&slot.touched);
        Some(slot.value)
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used() {
        let mut lru = TtlLru::new(2, Duration::from_secs(60));
        lru.insert("a", 1);
        lru.insert("b", 2);
        assert_eq!(lru.get("a"), Some(&1));
        lru.insert("c", 3);
        assert!(lru.get("b").is_none());
        assert_eq!(lru.get("a"), Some(&1));
        assert_eq!(lru.get("c"), Some(&3));
        assert_eq!(lru.len(), 2);
    }

    #[test]
    fn expires_entries() {
        let mut lru = TtlLru::new(10, Duration::from_millis(0));
        lru.insert("a", 1);
        std::thread::sleep(Duration::from_millis(2));
        assert!(!lru.contains("a"));
        assert_eq!(lru.len(), 0);
    }
}
