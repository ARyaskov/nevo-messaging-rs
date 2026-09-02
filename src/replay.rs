//! Rejects envelopes whose uuid was already seen inside a time window.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::error::{Error, ErrorCode, Result};
use crate::lru::TtlLru;
use crate::time::unix_millis;

#[derive(Debug)]
pub struct ReplayGuard {
    window: Duration,
    max_entries: usize,
    seen: Option<Mutex<TtlLru<()>>>,
    capacity_warned: AtomicBool,
}

impl ReplayGuard {
    pub fn new(window: Duration) -> Self {
        Self::with_capacity(window, 50_000)
    }

    pub fn with_capacity(window: Duration, max_entries: usize) -> Self {
        Self {
            window,
            max_entries,
            seen: (!window.is_zero()).then(|| Mutex::new(TtlLru::new(max_entries, window))),
            capacity_warned: AtomicBool::new(false),
        }
    }

    pub fn disabled() -> Self {
        Self::new(Duration::ZERO)
    }

    pub fn is_enabled(&self) -> bool {
        self.seen.is_some()
    }

    /// Fails closed: a message without `uuid` or `ts` cannot be checked and is rejected.
    pub fn check(&self, uuid: &str, ts: Option<u64>) -> Result<()> {
        let Some(seen) = &self.seen else {
            return Ok(());
        };
        if uuid.is_empty() {
            return Err(Error::new(
                ErrorCode::ReplayDetected,
                "Message missing uuid; replay protection requires a uuid",
            ));
        }
        let Some(ts) = ts else {
            return Err(Error::new(
                ErrorCode::ReplayDetected,
                "Message missing ts; replay protection requires a timestamp",
            ));
        };
        let now = unix_millis();
        let window_ms = self.window.as_millis() as u64;
        if now.abs_diff(ts) > window_ms {
            return Err(Error::new(
                ErrorCode::ReplayDetected,
                format!("Message timestamp outside replay window ({window_ms}ms)"),
            )
            .with_detail("ts", ts));
        }
        let mut seen = seen.lock().unwrap();
        if seen.contains(uuid) {
            return Err(Error::new(
                ErrorCode::ReplayDetected,
                "Duplicate message uuid within replay window",
            )
            .with_detail("uuid", uuid));
        }
        seen.insert(uuid, ());
        if seen.len() >= self.max_entries && !self.capacity_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                event = "replay.capacity_reached",
                max_entries = self.max_entries,
                "ReplayGuard cache is full; effective replay window is shorter than configured"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_duplicates_and_stale_timestamps() {
        let guard = ReplayGuard::new(Duration::from_secs(60));
        let now = unix_millis();
        guard.check("u1", Some(now)).unwrap();
        assert_eq!(
            guard.check("u1", Some(now)).unwrap_err().code(),
            ErrorCode::ReplayDetected
        );
        assert!(guard.check("u2", Some(now - 120_000)).is_err());
        assert!(guard.check("", Some(now)).is_err());
        assert!(guard.check("u3", None).is_err());
        assert!(ReplayGuard::disabled().check("", None).is_ok());
    }
}
