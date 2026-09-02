//! In-flight tracking used to drain work before a client or server closes.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

#[derive(Debug, Default)]
struct Inner {
    count: AtomicUsize,
    shutting_down: AtomicBool,
    notify: Notify,
}

#[derive(Debug, Clone, Default)]
pub struct Inflight {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub struct InflightGuard {
    inner: Arc<Inner>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.inner.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.notify.notify_waiters();
        }
    }
}

impl Inflight {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enter(&self) -> InflightGuard {
        self.inner.count.fetch_add(1, Ordering::AcqRel);
        InflightGuard {
            inner: Arc::clone(&self.inner),
        }
    }

    pub async fn track<F: Future>(&self, future: F) -> F::Output {
        let _guard = self.enter();
        future.await
    }

    pub fn count(&self) -> usize {
        self.inner.count.load(Ordering::Acquire)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::Acquire)
    }

    pub fn begin_shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::Release);
    }

    /// Waits until nothing is in flight; returns `false` when `timeout` elapsed first.
    pub async fn drain(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.count() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.count() == 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_waits_for_tracked_work() {
        let inflight = Inflight::new();
        let tracked = inflight.clone();
        let handle = tokio::spawn(async move {
            tracked
                .track(tokio::time::sleep(Duration::from_millis(30)))
                .await;
        });
        tokio::task::yield_now().await;
        assert!(inflight.drain(Duration::from_millis(5)).await == (inflight.count() == 0));
        assert!(inflight.drain(Duration::from_secs(2)).await);
        handle.await.unwrap();
        assert_eq!(inflight.count(), 0);
    }
}
