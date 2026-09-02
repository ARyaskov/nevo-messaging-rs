use std::future::Future;
use std::time::Duration;

use crate::error::{Error, ErrorCode, Result};

/// Exponential backoff with full jitter; transient codes retry by default.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RetryPolicy {
    pub enabled: bool,
    pub max_attempts: u32,
    pub base: Duration,
    pub max_delay: Duration,
    pub jitter: bool,
    pub retry_on: Vec<ErrorCode>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
            base: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            jitter: true,
            retry_on: Vec::new(),
        }
    }
}

impl RetryPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    pub fn base(mut self, base: Duration) -> Self {
        self.base = base;
        self
    }

    pub fn max_delay(mut self, max: Duration) -> Self {
        self.max_delay = max;
        self
    }

    pub fn jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    pub fn retry_on(mut self, codes: impl IntoIterator<Item = ErrorCode>) -> Self {
        self.retry_on = codes.into_iter().collect();
        self
    }

    pub fn is_active(&self) -> bool {
        self.enabled && self.max_attempts > 1
    }

    pub fn should_retry(&self, err: &Error) -> bool {
        if !self.enabled {
            return false;
        }
        if self.retry_on.contains(&err.code()) {
            return true;
        }
        err.is_retryable()
    }

    /// Delay before attempt `attempt + 1`; a server `retryAfterMs` hint wins over backoff.
    pub fn delay_for(&self, attempt: u32, err: &Error) -> Duration {
        if let Some(hint) = err.retry_after() {
            return hint;
        }
        let exp = self
            .base
            .checked_mul(1u32 << attempt.saturating_sub(1).min(20))
            .unwrap_or(self.max_delay)
            .min(self.max_delay);
        if !self.jitter || exp.is_zero() {
            return exp;
        }
        Duration::from_nanos(rand::random_range(0..exp.as_nanos().max(1) as u64))
    }
}

/// Runs `attempt(n)` for `n = 1..=max_attempts`, sleeping between failures that the policy retries.
pub async fn with_retry<T, F, Fut>(policy: &RetryPolicy, mut attempt: F) -> Result<T>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    if !policy.is_active() {
        return attempt(1).await;
    }
    let mut n = 1;
    loop {
        match attempt(n).await {
            Ok(value) => return Ok(value),
            Err(err) if n < policy.max_attempts && policy.should_retry(&err) => {
                let delay = policy.delay_for(n, &err);
                tracing::debug!(
                    event = "client.retry",
                    attempt = n,
                    delay_ms = delay.as_millis() as u64,
                    code = ?err.code(),
                    "retrying after transient error"
                );
                tokio::time::sleep(delay).await;
                n += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn retries_transient_then_succeeds() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy::new()
            .base(Duration::from_millis(1))
            .jitter(false);
        let out = with_retry(&policy, |n| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 3 {
                    Err(Error::from_code(ErrorCode::Timeout))
                } else {
                    Ok(n)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(out, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_non_transient() {
        let policy = RetryPolicy::new().base(Duration::from_millis(1));
        let err = with_retry(&policy, |_| async {
            Err::<(), _>(Error::from_code(ErrorCode::ValidationFailed))
        })
        .await
        .unwrap_err();
        assert_eq!(err.code(), ErrorCode::ValidationFailed);
        assert!(policy.should_retry(&Error::from_code(ErrorCode::ConnectionLost)));
        assert!(!policy.should_retry(&Error::from_code(ErrorCode::Timeout).with_retryable(false)));
        assert!(
            RetryPolicy::new()
                .retry_on([ErrorCode::Internal])
                .should_retry(&Error::from_code(ErrorCode::Internal))
        );
    }

    #[test]
    fn delay_honours_hint_and_cap() {
        let policy = RetryPolicy::new()
            .jitter(false)
            .max_delay(Duration::from_millis(300));
        let plain = Error::from_code(ErrorCode::Timeout);
        assert_eq!(policy.delay_for(1, &plain), Duration::from_millis(100));
        assert_eq!(policy.delay_for(2, &plain), Duration::from_millis(200));
        assert_eq!(policy.delay_for(5, &plain), Duration::from_millis(300));
        let hinted = plain.with_detail("retryAfterMs", 42);
        assert_eq!(policy.delay_for(1, &hinted), Duration::from_millis(42));
    }
}
