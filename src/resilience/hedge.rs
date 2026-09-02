use std::future::Future;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;

use crate::error::Result;

/// Fires additional copies of a call after a delay; the first success wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct HedgePolicy {
    pub copies: u32,
    pub delay: Duration,
}

impl Default for HedgePolicy {
    fn default() -> Self {
        Self {
            copies: 2,
            delay: Duration::from_millis(50),
        }
    }
}

impl HedgePolicy {
    pub fn new(copies: u32, delay: Duration) -> Self {
        Self {
            copies: copies.max(1),
            delay,
        }
    }
}

pub async fn hedge<T, F, Fut>(policy: HedgePolicy, attempt: F) -> Result<T>
where
    F: Fn(u32) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let copies = policy.copies.max(1);
    if copies == 1 {
        return attempt(1).await;
    }
    let mut pending = FuturesUnordered::new();
    pending.push(attempt(1));
    let mut launched = 1;
    let mut failures = 0;
    let next_launch = tokio::time::sleep(policy.delay);
    tokio::pin!(next_launch);

    loop {
        tokio::select! {
            biased;
            outcome = pending.next(), if !pending.is_empty() => match outcome {
                Some(Ok(value)) => return Ok(value),
                Some(Err(err)) => {
                    failures += 1;
                    if failures >= copies {
                        return Err(err);
                    }
                }
                None => {}
            },
            _ = &mut next_launch, if launched < copies => {
                launched += 1;
                pending.push(attempt(launched));
                next_launch.as_mut().reset(tokio::time::Instant::now() + policy.delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, ErrorCode};

    #[tokio::test]
    async fn second_copy_wins_when_first_is_slow() {
        let policy = HedgePolicy::new(2, Duration::from_millis(5));
        let out = hedge(policy, |n| async move {
            if n == 1 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(n)
        })
        .await
        .unwrap();
        assert_eq!(out, 2);
    }

    #[tokio::test]
    async fn all_failures_surface_last_error() {
        let policy = HedgePolicy::new(3, Duration::from_millis(1));
        let err = hedge(policy, |n| async move {
            Err::<(), _>(Error::new(ErrorCode::Internal, format!("copy {n}")))
        })
        .await
        .unwrap_err();
        assert!(err.message().starts_with("copy"));
    }
}
