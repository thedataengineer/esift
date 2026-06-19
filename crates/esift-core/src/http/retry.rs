//! Generic retry-with-backoff helper.
//!
//! Convention: an operation is retried only when it returns
//! [`EsiftError::Transient`](crate::error::EsiftError::Transient). Any other
//! error, or success, returns immediately.
//!
//! Backoff is capped exponential: retry attempt `n` (1-based) waits
//! `min(base_backoff_ms * 2^(n-1), max_backoff_ms)` milliseconds before the
//! next call.

use crate::error::{EsiftError, Result};
use serde::{Deserialize, Serialize};
use std::future::Future;

/// Retry policy. Defaults to no retries so behavior is opt-in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum retries after the first attempt. 0 disables retries.
    #[serde(default)]
    pub max_retries: u32,
    /// Initial backoff before the first retry.
    #[serde(default = "default_base_backoff_ms")]
    pub base_backoff_ms: u64,
    /// Ceiling for exponential backoff.
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            base_backoff_ms: default_base_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
        }
    }
}

fn default_base_backoff_ms() -> u64 {
    200
}

fn default_max_backoff_ms() -> u64 {
    5_000
}

/// Run `op`, retrying transient failures per `policy`.
///
/// On `Ok`, returns immediately. On `Err(EsiftError::Transient(_))`, retries
/// up to `policy.max_retries` times, sleeping a capped exponential backoff
/// between attempts. Any other error returns immediately.
pub async fn run<F, Fut, T>(policy: &RetryPolicy, op: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut retries: u32 = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if matches!(e, EsiftError::Transient(_)) && retries < policy.max_retries {
                    retries += 1;
                    let shift = retries - 1;
                    let backoff_ms = policy
                        .base_backoff_ms
                        .checked_shl(shift)
                        .unwrap_or(u64::MAX)
                        .min(policy.max_backoff_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::EsiftError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retries_transient_until_success() {
        let policy = RetryPolicy {
            max_retries: 5,
            base_backoff_ms: 1,
            max_backoff_ms: 5,
        };
        let calls = AtomicUsize::new(0);

        let result = run(&policy, || async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(EsiftError::Transient("temporary".into()))
            } else {
                Ok(())
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn non_transient_is_not_retried() {
        let policy = RetryPolicy {
            max_retries: 5,
            base_backoff_ms: 1,
            max_backoff_ms: 5,
        };
        let calls = AtomicUsize::new(0);

        let result = run(&policy, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(EsiftError::Destination("permanent".into()))
        })
        .await;

        assert!(matches!(result, Err(EsiftError::Destination(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
