//! Generic retry-with-backoff helper.
//!
//! Convention: an operation is retried only when it returns
//! [`EsiftError::Transient`](crate::error::EsiftError::Transient). Any other
//! error, or success, returns immediately.
//!
//! Foundation stub: runs the operation exactly once, so the sink behaves
//! exactly as it did before the refactor. Lane 2 replaces [`run`] with a real
//! capped-exponential-backoff loop.

use crate::error::Result;
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
/// Foundation stub: a single attempt. Lane 2 implements the backoff loop that
/// retries while the error is `EsiftError::Transient` and attempts remain.
pub async fn run<F, Fut, T>(_policy: &RetryPolicy, op: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    op().await
}
