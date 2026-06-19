//! Drive a set of bulk sends, merging their outcomes.
//!
//! Foundation stub: sends serially. Lane 5 runs up to `max_in_flight` sends
//! concurrently (treating 0 or 1 as serial) while preserving error propagation.

use super::types::BulkOutcome;
use crate::error::Result;
use std::future::Future;

/// Run `send` over every chunk and merge the per-chunk outcomes.
pub(crate) async fn run<F, Fut>(
    chunks: Vec<super::types::BulkChunk>,
    _max_in_flight: usize,
    send: F,
) -> Result<BulkOutcome>
where
    F: Fn(super::types::BulkChunk) -> Fut,
    Fut: Future<Output = Result<BulkOutcome>>,
{
    let mut total = BulkOutcome::default();
    for chunk in chunks {
        total.merge(send(chunk).await?);
    }
    Ok(total)
}
