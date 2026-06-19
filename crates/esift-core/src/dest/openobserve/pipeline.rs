//! Drive a set of bulk sends, merging their outcomes.
//!
//! When `max_in_flight >= 2` this runs up to that many `send` futures
//! concurrently (treating 0 or 1 as serial), bounding the in-flight window
//! with a semaphore and merging every per-chunk outcome. The first send that
//! returns `Err` aborts the remaining work and is propagated to the caller.

use super::types::BulkOutcome;
use crate::error::Result;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Run `send` over every chunk and merge the per-chunk outcomes.
pub(crate) async fn run<F, Fut>(
    chunks: Vec<super::types::BulkChunk>,
    max_in_flight: usize,
    send: F,
) -> Result<BulkOutcome>
where
    F: Fn(super::types::BulkChunk) -> Fut,
    Fut: Future<Output = Result<BulkOutcome>> + Send + 'static,
{
    // Treat 0 or 1 as serial: no scheduling overhead, deterministic order.
    if max_in_flight <= 1 {
        let mut total = BulkOutcome::default();
        for chunk in chunks {
            total.merge(send(chunk).await?);
        }
        return Ok(total);
    }

    // Bound concurrency to `max_in_flight` outstanding sends at once.
    let permits = Arc::new(Semaphore::new(max_in_flight));
    let mut tasks: JoinSet<Result<BulkOutcome>> = JoinSet::new();

    for chunk in chunks {
        // Acquire before spawning so the loop itself paces task creation to the
        // concurrency window rather than queueing every chunk up front.
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let fut = send(chunk);
        tasks.spawn(async move {
            let _permit = permit;
            fut.await
        });
    }

    let mut total = BulkOutcome::default();
    while let Some(joined) = tasks.join_next().await {
        // A panicked send task surfaces as a join error; treat it as fatal and
        // stop draining the rest.
        let outcome = joined.map_err(|e| {
            crate::error::EsiftError::Destination(format!("bulk send task failed: {e}"))
        })?;
        total.merge(outcome?);
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::super::types::{BulkChunk, BulkOutcome};
    use super::run;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test]
    async fn runs_sends_concurrently_within_window() {
        let chunks: Vec<BulkChunk> = (0..8)
            .map(|_| BulkChunk {
                body: String::new(),
                docs: vec![],
            })
            .collect();

        let started = Instant::now();
        let outcome = run(chunks, 8, |_chunk| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(BulkOutcome {
                accepted: 1,
                rejected: vec![],
            })
        })
        .await
        .expect("run should succeed");

        let elapsed = started.elapsed();
        assert_eq!(outcome.accepted, 8);
        // Serial execution would take ~400ms; concurrency keeps it near 50ms.
        assert!(
            elapsed < Duration::from_millis(250),
            "expected concurrent execution, took {elapsed:?}"
        );
    }
}
