//! Process-level extraction metrics, shared with the optional metrics endpoint.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct Metrics {
    docs_written: AtomicU64,
    batches: AtomicU64,
    errors: AtomicU64,
}

impl Metrics {
    /// Record a batch of `written` documents successfully delivered.
    pub fn record_batch(&self, written: u64) {
        self.docs_written.fetch_add(written, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a source or destination error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot of (docs_written, batches, errors).
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.docs_written.load(Ordering::Relaxed),
            self.batches.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

/// Shared handle passed to the extraction loop and the metrics endpoint.
pub type SharedMetrics = Arc<Metrics>;
