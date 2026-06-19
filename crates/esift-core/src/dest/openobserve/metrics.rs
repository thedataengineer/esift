//! Throughput and reject counters for the OpenObserve sink.
//!
//! Foundation stub: no-op handle. Lane 11 records submitted/accepted/rejected
//! counts (and retry counts) and exposes them.

use super::types::BulkOutcome;

/// Counters handle held by the destination.
#[derive(Default)]
pub(crate) struct Metrics {}

impl Metrics {
    /// Record that `n` documents were submitted in a batch.
    pub fn record_submitted(&mut self, _n: usize) {}

    /// Record the accepted/rejected accounting from a batch.
    pub fn record_outcome(&mut self, _outcome: &BulkOutcome) {}
}
