//! Throughput and reject counters for the OpenObserve sink.
//!
//! Tracks the running totals of documents submitted, accepted, and rejected so
//! the destination can report throughput and reject rates.

use super::types::BulkOutcome;

/// Counters handle held by the destination.
#[derive(Default)]
pub(crate) struct Metrics {
    submitted: u64,
    accepted: u64,
    rejected: u64,
}

impl Metrics {
    /// Record that `n` documents were submitted in a batch.
    pub fn record_submitted(&mut self, n: usize) {
        self.submitted += n as u64;
    }

    /// Record the accepted/rejected accounting from a batch.
    pub fn record_outcome(&mut self, outcome: &BulkOutcome) {
        self.accepted += outcome.accepted as u64;
        self.rejected += outcome.rejected.len() as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dest::openobserve::RejectedDoc;

    #[test]
    fn counters_accumulate_submitted_and_outcome() {
        let mut metrics = Metrics::default();
        assert_eq!(
            (metrics.submitted, metrics.accepted, metrics.rejected),
            (0, 0, 0)
        );

        metrics.record_submitted(5);

        let outcome = BulkOutcome {
            accepted: 4,
            rejected: vec![RejectedDoc {
                stream: "logs".to_string(),
                reason: "schema mismatch".to_string(),
                body: serde_json::json!({"k": "v"}),
            }],
        };
        metrics.record_outcome(&outcome);

        assert_eq!(metrics.submitted, 5);
        assert_eq!(metrics.accepted, 4);
        assert_eq!(metrics.rejected, 1);
    }
}
