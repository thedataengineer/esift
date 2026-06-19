//! Shared value types that flow through the OpenObserve sink seams.

use crate::Document;

/// A document paired with the destination stream it routes to.
pub struct RoutedDoc {
    pub stream: String,
    pub doc: Document,
}

/// One `_bulk` request payload plus the documents it carries, kept together so
/// a partial failure can be correlated back to the rejected source documents.
pub struct BulkChunk {
    pub body: String,
    pub docs: Vec<RoutedDoc>,
}

/// Accounting for one or more bulk requests: how many documents OpenObserve
/// accepted, and which it rejected.
#[derive(Default)]
pub struct BulkOutcome {
    pub accepted: usize,
    pub rejected: Vec<RejectedDoc>,
}

impl BulkOutcome {
    /// Fold another outcome into this one.
    pub fn merge(&mut self, other: BulkOutcome) {
        self.accepted += other.accepted;
        self.rejected.extend(other.rejected);
    }
}

/// A document OpenObserve refused, with the reason it gave.
pub struct RejectedDoc {
    pub stream: String,
    pub reason: String,
    pub body: serde_json::Value,
}
