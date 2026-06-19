//! Parse a successful `_bulk` response into accepted/rejected accounting.
//!
//! OpenObserve mirrors the Elasticsearch bulk response shape: a top-level
//! `errors` flag and an `items[]` array positionally aligned with the documents
//! that were submitted. When `errors` is false (or no `items` array is present)
//! every submitted document was accepted. Otherwise each item carries a per-doc
//! `status` and, on failure, an `error` object; rejected items are correlated
//! back to their source documents by position.

use super::types::{BulkOutcome, RejectedDoc, RoutedDoc};
use crate::error::Result;

/// Account a 2xx bulk response against the documents that were submitted.
pub(crate) async fn parse(resp: reqwest::Response, docs: &[RoutedDoc]) -> Result<BulkOutcome> {
    // Drain the body so the connection can be reused.
    let body = resp.json::<serde_json::Value>().await?;

    // Fast path: no errors flagged, or no per-item detail to inspect. Treat the
    // whole batch as accepted.
    let items = match body.get("items").and_then(|v| v.as_array()) {
        Some(items) if body.get("errors").and_then(|v| v.as_bool()) == Some(true) => items,
        _ => {
            return Ok(BulkOutcome {
                accepted: docs.len(),
                rejected: Vec::new(),
            });
        }
    };

    let mut outcome = BulkOutcome::default();
    for (i, item) in items.iter().enumerate() {
        // Guard against a response with more items than documents.
        let Some(routed) = docs.get(i) else { break };

        // Each item wraps the action result under its action key, e.g.
        // {"index": {"status": 201}}. Take the first (and only) inner object.
        let inner = item
            .as_object()
            .and_then(|m| m.values().next())
            .unwrap_or(item);

        let status = inner.get("status").and_then(|s| s.as_u64());
        let error = inner.get("error");

        let rejected = matches!(status, Some(s) if s >= 300) || error.is_some();
        if rejected {
            let reason = match error {
                Some(e) => e.to_string(),
                None => match status {
                    Some(s) => format!("status {s}"),
                    None => "unknown error".to_string(),
                },
            };
            outcome.rejected.push(RejectedDoc {
                stream: routed.stream.clone(),
                reason,
                body: routed.doc.body.clone(),
            });
        } else {
            outcome.accepted += 1;
        }
    }

    Ok(outcome)
}
