//! Parse a successful `_bulk` response into accepted/rejected accounting.
//!
//! Foundation stub: treats every submitted document as accepted, matching the
//! pre-refactor sink (which returned the submitted count and only logged on
//! `errors:true`). Lane 1 parses the `items[]` array, counts real successes,
//! and correlates rejected items back to their source documents by position.

use super::types::{BulkOutcome, RoutedDoc};
use crate::error::Result;

/// Account a 2xx bulk response against the documents that were submitted.
pub(crate) async fn parse(resp: reqwest::Response, docs: &[RoutedDoc]) -> Result<BulkOutcome> {
    // Drain the body so the connection can be reused; contents ignored in the
    // stub. Lane 1 inspects `errors` and `items[]` here.
    let _ = resp.text().await;

    Ok(BulkOutcome {
        accepted: docs.len(),
        rejected: Vec::new(),
    })
}
