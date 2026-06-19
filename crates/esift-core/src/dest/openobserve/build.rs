//! Build NDJSON `_bulk` payloads from routed documents.
//!
//! Foundation stub: emits a single payload containing every document, matching
//! the pre-refactor sink. Lane 4 splits the output so each payload stays under
//! `options.max_batch_bytes`.

use super::config::OpenObserveOptions;
use super::types::{BulkChunk, RoutedDoc};
use crate::error::Result;

/// Serialize routed docs into one or more bulk payloads.
pub(crate) fn chunks(
    docs: Vec<RoutedDoc>,
    _options: &OpenObserveOptions,
) -> Result<Vec<BulkChunk>> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }

    let mut body = String::new();
    for routed in &docs {
        // Action line: stream name goes in _index.
        body.push_str(&format!(
            "{{\"index\":{{\"_index\":\"{}\"}}}}\n",
            routed.stream
        ));
        body.push_str(&serde_json::to_string(&routed.doc.body)?);
        body.push('\n');
    }

    Ok(vec![BulkChunk { body, docs }])
}
