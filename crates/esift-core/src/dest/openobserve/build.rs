//! Build NDJSON `_bulk` payloads from routed documents.
//!
//! With `options.max_batch_bytes` unset, emits a single payload containing every
//! document, matching the pre-refactor sink. When set, splits the output so each
//! payload stays under the cap (a single oversized document still goes out alone).

use super::config::OpenObserveOptions;
use super::types::{BulkChunk, RoutedDoc};
use crate::error::Result;

/// Serialize routed docs into one or more bulk payloads.
pub(crate) fn chunks(docs: Vec<RoutedDoc>, options: &OpenObserveOptions) -> Result<Vec<BulkChunk>> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }

    match options.max_batch_bytes {
        None => Ok(vec![single_chunk(docs)?]),
        Some(max) => capped_chunks(docs, max),
    }
}

/// Render the action + body lines for one routed document.
fn lines_for(routed: &RoutedDoc) -> Result<String> {
    let mut s = String::new();
    // Action line: stream name goes in _index.
    s.push_str(&format!(
        "{{\"index\":{{\"_index\":\"{}\"}}}}\n",
        routed.stream
    ));
    s.push_str(&serde_json::to_string(&routed.doc.body)?);
    s.push('\n');
    Ok(s)
}

/// Pre-refactor behavior: every document in a single payload.
fn single_chunk(docs: Vec<RoutedDoc>) -> Result<BulkChunk> {
    let mut body = String::new();
    for routed in &docs {
        body.push_str(&lines_for(routed)?);
    }
    Ok(BulkChunk { body, docs })
}

/// Split documents into payloads that each stay under `max` bytes. A document
/// whose lines start a fresh chunk; if a single document's lines exceed `max`
/// it still goes out alone rather than being dropped or looping forever.
fn capped_chunks(docs: Vec<RoutedDoc>, max: usize) -> Result<Vec<BulkChunk>> {
    let mut chunks = Vec::new();
    let mut body = String::new();
    let mut batch: Vec<RoutedDoc> = Vec::new();

    for routed in docs {
        let lines = lines_for(&routed)?;
        // Start a new chunk before a doc that would push the current one over
        // the cap, but only if the current chunk already carries something.
        if !batch.is_empty() && body.len() + lines.len() > max {
            chunks.push(BulkChunk {
                body: std::mem::take(&mut body),
                docs: std::mem::take(&mut batch),
            });
        }
        body.push_str(&lines);
        batch.push(routed);
    }

    if !batch.is_empty() {
        chunks.push(BulkChunk { body, docs: batch });
    }

    Ok(chunks)
}
