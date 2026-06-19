//! Persist rejected documents for later inspection or replay.
//!
//! Foundation stub: no-op (rejects are logged by the orchestrator). Lane 10
//! appends each rejected document to `options.dead_letter_path` as NDJSON.

use super::config::OpenObserveOptions;
use super::types::RejectedDoc;
use crate::error::Result;

/// Write rejected documents to the dead-letter sink, if configured.
pub(crate) fn write(_options: &OpenObserveOptions, _rejected: &[RejectedDoc]) -> Result<()> {
    Ok(())
}
