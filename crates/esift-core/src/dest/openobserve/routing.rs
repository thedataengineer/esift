//! Decide which stream a document is written to.
//!
//! Foundation stub: always the configured fixed stream. Lane 6 derives the
//! stream from `options.stream_field` when set, falling back to the default.

use super::config::OpenObserveOptions;
use crate::Document;

/// Resolve the destination stream for one document.
pub(crate) fn stream_for(
    _doc: &Document,
    _options: &OpenObserveOptions,
    default_stream: &str,
) -> String {
    default_stream.to_string()
}
