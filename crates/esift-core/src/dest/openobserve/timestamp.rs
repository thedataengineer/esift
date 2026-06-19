//! Derive OpenObserve's `_timestamp` field on a document.
//!
//! Foundation stub: no-op. Lane 7 copies/parses `options.timestamp_field` into
//! `_timestamp` when configured.

use super::config::OpenObserveOptions;
use crate::Document;

/// Set `_timestamp` on the document body in place, if configured.
pub(crate) fn apply(_doc: &mut Document, _options: &OpenObserveOptions) {}
