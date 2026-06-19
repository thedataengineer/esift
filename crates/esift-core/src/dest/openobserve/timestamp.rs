//! Derive OpenObserve's `_timestamp` field on a document.
//!
//! Copies `options.timestamp_field` into `_timestamp` when configured and the
//! body is a JSON object. The source field is left in place.

use super::config::OpenObserveOptions;
use crate::Document;

/// Set `_timestamp` on the document body in place, if configured.
pub(crate) fn apply(doc: &mut Document, options: &OpenObserveOptions) {
    let Some(field) = options.timestamp_field.as_deref() else {
        return;
    };
    let Some(obj) = doc.body.as_object_mut() else {
        return;
    };
    if let Some(value) = obj.get(field).cloned() {
        obj.insert("_timestamp".to_string(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn options_with(timestamp_field: Option<&str>) -> OpenObserveOptions {
        OpenObserveOptions {
            timestamp_field: timestamp_field.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn copies_field_into_timestamp_and_keeps_source() {
        let mut doc = Document::new("idx", "1", json!({ "created_at": "2024-01-01" }));
        apply(&mut doc, &options_with(Some("created_at")));

        assert_eq!(doc.body["_timestamp"], json!("2024-01-01"));
        assert_eq!(doc.body["created_at"], json!("2024-01-01"));
    }

    #[test]
    fn non_object_body_is_unchanged() {
        let mut doc = Document::new("idx", "1", json!([1, 2, 3]));
        apply(&mut doc, &options_with(Some("created_at")));

        assert_eq!(doc.body, json!([1, 2, 3]));
    }

    #[test]
    fn no_timestamp_field_is_unchanged() {
        let mut doc = Document::new("idx", "1", json!({ "created_at": "2024-01-01" }));
        apply(&mut doc, &options_with(None));

        assert_eq!(doc.body, json!({ "created_at": "2024-01-01" }));
    }

    #[test]
    fn missing_source_field_is_unchanged() {
        let mut doc = Document::new("idx", "1", json!({ "other": "x" }));
        apply(&mut doc, &options_with(Some("created_at")));

        assert_eq!(doc.body, json!({ "other": "x" }));
    }
}
