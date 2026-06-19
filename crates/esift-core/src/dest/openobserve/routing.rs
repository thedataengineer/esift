//! Decide which stream a document is written to.
//!
//! When `options.stream_field` names a field whose value is a JSON string, that
//! value selects the stream; otherwise documents go to the configured fixed
//! stream.

use super::config::OpenObserveOptions;
use crate::Document;

/// Resolve the destination stream for one document.
pub(crate) fn stream_for(
    doc: &Document,
    options: &OpenObserveOptions,
    default_stream: &str,
) -> String {
    if let Some(field) = &options.stream_field {
        if let Some(value) = doc.body.get(field).and_then(|v| v.as_str()) {
            return value.to_string();
        }
    }
    default_stream.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn uses_routing_field_value_when_present() {
        let doc = Document::new("src", "1", json!({ "service": "payments" }));
        let options = OpenObserveOptions {
            stream_field: Some("service".to_string()),
            ..Default::default()
        };

        assert_eq!(stream_for(&doc, &options, "default"), "payments");
    }

    #[test]
    fn falls_back_to_default_when_field_missing() {
        let doc = Document::new("src", "1", json!({ "other": "value" }));
        let options = OpenObserveOptions {
            stream_field: Some("service".to_string()),
            ..Default::default()
        };

        assert_eq!(stream_for(&doc, &options, "default"), "default");
    }

    #[test]
    fn falls_back_to_default_when_routing_disabled() {
        let doc = Document::new("src", "1", json!({ "service": "payments" }));
        let options = OpenObserveOptions::default();

        assert_eq!(stream_for(&doc, &options, "default"), "default");
    }
}
