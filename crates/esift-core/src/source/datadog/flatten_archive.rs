//! Flatten a Datadog *Log Archive* record into one flat JSON object.
//!
//! # Assumed archive record schema
//!
//! Datadog Log Archives are written to cloud storage as compressed **NDJSON**
//! (one JSON object per line); see the Log Archives docs:
//! <https://docs.datadoghq.com/logs/log_configuration/archives/> and the
//! Rehydration docs:
//! <https://docs.datadoghq.com/logs/log_configuration/rehydrating/>.
//!
//! Each archived record carries the standard/reserved log fields at the top
//! level alongside an `attributes` object holding the structured log content.
//! The reserved attributes Datadog pre-processes and surfaces at the top level
//! are documented on the archives / standard-attributes pages
//! (<https://docs.datadoghq.com/standard-attributes/>): `date` (timestamp),
//! `host`, `source`, `service`, `status`, and `message`. Records also carry an
//! `_id` and, when archive tag retention is enabled, the event's `tags`.
//!
//! A representative archive line therefore looks like:
//!
//! ```json
//! {
//!   "_id": "AQAAAY...",
//!   "date": "2024-05-01T12:34:56.789Z",
//!   "host": "web-01",
//!   "source": "nginx",
//!   "service": "web",
//!   "status": "info",
//!   "message": "GET /login 200",
//!   "tags": ["env:prod", "version:1.2.3"],
//!   "attributes": {
//!     "http": { "method": "GET", "status_code": 200 },
//!     "attributes": { "user_id": "u7", "path": "/login" }
//!   }
//! }
//! ```
//!
//! Notes confirmed from the docs and observed archive output:
//! * Tags, when retained, are stored as an **array** of `key:value` strings
//!   (the docs describe tags being archived with the event). Some exporters
//!   instead emit a comma-separated `ddtags` (or string `tags`) field; we
//!   normalize either form into a `tags` array.
//! * Datadog's own pipeline-extracted attributes are commonly **double-nested**
//!   under `attributes.attributes`, so we hoist recursively.
//!
//! # Behavior
//!
//! `flatten` produces a single flat object:
//! * Hoists `attributes` (and any further-nested `attributes.attributes`) to the
//!   top level. Outer/standard fields win on a key conflict, preserving the
//!   reserved top-level fields (`date`, `host`, `source`, `service`, `status`,
//!   `message`, `_id`).
//! * Normalizes tags: a comma-separated `ddtags` (or string `tags`) field is
//!   split into a `tags` array of strings; an existing `tags` array is left
//!   untouched.
//! * Keeps unrecognized top-level keys as-is (nothing is silently dropped).
//! * Non-object input is returned unchanged.

use serde_json::{Map, Value};

/// Flatten one archive event. Non-object inputs are returned unchanged.
pub fn flatten(event: Value) -> Value {
    let Value::Object(mut top) = event else {
        return event;
    };
    if let Some(Value::Object(attrs)) = top.remove("attributes") {
        merge_nested(&mut top, attrs);
    }
    normalize_tags(&mut top);
    Value::Object(top)
}

/// Merge `attrs` into `top` without clobbering existing keys, recursing into a
/// further nested `attributes` object first so the deepest fields are hoisted.
fn merge_nested(top: &mut Map<String, Value>, mut attrs: Map<String, Value>) {
    if let Some(Value::Object(inner)) = attrs.remove("attributes") {
        merge_nested(top, inner);
    }
    for (k, v) in attrs {
        top.entry(k).or_insert(v);
    }
}

/// Normalize the event's tags into a `tags` array of strings.
///
/// Datadog archives store retained tags as an array of `key:value` strings, but
/// some exporters emit a comma-separated `ddtags` (or string `tags`) field
/// instead. We accept either: an existing array `tags` is left as-is; a string
/// `tags` is split; and a `ddtags` string is split and promoted to `tags`
/// (without overwriting an array `tags` already present).
fn normalize_tags(top: &mut Map<String, Value>) {
    // If `tags` is already an array, keep it and drop any redundant `ddtags`.
    if matches!(top.get("tags"), Some(Value::Array(_))) {
        if matches!(top.get("ddtags"), Some(Value::String(_))) {
            top.remove("ddtags");
        }
        return;
    }

    // Prefer a string `tags`, otherwise fall back to a string `ddtags`.
    let raw = match top.get("tags") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => match top.remove("ddtags") {
            Some(Value::String(s)) => Some(s),
            other => {
                // Put back a non-string ddtags we removed but did not consume.
                if let Some(v) = other {
                    top.insert("ddtags".to_owned(), v);
                }
                None
            }
        },
    };

    if let Some(raw) = raw {
        top.insert("tags".to_owned(), Value::Array(split_tags(&raw)));
    }
}

/// Split a comma-separated tag string into a vector of trimmed, non-empty
/// string `Value`s.
fn split_tags(raw: &str) -> Vec<Value> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Value::String(s.to_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hoists_nested_attributes() {
        let event = json!({
            "host": "h1",
            "service": "web",
            "attributes": {
                "status": "info",
                "attributes": { "user_id": "u7", "path": "/login" }
            }
        });
        let flat = flatten(event);
        assert_eq!(
            flat,
            json!({
                "host": "h1",
                "service": "web",
                "status": "info",
                "user_id": "u7",
                "path": "/login"
            })
        );
    }

    #[test]
    fn outer_keys_win_on_conflict() {
        let event = json!({
            "service": "outer",
            "attributes": { "service": "inner" }
        });
        assert_eq!(flatten(event), json!({ "service": "outer" }));
    }

    #[test]
    fn non_object_passes_through() {
        assert_eq!(flatten(json!("scalar")), json!("scalar"));
    }

    /// A realistic Datadog Log Archive NDJSON line: reserved fields at the top
    /// level, a `tags` array, and pipeline attributes double-nested under
    /// `attributes.attributes`.
    #[test]
    fn realistic_archive_record() {
        let event = json!({
            "_id": "AQAAAY1234567890",
            "date": "2024-05-01T12:34:56.789Z",
            "host": "web-01",
            "source": "nginx",
            "service": "web",
            "status": "info",
            "message": "GET /login 200",
            "tags": ["env:prod", "version:1.2.3"],
            "attributes": {
                "http": { "method": "GET", "status_code": 200 },
                "attributes": { "user_id": "u7", "path": "/login" }
            }
        });
        let flat = flatten(event);
        assert_eq!(
            flat,
            json!({
                "_id": "AQAAAY1234567890",
                "date": "2024-05-01T12:34:56.789Z",
                "host": "web-01",
                "source": "nginx",
                "service": "web",
                "status": "info",
                "message": "GET /login 200",
                "tags": ["env:prod", "version:1.2.3"],
                "http": { "method": "GET", "status_code": 200 },
                "user_id": "u7",
                "path": "/login"
            })
        );
    }

    #[test]
    fn ddtags_string_becomes_tags_array() {
        let event = json!({
            "service": "web",
            "ddtags": "env:prod,version:1.2.3, region:us-east-1",
            "attributes": { "k": "v" }
        });
        let flat = flatten(event);
        assert_eq!(
            flat,
            json!({
                "service": "web",
                "tags": ["env:prod", "version:1.2.3", "region:us-east-1"],
                "k": "v"
            })
        );
    }

    #[test]
    fn string_tags_field_is_split() {
        let event = json!({
            "service": "web",
            "tags": "env:prod,team:core"
        });
        let flat = flatten(event);
        assert_eq!(
            flat,
            json!({
                "service": "web",
                "tags": ["env:prod", "team:core"]
            })
        );
    }

    #[test]
    fn existing_tags_array_wins_over_ddtags() {
        let event = json!({
            "tags": ["env:prod"],
            "ddtags": "env:staging,extra:1"
        });
        let flat = flatten(event);
        assert_eq!(flat, json!({ "tags": ["env:prod"] }));
    }
}
