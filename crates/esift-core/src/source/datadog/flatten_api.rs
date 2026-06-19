//! Flatten one Datadog Logs Search API event.
//!
//! API events nest log fields under `attributes`, and Datadog further nests the
//! custom fields under `attributes.attributes`. We collapse both levels into a
//! single flat object: the standard fields (timestamp, status, service, host,
//! tags, message, …) merged with the custom attribute map, standard fields
//! winning on a key conflict. The envelope `id` is preserved when present.

use serde_json::{Map, Value};

/// Flatten one `data[]` event. Non-object inputs are returned unchanged.
pub fn flatten(event: Value) -> Value {
    let Value::Object(mut env) = event else {
        return event;
    };

    let mut out = Map::new();
    if let Some(id) = env.remove("id") {
        out.insert("id".into(), id);
    }

    match env.remove("attributes") {
        Some(Value::Object(mut attrs)) => {
            let inner = attrs.remove("attributes");
            // Standard fields first so they win over custom attributes.
            for (k, v) in attrs {
                out.entry(k).or_insert(v);
            }
            if let Some(Value::Object(inner)) = inner {
                for (k, v) in inner {
                    out.entry(k).or_insert(v);
                }
            }
        }
        Some(other) => {
            out.insert("attributes".into(), other);
        }
        None => {}
    }

    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn collapses_double_nested_attributes() {
        let event = json!({
            "id": "AAAA",
            "type": "log",
            "attributes": {
                "timestamp": "2025-01-01T00:00:00Z",
                "service": "web",
                "host": "h1",
                "status": "info",
                "tags": ["env:prod"],
                "attributes": { "user_id": "u7", "latency_ms": 12 }
            }
        });
        let flat = flatten(event);
        assert_eq!(
            flat,
            json!({
                "id": "AAAA",
                "timestamp": "2025-01-01T00:00:00Z",
                "service": "web",
                "host": "h1",
                "status": "info",
                "tags": ["env:prod"],
                "user_id": "u7",
                "latency_ms": 12
            })
        );
    }

    #[test]
    fn standard_fields_win_over_custom() {
        let event = json!({
            "attributes": {
                "service": "standard",
                "attributes": { "service": "custom" }
            }
        });
        assert_eq!(flatten(event), json!({ "service": "standard" }));
    }

    #[test]
    fn non_object_passes_through() {
        assert_eq!(flatten(json!(42)), json!(42));
    }
}
