//! Flatten a Datadog *archive* log event into one flat JSON object.
//!
//! Archive records carry log metadata at the top level alongside an
//! `attributes` object holding the structured log fields; Datadog itself
//! double-nests custom attributes under `attributes.attributes`. We hoist the
//! inner attribute maps up to the top level so downstream consumers see a
//! single flat object, without overwriting keys already present at the outer
//! level (outer metadata wins on a conflict).
//!
//! The exact archive shape is customer/version dependent (see the open
//! questions in `DATADOG-PLAN.md`); this is a deliberately generic hoist that
//! leaves non-`attributes` fields untouched.

use serde_json::{Map, Value};

/// Flatten one archive event. Non-object inputs are returned unchanged.
pub fn flatten(event: Value) -> Value {
    let Value::Object(mut top) = event else {
        return event;
    };
    if let Some(Value::Object(attrs)) = top.remove("attributes") {
        merge_nested(&mut top, attrs);
    }
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
}
