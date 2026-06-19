//! Field mapping and document transformation.
//!
//! Transforms are optional. If none are configured, documents pass through unchanged.
//! They run after extraction and before writing to the destination.
//!
//! Current ops:
//!   rename       — rename a field
//!   drop         — remove a field entirely
//!   set_timestamp — copy a field value into _timestamp (OpenObserve's expected field)
//!   lowercase    — lowercase a string field in place
//!   copy         — clone a field's value into another key (keeps the source)
//!   coerce       — best-effort coerce a field to string/int/float/bool
//!   json_parse   — parse a JSON-string field and replace it with the parsed JSON

use esift_core::Document;

// Re-export so existing `esift_transform::mapping::Transform` paths keep working.
pub use esift_core::transform::Transform;

pub struct Transformer {
    transforms: Vec<Transform>,
}

impl Transformer {
    pub fn new(transforms: Vec<Transform>) -> Self {
        Self { transforms }
    }

    pub fn identity() -> Self {
        Self { transforms: vec![] }
    }

    pub fn apply(&self, mut doc: Document) -> Document {
        if self.transforms.is_empty() {
            return doc;
        }

        let body = match doc.body.as_object_mut() {
            Some(obj) => obj,
            None => return doc,
        };

        for transform in &self.transforms {
            match transform {
                Transform::Rename { from, to } => {
                    if let Some(value) = body.remove(from.as_str()) {
                        body.insert(to.clone(), value);
                    }
                }
                Transform::Drop { field } => {
                    body.remove(field.as_str());
                }
                Transform::SetTimestamp { from } => {
                    if let Some(value) = body.get(from.as_str()).cloned() {
                        body.insert("_timestamp".to_string(), value);
                    }
                }
                Transform::Lowercase { field } => {
                    if let Some(serde_json::Value::String(s)) = body.get(field.as_str()) {
                        let lowered = s.to_lowercase();
                        body.insert(field.clone(), serde_json::Value::String(lowered));
                    }
                }
                Transform::Copy { from, to } => {
                    if let Some(value) = body.get(from.as_str()).cloned() {
                        body.insert(to.clone(), value);
                    }
                }
                Transform::Coerce { field, to } => {
                    if let Some(value) = body.get(field.as_str()) {
                        if let Some(coerced) = coerce_value(value, to) {
                            body.insert(field.clone(), coerced);
                        }
                    }
                }
                Transform::JsonParse { field } => {
                    if let Some(serde_json::Value::String(s)) = body.get(field.as_str()) {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                            body.insert(field.clone(), parsed);
                        }
                    }
                }
            }
        }

        doc
    }

    pub fn apply_batch(&self, docs: Vec<Document>) -> Vec<Document> {
        if self.transforms.is_empty() {
            return docs;
        }
        docs.into_iter().map(|d| self.apply(d)).collect()
    }
}

/// Best-effort coercion of a JSON value to the requested target type.
///
/// Returns `None` when the value cannot be coerced, in which case the caller
/// leaves the field unchanged. Recognized targets: `string`, `int`, `float`, `bool`.
fn coerce_value(value: &serde_json::Value, to: &str) -> Option<serde_json::Value> {
    use serde_json::Value;

    match to {
        "string" => match value {
            Value::String(_) => Some(value.clone()),
            Value::Number(n) => Some(Value::String(n.to_string())),
            Value::Bool(b) => Some(Value::String(b.to_string())),
            _ => None,
        },
        "int" => match value {
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_f64().map(|f| f as i64))
                .map(|i| Value::Number(i.into())),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .ok()
                .or_else(|| s.trim().parse::<f64>().ok().map(|f| f as i64))
                .map(|i| Value::Number(i.into())),
            Value::Bool(b) => Some(Value::Number(i64::from(*b).into())),
            _ => None,
        },
        "float" => match value {
            Value::Number(n) => n
                .as_f64()
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number),
            Value::Bool(b) => {
                serde_json::Number::from_f64(if *b { 1.0 } else { 0.0 }).map(Value::Number)
            }
            _ => None,
        },
        "bool" => match value {
            Value::Bool(_) => Some(value.clone()),
            Value::String(s) => match s.trim().to_lowercase().as_str() {
                "true" | "1" | "yes" => Some(Value::Bool(true)),
                "false" | "0" | "no" => Some(Value::Bool(false)),
                _ => None,
            },
            Value::Number(n) => n.as_i64().map(|i| Value::Bool(i != 0)),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(body: serde_json::Value) -> Document {
        Document::new("idx", "id1", body)
    }

    #[test]
    fn identity_passes_body_through_unchanged() {
        let out = Transformer::identity().apply(doc(json!({"a": 1, "b": "x"})));
        assert_eq!(out.body, json!({"a": 1, "b": "x"}));
    }

    #[test]
    fn rename_moves_value_to_new_key() {
        let t = Transformer::new(vec![Transform::Rename {
            from: "a".into(),
            to: "z".into(),
        }]);
        assert_eq!(t.apply(doc(json!({"a": 1}))).body, json!({"z": 1}));
    }

    #[test]
    fn drop_removes_the_field() {
        let t = Transformer::new(vec![Transform::Drop {
            field: "secret".into(),
        }]);
        assert_eq!(
            t.apply(doc(json!({"secret": "x", "keep": 1}))).body,
            json!({"keep": 1})
        );
    }

    #[test]
    fn set_timestamp_copies_field_without_removing_source() {
        let t = Transformer::new(vec![Transform::SetTimestamp {
            from: "created_at".into(),
        }]);
        let out = t.apply(doc(json!({"created_at": "2024-01-01"}))).body;
        assert_eq!(out["_timestamp"], json!("2024-01-01"));
        assert_eq!(out["created_at"], json!("2024-01-01"));
    }

    #[test]
    fn non_object_body_is_left_untouched() {
        let t = Transformer::new(vec![Transform::Drop { field: "x".into() }]);
        assert_eq!(t.apply(doc(json!([1, 2, 3]))).body, json!([1, 2, 3]));
    }

    #[test]
    fn lowercase_lowercases_a_string_field_in_place() {
        let t = Transformer::new(vec![Transform::Lowercase {
            field: "level".into(),
        }]);
        let out = t.apply(doc(json!({"level": "ERROR", "n": 1}))).body;
        assert_eq!(out, json!({"level": "error", "n": 1}));
    }

    #[test]
    fn lowercase_leaves_non_string_values_unchanged() {
        let t = Transformer::new(vec![Transform::Lowercase { field: "n".into() }]);
        assert_eq!(t.apply(doc(json!({"n": 5}))).body, json!({"n": 5}));
    }

    #[test]
    fn copy_clones_value_into_new_key_keeping_source() {
        let t = Transformer::new(vec![Transform::Copy {
            from: "a".into(),
            to: "b".into(),
        }]);
        let out = t.apply(doc(json!({"a": "x"}))).body;
        assert_eq!(out, json!({"a": "x", "b": "x"}));
    }

    #[test]
    fn copy_is_a_noop_when_source_is_missing() {
        let t = Transformer::new(vec![Transform::Copy {
            from: "missing".into(),
            to: "b".into(),
        }]);
        assert_eq!(t.apply(doc(json!({"a": 1}))).body, json!({"a": 1}));
    }

    #[test]
    fn coerce_string_to_int() {
        let t = Transformer::new(vec![Transform::Coerce {
            field: "n".into(),
            to: "int".into(),
        }]);
        assert_eq!(t.apply(doc(json!({"n": "42"}))).body, json!({"n": 42}));
    }

    #[test]
    fn coerce_int_to_string() {
        let t = Transformer::new(vec![Transform::Coerce {
            field: "n".into(),
            to: "string".into(),
        }]);
        assert_eq!(t.apply(doc(json!({"n": 42}))).body, json!({"n": "42"}));
    }

    #[test]
    fn coerce_string_to_float() {
        let t = Transformer::new(vec![Transform::Coerce {
            field: "n".into(),
            to: "float".into(),
        }]);
        assert_eq!(t.apply(doc(json!({"n": "3.5"}))).body, json!({"n": 3.5}));
    }

    #[test]
    fn coerce_string_to_bool() {
        let t = Transformer::new(vec![Transform::Coerce {
            field: "flag".into(),
            to: "bool".into(),
        }]);
        assert_eq!(
            t.apply(doc(json!({"flag": "true"}))).body,
            json!({"flag": true})
        );
    }

    #[test]
    fn coerce_leaves_uncoercible_value_unchanged() {
        let t = Transformer::new(vec![Transform::Coerce {
            field: "n".into(),
            to: "int".into(),
        }]);
        assert_eq!(
            t.apply(doc(json!({"n": "not-a-number"}))).body,
            json!({"n": "not-a-number"})
        );
    }

    #[test]
    fn json_parse_replaces_string_with_parsed_json() {
        let t = Transformer::new(vec![Transform::JsonParse {
            field: "payload".into(),
        }]);
        let out = t.apply(doc(json!({"payload": "{\"k\": [1, 2]}"}))).body;
        assert_eq!(out, json!({"payload": {"k": [1, 2]}}));
    }

    #[test]
    fn json_parse_leaves_invalid_json_unchanged() {
        let t = Transformer::new(vec![Transform::JsonParse {
            field: "payload".into(),
        }]);
        assert_eq!(
            t.apply(doc(json!({"payload": "not json"}))).body,
            json!({"payload": "not json"})
        );
    }

    #[test]
    fn new_ops_leave_non_object_body_untouched() {
        let t = Transformer::new(vec![
            Transform::Lowercase { field: "a".into() },
            Transform::Copy {
                from: "a".into(),
                to: "b".into(),
            },
            Transform::Coerce {
                field: "a".into(),
                to: "int".into(),
            },
            Transform::JsonParse { field: "a".into() },
        ]);
        assert_eq!(t.apply(doc(json!([1, 2, 3]))).body, json!([1, 2, 3]));
    }

    #[test]
    fn apply_batch_transforms_every_document() {
        let t = Transformer::new(vec![Transform::Drop {
            field: "drop_me".into(),
        }]);
        let out = t.apply_batch(vec![
            doc(json!({"drop_me": 1, "k": 1})),
            doc(json!({"drop_me": 2, "k": 2})),
        ]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].body, json!({"k": 1}));
        assert_eq!(out[1].body, json!({"k": 2}));
    }
}
