//! Field mapping and document transformation.
//!
//! Transforms are optional. If none are configured, documents pass through unchanged.
//! They run after extraction and before writing to the destination.
//!
//! Current ops:
//!   rename       — rename a field
//!   drop         — remove a field entirely
//!   set_timestamp — copy a field value into _timestamp (OpenObserve's expected field)

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
