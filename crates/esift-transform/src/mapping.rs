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
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Transform {
    Rename { from: String, to: String },
    Drop { field: String },
    SetTimestamp { from: String },
}

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
