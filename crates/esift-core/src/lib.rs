pub mod checkpoint;
pub mod config;
pub mod dest;
pub mod error;
pub mod http;
pub mod source;
pub mod transform;

/// A single document extracted from a source.
///
/// The body is the raw JSON (_source field) exactly as the source returned it.
/// Metadata is carried alongside so destinations can route or annotate without
/// re-parsing the body.
#[derive(Debug, Clone)]
pub struct Document {
    /// The source index this document came from
    pub index: String,
    /// The document _id from the source
    pub id: String,
    /// The raw document body (_source field)
    pub body: serde_json::Value,
}

impl Document {
    pub fn new(index: impl Into<String>, id: impl Into<String>, body: serde_json::Value) -> Self {
        Self {
            index: index.into(),
            id: id.into(),
            body,
        }
    }
}
