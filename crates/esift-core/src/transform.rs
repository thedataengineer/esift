//! Serializable document transform operations.
//!
//! These describe *what* to do; the engine that applies them lives in the
//! `esift-transform` crate. They live in core so configuration (also defined
//! here) can reference them without a crate dependency cycle.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Transform {
    Rename { from: String, to: String },
    Drop { field: String },
    SetTimestamp { from: String },
    Lowercase { field: String },
    Copy { from: String, to: String },
    Coerce { field: String, to: String },
    JsonParse { field: String },
}
