pub mod file;
pub mod opensearch;

use crate::{error::Result, Document};
use async_trait::async_trait;

/// Anything esift can read documents from.
///
/// The extraction loop calls next_batch() repeatedly until it returns None.
/// Implementations own their pagination cursor internally.
/// close() must be called even on error paths to release server-side resources.
#[async_trait]
pub trait Source: Send + Sync {
    /// Initialize: open a PIT, validate connectivity, etc.
    async fn open(&mut self) -> Result<()>;

    /// Return the next batch, or None when the source is exhausted.
    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>>;

    /// Release server-side resources (e.g. close a PIT).
    async fn close(&mut self) -> Result<()>;

    /// Human-readable label for progress output.
    fn description(&self) -> String;

    /// Current resume cursor, persisted to the checkpoint after each batch so a
    /// later run can continue from this position. Sources that cannot resume
    /// return None (the default).
    fn cursor(&self) -> Option<Vec<serde_json::Value>> {
        None
    }
}
