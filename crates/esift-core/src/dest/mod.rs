pub mod openobserve;
pub mod stdout;

use crate::{error::Result, Document};
use async_trait::async_trait;

/// Anything esift can write documents to.
#[async_trait]
pub trait Destination: Send + Sync {
    /// Write a batch. Returns the count of documents accepted.
    async fn write_batch(&mut self, docs: Vec<Document>) -> Result<usize>;

    /// Flush any buffered writes and confirm delivery.
    async fn flush(&mut self) -> Result<()>;

    /// Human-readable label for progress output.
    fn description(&self) -> String;
}
