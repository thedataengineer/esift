//! Newline-delimited JSON file destination.
//!
//! Foundation stub: construction works and `description` reports the path, but
//! `write_batch` reports that the sink is not yet implemented. Lane 2 writes
//! each batch as NDJSON to the file (create/append).

use super::Destination;
use crate::error::{EsiftError, Result};
use crate::Document;
use async_trait::async_trait;

pub struct FileDestination {
    path: String,
}

impl FileDestination {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        Ok(Self { path: path.into() })
    }
}

#[async_trait]
impl Destination for FileDestination {
    async fn write_batch(&mut self, _docs: Vec<Document>) -> Result<usize> {
        Err(EsiftError::Destination(
            "file destination is not yet implemented".to_string(),
        ))
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("File {}", self.path)
    }
}
