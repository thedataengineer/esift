//! NDJSON file source: read documents from a newline-delimited JSON file.
//!
//! Foundation stub: construction works and `description` reports the path, but
//! `open` reports that the source is not yet implemented. Lane 3 implements
//! streaming reads with line-offset resume (and dead-letter replay).

use super::Source;
use crate::error::{EsiftError, Result};
use crate::Document;
use async_trait::async_trait;

pub struct FileSource {
    path: String,
}

impl FileSource {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        Ok(Self { path: path.into() })
    }
}

#[async_trait]
impl Source for FileSource {
    async fn open(&mut self) -> Result<()> {
        Err(EsiftError::Source(
            "file source is not yet implemented".to_string(),
        ))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("File {}", self.path)
    }
}
