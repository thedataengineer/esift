//! NDJSON file source: read documents from a newline-delimited JSON file.
//!
//! Each non-empty line is parsed as one JSON value and wrapped in a Document
//! whose `id` is the 1-based line number. Blank lines are skipped but still
//! advance the line counter, so the `id` always matches the physical line in
//! the file. The line offset is exposed as the resume cursor: a later run can
//! seed `with_resume(offset)` to skip lines already processed, which is what
//! makes a dead-letter NDJSON file replayable from where a prior run stopped.

use super::Source;
use crate::error::{EsiftError, Result};
use crate::Document;
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

/// Number of lines read per `next_batch` call by default.
const DEFAULT_BATCH_LINES: usize = 1000;

pub struct FileSource {
    path: String,
    batch_lines: usize,
    /// Lines already consumed; doubles as the resume cursor and the next id.
    offset: usize,
    /// Lines to skip on open when resuming from a checkpoint.
    resume_offset: usize,
    reader: Option<Lines<BufReader<File>>>,
    exhausted: bool,
}

impl FileSource {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        Ok(Self {
            path: path.into(),
            batch_lines: DEFAULT_BATCH_LINES,
            offset: 0,
            resume_offset: 0,
            reader: None,
            exhausted: false,
        })
    }

    /// Seed the resume offset so `open` skips lines already processed by a prior
    /// run. Used internally (and by tests); `new`'s signature is unchanged.
    pub fn with_resume(mut self, offset: usize) -> Self {
        self.resume_offset = offset;
        self
    }
}

#[async_trait]
impl Source for FileSource {
    async fn open(&mut self) -> Result<()> {
        let file = File::open(&self.path)
            .await
            .map_err(|e| EsiftError::Source(format!("Cannot open file '{}': {}", self.path, e)))?;
        let mut lines = BufReader::new(file).lines();

        // Honor a resume offset by discarding lines already processed.
        for _ in 0..self.resume_offset {
            match lines.next_line().await? {
                Some(_) => self.offset += 1,
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        self.reader = Some(lines);
        Ok(())
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        if self.exhausted {
            return Ok(None);
        }

        let lines = self
            .reader
            .as_mut()
            .ok_or_else(|| EsiftError::Source("Call open() before next_batch()".into()))?;

        let mut batch: Vec<Document> = Vec::new();

        while batch.len() < self.batch_lines {
            match lines.next_line().await? {
                Some(line) => {
                    self.offset += 1;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let body: Value = serde_json::from_str(&line).map_err(|e| {
                        EsiftError::Source(format!("Invalid JSON on line {}: {}", self.offset, e))
                    })?;
                    batch.push(Document::new("file", self.offset.to_string(), body));
                }
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.reader = None;
        Ok(())
    }

    fn description(&self) -> String {
        format!("File {}", self.path)
    }

    fn cursor(&self) -> Option<Vec<Value>> {
        Some(vec![json!(self.offset)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_ndjson(contents: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("create temp file");
        f.write_all(contents.as_bytes()).expect("write temp file");
        f.flush().expect("flush temp file");
        f
    }

    #[tokio::test]
    async fn reads_three_documents_then_none() {
        let f = write_ndjson("{\"a\":1}\n{\"b\":\"two\"}\n{\"c\":[3,3,3]}\n");
        let mut src = FileSource::new(f.path().to_str().unwrap()).expect("construct");
        src.open().await.expect("open");

        let batch = src
            .next_batch()
            .await
            .expect("next_batch")
            .expect("first batch present");
        assert_eq!(batch.len(), 3);

        assert_eq!(batch[0].index, "file");
        assert_eq!(batch[0].id, "1");
        assert_eq!(batch[0].body, json!({"a": 1}));

        assert_eq!(batch[1].id, "2");
        assert_eq!(batch[1].body, json!({"b": "two"}));

        assert_eq!(batch[2].id, "3");
        assert_eq!(batch[2].body, json!({"c": [3, 3, 3]}));

        assert!(src.next_batch().await.expect("eof").is_none());
    }

    #[tokio::test]
    async fn skips_blank_lines_and_keeps_line_numbers() {
        // Blank lines at the start, between, and at the end are dropped, but the
        // id still reflects the physical line number of each JSON record.
        let f = write_ndjson("\n{\"a\":1}\n\n   \n{\"b\":2}\n\n");
        let mut src = FileSource::new(f.path().to_str().unwrap()).expect("construct");
        src.open().await.expect("open");

        let batch = src
            .next_batch()
            .await
            .expect("next_batch")
            .expect("batch present");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].id, "2");
        assert_eq!(batch[0].body, json!({"a": 1}));
        assert_eq!(batch[1].id, "5");
        assert_eq!(batch[1].body, json!({"b": 2}));

        assert!(src.next_batch().await.expect("eof").is_none());
    }

    #[tokio::test]
    async fn missing_path_errors_clearly() {
        let mut src = FileSource::new("/no/such/esift/file.ndjson").expect("construct");
        let err = src.open().await.expect_err("open should fail");
        let msg = err.to_string();
        assert!(msg.contains("Cannot open file"), "got: {msg}");
    }

    #[tokio::test]
    async fn cursor_tracks_line_offset() {
        let f = write_ndjson("{\"a\":1}\n{\"b\":2}\n");
        let mut src = FileSource::new(f.path().to_str().unwrap()).expect("construct");
        src.open().await.expect("open");
        assert_eq!(src.cursor(), Some(vec![json!(0)]));
        src.next_batch().await.expect("batch");
        assert_eq!(src.cursor(), Some(vec![json!(2)]));
    }

    #[tokio::test]
    async fn resume_skips_processed_lines() {
        let f = write_ndjson("{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");
        let mut src = FileSource::new(f.path().to_str().unwrap())
            .expect("construct")
            .with_resume(2);
        src.open().await.expect("open");

        let batch = src
            .next_batch()
            .await
            .expect("next_batch")
            .expect("batch present");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, "3");
        assert_eq!(batch[0].body, json!({"c": 3}));
        assert!(src.next_batch().await.expect("eof").is_none());
    }
}
