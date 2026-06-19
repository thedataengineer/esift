//! Newline-delimited JSON file destination.
//!
//! Each batch is appended to the configured file as NDJSON: one line per
//! document body. The file is created on first write and opened in append mode
//! (like `dest/openobserve/deadletter.rs`), so repeated batches accumulate.

use super::Destination;
use crate::error::Result;
use crate::Document;
use async_trait::async_trait;
use std::fs::OpenOptions;
use std::io::Write;

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
    async fn write_batch(&mut self, docs: Vec<Document>) -> Result<usize> {
        let count = docs.len();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        for doc in docs {
            let line = serde_json::to_string(&doc.body)?;
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }
        Ok(count)
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("File {}", self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn writes_one_ndjson_line_per_doc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.ndjson");

        let mut dest = FileDestination::new(path.to_str().unwrap()).unwrap();
        let docs = vec![
            Document::new("logs", "1", serde_json::json!({ "id": 1, "msg": "first" })),
            Document::new("logs", "2", serde_json::json!({ "id": 2, "msg": "second" })),
        ];

        let written = dest.write_batch(docs).await.unwrap();
        assert_eq!(written, 2);

        let contents = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);

        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["msg"], "first");
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["id"], 2);
    }
}
