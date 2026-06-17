//! Stdout destination: writes documents as NDJSON.
//!
//! Use this to inspect output before committing to a real destination.
//!   esift extract --source-url ... --dest stdout | jq .

use super::Destination;
use crate::{Document, error::Result};
use async_trait::async_trait;

pub struct StdoutDestination;

#[async_trait]
impl Destination for StdoutDestination {
    async fn write_batch(&mut self, docs: Vec<Document>) -> Result<usize> {
        let count = docs.len();
        for doc in docs {
            let line = serde_json::json!({
                "_index": doc.index,
                "_id": doc.id,
                "_source": doc.body,
            });
            println!("{}", line);
        }
        Ok(count)
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        "stdout (NDJSON)".into()
    }
}
