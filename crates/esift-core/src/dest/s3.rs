//! S3 object-store destination.
//!
//! With the `s3` feature, each batch is serialized to NDJSON and uploaded as a
//! single object under `{prefix}{seq}.ndjson`, where `seq` is an internal
//! counter incremented per batch so keys never collide. Without the feature,
//! `write_batch` reports that the binary must be built with `--features s3`.

use super::Destination;
use crate::error::Result;
use crate::Document;
use async_trait::async_trait;

pub struct S3Destination {
    bucket: String,
    prefix: String,
    region: Option<String>,
    // Incremented per batch to make each object key unique. Only read by the
    // `s3`-gated `write_batch`; without that feature it is write-only.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    seq: u64,
}

impl S3Destination {
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            bucket: bucket.into(),
            prefix: prefix.into(),
            region,
            seq: 0,
        })
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl Destination for S3Destination {
    async fn write_batch(&mut self, docs: Vec<Document>) -> Result<usize> {
        use crate::error::EsiftError;

        let count = docs.len();

        let mut body = Vec::new();
        for doc in &docs {
            let line = serde_json::to_string(&doc.body)?;
            body.extend_from_slice(line.as_bytes());
            body.push(b'\n');
        }

        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &self.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        let config = loader.load().await;
        let client = aws_sdk_s3::Client::new(&config);

        let key = format!("{}{}.ndjson", self.prefix, self.seq);
        self.seq += 1;

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body.into())
            .send()
            .await
            .map_err(|e| EsiftError::Destination(format!("S3 put_object failed: {e}")))?;

        Ok(count)
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "S3 bucket={} prefix={} region={:?}",
            self.bucket, self.prefix, self.region
        )
    }
}

#[cfg(not(feature = "s3"))]
#[async_trait]
impl Destination for S3Destination {
    async fn write_batch(&mut self, _docs: Vec<Document>) -> Result<usize> {
        Err(crate::error::EsiftError::Destination(
            "S3 destination requires building with --features s3".to_string(),
        ))
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "S3 bucket={} prefix={} region={:?}",
            self.bucket, self.prefix, self.region
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_succeeds() {
        let dest = S3Destination::new("my-bucket", "exports/", Some("us-east-1".to_string()));
        assert!(dest.is_ok());
    }
}
