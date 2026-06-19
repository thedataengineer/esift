//! S3 object-store destination.
//!
//! Foundation stub: construction works and `description` reports the target,
//! but `write_batch` reports that the sink needs the `s3` build feature. Lane 2
//! adds the `s3` feature + aws-sdk-s3 and writes each batch as an object under
//! the configured bucket/prefix.

use super::Destination;
use crate::error::{EsiftError, Result};
use crate::Document;
use async_trait::async_trait;

pub struct S3Destination {
    bucket: String,
    prefix: String,
    region: Option<String>,
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
        })
    }
}

#[async_trait]
impl Destination for S3Destination {
    async fn write_batch(&mut self, _docs: Vec<Document>) -> Result<usize> {
        Err(EsiftError::Destination(
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
