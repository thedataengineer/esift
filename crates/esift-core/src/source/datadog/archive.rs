//! Datadog archive source (Path 1): read compressed-JSON logs straight from
//! object storage. Requires the `datadog-s3` feature.
//!
//! Foundation stub: the `Source` impl returns "not yet implemented"; Lane 1
//! fills in S3 listing/download, file-level resume, and wiring through
//! [`super::decompress`] and [`super::flatten_archive`]. The struct, its
//! constructor signature, and the `Compression` selector are fixed here so the
//! CLI dispatch and Lane 1 agree on the public shape.

use super::decompress;
use crate::error::{EsiftError, Result};
use crate::source::Source;
use crate::Document;
use async_trait::async_trait;
use serde_json::Value;

/// How to choose the decompression codec for archive objects.
#[derive(Debug, Clone)]
pub enum Compression {
    /// Pick per object from the key suffix (`.zst` / `.gz`).
    Auto,
    /// Force one codec for every object.
    Fixed(decompress::Codec),
}

pub struct DatadogArchiveSource {
    bucket: String,
    prefix: String,
    #[allow(dead_code)]
    region: Option<String>,
    #[allow(dead_code)]
    from: Option<String>,
    #[allow(dead_code)]
    to: Option<String>,
    #[allow(dead_code)]
    compression: Compression,
    /// Opaque resume blob from a prior checkpoint cursor; decoded by Lane 1.
    #[allow(dead_code)]
    resume_after: Option<Vec<Value>>,
}

impl DatadogArchiveSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: Option<String>,
        from: Option<String>,
        to: Option<String>,
        compression: Compression,
        resume_after: Option<Vec<Value>>,
    ) -> Result<Self> {
        Ok(Self {
            bucket: bucket.into(),
            prefix: prefix.into(),
            region,
            from,
            to,
            compression,
            resume_after,
        })
    }
}

#[cfg(feature = "datadog-s3")]
#[async_trait]
impl Source for DatadogArchiveSource {
    async fn open(&mut self) -> Result<()> {
        Err(EsiftError::Source(
            "Datadog archive source not yet implemented (see DATADOG-PLAN.md, Lane 1)".into(),
        ))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "Datadog archive bucket={} prefix={}",
            self.bucket, self.prefix
        )
    }
}

#[cfg(not(feature = "datadog-s3"))]
#[async_trait]
impl Source for DatadogArchiveSource {
    async fn open(&mut self) -> Result<()> {
        Err(EsiftError::Source(
            "Datadog archive source requires building with --features datadog-s3".into(),
        ))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "Datadog archive bucket={} prefix={}",
            self.bucket, self.prefix
        )
    }
}
