//! Datadog Logs Search API source (Path 2). Requires the `datadog-api` feature.
//!
//! Foundation stub: the `Source` impl returns "not yet implemented"; Lane 2
//! fills in cursor pagination, time-window chunking, and 429 backoff, wiring
//! through [`super::site`] (regional base URL) and [`super::flatten_api`]. The
//! struct and constructor signature are fixed here so the CLI dispatch and
//! Lane 2 agree on the public shape.

use crate::error::{EsiftError, Result};
use crate::source::Source;
use crate::Document;
use async_trait::async_trait;
use serde_json::Value;

pub struct DatadogApiSource {
    site: String,
    #[allow(dead_code)]
    api_key: String,
    #[allow(dead_code)]
    app_key: String,
    #[allow(dead_code)]
    query: String,
    #[allow(dead_code)]
    from: Option<String>,
    #[allow(dead_code)]
    to: Option<String>,
    #[allow(dead_code)]
    window_minutes: u64,
    /// Opaque resume blob from a prior checkpoint cursor; decoded by Lane 2.
    #[allow(dead_code)]
    resume_after: Option<Vec<Value>>,
}

impl DatadogApiSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        site: impl Into<String>,
        api_key: impl Into<String>,
        app_key: impl Into<String>,
        query: impl Into<String>,
        from: Option<String>,
        to: Option<String>,
        window_minutes: u64,
        resume_after: Option<Vec<Value>>,
    ) -> Result<Self> {
        Ok(Self {
            site: site.into(),
            api_key: api_key.into(),
            app_key: app_key.into(),
            query: query.into(),
            from,
            to,
            window_minutes,
            resume_after,
        })
    }
}

#[cfg(feature = "datadog-api")]
#[async_trait]
impl Source for DatadogApiSource {
    async fn open(&mut self) -> Result<()> {
        Err(EsiftError::Source(
            "Datadog API source not yet implemented (see DATADOG-PLAN.md, Lane 2)".into(),
        ))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("Datadog API site={}", self.site)
    }
}

#[cfg(not(feature = "datadog-api"))]
#[async_trait]
impl Source for DatadogApiSource {
    async fn open(&mut self) -> Result<()> {
        Err(EsiftError::Source(
            "Datadog API source requires building with --features datadog-api".into(),
        ))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("Datadog API site={}", self.site)
    }
}
