//! OpenObserve destination via the _bulk ingestion API.
//!
//! Confirmed endpoint from OpenObserve docs:
//!   POST /api/{org}/_bulk
//!
//! The stream name goes in the action line _index field, not the URL:
//!   {"index": {"_index": "stream_name"}}
//!   {...document body...}
//!
//! Ref: https://openobserve.ai/docs/api/ingestion/logs/bulk/
//!
//! The sink is split into seams wired by this orchestrator: each document is
//! timestamped (`timestamp`) and routed to a stream (`routing`), batched into
//! one or more payloads (`build`), sent (`transport`) with retry (`http::retry`)
//! across an optional concurrency window (`pipeline`), and the response is
//! accounted (`response`) with rejects sent to a dead-letter sink (`deadletter`)
//! and counted (`metrics`). Each seam ships a behavior-preserving stub that the
//! corresponding feature lane fleshes out independently.

mod auth;
mod build;
pub mod config;
mod deadletter;
mod metrics;
mod pipeline;
mod response;
mod routing;
mod timestamp;
mod tls;
mod transport;
mod types;

use super::Destination;
use crate::{error::Result, Document};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, warn};

pub use self::config::OpenObserveOptions;
pub use self::types::RejectedDoc;
use self::types::{BulkChunk, BulkOutcome, RoutedDoc};

/// Cheaply-cloneable context shared across concurrent bulk sends. `client` is
/// `Arc`-backed inside reqwest; the whole context is wrapped in an `Arc` so the
/// concurrency lane can fan out sends without re-cloning per request.
pub(crate) struct SinkContext {
    pub client: reqwest::Client,
    pub bulk_url: String,
    pub username: String,
    pub password: String,
    pub options: OpenObserveOptions,
}

pub struct OpenObserveDestination {
    ctx: Arc<SinkContext>,
    stream: String,
    metrics: metrics::Metrics,
}

impl OpenObserveDestination {
    pub fn new(
        base_url: impl Into<String>,
        org: impl Into<String>,
        stream: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        options: OpenObserveOptions,
    ) -> Result<Self> {
        let base_url = base_url.into();
        let org = org.into();
        let client = tls::build_client(&options.tls)?;
        // Correct OpenObserve bulk endpoint: /api/{org}/_bulk.
        // Stream is set per-document in the action line _index field.
        let bulk_url = format!("{}/api/{}/_bulk", base_url, org);

        Ok(Self {
            ctx: Arc::new(SinkContext {
                client,
                bulk_url,
                username: username.into(),
                password: password.into(),
                options,
            }),
            stream: stream.into(),
            metrics: metrics::Metrics::default(),
        })
    }
}

#[async_trait]
impl Destination for OpenObserveDestination {
    async fn write_batch(&mut self, docs: Vec<Document>) -> Result<usize> {
        if docs.is_empty() {
            return Ok(0);
        }
        self.metrics.record_submitted(docs.len());

        // Stamp and route every document.
        let routed: Vec<RoutedDoc> = docs
            .into_iter()
            .map(|mut doc| {
                timestamp::apply(&mut doc, &self.ctx.options);
                let stream = routing::stream_for(&doc, &self.ctx.options, &self.stream);
                RoutedDoc { stream, doc }
            })
            .collect();

        // Build one or more bulk payloads (byte-size aware in lane 4).
        let chunks = build::chunks(routed, &self.ctx.options)?;
        debug!(
            "POSTing {} bulk request(s) to {}",
            chunks.len(),
            self.ctx.bulk_url
        );

        // Send, retrying transient failures, across the concurrency window.
        let ctx = self.ctx.clone();
        let max_in_flight = ctx.options.max_in_flight;
        let outcome = pipeline::run(chunks, max_in_flight, move |chunk| {
            let ctx = ctx.clone();
            async move { send_chunk(&ctx, chunk).await }
        })
        .await?;

        self.metrics.record_outcome(&outcome);

        if !outcome.rejected.is_empty() {
            warn!(
                "OpenObserve rejected {} of {} documents",
                outcome.rejected.len(),
                outcome.accepted + outcome.rejected.len()
            );
            deadletter::write(&self.ctx.options, &outcome.rejected)?;
        }

        Ok(outcome.accepted)
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("OpenObserve {} stream={}", self.ctx.bulk_url, self.stream)
    }
}

/// Send one chunk with retry, then account its accepted/rejected documents.
async fn send_chunk(ctx: &SinkContext, chunk: BulkChunk) -> Result<BulkOutcome> {
    let resp = crate::http::retry::run(&ctx.options.retry, || async {
        let resp = transport::send(ctx, &chunk).await?;
        transport::classify(resp).await
    })
    .await?;

    response::parse(resp, &chunk.docs).await
}
