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
//! accounted (`response`) with rejects sent to a dead-letter sink (`deadletter`).
//! Throughput is reported through the global `metrics` facade so the counters
//! surface on the binary's Prometheus endpoint alongside everything else. Each
//! seam ships a behavior-preserving stub that the corresponding feature lane
//! fleshes out independently.

mod auth;
mod build;
pub mod config;
mod deadletter;
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
use metrics::{counter, describe_counter};
use std::sync::Arc;
use tracing::{debug, warn};

/// Counter: documents handed to the sink in a batch.
const SUBMITTED: &str = "esift_oo_submitted_total";
/// Counter: documents OpenObserve accepted.
const ACCEPTED: &str = "esift_oo_accepted_total";
/// Counter: documents OpenObserve rejected.
const REJECTED: &str = "esift_oo_rejected_total";

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

        describe_counter!(SUBMITTED, "Documents submitted to OpenObserve in a batch");
        describe_counter!(ACCEPTED, "Documents accepted by OpenObserve");
        describe_counter!(REJECTED, "Documents rejected by OpenObserve");

        Ok(Self {
            ctx: Arc::new(SinkContext {
                client,
                bulk_url,
                username: username.into(),
                password: password.into(),
                options,
            }),
            stream: stream.into(),
        })
    }
}

#[async_trait]
impl Destination for OpenObserveDestination {
    async fn write_batch(&mut self, docs: Vec<Document>) -> Result<usize> {
        if docs.is_empty() {
            return Ok(0);
        }
        let submitted = docs.len();
        counter!(SUBMITTED).increment(submitted as u64);

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

        let rejected = outcome.rejected.len();
        record_outcome_metrics(&outcome);

        debug!(
            submitted,
            accepted = outcome.accepted,
            rejected,
            "OpenObserve bulk batch complete"
        );

        if rejected > 0 {
            warn!(
                "OpenObserve rejected {} of {} documents",
                rejected,
                outcome.accepted + rejected
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

/// Emit the accepted/rejected facade counters for one bulk outcome.
fn record_outcome_metrics(outcome: &BulkOutcome) {
    counter!(ACCEPTED).increment(outcome.accepted as u64);
    counter!(REJECTED).increment(outcome.rejected.len() as u64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusBuilder;

    /// A representative outcome flowing through the sink emits the submitted,
    /// accepted, and rejected facade counters so they surface on the Prometheus
    /// endpoint the binary installs.
    #[test]
    fn outcome_increments_facade_counters() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            // `submitted` is emitted up front in `write_batch`; the accepted and
            // rejected counters come from the real outcome-recording path.
            counter!(SUBMITTED).increment(5);
            let outcome = BulkOutcome {
                accepted: 4,
                rejected: vec![RejectedDoc {
                    stream: "logs".to_string(),
                    reason: "schema mismatch".to_string(),
                    body: serde_json::json!({ "k": "v" }),
                }],
            };
            record_outcome_metrics(&outcome);
        });

        let rendered = handle.render();
        assert!(
            rendered.contains("esift_oo_submitted_total 5"),
            "missing submitted counter in:\n{rendered}"
        );
        assert!(
            rendered.contains("esift_oo_accepted_total 4"),
            "missing accepted counter in:\n{rendered}"
        );
        assert!(
            rendered.contains("esift_oo_rejected_total 1"),
            "missing rejected counter in:\n{rendered}"
        );
    }
}
