//! Optional Prometheus-style metrics HTTP endpoint.
//!
//! Foundation stub: `serve` logs that the endpoint was requested and returns,
//! so nothing is served yet. Lane 5 starts a small HTTP server on `addr` that
//! exposes [`SharedMetrics`](crate::metrics::SharedMetrics) in Prometheus text
//! format at `/metrics`.

use crate::metrics::SharedMetrics;
use anyhow::Result;

pub async fn serve(addr: String, metrics: SharedMetrics) -> Result<()> {
    let (docs, batches, errors) = metrics.snapshot();
    tracing::warn!(
        "metrics endpoint requested at {addr} ({docs} docs, {batches} batches, {errors} errors) \
         but is not yet implemented"
    );
    Ok(())
}
