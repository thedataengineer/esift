//! Send a prepared bulk payload and classify the HTTP response.
//!
//! Foundation stub: a plain POST with basic/token auth and no compression, then
//! status classification. Lane 3 adds gzip request compression here.

use super::config::Compression;
use super::types::BulkChunk;
use super::{auth, SinkContext};
use crate::error::{EsiftError, Result};
use flate2::write::GzEncoder;
use flate2::Compression as GzLevel;
use std::io::Write;

/// POST one chunk to the bulk endpoint.
pub(crate) async fn send(ctx: &SinkContext, chunk: &BulkChunk) -> Result<reqwest::Response> {
    let builder = ctx
        .client
        .post(&ctx.bulk_url)
        .header("Content-Type", "application/x-ndjson");
    let builder = auth::apply(builder, ctx);
    // body is cloned/encoded fresh because retry may resend.
    let builder = match ctx.options.compression {
        Compression::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), GzLevel::default());
            encoder.write_all(chunk.body.as_bytes())?;
            let compressed = encoder.finish()?;
            builder.header("Content-Encoding", "gzip").body(compressed)
        }
        Compression::None => builder.body(chunk.body.clone()),
    };
    Ok(builder.send().await?)
}

/// Map an HTTP status to a retry decision. 2xx passes the response through for
/// body parsing; 429 and 5xx become `Transient` (retryable); other non-2xx
/// become a terminal `Destination` error. The error message carries the status
/// and body for diagnostics.
pub(crate) async fn classify(resp: reqwest::Response) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }

    let body = resp
        .text()
        .await
        .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
    let msg = format!("OpenObserve bulk failed: HTTP {} — {}", status, body);

    if status.as_u16() == 429 || status.is_server_error() {
        Err(EsiftError::Transient(msg))
    } else {
        Err(EsiftError::Destination(msg))
    }
}
