//! Optional Prometheus-style metrics HTTP endpoint.
//!
//! [`serve`] starts a small HTTP server on `addr` that exposes
//! [`SharedMetrics`](crate::metrics::SharedMetrics) in Prometheus text format.
//! The path is ignored: every request gets the metrics body.

use crate::metrics::SharedMetrics;
use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Render the current metrics snapshot as Prometheus text exposition format.
pub fn render_prometheus(metrics: &SharedMetrics) -> String {
    let (docs, batches, errors) = metrics.snapshot();
    let mut out = String::new();
    render_counter(
        &mut out,
        "esift_docs_written_total",
        "Total documents written to the destination.",
        docs,
    );
    render_counter(
        &mut out,
        "esift_batches_total",
        "Total batches written to the destination.",
        batches,
    );
    render_counter(
        &mut out,
        "esift_errors_total",
        "Total source or destination errors.",
        errors,
    );
    out
}

fn render_counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" counter\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

/// Bind to `addr` and serve the metrics snapshot over HTTP until the process
/// exits. Per-connection errors are logged and do not stop the accept loop.
pub async fn serve(addr: String, metrics: SharedMetrics) -> Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("metrics endpoint listening on {addr}");
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let body = render_prometheus(&metrics);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, body).await {
                        tracing::warn!("metrics connection error: {e}");
                    }
                });
            }
            Err(e) => {
                tracing::warn!("metrics accept error: {e}");
            }
        }
    }
}

/// Read the request line (and trust the path), then write the metrics body as a
/// minimal HTTP/1.1 200 response.
async fn handle_connection(mut stream: TcpStream, body: String) -> Result<()> {
    // Drain the request line; we do not route on the path.
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf).await?;

    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use std::sync::Arc;

    #[test]
    fn render_prometheus_emits_headers_and_values() {
        let metrics = Metrics::default();
        metrics.record_batch(40); // docs_written += 40, batches += 1
        metrics.record_batch(2); // docs_written == 42 total, batches == 2 total
        metrics.record_error();
        metrics.record_error();
        metrics.record_error(); // errors == 3
        let shared: SharedMetrics = Arc::new(metrics);

        let out = render_prometheus(&shared);

        // Counter values.
        assert!(
            out.contains("esift_docs_written_total 42\n"),
            "missing docs line:\n{out}"
        );
        assert!(
            out.contains("esift_batches_total 2\n"),
            "missing batches line:\n{out}"
        );
        assert!(
            out.contains("esift_errors_total 3\n"),
            "missing errors line:\n{out}"
        );

        // TYPE headers.
        assert!(out.contains("# TYPE esift_docs_written_total counter\n"));
        assert!(out.contains("# TYPE esift_batches_total counter\n"));
        assert!(out.contains("# TYPE esift_errors_total counter\n"));

        // HELP headers.
        assert!(out.contains("# HELP esift_docs_written_total "));
        assert!(out.contains("# HELP esift_batches_total "));
        assert!(out.contains("# HELP esift_errors_total "));
    }

    #[test]
    fn render_prometheus_zeroed_by_default() {
        let shared: SharedMetrics = Arc::new(Metrics::default());
        let out = render_prometheus(&shared);
        assert!(out.contains("esift_docs_written_total 0\n"));
        assert!(out.contains("esift_batches_total 0\n"));
        assert!(out.contains("esift_errors_total 0\n"));
    }
}
