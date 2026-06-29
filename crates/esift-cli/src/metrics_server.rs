//! Optional Prometheus metrics HTTP endpoint.
//!
//! [`serve`] starts a small HTTP server on `addr` that renders the process's
//! metrics — recorded through the [`metrics`] facade into a
//! [`PrometheusHandle`] — in Prometheus text exposition format. The request
//! path is ignored: every request gets the metrics body.

use anyhow::Result;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Bind to `addr` and serve the current metrics snapshot over HTTP until the
/// process exits. Per-connection errors are logged and do not stop the accept
/// loop. The body is rendered fresh per request from `handle`.
pub async fn serve(addr: String, handle: PrometheusHandle) -> Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("metrics endpoint listening on {addr}");
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let body = handle.render();
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

/// Read (and ignore) the request line, then write `body` as a minimal HTTP/1.1
/// 200 response in Prometheus text format.
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
    use metrics_exporter_prometheus::PrometheusBuilder;

    /// A local (non-global) recorder lets us record through the facade and
    /// confirm the handle renders the values in Prometheus text format.
    #[test]
    fn handle_renders_recorded_counters() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("esift_docs_written_total").increment(42);
            metrics::counter!("esift_errors_total").increment(3);
        });

        let out = handle.render();
        assert!(
            out.contains("esift_docs_written_total 42"),
            "missing docs counter:\n{out}"
        );
        assert!(
            out.contains("esift_errors_total 3"),
            "missing errors counter:\n{out}"
        );
        // The exporter emits the standard TYPE header for each counter.
        assert!(out.contains("# TYPE esift_docs_written_total counter"));
    }
}
