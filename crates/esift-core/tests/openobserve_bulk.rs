//! Integration tests for the OpenObserve destination's _bulk ingest path,
//! driven against a wiremock HTTP server.

use esift_core::dest::{
    openobserve::{OpenObserveDestination, OpenObserveOptions},
    Destination,
};
use esift_core::Document;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn bulk_returns_accepted_count_on_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/default/_bulk"))
        .and(header("content-type", "application/x-ndjson"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false })))
        .mount(&server)
        .await;

    let mut dest = OpenObserveDestination::new(
        server.uri(),
        "default",
        "mystream",
        "user",
        "pass",
        OpenObserveOptions::default(),
    )
    .unwrap();

    let written = dest
        .write_batch(vec![
            Document::new("logs", "1", json!({ "a": 1 })),
            Document::new("logs", "2", json!({ "a": 2 })),
        ])
        .await
        .unwrap();

    assert_eq!(written, 2);
}

#[tokio::test]
async fn bulk_error_propagates_status_and_body() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/default/_bulk"))
        .respond_with(ResponseTemplate::new(500).set_body_string("ingest-down"))
        .mount(&server)
        .await;

    let mut dest = OpenObserveDestination::new(
        server.uri(),
        "default",
        "mystream",
        "user",
        "pass",
        OpenObserveOptions::default(),
    )
    .unwrap();

    let err = dest
        .write_batch(vec![Document::new("logs", "1", json!({ "a": 1 }))])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("500"), "status missing from error: {msg}");
    assert!(
        msg.contains("ingest-down"),
        "body missing from error: {msg}"
    );
}

#[tokio::test]
async fn empty_batch_is_a_noop() {
    // No server required: an empty batch short-circuits before any request.
    let mut dest = OpenObserveDestination::new(
        "http://unused.invalid",
        "default",
        "s",
        "u",
        "p",
        OpenObserveOptions::default(),
    )
    .unwrap();
    assert_eq!(dest.write_batch(vec![]).await.unwrap(), 0);
}
