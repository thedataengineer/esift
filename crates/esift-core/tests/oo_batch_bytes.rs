//! Integration test for the OpenObserve sink's byte-size batch cap: a small
//! `max_batch_bytes` should split one `write_batch` across multiple `_bulk`
//! requests while still accepting every document.

use esift_core::dest::{
    openobserve::{OpenObserveDestination, OpenObserveOptions},
    Destination,
};
use esift_core::Document;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn batch_bytes_cap_splits_into_multiple_requests() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/default/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false })))
        .mount(&server)
        .await;

    let mut dest = OpenObserveDestination::new(
        server.uri(),
        "default",
        "mystream",
        "user",
        "pass",
        OpenObserveOptions {
            max_batch_bytes: Some(60),
            ..Default::default()
        },
    )
    .unwrap();

    let docs: Vec<Document> = (0..6)
        .map(|i| Document::new("logs", i.to_string(), json!({ "a": i })))
        .collect();

    let written = dest.write_batch(docs).await.unwrap();
    assert_eq!(written, 6, "every document should be accepted");

    let reqs = server.received_requests().await.unwrap();
    assert!(
        reqs.len() >= 2,
        "expected the byte cap to split into >= 2 requests, got {}",
        reqs.len()
    );
}
