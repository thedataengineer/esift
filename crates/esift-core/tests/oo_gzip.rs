//! Integration test for gzip request compression on the OpenObserve _bulk
//! path: the mock only matches when the request carries
//! `Content-Encoding: gzip`, so a passing assertion proves the body was sent
//! compressed.

use esift_core::dest::{
    openobserve::{config::Compression, OpenObserveDestination, OpenObserveOptions},
    Destination,
};
use esift_core::Document;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn gzip_sets_content_encoding_header() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/default/_bulk"))
        .and(header("content-encoding", "gzip"))
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
            compression: Compression::Gzip,
            ..Default::default()
        },
    )
    .unwrap();

    let written = dest
        .write_batch(vec![Document::new("logs", "1", json!({ "a": 1 }))])
        .await
        .unwrap();

    assert_eq!(written, 1);
}
