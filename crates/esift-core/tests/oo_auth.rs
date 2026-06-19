//! Integration test for the OpenObserve destination's authentication path:
//! when `token` is set, the bulk request carries a bearer token rather than
//! basic auth. Driven against a wiremock HTTP server.

use esift_core::dest::{
    openobserve::{OpenObserveDestination, OpenObserveOptions},
    Destination,
};
use esift_core::Document;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn token_option_sends_bearer_auth_header() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/default/_bulk"))
        .and(header("authorization", "Bearer testtoken"))
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
            token: Some("testtoken".into()),
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
