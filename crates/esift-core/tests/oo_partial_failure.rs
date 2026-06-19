//! Integration test for the OpenObserve destination's partial-failure
//! accounting: a 200 response with `errors:true` and a mix of accepted and
//! rejected items must yield only the accepted count.

use esift_core::dest::{
    openobserve::{OpenObserveDestination, OpenObserveOptions},
    Destination,
};
use esift_core::Document;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn bulk_counts_only_accepted_on_partial_failure() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/default/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": true,
            "items": [
                { "index": { "status": 200 } },
                { "index": { "status": 400, "error": { "reason": "bad" } } },
            ],
        })))
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

    assert_eq!(written, 1);
}
