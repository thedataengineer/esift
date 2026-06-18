//! Integration tests for the OpenSearch source: PIT + search_after pagination
//! and error-body propagation, driven against a wiremock HTTP server. These run
//! on the default feature set (no `aws`), so `cargo test` exercises them.

use esift_core::source::{
    opensearch::{Auth, OpenSearchSource},
    Source,
};
use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn paginates_with_pit_and_advances_cursor() {
    let server = MockServer::start().await;

    // PIT create (OpenSearch flavor).
    Mock::given(method("POST"))
        .and(path("/logs/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pit_id": "PIT123" })))
        .mount(&server)
        .await;

    // First /_search: a full page of two hits with a `sort` cursor. Priority 1
    // (lower number wins) and one-shot, so it serves exactly the first call.
    Mock::given(method("POST"))
        .and(path("/_search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "pit_id": "PIT123",
            "hits": { "hits": [
                { "_id": "1", "_index": "logs", "_source": { "msg": "a" }, "sort": ["1"] },
                { "_id": "2", "_index": "logs", "_source": { "msg": "b" }, "sort": ["2"] }
            ] }
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;

    // Catch-all for later searches: an empty page signals exhaustion. Default
    // priority (5), so it only wins once the one-shot page mock is spent.
    Mock::given(method("POST"))
        .and(path("/_search"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "pit_id": "PIT123", "hits": { "hits": [] } })),
        )
        .mount(&server)
        .await;

    // PIT delete on close.
    Mock::given(method("DELETE"))
        .and(path("/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "succeeded": true })))
        .mount(&server)
        .await;

    let mut source = OpenSearchSource::new(
        server.uri(),
        "logs",
        json!({ "match_all": {} }),
        2,
        Auth::None,
        None,
    )
    .unwrap();

    source.open().await.unwrap();

    let first = source
        .next_batch()
        .await
        .unwrap()
        .expect("first page present");
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].id, "1");
    assert_eq!(first[0].index, "logs");
    assert_eq!(first[0].body, json!({ "msg": "a" }));
    // Cursor must advance to the last hit's sort value so a resume continues here.
    assert_eq!(source.cursor(), Some(vec![json!("2")]));

    let second = source.next_batch().await.unwrap();
    assert!(second.is_none(), "empty page should exhaust the source");

    source.close().await.unwrap();
}

#[tokio::test]
async fn search_error_propagates_status_and_body() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/logs/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pit_id": "P" })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/_search"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom-reason"))
        .mount(&server)
        .await;

    let mut source = OpenSearchSource::new(
        server.uri(),
        "logs",
        json!({ "match_all": {} }),
        10,
        Auth::None,
        None,
    )
    .unwrap();

    source.open().await.unwrap();
    let err = source.next_batch().await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("500"), "status missing from error: {msg}");
    assert!(
        msg.contains("boom-reason"),
        "body missing from error: {msg}"
    );
}

#[tokio::test]
async fn open_falls_back_to_elasticsearch_pit_path() {
    let server = MockServer::start().await;

    // OpenSearch PIT path 404s; source should fall back to the ES `_pit` path.
    Mock::given(method("POST"))
        .and(path("/logs/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(404).set_body_string("no such path"))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/logs/_pit"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "ES_PIT" })))
        .mount(&server)
        .await;

    let mut source = OpenSearchSource::new(
        server.uri(),
        "logs",
        json!({ "match_all": {} }),
        10,
        Auth::None,
        None,
    )
    .unwrap();

    source.open().await.expect("ES fallback should open a PIT");
}

#[tokio::test]
async fn resume_seeds_saved_cursor_into_first_query() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/logs/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pit_id": "P" })))
        .mount(&server)
        .await;

    // This mock only matches if the search body carries the seeded cursor. If
    // the resume seed regressed (the original bug), the body would lack
    // `search_after`, the request would 404, and next_batch would error.
    Mock::given(method("POST"))
        .and(path("/_search"))
        .and(body_partial_json(json!({ "search_after": ["5"] })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "hits": { "hits": [] } })))
        .mount(&server)
        .await;

    let mut source = OpenSearchSource::new(
        server.uri(),
        "logs",
        json!({ "match_all": {} }),
        10,
        Auth::None,
        Some(vec![json!("5")]),
    )
    .unwrap();

    source.open().await.unwrap();
    let batch = source
        .next_batch()
        .await
        .expect("seeded cursor should produce a matching, successful query");
    assert!(batch.is_none());
}
