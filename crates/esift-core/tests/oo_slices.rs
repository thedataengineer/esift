//! Integration test for sliced parallel extraction (sliced PIT). With
//! `slices > 1` the source opens one PIT and maintains one `search_after`
//! cursor per slice, tagging each `_search` body with `{"slice": {"id", "max"}}`.
//! It drains slices round-robin and is exhausted only once every slice empties.
//! Runs on the default feature set (no `aws`), so `cargo test` exercises it.

use esift_core::source::{
    opensearch::{Auth, OpenSearchSource},
    Source,
};
use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn sliced_extraction_drains_all_slices_and_completes() {
    let server = MockServer::start().await;

    // One PIT for the whole run (OpenSearch flavor).
    Mock::given(method("POST"))
        .and(path("/logs/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pit_id": "PIT123" })))
        .mount(&server)
        .await;

    // Slice 0: a single full page (one hit), then empty pages thereafter.
    // The one-shot page mock matches only bodies carrying slice id 0 and wins
    // on priority; the per-slice catch-all serves the follow-up empty page.
    Mock::given(method("POST"))
        .and(path("/_search"))
        .and(body_partial_json(json!({ "slice": { "id": 0, "max": 2 } })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "pit_id": "PIT123",
            "hits": { "hits": [
                { "_id": "s0-1", "_index": "logs", "_source": { "msg": "zero" }, "sort": ["s0-1"] }
            ] }
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/_search"))
        .and(body_partial_json(json!({ "slice": { "id": 0, "max": 2 } })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "pit_id": "PIT123", "hits": { "hits": [] } })),
        )
        .with_priority(2)
        .mount(&server)
        .await;

    // Slice 1: same shape, one hit then empty.
    Mock::given(method("POST"))
        .and(path("/_search"))
        .and(body_partial_json(json!({ "slice": { "id": 1, "max": 2 } })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "pit_id": "PIT123",
            "hits": { "hits": [
                { "_id": "s1-1", "_index": "logs", "_source": { "msg": "one" }, "sort": ["s1-1"] }
            ] }
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/_search"))
        .and(body_partial_json(json!({ "slice": { "id": 1, "max": 2 } })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "pit_id": "PIT123", "hits": { "hits": [] } })),
        )
        .with_priority(2)
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
        // batch_size 1: a one-hit page is "full", so exhaustion comes from the
        // follow-up empty page rather than an early short-page signal.
        1,
        Auth::None,
        None,
    )
    .unwrap()
    .with_slices(2);

    source.open().await.unwrap();

    // With slices > 1 the per-slice cursors are encoded into one opaque value,
    // so a sliced run exposes a resume cursor once the PIT is open.
    assert!(
        source.cursor().is_some(),
        "sliced runs expose an encoded resume cursor"
    );

    // Drain every batch until the source reports exhaustion, collecting ids.
    let mut ids = Vec::new();
    while let Some(batch) = source.next_batch().await.unwrap() {
        for doc in batch {
            assert_eq!(doc.index, "logs");
            ids.push(doc.id);
        }
        assert!(source.cursor().is_some(), "cursor present mid-run");
    }

    ids.sort();
    assert_eq!(
        ids,
        vec!["s0-1".to_string(), "s1-1".to_string()],
        "documents from both slices must arrive"
    );

    // Once exhausted the source stays exhausted.
    assert!(source.next_batch().await.unwrap().is_none());

    source.close().await.unwrap();
}
