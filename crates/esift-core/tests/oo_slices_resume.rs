//! Integration test: a sliced run (slices > 1) resumes mid-flight from the
//! checkpoint cursor. Phase 1 drains one slice and captures `cursor()`; phase 2
//! builds a fresh source seeded with that cursor and must continue from each
//! slice's saved position rather than restarting.

use esift_core::source::{
    opensearch::{Auth, OpenSearchSource},
    Source,
};
use serde_json::{json, Value};
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn build(uri: String, resume: Option<Vec<Value>>) -> OpenSearchSource {
    OpenSearchSource::new(
        uri,
        "logs",
        json!({ "match_all": {} }),
        1,
        Auth::None,
        resume,
    )
    .unwrap()
    .with_slices(2)
}

#[tokio::test]
async fn sliced_run_resumes_from_checkpoint_cursor() {
    let server = MockServer::start().await;

    // PIT create + delete, used by both phases.
    Mock::given(method("POST"))
        .and(path("/logs/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pit_id": "PIT123" })))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_search/point_in_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "succeeded": true })))
        .mount(&server)
        .await;

    // Phase 1: slice 0's first page (no search_after) yields one doc.
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

    // Phase 2: slice 0 resumed (search_after carries the saved cursor) is empty.
    Mock::given(method("POST"))
        .and(path("/_search"))
        .and(body_partial_json(
            json!({ "slice": { "id": 0, "max": 2 }, "search_after": ["s0-1"] }),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "pit_id": "PIT123", "hits": { "hits": [] } })),
        )
        .with_priority(1)
        .mount(&server)
        .await;

    // Slice 1's first page yields one doc; its follow-up is empty.
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

    // Per-slice catch-alls return empty pages once the one-shot mocks are spent.
    Mock::given(method("POST"))
        .and(path("/_search"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "pit_id": "PIT123", "hits": { "hits": [] } })),
        )
        .with_priority(5)
        .mount(&server)
        .await;

    // --- Phase 1: drain a single batch (slice 0), capture the cursor. ---
    let mut s1 = build(server.uri(), None);
    s1.open().await.unwrap();
    let first = s1.next_batch().await.unwrap().expect("first batch present");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].id, "s0-1");
    let resume = s1.cursor().expect("sliced run exposes a cursor");
    s1.close().await.unwrap();

    // --- Phase 2: resume from the captured cursor. ---
    let mut s2 = build(server.uri(), Some(resume));
    s2.open().await.unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = s2.next_batch().await.unwrap() {
        for doc in batch {
            ids.push(doc.id);
        }
    }
    s2.close().await.unwrap();

    // Slice 0 was already consumed, so only slice 1's doc arrives — the resumed
    // run does not re-emit s0-1.
    assert_eq!(
        ids,
        vec!["s1-1".to_string()],
        "resume must not re-emit s0-1"
    );

    // Prove the seeded cursor reached the wire: some slice-0 search carried
    // search_after ["s0-1"].
    let requests = server.received_requests().await.unwrap();
    let seeded = requests.iter().any(|r| {
        serde_json::from_slice::<Value>(&r.body)
            .ok()
            .map(|b| b["slice"]["id"] == json!(0) && b["search_after"] == json!(["s0-1"]))
            .unwrap_or(false)
    });
    assert!(
        seeded,
        "phase 2 must send slice 0 with the resumed search_after"
    );
}
