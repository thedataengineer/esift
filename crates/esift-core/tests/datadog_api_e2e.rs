//! Holistic Datadog Logs Search API E2E (Lane 5 scope for Path 2).
//!
//! Drives [`DatadogApiSource`] against a `wiremock` server over a MULTI-window
//! time range with multiple pages per window, and asserts the cross-window
//! contract: every expected document arrives exactly once (no duplicates, no
//! gaps at window boundaries) and the total count is exact.
//!
//! Window plan: `[00:00, 03:00)` at 60-minute windows => 3 windows. Each window
//! serves two pages: page 1 returns a per-window cursor, page 2 drains the
//! window with `meta.page.after: null`. Mocks key page 1 on the window's
//! `filter.from` (so windows are disjoint) and page 2 on the window's unique
//! cursor.

#![cfg(feature = "datadog-api")]

use esift_core::source::datadog::api::DatadogApiSource;
use esift_core::source::Source;
use serde_json::json;
use std::collections::HashSet;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SEARCH_PATH: &str = "/api/v2/logs/events/search";

/// Mount the two pages for one window, keyed so requests route unambiguously.
async fn mount_window(
    server: &MockServer,
    window_from: &str,
    cursor: &str,
    page1_ids: &[&str],
    page2_ids: &[&str],
) {
    let event = |id: &str| {
        json!({
            "id": id,
            "type": "log",
            "attributes": {
                "timestamp": "2025-01-01T00:00:00Z",
                "service": "web",
                "attributes": { "doc_id": id }
            }
        })
    };

    // Page 1: matched by this window's `from` and the ABSENCE of a cursor is
    // implied by priority (the cursor mock for this window has higher priority
    // for cursor-bearing requests). Returns the per-window cursor.
    let page1: Vec<_> = page1_ids.iter().map(|id| event(id)).collect();
    Mock::given(method("POST"))
        .and(path(SEARCH_PATH))
        .and(body_partial_json(
            json!({ "filter": { "from": window_from } }),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": page1,
            "meta": { "page": { "after": cursor } }
        })))
        .up_to_n_times(1)
        .with_priority(2)
        .mount(server)
        .await;

    // Page 2: matched by this window's unique cursor. Drains the window.
    let page2: Vec<_> = page2_ids.iter().map(|id| event(id)).collect();
    Mock::given(method("POST"))
        .and(path(SEARCH_PATH))
        .and(body_partial_json(json!({ "page": { "cursor": cursor } })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": page2,
            "meta": { "page": { "after": null } }
        })))
        .with_priority(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn multi_window_pagination_no_dups_no_gaps() {
    let server = MockServer::start().await;

    // Window 0: [00:00, 01:00)
    mount_window(
        &server,
        "2025-01-01T00:00:00Z",
        "W0C1",
        &["w0-a", "w0-b"],
        &["w0-c"],
    )
    .await;
    // Window 1: [01:00, 02:00)
    mount_window(
        &server,
        "2025-01-01T01:00:00Z",
        "W1C1",
        &["w1-a", "w1-b"],
        &["w1-c"],
    )
    .await;
    // Window 2: [02:00, 03:00)
    mount_window(
        &server,
        "2025-01-01T02:00:00Z",
        "W2C1",
        &["w2-a", "w2-b"],
        &["w2-c"],
    )
    .await;

    let mut src = DatadogApiSource::new(
        "datadoghq.com",
        "api-key",
        "app-key",
        "service:web",
        Some("2025-01-01T00:00:00Z".into()),
        Some("2025-01-01T03:00:00Z".into()),
        60,
        None,
    )
    .unwrap()
    .with_base_url(server.uri());

    src.open().await.unwrap();

    let mut ids = Vec::new();
    while let Some(batch) = src.next_batch().await.unwrap() {
        for doc in batch {
            assert_eq!(doc.index, "datadog");
            // Flatten collapsed the double-nested attributes.
            assert_eq!(
                doc.body.get("service").and_then(|v| v.as_str()),
                Some("web")
            );
            assert_eq!(
                doc.body.get("doc_id").and_then(|v| v.as_str()),
                Some(doc.id.as_str())
            );
            ids.push(doc.id);
        }
    }

    // Exact expected set across all three windows.
    let expected: Vec<String> = [
        "w0-a", "w0-b", "w0-c", "w1-a", "w1-b", "w1-c", "w2-a", "w2-b", "w2-c",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    // No duplicates.
    let unique: HashSet<&String> = ids.iter().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "duplicate documents emitted across window boundaries: {ids:?}"
    );

    // No missing docs and exact total count.
    let mut sorted = ids.clone();
    sorted.sort();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(
        sorted, expected_sorted,
        "emitted document set must equal the expected set exactly"
    );
    assert_eq!(ids.len(), 9, "exact total document count");

    // Exhausted and stays exhausted.
    assert!(src.next_batch().await.unwrap().is_none());
    src.close().await.unwrap();
}
