//! Datadog Logs Search API source (Path 2). Requires the `datadog-api` feature.
//!
//! Reads logs via `POST /api/v2/logs/events/search`, cursor-paginated and
//! time-window chunked. The `[from, to]` range is split into `window_minutes`
//! windows; each window is paginated to exhaustion (following
//! `meta.page.after`) before the next window opens. Regional base URL comes
//! from [`super::site::base_url`]; each `data[]` event is collapsed by
//! [`super::flatten_api::flatten`]. On HTTP 429 the driver honours
//! `X-RateLimit-Reset` (unix seconds), sleeps to that instant plus a small
//! jitter, and retries.
//!
//! Resume: [`DatadogApiSource::cursor`] self-encodes the live window bounds,
//! the in-window cursor, and the count of finished windows into the opaque
//! `Vec<Value>` checkpoint channel. [`DatadogApiSource::open`] decodes that blob
//! (when present) to continue mid-window.

use crate::error::{EsiftError, Result};
use crate::source::Source;
use crate::Document;
use async_trait::async_trait;
use serde_json::Value;

#[cfg(feature = "datadog-api")]
use reqwest::{header, Client};
#[cfg(feature = "datadog-api")]
use serde_json::json;
#[cfg(feature = "datadog-api")]
use tracing::{debug, info, warn};

/// Index label attached to every emitted [`Document`]. The Logs Search API has
/// no per-event index, so a stable constant keeps downstream routing simple.
#[cfg(feature = "datadog-api")]
const DD_INDEX: &str = "datadog";

/// Page size requested from the Logs Search API.
#[cfg(feature = "datadog-api")]
const PAGE_LIMIT: u64 = 1000;

// --- Metric names -----------------------------------------------------------
//
// Conventions: `esift_`-prefixed; counters end `_total`; duration histograms end
// `_seconds`. Labels are kept low-cardinality (never the query, cursor, or
// window bounds — those live in tracing fields).

/// Count of HTTP requests issued to the Logs Search API (one per attempt,
/// including 429 retries). Labelled `result` = `ok` | `error`.
#[cfg(feature = "datadog-api")]
const M_REQUESTS_TOTAL: &str = "esift_datadog_api_requests_total";
/// Wall-clock duration of each Logs Search request/response round-trip
/// (seconds). Labelled `result` = `ok` | `error`.
#[cfg(feature = "datadog-api")]
const M_REQUEST_SECONDS: &str = "esift_datadog_api_request_seconds";
/// Count of pages fetched (successful responses) from the Logs Search API.
#[cfg(feature = "datadog-api")]
const M_PAGES_TOTAL: &str = "esift_datadog_api_pages_total";
/// Count of documents emitted, incremented by each batch's size.
#[cfg(feature = "datadog-api")]
const M_DOCS_TOTAL: &str = "esift_datadog_api_docs_total";
/// Count of time-windows fully drained.
#[cfg(feature = "datadog-api")]
const M_WINDOWS_TOTAL: &str = "esift_datadog_api_windows_total";
/// Count of HTTP 429 rate-limit responses encountered.
#[cfg(feature = "datadog-api")]
const M_RATE_LIMITED_TOTAL: &str = "esift_datadog_rate_limited_total";
/// Duration slept honouring a rate-limit backoff (seconds).
#[cfg(feature = "datadog-api")]
const M_RATE_LIMIT_WAIT_SECONDS: &str = "esift_datadog_rate_limit_wait_seconds";

/// Register descriptions/units for every metric this source emits. Idempotent
/// and cheap; called once from [`DatadogApiSource::open`]. Without a global
/// recorder installed (e.g. unit tests) these calls are no-ops.
#[cfg(feature = "datadog-api")]
fn describe_metrics() {
    use metrics::{describe_counter, describe_histogram, Unit};

    describe_counter!(
        M_REQUESTS_TOTAL,
        "Total HTTP requests to the Datadog Logs Search API (per attempt, including 429 retries)"
    );
    describe_histogram!(
        M_REQUEST_SECONDS,
        Unit::Seconds,
        "Latency of each Datadog Logs Search API request/response"
    );
    describe_counter!(
        M_PAGES_TOTAL,
        "Total paginated pages fetched from the Datadog Logs Search API"
    );
    describe_counter!(
        M_DOCS_TOTAL,
        "Total documents emitted from the Datadog Logs Search API"
    );
    describe_counter!(
        M_WINDOWS_TOTAL,
        "Total time-windows fully drained by the Datadog Logs Search API source"
    );
    describe_counter!(
        M_RATE_LIMITED_TOTAL,
        "Total HTTP 429 rate-limit responses from the Datadog Logs Search API"
    );
    describe_histogram!(
        M_RATE_LIMIT_WAIT_SECONDS,
        Unit::Seconds,
        "Duration slept honouring a Datadog Logs Search API rate-limit backoff"
    );
}

// Without the feature, only `site` is read (by the fallback `Source` impl); the
// remaining fields exist so the constructor signature is stable across builds.
#[cfg_attr(not(feature = "datadog-api"), allow(dead_code))]
pub struct DatadogApiSource {
    site: String,
    api_key: String,
    app_key: String,
    query: String,
    from: Option<String>,
    to: Option<String>,
    window_minutes: u64,
    /// Opaque resume blob from a prior checkpoint cursor.
    resume_after: Option<Vec<Value>>,

    /// Built in `open()`; the HTTP client carrying the auth headers.
    #[cfg(feature = "datadog-api")]
    client: Option<Client>,
    /// Resolved API base URL (or a test override via [`Self::with_base_url`]).
    #[cfg(feature = "datadog-api")]
    base_url: Option<String>,
    /// Test-only base-URL override, applied in `open()` instead of `site`.
    #[cfg(feature = "datadog-api")]
    base_url_override: Option<String>,
    /// The windows still to process, in order. The front element is the live
    /// window. Each is a half-open `[from, to]` ISO8601 pair (shared boundary
    /// between adjacent windows; the per-window cursor prevents double reads).
    #[cfg(feature = "datadog-api")]
    windows: std::collections::VecDeque<Window>,
    /// Pagination cursor inside the live window. `None` means "first page of the
    /// live window"; the window is exhausted when a response carries no
    /// `meta.page.after`.
    #[cfg(feature = "datadog-api")]
    after: Option<String>,
    /// Count of fully drained windows, surfaced in the resume cursor.
    #[cfg(feature = "datadog-api")]
    windows_done: usize,
    /// Bounds of the live window, mirrored for the resume cursor. `None` once
    /// every window is exhausted.
    #[cfg(feature = "datadog-api")]
    current: Option<Window>,
    /// Set once all windows drain so `next_batch` short-circuits to `None`.
    #[cfg(feature = "datadog-api")]
    exhausted: bool,
}

/// A single time window: an optional inclusive `[from, to]` ISO8601 pair. Either
/// bound may be absent (then it is simply omitted from the request filter).
#[cfg(feature = "datadog-api")]
#[derive(Debug, Clone, PartialEq)]
struct Window {
    from: Option<String>,
    to: Option<String>,
}

impl DatadogApiSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        site: impl Into<String>,
        api_key: impl Into<String>,
        app_key: impl Into<String>,
        query: impl Into<String>,
        from: Option<String>,
        to: Option<String>,
        window_minutes: u64,
        resume_after: Option<Vec<Value>>,
    ) -> Result<Self> {
        Ok(Self {
            site: site.into(),
            api_key: api_key.into(),
            app_key: app_key.into(),
            query: query.into(),
            from,
            to,
            window_minutes,
            resume_after,
            #[cfg(feature = "datadog-api")]
            client: None,
            #[cfg(feature = "datadog-api")]
            base_url: None,
            #[cfg(feature = "datadog-api")]
            base_url_override: None,
            #[cfg(feature = "datadog-api")]
            windows: std::collections::VecDeque::new(),
            #[cfg(feature = "datadog-api")]
            after: None,
            #[cfg(feature = "datadog-api")]
            windows_done: 0,
            #[cfg(feature = "datadog-api")]
            current: None,
            #[cfg(feature = "datadog-api")]
            exhausted: false,
        })
    }

    /// Point the source at an explicit base URL (e.g. a mock server), bypassing
    /// `site::base_url`. Test/wiring hook only.
    #[cfg(feature = "datadog-api")]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url_override = Some(base_url.into());
        self
    }
}

// --- Time-window chunking ---------------------------------------------------
//
// `esift-core` does not depend on `chrono`, so the ISO8601 <-> epoch math the
// windowing needs is implemented here against a small, self-contained civil-date
// algorithm (Howard Hinnant's `days_from_civil`). Inputs are expected as
// `YYYY-MM-DDTHH:MM:SS` with an optional fractional part and an optional `Z` /
// offset suffix; only second granularity is used for window boundaries.

/// Parse an ISO8601/RFC3339 timestamp into Unix epoch seconds (UTC). Returns
/// `None` for anything it cannot interpret, so callers can degrade to a single
/// unbounded window rather than panic.
#[cfg(feature = "datadog-api")]
fn parse_epoch_secs(ts: &str) -> Option<i64> {
    let bytes = ts.as_bytes();
    // Need at least "YYYY-MM-DDTHH:MM:SS".
    if bytes.len() < 19 {
        return None;
    }
    let num = |s: &str| -> Option<i64> { s.parse::<i64>().ok() };
    let year = num(&ts[0..4])?;
    if &ts[4..5] != "-" || &ts[7..8] != "-" {
        return None;
    }
    let month = num(&ts[5..7])?;
    let day = num(&ts[8..10])?;
    // Separator between date and time is 'T' or ' '.
    let sep = &ts[10..11];
    if sep != "T" && sep != "t" && sep != " " {
        return None;
    }
    if &ts[13..14] != ":" || &ts[16..17] != ":" {
        return None;
    }
    let hour = num(&ts[11..13])?;
    let min = num(&ts[14..16])?;
    let sec = num(&ts[17..19])?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Trailing part after seconds: optional ".fff" then optional "Z"/offset.
    let mut rest = &ts[19..];
    if let Some(stripped) = rest.strip_prefix('.') {
        // Skip fractional digits.
        let digits = stripped.bytes().take_while(|b| b.is_ascii_digit()).count();
        rest = &stripped[digits..];
    }
    // Offset handling: "Z"/"z"/empty => UTC; "+HH:MM" / "-HH:MM" => apply.
    let mut offset_secs: i64 = 0;
    if !rest.is_empty() && rest != "Z" && rest != "z" {
        let sign = match &rest[0..1] {
            "+" => 1,
            "-" => -1,
            _ => return None,
        };
        if rest.len() < 3 {
            return None;
        }
        let oh = num(&rest[1..3])?;
        let om = if rest.len() >= 6 && &rest[3..4] == ":" {
            num(&rest[4..6])?
        } else if rest.len() >= 5 {
            num(&rest[3..5])?
        } else {
            0
        };
        offset_secs = sign * (oh * 3600 + om * 60);
    }

    let days = days_from_civil(year, month as u32, day as u32);
    let secs = days * 86_400 + hour * 3600 + min * 60 + sec - offset_secs;
    Some(secs)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian civil date.
/// Howard Hinnant's algorithm; valid across the full range we care about.
#[cfg(feature = "datadog-api")]
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Civil date `(year, month, day)` from days since the Unix epoch. Inverse of
/// [`days_from_civil`].
#[cfg(feature = "datadog-api")]
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Format Unix epoch seconds as `YYYY-MM-DDTHH:MM:SSZ` (UTC).
#[cfg(feature = "datadog-api")]
fn format_epoch_secs(epoch: i64) -> String {
    let days = epoch.div_euclid(86_400);
    let tod = epoch.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let hh = tod / 3600;
    let mm = (tod % 3600) / 60;
    let ss = tod % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Split `[from, to]` into contiguous windows of `window_minutes`. Adjacent
/// windows share a boundary (window i ends where window i+1 begins); the
/// per-window pagination cursor prevents a boundary event being read twice.
///
/// If either bound is missing or unparseable, or `window_minutes == 0`, returns
/// a single window carrying the original bounds unchanged.
#[cfg(feature = "datadog-api")]
fn build_windows(from: &Option<String>, to: &Option<String>, window_minutes: u64) -> Vec<Window> {
    let single = || {
        vec![Window {
            from: from.clone(),
            to: to.clone(),
        }]
    };

    if window_minutes == 0 {
        return single();
    }
    let (Some(f), Some(t)) = (from.as_deref(), to.as_deref()) else {
        return single();
    };
    let (Some(start), Some(end)) = (parse_epoch_secs(f), parse_epoch_secs(t)) else {
        return single();
    };
    if end <= start {
        return single();
    }

    let step = window_minutes as i64 * 60;
    let mut windows = Vec::new();
    let mut cursor = start;
    while cursor < end {
        let next = (cursor + step).min(end);
        windows.push(Window {
            // Preserve the caller's exact original endpoints at the extremes so
            // the request bounds round-trip byte-for-byte at the edges.
            from: Some(if cursor == start {
                f.to_string()
            } else {
                format_epoch_secs(cursor)
            }),
            to: Some(if next == end {
                t.to_string()
            } else {
                format_epoch_secs(next)
            }),
        });
        cursor = next;
    }

    if windows.is_empty() {
        single()
    } else {
        windows
    }
}

// --- Resume cursor (de)coding ----------------------------------------------

/// Marker key identifying a Datadog-API resume cursor inside the opaque
/// checkpoint value.
#[cfg(feature = "datadog-api")]
const DD_API_MARKER: &str = "dd_api";

/// Decode a resume blob produced by [`DatadogApiSource::cursor`]. Returns the
/// live window's `(from, to, after, windows_done)` when the blob is one this
/// source wrote; `None` otherwise (start fresh).
#[cfg(feature = "datadog-api")]
fn decode_resume(blob: &[Value]) -> Option<(Window, Option<String>, usize)> {
    let obj = blob.first()?.as_object()?.get(DD_API_MARKER)?.as_object()?;
    let from = obj
        .get("win_from")
        .and_then(|v| v.as_str())
        .map(String::from);
    let to = obj.get("win_to").and_then(|v| v.as_str()).map(String::from);
    let after = obj.get("after").and_then(|v| v.as_str()).map(String::from);
    let windows_done = obj
        .get("windows_done")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    Some((Window { from, to }, after, windows_done))
}

#[cfg(feature = "datadog-api")]
impl DatadogApiSource {
    /// Build the auth-header HTTP client.
    fn build_client(&self) -> Result<Client> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            "DD-API-KEY",
            header::HeaderValue::from_str(&self.api_key)
                .map_err(|e| EsiftError::Source(format!("Invalid DD-API-KEY: {e}")))?,
        );
        headers.insert(
            "DD-APPLICATION-KEY",
            header::HeaderValue::from_str(&self.app_key)
                .map_err(|e| EsiftError::Source(format!("Invalid DD-APPLICATION-KEY: {e}")))?,
        );
        Client::builder()
            .default_headers(headers)
            .build()
            .map_err(EsiftError::from)
    }

    /// Build the request body for the live window and current cursor.
    fn search_body(&self, window: &Window) -> Value {
        let mut filter = serde_json::Map::new();
        filter.insert("query".into(), json!(self.query));
        if let Some(f) = &window.from {
            filter.insert("from".into(), json!(f));
        }
        if let Some(t) = &window.to {
            filter.insert("to".into(), json!(t));
        }
        let mut page = serde_json::Map::new();
        page.insert("limit".into(), json!(PAGE_LIMIT));
        if let Some(after) = &self.after {
            page.insert("cursor".into(), json!(after));
        }
        json!({ "filter": Value::Object(filter), "page": Value::Object(page) })
    }

    /// Issue one search request, handling 429 by sleeping to `X-RateLimit-Reset`
    /// then retrying once. Returns the parsed JSON response body.
    async fn search_once(&self, window: &Window) -> Result<Value> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| EsiftError::Source("Call open() before next_batch()".into()))?;
        let base = self
            .base_url
            .as_ref()
            .ok_or_else(|| EsiftError::Source("Call open() before next_batch()".into()))?;
        let url = format!("{base}/api/v2/logs/events/search");
        let body = self.search_body(window);

        loop {
            // Time the full request/response round-trip so the histogram captures
            // network + server latency for each attempt.
            let started = std::time::Instant::now();
            let send_result = client.post(&url).json(&body).send().await;
            let resp = match send_result {
                Ok(resp) => resp,
                Err(e) => {
                    let elapsed = started.elapsed().as_secs_f64();
                    metrics::counter!(M_REQUESTS_TOTAL, "result" => "error").increment(1);
                    metrics::histogram!(M_REQUEST_SECONDS, "result" => "error").record(elapsed);
                    return Err(EsiftError::from(e));
                }
            };
            let elapsed = started.elapsed().as_secs_f64();
            let status = resp.status();

            if status.as_u16() == 429 {
                // 429 counts as a (failed) request attempt for rate-tracking.
                metrics::counter!(M_REQUESTS_TOTAL, "result" => "error").increment(1);
                metrics::histogram!(M_REQUEST_SECONDS, "result" => "error").record(elapsed);

                let reset = resp
                    .headers()
                    .get("X-RateLimit-Reset")
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                let wait = rate_limit_wait(resp.headers());
                metrics::counter!(M_RATE_LIMITED_TOTAL).increment(1);
                metrics::histogram!(M_RATE_LIMIT_WAIT_SECONDS).record(wait.as_secs_f64());
                warn!(
                    reset = reset.as_deref().unwrap_or("<absent>"),
                    wait_ms = wait.as_millis() as u64,
                    "Datadog API 429 rate-limited; sleeping toward X-RateLimit-Reset before retry"
                );
                tokio::time::sleep(wait).await;
                continue;
            }

            if !status.is_success() {
                metrics::counter!(M_REQUESTS_TOTAL, "result" => "error").increment(1);
                metrics::histogram!(M_REQUEST_SECONDS, "result" => "error").record(elapsed);
                let text = resp
                    .text()
                    .await
                    .unwrap_or_else(|e| format!("<failed to read body: {e}>"));
                return Err(EsiftError::Source(format!(
                    "Datadog logs search failed: HTTP {status} — {text}"
                )));
            }

            metrics::counter!(M_REQUESTS_TOTAL, "result" => "ok").increment(1);
            metrics::histogram!(M_REQUEST_SECONDS, "result" => "ok").record(elapsed);
            return resp.json::<Value>().await.map_err(EsiftError::from);
        }
    }

    /// Advance to the next window: pop the live one, reset the in-window cursor.
    /// Marks the run exhausted once no windows remain.
    fn advance_window(&mut self) {
        // Bounds of the window being drained, for the completion log.
        let drained = self.windows.front().cloned();
        self.windows_done += 1;
        self.after = None;
        self.windows.pop_front();
        self.current = self.windows.front().cloned();
        if self.current.is_none() {
            self.exhausted = true;
        }
        metrics::counter!(M_WINDOWS_TOTAL).increment(1);
        info!(
            win_from = drained
                .as_ref()
                .and_then(|w| w.from.as_deref())
                .unwrap_or("<none>"),
            win_to = drained
                .as_ref()
                .and_then(|w| w.to.as_deref())
                .unwrap_or("<none>"),
            windows_done = self.windows_done,
            remaining = self.windows.len(),
            "Datadog API window drained"
        );
    }
}

/// Compute how long to sleep after a 429, from `X-RateLimit-Reset` (unix
/// seconds). Sleeps to that instant plus a small fixed jitter; clamps to a sane
/// ceiling and falls back to a short wait when the header is absent or in the
/// past.
#[cfg(feature = "datadog-api")]
fn rate_limit_wait(headers: &reqwest::header::HeaderMap) -> std::time::Duration {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const JITTER: Duration = Duration::from_millis(50);
    const FALLBACK: Duration = Duration::from_millis(200);
    const CEILING: Duration = Duration::from_secs(60);

    let reset = headers
        .get("X-RateLimit-Reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok());

    let Some(reset_unix) = reset else {
        return FALLBACK;
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let delta = reset_unix - now;
    if delta <= 0 {
        return FALLBACK + JITTER;
    }
    (Duration::from_secs(delta as u64) + JITTER).min(CEILING)
}

#[cfg(feature = "datadog-api")]
#[async_trait]
impl Source for DatadogApiSource {
    #[tracing::instrument(
        skip(self),
        fields(
            site = %self.site,
            query = %self.query,
            from = self.from.as_deref().unwrap_or("<none>"),
            to = self.to.as_deref().unwrap_or("<none>"),
            window_minutes = self.window_minutes,
        )
    )]
    async fn open(&mut self) -> Result<()> {
        // Register metric descriptions once per source open. No-op without a
        // global recorder (e.g. unit tests).
        describe_metrics();

        // Validate site even when a test override is set, so a bad config is
        // still caught.
        let resolved = super::site::base_url(&self.site)?;
        self.base_url = Some(self.base_url_override.clone().unwrap_or(resolved));
        self.client = Some(self.build_client()?);

        // Build the full window list from the configured range.
        let mut windows: std::collections::VecDeque<Window> =
            build_windows(&self.from, &self.to, self.window_minutes)
                .into_iter()
                .collect();

        // Seed from a resume blob, if the checkpoint carried one we wrote.
        if let Some(blob) = &self.resume_after {
            if let Some((win, after, done)) = decode_resume(blob) {
                // Drop windows already finished and any not matching the saved
                // live window, then continue from the saved cursor.
                while let Some(front) = windows.front() {
                    if *front == win {
                        break;
                    }
                    windows.pop_front();
                }
                if windows.front().map(|w| *w == win).unwrap_or(false) {
                    self.after = after;
                    self.windows_done = done;
                } else {
                    // Saved window not found in this run's plan: fall back to the
                    // saved window alone so resume still makes forward progress.
                    windows = std::collections::VecDeque::from([win]);
                    self.after = after;
                    self.windows_done = done;
                }
            }
        }

        self.current = windows.front().cloned();
        if self.current.is_none() {
            self.exhausted = true;
        }
        self.windows = windows;
        info!(
            windows_queued = self.windows.len(),
            windows_done = self.windows_done,
            "Datadog API source opened"
        );
        if let Some(win) = self.current.as_ref() {
            info!(
                win_from = win.from.as_deref().unwrap_or("<none>"),
                win_to = win.to.as_deref().unwrap_or("<none>"),
                resuming = self.after.is_some(),
                "Datadog API window open"
            );
        }
        Ok(())
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        loop {
            if self.exhausted {
                return Ok(None);
            }
            let Some(window) = self.current.clone() else {
                self.exhausted = true;
                return Ok(None);
            };

            let page = self.search_once(&window).await?;

            // Track the next cursor; absent/null => window exhausted.
            let next_after = page
                .get("meta")
                .and_then(|m| m.get("page"))
                .and_then(|p| p.get("after"))
                .and_then(|a| a.as_str())
                .map(String::from);

            let data = page
                .get("data")
                .and_then(|d| d.as_array())
                .cloned()
                .unwrap_or_default();

            let docs: Vec<Document> = data
                .into_iter()
                .map(|event| {
                    let id = event
                        .get("id")
                        .and_then(|i| i.as_str())
                        .map(String::from)
                        .unwrap_or_default();
                    let body = super::flatten_api::flatten(event);
                    let id = if id.is_empty() {
                        body.get("id")
                            .and_then(|i| i.as_str())
                            .map(String::from)
                            .unwrap_or_default()
                    } else {
                        id
                    };
                    Document::new(DD_INDEX, id, body)
                })
                .collect();

            // One page fetched; record it and (by count) the documents it
            // carried. A non-empty page is always returned immediately, so
            // incrementing by `docs.len()` here counts every emitted document
            // exactly once (empty pages add zero).
            let doc_count = docs.len();
            metrics::counter!(M_PAGES_TOTAL).increment(1);
            metrics::counter!(M_DOCS_TOTAL).increment(doc_count as u64);
            debug!(
                docs = doc_count,
                has_after = next_after.is_some(),
                win_from = window.from.as_deref().unwrap_or("<none>"),
                win_to = window.to.as_deref().unwrap_or("<none>"),
                "Datadog API page fetched"
            );

            match next_after {
                Some(after) => {
                    self.after = Some(after);
                }
                None => {
                    // Window drained: advance before returning this page so the
                    // resume cursor already reflects the next window.
                    self.advance_window();
                }
            }

            if docs.is_empty() {
                // Empty page (e.g. an empty window): keep going rather than
                // returning an empty batch, which the loop treats as progress.
                if self.exhausted {
                    return Ok(None);
                }
                continue;
            }

            return Ok(Some(docs));
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("Datadog API site={}", self.site)
    }

    fn cursor(&self) -> Option<Vec<Value>> {
        let win = self.current.as_ref();
        Some(vec![json!({
            DD_API_MARKER: {
                "win_from": win.and_then(|w| w.from.clone()),
                "win_to": win.and_then(|w| w.to.clone()),
                "after": self.after,
                "windows_done": self.windows_done,
            }
        })])
    }
}

#[cfg(not(feature = "datadog-api"))]
#[async_trait]
impl Source for DatadogApiSource {
    async fn open(&mut self) -> Result<()> {
        Err(EsiftError::Source(
            "Datadog API source requires building with --features datadog-api".into(),
        ))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!("Datadog API site={}", self.site)
    }
}

#[cfg(all(test, feature = "datadog-api"))]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[test]
    fn parse_and_format_round_trip() {
        // 2025-01-02T03:04:05Z is a known epoch.
        let epoch = parse_epoch_secs("2025-01-02T03:04:05Z").unwrap();
        assert_eq!(format_epoch_secs(epoch), "2025-01-02T03:04:05Z");
        // Unix epoch itself.
        assert_eq!(parse_epoch_secs("1970-01-01T00:00:00Z").unwrap(), 0);
        // A fractional + offset form parses to the same UTC instant.
        let a = parse_epoch_secs("2025-06-01T12:30:00.500Z").unwrap();
        let b = parse_epoch_secs("2025-06-01T14:30:00+02:00").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn build_windows_chunks_contiguously() {
        let from = Some("2025-01-01T00:00:00Z".to_string());
        let to = Some("2025-01-01T02:30:00Z".to_string());
        let windows = build_windows(&from, &to, 60);
        assert_eq!(windows.len(), 3);
        // Boundaries are contiguous and edges preserved exactly.
        assert_eq!(windows[0].from.as_deref(), Some("2025-01-01T00:00:00Z"));
        assert_eq!(windows[0].to.as_deref(), Some("2025-01-01T01:00:00Z"));
        assert_eq!(windows[1].from.as_deref(), Some("2025-01-01T01:00:00Z"));
        assert_eq!(windows[1].to.as_deref(), Some("2025-01-01T02:00:00Z"));
        assert_eq!(windows[2].from.as_deref(), Some("2025-01-01T02:00:00Z"));
        assert_eq!(windows[2].to.as_deref(), Some("2025-01-01T02:30:00Z"));
    }

    #[test]
    fn build_windows_single_when_unbounded() {
        let windows = build_windows(&None, &Some("2025-01-01T00:00:00Z".into()), 60);
        assert_eq!(windows.len(), 1);
    }

    #[test]
    fn resume_cursor_round_trips() {
        let src = DatadogApiSource::new(
            "datadoghq.com",
            "k",
            "a",
            "*",
            Some("2025-01-01T00:00:00Z".into()),
            Some("2025-01-01T01:00:00Z".into()),
            60,
            None,
        )
        .unwrap();
        // Cursor before open reflects the configured (single) window as current
        // only after open; before open `current` is None.
        let _ = src.cursor();
        let win = Window {
            from: Some("2025-01-01T00:00:00Z".into()),
            to: Some("2025-01-01T01:00:00Z".into()),
        };
        let blob = vec![json!({
            DD_API_MARKER: {
                "win_from": win.from,
                "win_to": win.to,
                "after": "CUR9",
                "windows_done": 2,
            }
        })];
        let (decoded_win, after, done) = decode_resume(&blob).unwrap();
        assert_eq!(decoded_win.from.as_deref(), Some("2025-01-01T00:00:00Z"));
        assert_eq!(after.as_deref(), Some("CUR9"));
        assert_eq!(done, 2);
    }

    /// Two-page pagination within a single window: first page carries a cursor,
    /// second page has `meta.page.after: null`, draining the window.
    #[tokio::test]
    async fn two_page_pagination_single_window() {
        let server = MockServer::start().await;

        // Page 1: no cursor in the request, returns after="CUR1".
        Mock::given(method("POST"))
            .and(path("/api/v2/logs/events/search"))
            .and(body_partial_json(json!({ "page": { "limit": 1000 } })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": "a1", "type": "log", "attributes": { "message": "one",
                        "attributes": { "k": 1 } } },
                    { "id": "a2", "type": "log", "attributes": { "message": "two",
                        "attributes": { "k": 2 } } }
                ],
                "meta": { "page": { "after": "CUR1" } }
            })))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;

        // Page 2: request carries cursor=CUR1, returns after=null (drained).
        Mock::given(method("POST"))
            .and(path("/api/v2/logs/events/search"))
            .and(body_partial_json(json!({ "page": { "cursor": "CUR1" } })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": "a3", "type": "log", "attributes": { "message": "three",
                        "attributes": { "k": 3 } } }
                ],
                "meta": { "page": { "after": null } }
            })))
            .with_priority(2)
            .mount(&server)
            .await;

        let mut src = DatadogApiSource::new(
            "datadoghq.com",
            "key",
            "app",
            "service:web",
            Some("2025-01-01T00:00:00Z".into()),
            Some("2025-01-01T00:30:00Z".into()),
            60,
            None,
        )
        .unwrap()
        .with_base_url(server.uri());

        src.open().await.unwrap();

        let mut ids = Vec::new();
        while let Some(batch) = src.next_batch().await.unwrap() {
            for d in batch {
                assert_eq!(d.index, "datadog");
                // Flatten collapsed the double nesting.
                assert!(d.body.get("message").is_some());
                assert!(d.body.get("k").is_some());
                ids.push(d.id);
            }
        }
        assert_eq!(ids, vec!["a1", "a2", "a3"]);
        // Window drained -> run exhausted.
        assert!(src.next_batch().await.unwrap().is_none());

        src.close().await.unwrap();
    }

    /// A 429 carrying `X-RateLimit-Reset` is honoured: the driver sleeps to the
    /// reset instant (~now/+1s) then the retry succeeds.
    #[tokio::test]
    async fn rate_limited_then_succeeds() {
        let server = MockServer::start().await;

        // First response: 429 with reset ~1s in the future (fast test).
        let reset = (now_unix() + 1).to_string();
        Mock::given(method("POST"))
            .and(path("/api/v2/logs/events/search"))
            .respond_with(
                ResponseTemplate::new(429).insert_header("X-RateLimit-Reset", reset.as_str()),
            )
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;

        // Retry succeeds with one drained page.
        Mock::given(method("POST"))
            .and(path("/api/v2/logs/events/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": "r1", "type": "log", "attributes": { "message": "ok",
                        "attributes": {} } }
                ],
                "meta": { "page": { "after": null } }
            })))
            .with_priority(2)
            .mount(&server)
            .await;

        let mut src = DatadogApiSource::new(
            "datadoghq.com",
            "key",
            "app",
            "*",
            Some("2025-01-01T00:00:00Z".into()),
            Some("2025-01-01T00:10:00Z".into()),
            60,
            None,
        )
        .unwrap()
        .with_base_url(server.uri());

        src.open().await.unwrap();

        let started = std::time::Instant::now();
        let batch = src.next_batch().await.unwrap().expect("a page after retry");
        // We waited roughly to the reset instant (≈1s) before the retry landed.
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(500),
            "should have slept toward the rate-limit reset"
        );
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, "r1");
        assert!(src.next_batch().await.unwrap().is_none());

        src.close().await.unwrap();
    }

    /// With a local Prometheus recorder installed, draining a mocked single
    /// window records the emitted documents in `esift_datadog_api_docs_total`.
    /// Mirrors the wiremock setup of `two_page_pagination_single_window` but
    /// runs the drain inside `metrics::with_local_recorder` (which takes a sync
    /// closure, so the async work is driven via `block_on`).
    #[test]
    fn docs_total_counter_increments() {
        use metrics_exporter_prometheus::PrometheusBuilder;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Stand up the mock server (single window, three docs across two pages)
        // outside the recorder scope.
        let server = rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v2/logs/events/search"))
                .and(body_partial_json(json!({ "page": { "limit": 1000 } })))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data": [
                        { "id": "m1", "type": "log", "attributes": { "message": "one" } },
                        { "id": "m2", "type": "log", "attributes": { "message": "two" } }
                    ],
                    "meta": { "page": { "after": "CUR1" } }
                })))
                .up_to_n_times(1)
                .with_priority(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v2/logs/events/search"))
                .and(body_partial_json(json!({ "page": { "cursor": "CUR1" } })))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data": [
                        { "id": "m3", "type": "log", "attributes": { "message": "three" } }
                    ],
                    "meta": { "page": { "after": null } }
                })))
                .with_priority(2)
                .mount(&server)
                .await;
            server
        });

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        let total = metrics::with_local_recorder(&recorder, || {
            rt.block_on(async {
                let mut src = DatadogApiSource::new(
                    "datadoghq.com",
                    "key",
                    "app",
                    "service:web",
                    Some("2025-01-01T00:00:00Z".into()),
                    Some("2025-01-01T00:30:00Z".into()),
                    60,
                    None,
                )
                .unwrap()
                .with_base_url(server.uri());

                src.open().await.unwrap();
                let mut count = 0usize;
                while let Some(batch) = src.next_batch().await.unwrap() {
                    count += batch.len();
                }
                src.close().await.unwrap();
                count
            })
        });

        // Three documents flowed through the batches...
        assert_eq!(total, 3);

        // ...and the Prometheus scrape reflects the docs counter at 3.
        let rendered = handle.render();
        let docs_line = rendered
            .lines()
            .find(|l| l.starts_with("esift_datadog_api_docs_total"))
            .unwrap_or_else(|| {
                panic!("esift_datadog_api_docs_total missing from scrape:\n{rendered}")
            });
        let value: f64 = docs_line
            .rsplit(' ')
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("could not parse docs_total from line: {docs_line}"));
        assert_eq!(value, 3.0, "full scrape:\n{rendered}");
    }
}
