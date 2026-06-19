//! OpenSearch / Elasticsearch source using PIT + search_after.
//!
//! API differences between OpenSearch and Elasticsearch:
//!
//! OpenSearch 2.4+:
//!   Create: POST /{index}/_search/point_in_time?keep_alive=5m
//!   Response field: "pit_id"
//!   Delete: DELETE /_search/point_in_time  body: {"pit_id": ["<id>"]}
//!   NOTE: _shard_doc sort is NOT supported. Sort by a real document field only.
//!         Confirmed broken in all OpenSearch versions as of 2.17.
//!         Ref: https://forum.opensearch.org/t/point-in-time-errors/14068
//!
//! Elasticsearch 7.10+:
//!   Create: POST /{index}/_pit?keep_alive=5m
//!   Response field: "id"
//!   Delete: DELETE /_pit  body: {"id": "<id>"}
//!   NOTE: _shard_doc IS supported as a tiebreaker on ES 7.12+.
//!
//! Sort strategy:
//!   We sort by _id ascending. It is always present, requires no mapping,
//!   and provides a stable, unique cursor for search_after on both platforms.
//!   The trade-off: _id sort uses fielddata on text fields in older ES versions,
//!   but on OpenSearch and modern ES it uses keyword-type _id which is fine.

use super::Source;
use crate::{
    error::{EsiftError, Result},
    Document,
};
use async_trait::async_trait;
use reqwest::{header, Client};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub enum Auth {
    None,
    Basic {
        username: String,
        password: Option<String>,
    },
    #[cfg(feature = "aws")]
    AwsSigV4 {
        region: String,
        provider: aws_credential_types::provider::SharedCredentialsProvider,
    },
}

pub struct OpenSearchSource {
    client: Client,
    base_url: String,
    index: String,
    query: Value,
    batch_size: usize,
    auth: Auth,
    pit_id: Option<String>,
    search_after: Option<Vec<Value>>,
    exhausted: bool,
    flavor: ApiFlavor,
    slices: usize,
    /// Per-slice extraction state, used only when `slices > 1`. Empty on the
    /// single-slice path, which relies on `search_after`/`exhausted` instead.
    slice_state: Vec<SliceState>,
    /// Round-robin pointer into `slice_state` for the sliced path.
    next_slice: usize,
}

/// Per-slice cursor for sliced PIT extraction. Each slice owns its own
/// `search_after` cursor and exhaustion flag; the shared PIT id lives on the
/// parent struct.
#[derive(Debug, Clone, Default)]
struct SliceState {
    search_after: Option<Vec<Value>>,
    exhausted: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum ApiFlavor {
    Unknown,
    OpenSearch,
    Elasticsearch,
}

/// Read an error response body for diagnostics. If the body cannot be read,
/// report that rather than substituting an empty string, so a failed read is
/// never silently indistinguishable from an empty response.
async fn response_body(resp: reqwest::Response) -> String {
    resp.text()
        .await
        .unwrap_or_else(|e| format!("<failed to read response body: {e}>"))
}

/// Marker key identifying a sliced-resume cursor inside the opaque checkpoint
/// cursor. A single-slice cursor is an array of `sort` values and never a
/// one-element array holding an object with this key, so the two are
/// unambiguous.
const SLICE_MARKER: &str = "__esift_slices";

/// Whether `cursor` is a sliced-resume encoding produced by [`OpenSearchSource::cursor`].
fn is_sliced_encoding(cursor: &[Value]) -> bool {
    cursor.len() == 1
        && cursor[0]
            .as_object()
            .is_some_and(|o| o.contains_key(SLICE_MARKER))
}

/// Decode a sliced-resume cursor into per-slice state. Returns `None` (start
/// fresh) when the encoding is malformed or its slice count differs from this
/// run's `slices`.
fn decode_slice_cursors(cursor: &[Value], slices: usize) -> Option<Vec<SliceState>> {
    let obj = cursor.first()?.as_object()?;
    let saved = obj.get(SLICE_MARKER)?.as_u64()? as usize;
    if saved != slices {
        warn!(
            "checkpoint was written with {saved} slices but this run uses {slices}; \
             starting sliced extraction fresh"
        );
        return None;
    }
    let entries = obj.get("cursors")?.as_array()?;
    if entries.len() != slices {
        return None;
    }
    Some(
        entries
            .iter()
            .map(|entry| SliceState {
                search_after: entry.get("after").and_then(|a| a.as_array().cloned()),
                exhausted: entry.get("done").and_then(|d| d.as_bool()).unwrap_or(false),
            })
            .collect(),
    )
}

impl OpenSearchSource {
    /// Build a source. `resume_after` seeds the `search_after` cursor from a
    /// prior checkpoint so a resumed run continues where it left off; pass
    /// `None` to start from the beginning.
    pub fn new(
        base_url: impl Into<String>,
        index: impl Into<String>,
        query: Value,
        batch_size: usize,
        auth: Auth,
        resume_after: Option<Vec<Value>>,
    ) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        let client = Client::builder().default_headers(headers).build()?;

        Ok(Self {
            client,
            base_url: base_url.into(),
            index: index.into(),
            query,
            batch_size,
            auth,
            pit_id: None,
            search_after: resume_after,
            exhausted: false,
            flavor: ApiFlavor::Unknown,
            slices: 1,
            slice_state: Vec::new(),
            next_slice: 0,
        })
    }

    /// Set the number of parallel extraction slices (sliced PIT). Values below
    /// 1 are treated as 1. Lane 4 fans extraction out across these slices.
    pub fn with_slices(mut self, slices: usize) -> Self {
        self.slices = slices.max(1);
        self
    }

    async fn execute_request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<reqwest::Response> {
        let url_str = if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        };
        let url = reqwest::Url::parse(&url_str)
            .map_err(|e| EsiftError::Source(format!("Invalid URL: {}", e)))?;

        let mut req_builder = self.client.request(method, url);

        if let Some(ref b) = body {
            req_builder = req_builder.json(b);
        }

        match &self.auth {
            Auth::None => {
                let req = req_builder.build()?;
                Ok(self.client.execute(req).await?)
            }
            Auth::Basic { username, password } => {
                let req = req_builder
                    .basic_auth(username, password.as_deref())
                    .build()?;
                Ok(self.client.execute(req).await?)
            }
            #[cfg(feature = "aws")]
            Auth::AwsSigV4 { region, provider } => {
                let mut req = req_builder.build()?;
                self.sign_request_aws(&mut req, region, provider).await?;
                Ok(self.client.execute(req).await?)
            }
        }
    }

    #[cfg(feature = "aws")]
    async fn sign_request_aws(
        &self,
        req: &mut reqwest::Request,
        region: &str,
        provider: &aws_credential_types::provider::SharedCredentialsProvider,
    ) -> Result<()> {
        use aws_credential_types::provider::ProvideCredentials;
        use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
        use aws_sigv4::sign::v4;
        use std::time::SystemTime;

        // 1. Load credentials asynchronously
        let credentials = provider
            .provide_credentials()
            .await
            .map_err(|e| EsiftError::Source(format!("Failed to resolve AWS credentials: {}", e)))?;

        let identity = aws_smithy_runtime_api::client::identity::Identity::new(
            credentials.clone(),
            credentials.expiry(),
        );

        let signing_settings = SigningSettings::default();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name("es") // Amazon OpenSearch Service uses "es"
            .time(SystemTime::now())
            .settings(signing_settings)
            .build()
            .map_err(|e| EsiftError::Source(format!("Failed to build signing params: {}", e)))?
            .into();

        // 2. Prepare request fields
        let method = req.method().as_str();
        let uri = req.url().as_str();

        let host = req
            .url()
            .host_str()
            .ok_or_else(|| EsiftError::Source("Request URL is missing a host name".to_string()))?;

        // 3. Build http::Request<&[u8]>
        let mut http_req_builder = http::Request::builder().method(method).uri(uri);

        // Add Host header explicitly as required for SigV4 signing
        http_req_builder = http_req_builder.header("host", host);

        // Copy other headers
        for (name, value) in req.headers() {
            if name.as_str().eq_ignore_ascii_case("host") {
                continue;
            }
            http_req_builder = http_req_builder.header(name.as_str(), value.as_bytes());
        }

        let body_bytes = match req.body() {
            Some(body) => match body.as_bytes() {
                Some(bytes) => bytes.to_vec(),
                None => {
                    return Err(EsiftError::Source(
                        "Streaming request body is not supported for AWS SigV4 signing".to_string(),
                    ));
                }
            },
            None => Vec::new(),
        };

        let mut http_req = http_req_builder.body(&body_bytes).map_err(|e| {
            EsiftError::Source(format!("Failed to build temporary http request: {}", e))
        })?;

        // 4. Sign request
        let headers_vec: Vec<(&str, &str)> = http_req
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                let name_str = name.as_str();
                let value_str = value.to_str().ok()?;
                Some((name_str, value_str))
            })
            .collect();

        let signable_request = SignableRequest::new(
            method,
            uri,
            headers_vec.into_iter(),
            SignableBody::Bytes(&body_bytes),
        )
        .map_err(|e| EsiftError::Source(format!("Failed to create signable request: {}", e)))?;

        let (signing_instructions, _signature) = sign(signable_request, &signing_params)
            .map_err(|e| EsiftError::Source(format!("Failed to sign request: {}", e)))?
            .into_parts();

        // 5. Apply the signing instructions to the temporary request
        signing_instructions.apply_to_request_http1x(&mut http_req);

        // 6. Copy headers and URL back to reqwest::Request
        for (name, value) in http_req.headers() {
            let name_header = reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes())
                .map_err(|e| EsiftError::Source(format!("Invalid header name: {}", e)))?;
            let value_header = reqwest::header::HeaderValue::from_bytes(value.as_bytes())
                .map_err(|e| EsiftError::Source(format!("Invalid header value: {}", e)))?;
            req.headers_mut().insert(name_header, value_header);
        }

        let signed_uri = http_req.uri().to_string();
        *req.url_mut() = reqwest::Url::parse(&signed_uri)
            .map_err(|e| EsiftError::Source(format!("Failed to parse signed URL: {}", e)))?;

        Ok(())
    }

    /// Prepare per-slice cursors after the PIT is established. For the sliced
    /// path this allocates one cursor per slice and, when the checkpoint carries
    /// a sliced-resume encoding, restores each slice's `search_after` and
    /// exhaustion so the run continues where it left off. For the single-slice
    /// path it discards an incompatible sliced encoding (e.g. a run that dropped
    /// from many slices to one) and starts fresh. Called from `open()`.
    fn seed_cursors(&mut self) {
        let sliced_encoding = self.search_after.as_deref().is_some_and(is_sliced_encoding);

        if self.slices > 1 {
            self.slice_state = vec![SliceState::default(); self.slices];
            self.next_slice = 0;
            if sliced_encoding {
                if let Some(decoded) =
                    decode_slice_cursors(self.search_after.as_ref().unwrap(), self.slices)
                {
                    if decoded.iter().all(|s| s.exhausted) {
                        self.exhausted = true;
                    }
                    self.slice_state = decoded;
                }
            }
            // A single-slice cursor is meaningless in sliced mode.
            self.search_after = None;
        } else if sliced_encoding {
            warn!(
                "checkpoint was written with sliced extraction; a single-slice run \
                 cannot resume from it, starting fresh"
            );
            self.search_after = None;
        }
    }

    /// Issue one `_search` against the shared PIT. `slice` carries the
    /// `{id, max}` pair for sliced extraction, or `None` for the single-slice
    /// path. `search_after` seeds the per-request cursor. Returns the parsed
    /// documents and the next cursor (last hit's `sort`), or `None` when the
    /// page is empty. Rotates the PIT id from the response when present.
    async fn search_one(
        &mut self,
        slice: Option<usize>,
        search_after: Option<&Vec<Value>>,
    ) -> Result<Option<(Vec<Document>, Option<Vec<Value>>)>> {
        let pit_id = self
            .pit_id
            .as_ref()
            .ok_or_else(|| EsiftError::Source("Call open() before next_batch()".into()))?;

        // Sort by _id ascending.
        // _shard_doc is NOT supported in OpenSearch (confirmed broken through 2.17).
        // _id is always present, requires no mapping, and gives a stable unique cursor.
        let mut body = json!({
            "size": self.batch_size,
            "query": self.query,
            "sort": [{ "_id": "asc" }],
            "pit": {
                "id": pit_id,
                "keep_alive": "5m"
            },
            "track_total_hits": false
        });

        if let Some(id) = slice {
            body["slice"] = json!({ "id": id, "max": self.slices });
        }

        if let Some(cursor) = search_after {
            body["search_after"] = Value::Array(cursor.clone());
        }

        let resp = self
            .execute_request(reqwest::Method::POST, "/_search", Some(body))
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = response_body(resp).await;
            return Err(EsiftError::Source(format!(
                "Search failed: HTTP {} — {}",
                status, text
            )));
        }

        let page: Value = resp.json().await?;

        // Server may rotate the PIT id; always use the latest
        if let Some(new_pit_id) = page["pit_id"].as_str() {
            self.pit_id = Some(new_pit_id.to_string());
        }

        let hits = page["hits"]["hits"]
            .as_array()
            .ok_or_else(|| EsiftError::Source("Response missing hits.hits".into()))?;

        if hits.is_empty() {
            return Ok(None);
        }

        let next_cursor = hits
            .last()
            .and_then(|last| last["sort"].as_array().cloned());

        let index = self.index.clone();
        let docs: Vec<Document> = hits
            .iter()
            .filter_map(|hit| {
                let id = hit["_id"].as_str()?.to_string();
                let source = hit["_source"].clone();
                let idx = hit["_index"].as_str().unwrap_or(&index).to_string();
                Some(Document::new(idx, id, source))
            })
            .collect();

        Ok(Some((docs, next_cursor)))
    }

    /// Single-slice extraction: one PIT, one `search_after` cursor. Preserves
    /// the original behavior, including the `hits.len() < batch_size` early
    /// exhaustion and resume-cursor seeding.
    async fn next_batch_single(&mut self) -> Result<Option<Vec<Document>>> {
        let cursor = self.search_after.clone();
        match self.search_one(None, cursor.as_ref()).await? {
            None => {
                self.exhausted = true;
                Ok(None)
            }
            Some((docs, next_cursor)) => {
                if docs.len() < self.batch_size {
                    self.exhausted = true;
                }
                if let Some(next) = next_cursor {
                    self.search_after = Some(next);
                }
                debug!("Fetched {} documents", docs.len());
                Ok(Some(docs))
            }
        }
    }

    /// Sliced extraction: one shared PIT, one `search_after` cursor per slice.
    /// Advances slices round-robin, skipping exhausted ones, and is exhausted
    /// only once every slice has drained. `cursor()` encodes all per-slice
    /// cursors into one opaque checkpoint value, so a sliced run resumes
    /// mid-flight when restarted with the same `slices`.
    async fn next_batch_sliced(&mut self) -> Result<Option<Vec<Document>>> {
        let count = self.slice_state.len();
        for _ in 0..count {
            let idx = self.next_slice;
            self.next_slice = (self.next_slice + 1) % count;

            if self.slice_state[idx].exhausted {
                continue;
            }

            let cursor = self.slice_state[idx].search_after.clone();
            match self.search_one(Some(idx), cursor.as_ref()).await? {
                None => {
                    self.slice_state[idx].exhausted = true;
                    continue;
                }
                Some((docs, next_cursor)) => {
                    if docs.len() < self.batch_size {
                        self.slice_state[idx].exhausted = true;
                    }
                    if let Some(next) = next_cursor {
                        self.slice_state[idx].search_after = Some(next);
                    }
                    debug!("Fetched {} documents from slice {}", docs.len(), idx);
                    return Ok(Some(docs));
                }
            }
        }

        // Every slice drained.
        self.exhausted = true;
        Ok(None)
    }
}

#[async_trait]
impl Source for OpenSearchSource {
    async fn open(&mut self) -> Result<()> {
        info!("Opening PIT on index '{}'", self.index);

        // Try OpenSearch path first: POST /{index}/_search/point_in_time?keep_alive=5m
        let os_path = format!("/{}/_search/point_in_time?keep_alive=5m", self.index);

        let resp = self
            .execute_request(reqwest::Method::POST, &os_path, None)
            .await?;
        let status = resp.status();

        if status.is_success() {
            let body: Value = resp.json().await?;
            let pit_id = body["pit_id"]
                .as_str()
                .ok_or_else(|| EsiftError::Source("PIT response missing 'pit_id'".into()))?
                .to_string();
            info!("PIT opened (OpenSearch)");
            debug!("PIT id: {}", pit_id);
            self.pit_id = Some(pit_id);
            self.flavor = ApiFlavor::OpenSearch;
            self.seed_cursors();
            return Ok(());
        }

        // Fall back to Elasticsearch path: POST /{index}/_pit?keep_alive=5m
        if status.as_u16() == 404 || status.as_u16() == 405 {
            warn!(
                "OpenSearch PIT path returned {}, trying Elasticsearch path",
                status
            );
            let es_path = format!("/{}/_pit?keep_alive=5m", self.index);
            let resp = self
                .execute_request(reqwest::Method::POST, &es_path, None)
                .await?;

            if resp.status().is_success() {
                let body: Value = resp.json().await?;
                let pit_id = body["id"]
                    .as_str()
                    .ok_or_else(|| EsiftError::Source("PIT response missing 'id'".into()))?
                    .to_string();
                info!("PIT opened (Elasticsearch)");
                self.pit_id = Some(pit_id);
                self.flavor = ApiFlavor::Elasticsearch;
                self.seed_cursors();
                return Ok(());
            }

            let s = resp.status();
            let body = response_body(resp).await;
            return Err(EsiftError::Source(format!(
                "PIT open failed (ES fallback): HTTP {} — {}",
                s, body
            )));
        }

        let body = response_body(resp).await;
        Err(EsiftError::Source(format!(
            "PIT open failed: HTTP {} — {}",
            status, body
        )))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        if self.exhausted {
            return Ok(None);
        }

        if self.slices > 1 {
            self.next_batch_sliced().await
        } else {
            self.next_batch_single().await
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pit_id) = self.pit_id.take() {
            info!("Closing PIT");
            match self.flavor {
                ApiFlavor::OpenSearch | ApiFlavor::Unknown => {
                    let path = "/_search/point_in_time";
                    let body = json!({ "pit_id": [pit_id] });
                    if let Err(e) = self
                        .execute_request(reqwest::Method::DELETE, path, Some(body))
                        .await
                    {
                        warn!("PIT close failed (non-fatal): {}", e);
                    }
                }
                ApiFlavor::Elasticsearch => {
                    let path = "/_pit";
                    let body = json!({ "id": pit_id });
                    if let Err(e) = self
                        .execute_request(reqwest::Method::DELETE, path, Some(body))
                        .await
                    {
                        warn!("PIT close failed (non-fatal): {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "OpenSearch {} index={} slices={}",
            self.base_url, self.index, self.slices
        )
    }

    fn cursor(&self) -> Option<Vec<Value>> {
        if self.slices > 1 {
            // Encode every slice's cursor and exhaustion into one opaque value
            // so the checkpoint layer can persist it like any other cursor; only
            // this source interprets it (see `seed_cursors`). None before the
            // slices are initialized (i.e. before `open`).
            if self.slice_state.is_empty() {
                return None;
            }
            let cursors: Vec<Value> = self
                .slice_state
                .iter()
                .map(|s| json!({ "after": s.search_after, "done": s.exhausted }))
                .collect();
            return Some(vec![
                json!({ (SLICE_MARKER): self.slices, "cursors": cursors }),
            ]);
        }
        self.search_after.clone()
    }
}

#[cfg(test)]
mod resume_tests {
    use super::*;

    fn source(slices: usize) -> OpenSearchSource {
        OpenSearchSource::new(
            "http://localhost:9200",
            "idx",
            json!({ "match_all": {} }),
            10,
            Auth::None,
            None,
        )
        .unwrap()
        .with_slices(slices)
    }

    #[test]
    fn sliced_cursor_round_trips_through_seed() {
        let mut src = source(2);
        src.slice_state = vec![
            SliceState {
                search_after: Some(vec![json!("a")]),
                exhausted: false,
            },
            SliceState {
                search_after: Some(vec![json!("b")]),
                exhausted: true,
            },
        ];

        let encoded = src.cursor().expect("sliced cursor should encode");
        assert!(is_sliced_encoding(&encoded));

        let mut resumed = OpenSearchSource::new(
            "http://localhost:9200",
            "idx",
            json!({ "match_all": {} }),
            10,
            Auth::None,
            Some(encoded),
        )
        .unwrap()
        .with_slices(2);
        resumed.seed_cursors();

        assert_eq!(resumed.slice_state[0].search_after, Some(vec![json!("a")]));
        assert!(!resumed.slice_state[0].exhausted);
        assert_eq!(resumed.slice_state[1].search_after, Some(vec![json!("b")]));
        assert!(resumed.slice_state[1].exhausted);
        assert!(resumed.search_after.is_none());
    }

    #[test]
    fn all_slices_done_marks_run_exhausted() {
        let mut src = source(2);
        src.slice_state = vec![
            SliceState {
                search_after: Some(vec![json!("a")]),
                exhausted: true,
            },
            SliceState {
                search_after: Some(vec![json!("b")]),
                exhausted: true,
            },
        ];
        let encoded = src.cursor().unwrap();

        let mut resumed = source(2);
        resumed.search_after = Some(encoded);
        resumed.seed_cursors();
        assert!(resumed.exhausted);
    }

    #[test]
    fn slice_count_mismatch_starts_fresh() {
        let mut src = source(2);
        src.slice_state = vec![
            SliceState {
                search_after: Some(vec![json!("a")]),
                exhausted: false,
            },
            SliceState::default(),
        ];
        let encoded = src.cursor().unwrap();

        let mut resumed = source(3);
        resumed.search_after = Some(encoded);
        resumed.seed_cursors();
        assert!(resumed
            .slice_state
            .iter()
            .all(|s| s.search_after.is_none() && !s.exhausted));
    }

    #[test]
    fn single_slice_discards_sliced_encoding() {
        let mut src = source(2);
        src.slice_state = vec![
            SliceState {
                search_after: Some(vec![json!("a")]),
                exhausted: false,
            },
            SliceState::default(),
        ];
        let encoded = src.cursor().unwrap();

        let mut single = source(1);
        single.search_after = Some(encoded);
        single.seed_cursors();
        assert!(single.search_after.is_none());
    }

    #[test]
    fn single_slice_keeps_normal_cursor() {
        let mut single = source(1);
        single.search_after = Some(vec![json!("doc-7")]);
        single.seed_cursors();
        // A plain single-slice cursor is preserved for resume.
        assert_eq!(single.search_after, Some(vec![json!("doc-7")]));
    }
}

#[cfg(all(test, feature = "aws"))]
mod tests {
    use super::*;
    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_credential_types::Credentials;
    use reqwest::Method;
    use serde_json::json;

    #[tokio::test]
    async fn test_sigv4_signing() {
        let creds = Credentials::new(
            "mock_access_key",
            "mock_secret_key",
            Some("mock_session_token".to_string()),
            None,
            "test-provider",
        );
        let provider = SharedCredentialsProvider::new(creds);
        let auth = Auth::AwsSigV4 {
            region: "us-east-1".to_string(),
            provider,
        };

        let source = OpenSearchSource::new(
            "https://search-my-domain.us-east-1.es.amazonaws.com",
            "my-index",
            json!({"match_all": {}}),
            100,
            auth,
            None,
        )
        .expect("source construction should succeed");

        // 1. Build a request to sign
        let url = format!("{}/_search", source.base_url);
        let body = json!({"size": 100});
        let mut req = source
            .client
            .request(Method::POST, url)
            .json(&body)
            .build()
            .unwrap();

        // Check host header and authorization are not set yet
        assert!(req.headers().get("host").is_none());
        assert!(req.headers().get("authorization").is_none());

        // 2. Sign request
        let region = "us-east-1";
        if let Auth::AwsSigV4 { provider, .. } = &source.auth {
            source
                .sign_request_aws(&mut req, region, provider)
                .await
                .expect("signing should succeed");
        } else {
            panic!("Expected Auth::AwsSigV4");
        }

        // 3. Verify signed request headers
        // host header should be set to domain
        let host_val = req
            .headers()
            .get("host")
            .expect("host header should be set")
            .to_str()
            .expect("host header should be valid ASCII");
        assert_eq!(host_val, "search-my-domain.us-east-1.es.amazonaws.com");

        // authorization header should be set
        let auth_val = req
            .headers()
            .get("authorization")
            .expect("authorization header should be set")
            .to_str()
            .expect("authorization header should be valid ASCII");
        assert!(auth_val.starts_with("AWS4-HMAC-SHA256"));
        assert!(auth_val.contains("Credential=mock_access_key/"));
        assert!(auth_val.contains("us-east-1/es/aws4_request"));

        // x-amz-security-token header should be set
        let token_val = req
            .headers()
            .get("x-amz-security-token")
            .expect("security token header should be set")
            .to_str()
            .expect("security token header should be valid ASCII");
        assert_eq!(token_val, "mock_session_token");

        // x-amz-date header should be set
        assert!(req.headers().get("x-amz-date").is_some());
    }
}
