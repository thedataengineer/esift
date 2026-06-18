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
        })
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

        if let Some(ref cursor) = self.search_after {
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
            self.exhausted = true;
            return Ok(None);
        }

        if hits.len() < self.batch_size {
            self.exhausted = true;
        }

        if let Some(last) = hits.last() {
            if let Some(sort_vals) = last["sort"].as_array() {
                self.search_after = Some(sort_vals.clone());
            }
        }

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

        debug!("Fetched {} documents", docs.len());
        Ok(Some(docs))
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
        format!("OpenSearch {} index={}", self.base_url, self.index)
    }

    fn cursor(&self) -> Option<Vec<Value>> {
        self.search_after.clone()
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
