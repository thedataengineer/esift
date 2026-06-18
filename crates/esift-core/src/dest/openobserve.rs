//! OpenObserve destination via the _bulk ingestion API.
//!
//! Confirmed endpoint from OpenObserve docs:
//!   POST /api/{org}/_bulk
//!
//! The stream name goes in the action line _index field, not the URL:
//!   {"index": {"_index": "stream_name"}}
//!   {...document body...}
//!
//! Ref: https://openobserve.ai/docs/api/ingestion/logs/bulk/

use super::Destination;
use crate::{
    error::{EsiftError, Result},
    Document,
};
use async_trait::async_trait;
use reqwest::Client;
use tracing::{debug, warn};

pub struct OpenObserveDestination {
    client: Client,
    base_url: String,
    org: String,
    stream: String,
    username: String,
    password: String,
}

impl OpenObserveDestination {
    pub fn new(
        base_url: impl Into<String>,
        org: impl Into<String>,
        stream: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            client: Client::builder().build()?,
            base_url: base_url.into(),
            org: org.into(),
            stream: stream.into(),
            username: username.into(),
            password: password.into(),
        })
    }

    fn bulk_url(&self) -> String {
        // Correct OpenObserve bulk endpoint: /api/{org}/_bulk
        // Stream is set per-document in the action line _index field
        format!("{}/api/{}/_bulk", self.base_url, self.org)
    }
}

#[async_trait]
impl Destination for OpenObserveDestination {
    async fn write_batch(&mut self, docs: Vec<Document>) -> Result<usize> {
        if docs.is_empty() {
            return Ok(0);
        }

        let count = docs.len();
        let mut body = String::new();

        for doc in &docs {
            // Action line: stream name goes in _index
            body.push_str(&format!(
                "{{\"index\":{{\"_index\":\"{}\"}}}}\n",
                self.stream
            ));
            body.push_str(&serde_json::to_string(&doc.body)?);
            body.push('\n');
        }

        debug!("POSTing {} docs to {}", count, self.bulk_url());

        let resp = self
            .client
            .post(self.bulk_url())
            .basic_auth(&self.username, Some(&self.password))
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            return Err(EsiftError::Destination(format!(
                "OpenObserve bulk failed: HTTP {} — {}",
                status, text
            )));
        }

        let bulk_response: serde_json::Value = resp.json().await?;
        if bulk_response["errors"].as_bool().unwrap_or(false) {
            warn!("Bulk response reported errors: {:?}", bulk_response);
        }

        Ok(count)
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "OpenObserve {} org={} stream={}",
            self.base_url, self.org, self.stream
        )
    }
}
