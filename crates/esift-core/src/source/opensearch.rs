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

pub struct OpenSearchSource {
    client: Client,
    base_url: String,
    index: String,
    query: Value,
    batch_size: usize,
    username: Option<String>,
    password: Option<String>,
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

impl OpenSearchSource {
    pub fn new(
        base_url: impl Into<String>,
        index: impl Into<String>,
        query: Value,
        batch_size: usize,
        username: Option<String>,
        password: Option<String>,
    ) -> anyhow::Result<Self> {
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
            username,
            password,
            pit_id: None,
            search_after: None,
            exhausted: false,
            flavor: ApiFlavor::Unknown,
        })
    }

    fn maybe_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match (&self.username, &self.password) {
            (Some(u), Some(p)) => req.basic_auth(u, Some(p)),
            _ => req,
        }
    }
}

#[async_trait]
impl Source for OpenSearchSource {
    async fn open(&mut self) -> Result<()> {
        info!("Opening PIT on index '{}'", self.index);

        // Try OpenSearch path first: POST /{index}/_search/point_in_time?keep_alive=5m
        let os_url = format!(
            "{}/{}/_search/point_in_time?keep_alive=5m",
            self.base_url, self.index
        );

        let resp = self.maybe_auth(self.client.post(&os_url)).send().await?;
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
            let es_url = format!("{}/{}/_pit?keep_alive=5m", self.base_url, self.index);
            let resp = self.maybe_auth(self.client.post(&es_url)).send().await?;

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
            let body = resp.text().await.unwrap_or_default();
            return Err(EsiftError::Source(format!(
                "PIT open failed (ES fallback): HTTP {} — {}",
                s, body
            )));
        }

        let body = resp.text().await.unwrap_or_default();
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

        let url = format!("{}/_search", self.base_url);
        let resp = self
            .maybe_auth(self.client.post(&url).json(&body))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(EsiftError::Source(format!(
                "Search failed: HTTP {} — {}",
                status, text
            )));
        }

        let result: Value = resp.json().await?;

        // Server may rotate the PIT id; always use the latest
        if let Some(new_pit_id) = result["pit_id"].as_str() {
            self.pit_id = Some(new_pit_id.to_string());
        }

        let hits = result["hits"]["hits"]
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
                    let url = format!("{}/_search/point_in_time", self.base_url);
                    let req = self.maybe_auth(
                        self.client
                            .delete(&url)
                            .json(&json!({ "pit_id": [pit_id] })),
                    );
                    if let Err(e) = req.send().await {
                        warn!("PIT close failed (non-fatal): {}", e);
                    }
                }
                ApiFlavor::Elasticsearch => {
                    let url = format!("{}/_pit", self.base_url);
                    let req =
                        self.maybe_auth(self.client.delete(&url).json(&json!({ "id": pit_id })));
                    if let Err(e) = req.send().await {
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
}
