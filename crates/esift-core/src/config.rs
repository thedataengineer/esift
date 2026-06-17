use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Base URL of the OpenSearch / Elasticsearch cluster
    pub url: String,
    /// Index name or pattern (e.g. "nginx-logs-*")
    pub index: String,
    /// Optional basic auth
    pub username: Option<String>,
    pub password: Option<String>,
    /// Query DSL as a JSON string. Default: match_all.
    #[serde(default = "default_query")]
    pub query: String,
    /// Documents per batch
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DestConfig {
    OpenObserve {
        url: String,
        org: String,
        stream: String,
        username: String,
        password: String,
    },
    Stdout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EsiftConfig {
    pub source: SourceConfig,
    pub destination: DestConfig,
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,
}

fn default_query() -> String {
    r#"{"match_all": {}}"#.into()
}

fn default_batch_size() -> usize {
    500
}

fn default_checkpoint_path() -> String {
    "./esift-checkpoint.json".into()
}
