use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Source kind: "opensearch" (default) or "file".
    #[serde(default = "default_source_kind")]
    pub kind: String,
    /// Base URL of the OpenSearch / Elasticsearch cluster (opensearch kind).
    #[serde(default)]
    pub url: String,
    /// Index name or pattern, e.g. "nginx-logs-*" (opensearch kind).
    #[serde(default)]
    pub index: String,
    /// Path to an NDJSON file (file kind).
    #[serde(default)]
    pub path: Option<String>,
    /// Optional basic auth
    pub username: Option<String>,
    pub password: Option<String>,
    /// Optional authentication type: basic or aws-sigv4
    #[serde(default)]
    pub auth_type: Option<String>,
    /// Optional AWS region for Signature Version 4 signing
    #[serde(default)]
    pub aws_region: Option<String>,
    /// Query DSL as a JSON string. Default: match_all.
    #[serde(default = "default_query")]
    pub query: String,
    /// Documents per batch
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Parallel extraction slices (sliced PIT). 1 = single slice.
    #[serde(default = "default_slices")]
    pub slices: usize,
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
        #[serde(default)]
        options: Box<crate::dest::openobserve::OpenObserveOptions>,
    },
    /// Newline-delimited JSON to a local file.
    File {
        path: String,
    },
    /// Objects written to an S3-compatible bucket.
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        region: Option<String>,
    },
    Stdout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EsiftConfig {
    pub source: SourceConfig,
    pub destination: DestConfig,
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,
    /// Document transforms applied between source and destination.
    #[serde(default)]
    pub transforms: Vec<crate::transform::Transform>,
    /// Optional address (e.g. "127.0.0.1:9090") to serve Prometheus metrics on.
    #[serde(default)]
    pub metrics_addr: Option<String>,
}

fn default_source_kind() -> String {
    "opensearch".into()
}

fn default_slices() -> usize {
    1
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

#[cfg(test)]
mod tests {
    use super::*;

    // Deserialize via serde_json: it exercises the same derive-generated
    // Deserialize impl the CLI drives through `toml`, without pulling toml in
    // as a test dependency here.

    #[test]
    fn source_config_fills_defaults_when_omitted() {
        let cfg: SourceConfig =
            serde_json::from_str(r#"{"url": "http://localhost:9200", "index": "logs-*"}"#).unwrap();
        assert_eq!(cfg.query, r#"{"match_all": {}}"#);
        assert_eq!(cfg.batch_size, 500);
        assert!(cfg.auth_type.is_none());
        assert!(cfg.aws_region.is_none());
        assert!(cfg.username.is_none());
    }

    #[test]
    fn source_config_parses_auth_fields() {
        let cfg: SourceConfig = serde_json::from_str(
            r#"{"url": "u", "index": "i", "auth_type": "aws-sigv4", "aws_region": "us-east-1", "batch_size": 100}"#,
        )
        .unwrap();
        assert_eq!(cfg.auth_type.as_deref(), Some("aws-sigv4"));
        assert_eq!(cfg.aws_region.as_deref(), Some("us-east-1"));
        assert_eq!(cfg.batch_size, 100);
    }

    #[test]
    fn esift_config_defaults_checkpoint_path_and_parses_stdout() {
        let cfg: EsiftConfig = serde_json::from_str(
            r#"{"source": {"url": "u", "index": "i"}, "destination": {"type": "stdout"}}"#,
        )
        .unwrap();
        assert_eq!(cfg.checkpoint_path, "./esift-checkpoint.json");
        assert!(matches!(cfg.destination, DestConfig::Stdout));
    }

    #[test]
    fn dest_config_parses_openobserve_variant() {
        let cfg: EsiftConfig = serde_json::from_str(
            r#"{"source": {"url": "u", "index": "i"}, "destination": {"type": "openobserve", "url": "http://oo", "org": "default", "stream": "s", "username": "a", "password": "b"}}"#,
        )
        .unwrap();
        match cfg.destination {
            DestConfig::OpenObserve { org, stream, .. } => {
                assert_eq!(org, "default");
                assert_eq!(stream, "s");
            }
            other => panic!("expected openobserve, got {other:?}"),
        }
    }
}
