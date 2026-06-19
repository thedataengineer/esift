//! Tunable options for the OpenObserve sink.
//!
//! Every field is `#[serde(default)]` and the whole struct derives `Default`,
//! so an omitted `[destination.options]` table yields the pre-refactor
//! behavior: no compression, single serial request, basic auth, default TLS,
//! fixed stream, no dead-letter, no retries.

use crate::http::retry::RetryPolicy;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenObserveOptions {
    /// Request body compression. Lane 3.
    #[serde(default)]
    pub compression: Compression,
    /// Cap each `_bulk` request body to this many bytes. None = one request
    /// per batch. Lane 4.
    #[serde(default)]
    pub max_batch_bytes: Option<usize>,
    /// Max concurrent in-flight bulk requests. 0 or 1 means serial. Lane 5.
    #[serde(default)]
    pub max_in_flight: usize,
    /// Token auth value; when set, used instead of basic auth. Lane 8.
    #[serde(default)]
    pub token: Option<String>,
    /// TLS client options. Lane 9.
    #[serde(default)]
    pub tls: TlsOptions,
    /// Document field whose value selects the stream. None = fixed stream.
    /// Lane 6.
    #[serde(default)]
    pub stream_field: Option<String>,
    /// Document field copied/parsed into `_timestamp`. Lane 7.
    #[serde(default)]
    pub timestamp_field: Option<String>,
    /// Write rejected docs here as NDJSON. None = drop after logging. Lane 10.
    #[serde(default)]
    pub dead_letter_path: Option<PathBuf>,
    /// Retry policy for transient failures. Lane 2.
    #[serde(default)]
    pub retry: RetryPolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    None,
    Gzip,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsOptions {
    /// Additional CA certificate to trust (PEM).
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
    /// Client certificate for mTLS (PEM).
    #[serde(default)]
    pub client_cert: Option<PathBuf>,
    /// Client private key for mTLS (PEM).
    #[serde(default)]
    pub client_key: Option<PathBuf>,
    /// Disable TLS verification. Dangerous; testing only.
    #[serde(default)]
    pub insecure: bool,
}
