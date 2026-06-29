//! Datadog archive source (Path 1): read compressed-JSON logs straight from
//! object storage. Works against S3 (feature `datadog-s3`), Google Cloud Storage
//! (feature `datadog-gcs`), or Azure Blob Storage (feature `datadog-azure`),
//! selected per source via [`CloudProvider`].
//!
//! Lane 1 implementation: lists archive objects under a prefix, downloads each,
//! hands the bytes to [`super::decompress`], splits the resulting NDJSON, maps
//! every line through [`super::flatten_archive`], and emits one
//! [`Document`] per line. One object is processed per `next_batch` call so the
//! caller checkpoints at file granularity; the resume blob in [`Source::cursor`]
//! records the last fully processed key so a later run skips it.
//!
//! Object-storage access is hidden behind the private [`ObjectStore`] seam so
//! the listing / download / decode / flatten pipeline is unit-testable against
//! an in-memory fake with no real cloud or emulator. Each cloud only implements
//! [`ObjectStore::list`] + [`ObjectStore::get`]; everything else is shared.

use super::decompress;
use crate::error::{EsiftError, Result};
use crate::source::Source;
use crate::Document;
use async_trait::async_trait;
use serde_json::Value;

/// Which object-storage backend an archive source reads from. Parsed from the
/// `dd_cloud` config / `--source-dd-cloud` flag; defaults to [`CloudProvider::S3`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CloudProvider {
    /// Amazon S3 (or any S3-compatible store). Feature `datadog-s3`.
    #[default]
    S3,
    /// Google Cloud Storage. Feature `datadog-gcs`.
    Gcs,
    /// Azure Blob Storage. Feature `datadog-azure`.
    Azure,
}

impl CloudProvider {
    /// Parse `"s3" | "gcs" | "azure"` (case-insensitive). An empty string is
    /// treated as the default ([`CloudProvider::S3`]); anything else errors.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "s3" => Ok(CloudProvider::S3),
            "gcs" => Ok(CloudProvider::Gcs),
            "azure" => Ok(CloudProvider::Azure),
            other => Err(EsiftError::Source(format!(
                "unknown dd_cloud '{other}'. Use 's3', 'gcs', or 'azure'"
            ))),
        }
    }

    /// Lowercase backend name, as used in feature flags and messages.
    pub fn as_str(self) -> &'static str {
        match self {
            CloudProvider::S3 => "s3",
            CloudProvider::Gcs => "gcs",
            CloudProvider::Azure => "azure",
        }
    }
}

/// How to choose the decompression codec for archive objects.
#[derive(Debug, Clone)]
pub enum Compression {
    /// Pick per object from the key suffix (`.zst` / `.gz`).
    Auto,
    /// Force one codec for every object.
    Fixed(decompress::Codec),
}

pub struct DatadogArchiveSource {
    bucket: String,
    prefix: String,
    /// Object-storage backend (S3 / GCS / Azure). Always present; the `Source`
    /// impl matches on it and dispatches to the feature-gated store.
    cloud: CloudProvider,
    // `region` is an S3-only concept; the GCS/Azure stores ignore it, so it is
    // only ever read by the S3 backend.
    #[cfg_attr(not(feature = "datadog-s3"), allow(dead_code))]
    region: Option<String>,
    #[cfg_attr(
        not(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        )),
        allow(dead_code)
    )]
    from: Option<String>,
    #[cfg_attr(
        not(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        )),
        allow(dead_code)
    )]
    to: Option<String>,
    #[cfg_attr(
        not(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        )),
        allow(dead_code)
    )]
    compression: Compression,
    /// Opaque resume blob from a prior checkpoint cursor; decoded in `open`.
    #[cfg_attr(
        not(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        )),
        allow(dead_code)
    )]
    resume_after: Option<Vec<Value>>,
    /// Runtime listing/position state, populated by `open`. Only meaningful when
    /// a cloud backend is compiled in; the fallback impl never reads it.
    #[cfg(any(
        feature = "datadog-s3",
        feature = "datadog-gcs",
        feature = "datadog-azure"
    ))]
    state: ArchiveState,
    /// Object-storage client, built once in `open` and reused by every
    /// `next_batch` call. Caching it avoids rebuilding a client — and, for the
    /// GCS/Azure backends, re-walking the credential chain and fetching a fresh
    /// OAuth token — on every archive file. `None` until `open` runs.
    #[cfg(any(
        feature = "datadog-s3",
        feature = "datadog-gcs",
        feature = "datadog-azure"
    ))]
    store: Option<Box<dyn ObjectStore>>,
}

impl DatadogArchiveSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: Option<String>,
        from: Option<String>,
        to: Option<String>,
        compression: Compression,
        resume_after: Option<Vec<Value>>,
    ) -> Result<Self> {
        Ok(Self {
            bucket: bucket.into(),
            prefix: prefix.into(),
            cloud: CloudProvider::default(),
            region,
            from,
            to,
            compression,
            resume_after,
            #[cfg(any(
                feature = "datadog-s3",
                feature = "datadog-gcs",
                feature = "datadog-azure"
            ))]
            state: ArchiveState::default(),
            #[cfg(any(
                feature = "datadog-s3",
                feature = "datadog-gcs",
                feature = "datadog-azure"
            ))]
            store: None,
        })
    }

    /// Select the object-storage backend (defaults to [`CloudProvider::S3`]).
    /// Mirrors `OpenSearchSource::with_slices`: chainable, returns `self`.
    pub fn with_cloud(mut self, cloud: CloudProvider) -> Self {
        self.cloud = cloud;
        self
    }
}

// ---------------------------------------------------------------------------
// Shared pipeline (any cloud backend): listing/position state + the ObjectStore
// seam + the open/next_batch helpers every backend drives.
// ---------------------------------------------------------------------------

/// Mutable listing/position state for an in-progress archive extraction.
#[cfg(any(
    feature = "datadog-s3",
    feature = "datadog-gcs",
    feature = "datadog-azure"
))]
#[derive(Default)]
struct ArchiveState {
    /// Sorted object keys remaining to process (`open` drops already-done ones).
    keys: Vec<String>,
    /// Index into `keys` of the next object `next_batch` will fetch.
    pos: usize,
    /// Key of the most recently completed object; the resume anchor.
    last_key: Option<String>,
    /// Number of objects fully processed so far.
    files_done: u64,
    /// True once `open` has run; guards `next_batch` against an un-opened source.
    opened: bool,
}

/// Object-store seam: the minimal listing/download surface the archive source
/// needs. Implemented by [`S3ObjectStore`] / [`GcsObjectStore`] /
/// [`AzureObjectStore`] in production and by an in-memory fake in tests, so the
/// extraction pipeline is exercised without any real cloud.
#[cfg(any(
    feature = "datadog-s3",
    feature = "datadog-gcs",
    feature = "datadog-azure"
))]
#[async_trait]
trait ObjectStore: Send + Sync {
    /// All object keys under `prefix`, in any order (the caller sorts).
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// The raw bytes of the object at `key`.
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
}

/// aws-sdk-s3-backed [`ObjectStore`]. Mirrors the client construction in
/// `dest/s3.rs`: default credential/region chain, optional region override.
#[cfg(feature = "datadog-s3")]
struct S3ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

#[cfg(feature = "datadog-s3")]
impl S3ObjectStore {
    async fn new(bucket: String, region: Option<String>) -> Self {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(aws_config::Region::new(region));
        }
        let config = loader.load().await;
        let client = aws_sdk_s3::Client::new(&config);
        Self { client, bucket }
    }
}

#[cfg(feature = "datadog-s3")]
#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = &continuation {
                req = req.continuation_token(token);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| EsiftError::Source(format!("S3 list_objects_v2 failed: {e}")))?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(|s| s.to_string());
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(keys)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| EsiftError::Source(format!("S3 get_object failed for {key}: {e}")))?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| EsiftError::Source(format!("S3 body read failed for {key}: {e}")))?;
        Ok(data.into_bytes().to_vec())
    }
}

// ---------------------------------------------------------------------------
// Google Cloud Storage backend (datadog-gcs)
// ---------------------------------------------------------------------------

/// `google-cloud-storage`-backed [`ObjectStore`]. Authenticates via Application
/// Default Credentials (the SDK's default chain: `GOOGLE_APPLICATION_CREDENTIALS`
/// service-account key file, gcloud user creds, GCE/GKE metadata, etc.).
///
/// The 1.x SDK splits responsibilities across two clients: object *data* (read)
/// lives on [`google_cloud_storage::client::Storage`], while object *metadata*
/// listing lives on [`google_cloud_storage::client::StorageControl`]. Both name
/// buckets with the resource path `projects/_/buckets/{bucket}`.
#[cfg(feature = "datadog-gcs")]
struct GcsObjectStore {
    data: google_cloud_storage::client::Storage,
    control: google_cloud_storage::client::StorageControl,
    /// Resource path `projects/_/buckets/{bucket}` reused for every call.
    parent: String,
}

#[cfg(feature = "datadog-gcs")]
impl GcsObjectStore {
    async fn new(bucket: String) -> Result<Self> {
        let data = google_cloud_storage::client::Storage::builder()
            .build()
            .await
            .map_err(|e| EsiftError::Source(format!("GCS client build failed: {e}")))?;
        let control = google_cloud_storage::client::StorageControl::builder()
            .build()
            .await
            .map_err(|e| EsiftError::Source(format!("GCS control client build failed: {e}")))?;
        Ok(Self {
            data,
            control,
            parent: format!("projects/_/buckets/{bucket}"),
        })
    }
}

#[cfg(feature = "datadog-gcs")]
#[async_trait]
impl ObjectStore for GcsObjectStore {
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // `by_item()` returns an ItemPaginator that transparently follows the
        // page token, yielding one `Object` at a time; we keep only its name.
        use google_cloud_gax::paginator::ItemPaginator as _;
        let mut keys = Vec::new();
        let mut paginator = self
            .control
            .list_objects()
            .set_parent(&self.parent)
            .set_prefix(prefix)
            .by_item();
        while let Some(item) = paginator.next().await {
            let object =
                item.map_err(|e| EsiftError::Source(format!("GCS list_objects failed: {e}")))?;
            keys.push(object.name);
        }
        Ok(keys)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let mut reader = self
            .data
            .read_object(&self.parent, key)
            .send()
            .await
            .map_err(|e| EsiftError::Source(format!("GCS read_object failed for {key}: {e}")))?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reader
            .next()
            .await
            .transpose()
            .map_err(|e| EsiftError::Source(format!("GCS read body failed for {key}: {e}")))?
        {
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

// ---------------------------------------------------------------------------
// Azure Blob Storage backend (datadog-azure)
// ---------------------------------------------------------------------------

/// `azure_storage_blob`-backed [`ObjectStore`] over a single container. The
/// "bucket" field carries the container name.
///
/// Endpoint: the storage account is taken from `AZURE_STORAGE_ACCOUNT`, forming
/// `https://{account}.blob.core.windows.net/{container}` (override the full host
/// with `AZURE_STORAGE_BLOB_ENDPOINT` for sovereign clouds or an emulator).
///
/// Auth (the GA `azure_storage_blob` 1.x takes an `Arc<dyn TokenCredential>`, so
/// we use the `azure_identity` chain rather than a connection string):
///   * If `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` are set,
///     a service-principal `ClientSecretCredential` is used (non-interactive).
///   * Otherwise `DeveloperToolsCredential` (Azure CLI / `azd` login) is used —
///     the 1.0 analog of `DefaultAzureCredential`.
#[cfg(feature = "datadog-azure")]
struct AzureObjectStore {
    container: azure_storage_blob::BlobContainerClient,
}

#[cfg(feature = "datadog-azure")]
impl AzureObjectStore {
    /// Resolve an Azure token credential. `azure_identity` 1.0 ships no
    /// `DefaultAzureCredential` (nor a public credential-chain type), so we
    /// select one deterministically by environment, covering the same ground
    /// its chain would for archive use:
    /// 1. **Workload Identity** (AKS federated tokens) when
    ///    `AZURE_FEDERATED_TOKEN_FILE` is set.
    /// 2. **Service principal** (`ClientSecretCredential`) when
    ///    `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` are set.
    /// 3. **Developer tools** (`az` / `azd` CLI) otherwise, for local use.
    ///
    /// Managed Identity (IMDS) has no clean env marker to auto-select on, so it
    /// is left as a follow-up.
    fn credential() -> Result<std::sync::Arc<dyn azure_core::credentials::TokenCredential>> {
        use std::env::var;
        if var("AZURE_FEDERATED_TOKEN_FILE").is_ok() {
            let cred = azure_identity::WorkloadIdentityCredential::new(None).map_err(|e| {
                EsiftError::Source(format!("Azure WorkloadIdentityCredential failed: {e}"))
            })?;
            return Ok(cred);
        }
        if let (Ok(tenant), Ok(client), Ok(secret)) = (
            var("AZURE_TENANT_ID"),
            var("AZURE_CLIENT_ID"),
            var("AZURE_CLIENT_SECRET"),
        ) {
            let cred =
                azure_identity::ClientSecretCredential::new(&tenant, client, secret.into(), None)
                    .map_err(|e| {
                    EsiftError::Source(format!("Azure ClientSecretCredential failed: {e}"))
                })?;
            return Ok(cred);
        }
        let cred = azure_identity::DeveloperToolsCredential::new(None).map_err(|e| {
            EsiftError::Source(format!("Azure DeveloperToolsCredential failed: {e}"))
        })?;
        Ok(cred)
    }

    fn new(container_name: String) -> Result<Self> {
        let container_url = match std::env::var("AZURE_STORAGE_BLOB_ENDPOINT") {
            Ok(endpoint) => {
                let base = endpoint.trim_end_matches('/');
                format!("{base}/{container_name}")
            }
            Err(_) => {
                let account = std::env::var("AZURE_STORAGE_ACCOUNT").map_err(|_| {
                    EsiftError::Source(
                        "Azure archive requires AZURE_STORAGE_ACCOUNT (or \
                         AZURE_STORAGE_BLOB_ENDPOINT) to locate the blob endpoint"
                            .into(),
                    )
                })?;
                format!("https://{account}.blob.core.windows.net/{container_name}")
            }
        };
        let url = azure_core::http::Url::parse(&container_url)
            .map_err(|e| EsiftError::Source(format!("invalid Azure container URL: {e}")))?;
        let container =
            azure_storage_blob::BlobContainerClient::new(url, Some(Self::credential()?), None)
                .map_err(|e| {
                    EsiftError::Source(format!("Azure container client build failed: {e}"))
                })?;
        Ok(Self { container })
    }
}

#[cfg(feature = "datadog-azure")]
#[async_trait]
impl ObjectStore for AzureObjectStore {
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // `Pager<ListBlobsResponse>` is an item-iterator: `ListBlobsResponse`
        // implements `Page<Item = BlobItem>`, so the stream flattens pages and
        // yields one `BlobItem` per step, transparently following the marker.
        use futures::TryStreamExt as _;
        let options = azure_storage_blob::models::BlobContainerClientListBlobsOptions {
            prefix: Some(prefix.to_string()),
            ..Default::default()
        };
        let mut pager = self
            .container
            .list_blobs(Some(options))
            .map_err(|e| EsiftError::Source(format!("Azure list_blobs failed: {e}")))?;
        let mut keys = Vec::new();
        while let Some(blob) = pager
            .try_next()
            .await
            .map_err(|e| EsiftError::Source(format!("Azure list_blobs failed: {e}")))?
        {
            if let Some(name) = blob.name {
                keys.push(name);
            }
        }
        Ok(keys)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let blob = self.container.blob_client(key);
        let resp = blob
            .download(None)
            .await
            .map_err(|e| EsiftError::Source(format!("Azure download failed for {key}: {e}")))?;
        let bytes =
            resp.body.collect().await.map_err(|e| {
                EsiftError::Source(format!("Azure body read failed for {key}: {e}"))
            })?;
        Ok(bytes.to_vec())
    }
}

/// Register descriptions/units for the archive metrics with the global recorder.
///
/// Descriptions route to whatever recorder the CLI installed before `open` runs;
/// with no recorder (e.g. in unit tests) every `describe_*!` is a no-op. Safe to
/// call repeatedly — `open_with_store` invokes it once per run.
#[cfg(any(
    feature = "datadog-s3",
    feature = "datadog-gcs",
    feature = "datadog-azure"
))]
fn describe_metrics() {
    use metrics::Unit;
    metrics::describe_histogram!(
        "esift_archive_list_seconds",
        Unit::Seconds,
        "Time spent listing archive objects under the prefix"
    );
    metrics::describe_counter!(
        "esift_archive_objects_listed_total",
        "Archive object keys returned by listing (after filtering)"
    );
    metrics::describe_histogram!(
        "esift_archive_get_seconds",
        Unit::Seconds,
        "Time spent downloading a single archive object"
    );
    metrics::describe_counter!(
        "esift_archive_files_total",
        "Archive objects downloaded and processed"
    );
    metrics::describe_counter!(
        "esift_archive_bytes_total",
        Unit::Bytes,
        "Compressed bytes downloaded from archive objects"
    );
    metrics::describe_counter!(
        "esift_archive_decode_errors_total",
        "Archive JSON lines that failed to parse"
    );
    metrics::describe_counter!(
        "esift_archive_decompressed_bytes_total",
        Unit::Bytes,
        "Decompressed bytes produced from archive objects"
    );
}

#[cfg(any(
    feature = "datadog-s3",
    feature = "datadog-gcs",
    feature = "datadog-azure"
))]
impl DatadogArchiveSource {
    /// Decode the `{"dd_archive":{"last_key":..,"files_done":..}}` resume blob
    /// written by [`Source::cursor`], if present. Returns the anchor key and the
    /// processed-file count to seed the new run's state.
    fn decode_resume(&self) -> (Option<String>, u64) {
        let Some(values) = &self.resume_after else {
            return (None, 0);
        };
        for v in values {
            if let Some(blob) = v.get("dd_archive") {
                let last_key = blob
                    .get("last_key")
                    .and_then(|k| k.as_str())
                    .map(|s| s.to_string());
                let files_done = blob.get("files_done").and_then(|n| n.as_u64()).unwrap_or(0);
                return (last_key, files_done);
            }
        }
        (None, 0)
    }

    /// Run the full open → exhaust-all-batches pipeline against `store`, a test
    /// convenience over the same `open_with_store` / `next_batch_with_store` the
    /// production [`Source`] impl drives, so tests exercise identical
    /// listing/decode/flatten logic without a real S3.
    #[cfg(test)]
    async fn run_with_store(&mut self, store: &dyn ObjectStore) -> Result<Vec<Document>> {
        self.open_with_store(store).await?;
        let mut all = Vec::new();
        while let Some(batch) = self.next_batch_with_store(store).await? {
            all.extend(batch);
        }
        Ok(all)
    }

    /// Extract the `(year, month, day, hour)` hour bucket from an archive key.
    ///
    /// Datadog lays keys out as `{prefix}/YYYY/MM/DD/HH/MM_hash.json.zst`. We
    /// strip `self.prefix`, split the remainder on `/`, and parse the first four
    /// segments as zero-padded numbers (widths 4,2,2,2). Returns `None` if the
    /// remainder doesn't have that shape, so callers can treat such keys
    /// conservatively (never silently dropped).
    fn key_hour_bucket(&self, key: &str) -> Option<(u32, u32, u32, u32)> {
        let rest = key.strip_prefix(&self.prefix)?;
        let rest = rest.strip_prefix('/').unwrap_or(rest);
        let mut segs = rest.split('/');
        let y = segs.next()?;
        let mo = segs.next()?;
        let d = segs.next()?;
        let h = segs.next()?;
        if y.len() != 4 || mo.len() != 2 || d.len() != 2 || h.len() != 2 {
            return None;
        }
        Some((
            y.parse().ok()?,
            mo.parse().ok()?,
            d.parse().ok()?,
            h.parse().ok()?,
        ))
    }

    /// Parse an ISO8601 timestamp (e.g. `2025-01-01T00:00:00Z`) into the same
    /// `(year, month, day, hour)` hour bucket by slicing fixed positions.
    /// Returns `None` if the string is too short or any field isn't numeric.
    fn iso_hour_bucket(ts: &str) -> Option<(u32, u32, u32, u32)> {
        if ts.len() < 13 {
            return None;
        }
        Some((
            ts.get(0..4)?.parse().ok()?,
            ts.get(5..7)?.parse().ok()?,
            ts.get(8..10)?.parse().ok()?,
            ts.get(11..13)?.parse().ok()?,
        ))
    }

    /// List + sort + apply resume, populating `self.state`.
    async fn open_with_store(&mut self, store: &dyn ObjectStore) -> Result<()> {
        describe_metrics();
        let cloud = self.cloud.as_str();

        let list_start = std::time::Instant::now();
        let mut keys = store.list(&self.prefix).await?;
        metrics::histogram!("esift_archive_list_seconds", "cloud" => cloud)
            .record(list_start.elapsed().as_secs_f64());
        let listed = keys.len();
        metrics::counter!("esift_archive_objects_listed_total", "cloud" => cloud)
            .increment(listed as u64);
        keys.sort();

        let (last_key, files_done) = self.decode_resume();
        if let Some(anchor) = &last_key {
            keys.retain(|k| k > anchor);
        }

        // Hour-granularity time-range filtering on `from`/`to`. A key is kept
        // when its hour bucket is `>= from`'s bucket (if set) AND `<= to`'s
        // bucket (if set). Because comparison is at hour granularity, the
        // boundary hours containing `from` and `to` are fully included (every
        // minute file in that hour passes). Keys whose date can't be parsed are
        // kept unconditionally — we never silently drop an object we can't
        // classify.
        let from_bucket = self.from.as_deref().and_then(Self::iso_hour_bucket);
        let to_bucket = self.to.as_deref().and_then(Self::iso_hour_bucket);
        if from_bucket.is_some() || to_bucket.is_some() {
            keys.retain(|k| match self.key_hour_bucket(k) {
                None => true,
                Some(bucket) => {
                    from_bucket.is_none_or(|f| bucket >= f) && to_bucket.is_none_or(|t| bucket <= t)
                }
            });
        }

        let remaining = keys.len();
        tracing::info!(
            cloud = %cloud,
            bucket = %self.bucket,
            prefix = %self.prefix,
            listed,
            remaining,
            "opened Datadog archive source; listing complete"
        );

        self.state = ArchiveState {
            keys,
            pos: 0,
            last_key,
            files_done,
            opened: true,
        };
        Ok(())
    }

    /// Process the next object via `store`, returning its documents, or `None`
    /// once the key list is exhausted.
    async fn next_batch_with_store(
        &mut self,
        store: &dyn ObjectStore,
    ) -> Result<Option<Vec<Document>>> {
        if !self.state.opened {
            return Err(EsiftError::Source(
                "Datadog archive source used before open()".into(),
            ));
        }
        let Some(key) = self.state.keys.get(self.state.pos).cloned() else {
            return Ok(None);
        };

        let codec = match &self.compression {
            Compression::Fixed(c) => *c,
            Compression::Auto => decompress::Codec::from_key(&key).ok_or_else(|| {
                EsiftError::Source(format!(
                    "cannot infer compression codec from key {key} (expected .zst/.gz); \
                     set an explicit compression"
                ))
            })?,
        };

        let cloud = self.cloud.as_str();
        let get_start = std::time::Instant::now();
        let raw = store.get(&key).await?;
        let elapsed = get_start.elapsed();
        metrics::histogram!("esift_archive_get_seconds", "cloud" => cloud)
            .record(elapsed.as_secs_f64());
        let bytes = raw.len();
        metrics::counter!("esift_archive_files_total", "cloud" => cloud).increment(1);
        metrics::counter!("esift_archive_bytes_total", "cloud" => cloud).increment(bytes as u64);
        tracing::debug!(
            %key,
            bytes,
            elapsed_ms = elapsed.as_millis(),
            "downloaded archive object"
        );

        let decoded = decompress::decompress(&raw, codec)?;
        let text = String::from_utf8(decoded)
            .map_err(|e| EsiftError::Source(format!("archive {key} is not valid UTF-8: {e}")))?;

        let mut docs = Vec::new();
        for (n, line) in text.split('\n').enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_str(line) {
                Ok(value) => value,
                Err(e) => {
                    metrics::counter!("esift_archive_decode_errors_total", "cloud" => cloud)
                        .increment(1);
                    tracing::warn!(%key, line = n, error = %e, "failed to parse archive JSON line");
                    return Err(EsiftError::Source(format!(
                        "invalid JSON in {key} line {n}: {e}"
                    )));
                }
            };
            let body = super::flatten_archive::flatten(value);
            docs.push(Document::new("datadog", format!("{key}#{n}"), body));
        }

        self.state.pos += 1;
        self.state.files_done += 1;
        self.state.last_key = Some(key);
        Ok(Some(docs))
    }
}

// ---------------------------------------------------------------------------
// Per-cloud store construction. Each is gated on its own feature; when the
// selected cloud's feature is off, the matching `Source` arm returns a clear
// build-with-feature error instead of calling these.
// ---------------------------------------------------------------------------

// Per-cloud `open` arms. Each builds its cloud's client, runs the initial
// listing, and caches the client in `self.store` so `next_batch` can reuse it
// rather than rebuilding (and re-authenticating) a client per archive file.
// Each is gated on its feature, so a cloud's code is genuinely compiled out
// when its feature is off.

#[cfg(feature = "datadog-s3")]
impl DatadogArchiveSource {
    async fn open_s3(&mut self) -> Result<()> {
        let store = S3ObjectStore::new(self.bucket.clone(), self.region.clone()).await;
        self.open_with_store(&store).await?;
        self.store = Some(Box::new(store));
        Ok(())
    }
}

#[cfg(feature = "datadog-gcs")]
impl DatadogArchiveSource {
    async fn open_gcs(&mut self) -> Result<()> {
        let store = GcsObjectStore::new(self.bucket.clone()).await?;
        self.open_with_store(&store).await?;
        self.store = Some(Box::new(store));
        Ok(())
    }
}

#[cfg(feature = "datadog-azure")]
impl DatadogArchiveSource {
    async fn open_azure(&mut self) -> Result<()> {
        let store = AzureObjectStore::new(self.bucket.clone())?;
        self.open_with_store(&store).await?;
        self.store = Some(Box::new(store));
        Ok(())
    }
}

// `next_batch` is cloud-agnostic once `open` has cached the client: it borrows
// the stored `ObjectStore` and reuses it across every file. We `take` the box
// out for the call so the `&mut self` borrow in `next_batch_with_store` doesn't
// alias the field, then put it straight back.
#[cfg(any(
    feature = "datadog-s3",
    feature = "datadog-gcs",
    feature = "datadog-azure"
))]
impl DatadogArchiveSource {
    async fn next_batch_cached(&mut self) -> Result<Option<Vec<Document>>> {
        let store = self.store.take().ok_or_else(|| {
            EsiftError::Source("Datadog archive source used before open()".into())
        })?;
        let result = self.next_batch_with_store(&*store).await;
        self.store = Some(store);
        result
    }
}

/// The clear, actionable error returned when a cloud is selected but its backend
/// feature wasn't compiled in.
#[cfg_attr(
    all(
        feature = "datadog-s3",
        feature = "datadog-gcs",
        feature = "datadog-azure"
    ),
    allow(dead_code)
)]
fn feature_error(cloud: CloudProvider) -> EsiftError {
    EsiftError::Source(format!(
        "Datadog archive source with cloud '{}' requires building with --features datadog-{}",
        cloud.as_str(),
        cloud.as_str()
    ))
}

#[async_trait]
impl Source for DatadogArchiveSource {
    #[tracing::instrument(
        skip(self),
        fields(cloud = %self.cloud.as_str(), bucket = %self.bucket, prefix = %self.prefix)
    )]
    async fn open(&mut self) -> Result<()> {
        match self.cloud {
            #[cfg(feature = "datadog-s3")]
            CloudProvider::S3 => self.open_s3().await,
            #[cfg(not(feature = "datadog-s3"))]
            CloudProvider::S3 => Err(feature_error(CloudProvider::S3)),

            #[cfg(feature = "datadog-gcs")]
            CloudProvider::Gcs => self.open_gcs().await,
            #[cfg(not(feature = "datadog-gcs"))]
            CloudProvider::Gcs => Err(feature_error(CloudProvider::Gcs)),

            #[cfg(feature = "datadog-azure")]
            CloudProvider::Azure => self.open_azure().await,
            #[cfg(not(feature = "datadog-azure"))]
            CloudProvider::Azure => Err(feature_error(CloudProvider::Azure)),
        }
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        // Once `open` has cached the client, `next_batch` is the same regardless
        // of cloud. If the selected cloud's feature wasn't compiled in, `open`
        // already returned `feature_error`, so reaching here means the store is
        // present.
        #[cfg(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        ))]
        {
            self.next_batch_cached().await
        }
        #[cfg(not(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        )))]
        {
            Err(feature_error(self.cloud))
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "Datadog archive cloud={} bucket={} prefix={}",
            self.cloud.as_str(),
            self.bucket,
            self.prefix
        )
    }

    fn cursor(&self) -> Option<Vec<Value>> {
        // `state` only exists when a cloud backend is compiled in. With no
        // backend the source can't run anyway (open() errors), so report no
        // resume cursor rather than fabricate one.
        #[cfg(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        ))]
        {
            Some(vec![serde_json::json!({
                "dd_archive": {
                    "last_key": self.state.last_key,
                    "files_done": self.state.files_done,
                }
            })])
        }
        #[cfg(not(any(
            feature = "datadog-s3",
            feature = "datadog-gcs",
            feature = "datadog-azure"
        )))]
        {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests: fake object store, no real cloud / emulator.
// ---------------------------------------------------------------------------

// Pipeline tests drive the shared `ObjectStore` seam, so they run under any
// cloud backend feature (the in-memory `FakeStore` needs no real cloud).
#[cfg(all(
    test,
    any(
        feature = "datadog-s3",
        feature = "datadog-gcs",
        feature = "datadog-azure"
    )
))]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression as GzLevel};
    use std::collections::BTreeMap;
    use std::io::Write;

    /// In-memory [`ObjectStore`] over a key → bytes map.
    struct FakeStore {
        objects: BTreeMap<String, Vec<u8>>,
    }

    #[async_trait]
    impl ObjectStore for FakeStore {
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            Ok(self
                .objects
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.objects
                .get(key)
                .cloned()
                .ok_or_else(|| EsiftError::Source(format!("no such key {key}")))
        }
    }

    fn gzip(ndjson: &str) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), GzLevel::default());
        enc.write_all(ndjson.as_bytes()).unwrap();
        enc.finish().unwrap()
    }

    /// Two small gzip NDJSON files under a dated prefix.
    fn seed_store() -> FakeStore {
        // File 1: two events, the second exercising the double-nested-attribute
        // hoist that `flatten_archive` performs.
        let f1 = "\
{\"host\":\"h1\",\"service\":\"web\",\"attributes\":{\"status\":\"info\"}}
{\"host\":\"h2\",\"attributes\":{\"attributes\":{\"user_id\":\"u7\"}}}
";
        // File 2: one event, plus a trailing blank line that must be skipped.
        let f2 = "{\"host\":\"h3\",\"service\":\"db\"}\n\n";

        let mut objects = BTreeMap::new();
        objects.insert("dd/2026/06/19/00/00_first.json.gz".to_string(), gzip(f1));
        objects.insert("dd/2026/06/19/00/01_second.json.gz".to_string(), gzip(f2));
        FakeStore { objects }
    }

    fn new_source(resume_after: Option<Vec<Value>>) -> DatadogArchiveSource {
        DatadogArchiveSource::new(
            "my-bucket",
            "dd/2026/06/19/",
            Some("us-east-1".to_string()),
            None,
            None,
            Compression::Auto,
            resume_after,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn lists_decodes_and_flattens_all_files() {
        let store = seed_store();
        let mut src = new_source(None);
        let docs = src.run_with_store(&store).await.unwrap();

        // 2 events from file 1 + 1 event from file 2 (blank line dropped).
        assert_eq!(docs.len(), 3);

        // Every document is indexed under "datadog".
        assert!(docs.iter().all(|d| d.index == "datadog"));

        // Ids are "{key}#{line}" with keys visited in sorted order.
        assert_eq!(docs[0].id, "dd/2026/06/19/00/00_first.json.gz#0");
        assert_eq!(docs[1].id, "dd/2026/06/19/00/00_first.json.gz#1");
        assert_eq!(docs[2].id, "dd/2026/06/19/00/01_second.json.gz#0");

        // Flattened bodies: top-level metadata + hoisted attributes.
        assert_eq!(
            docs[0].body,
            serde_json::json!({"host":"h1","service":"web","status":"info"})
        );
        assert_eq!(
            docs[1].body,
            serde_json::json!({"host":"h2","user_id":"u7"})
        );
        assert_eq!(
            docs[2].body,
            serde_json::json!({"host":"h3","service":"db"})
        );
    }

    #[tokio::test]
    async fn cursor_tracks_last_key_and_count() {
        let store = seed_store();
        let mut src = new_source(None);

        // Before any work the cursor reports a null anchor.
        let start = src.cursor().unwrap();
        assert_eq!(start[0]["dd_archive"]["last_key"], Value::Null);
        assert_eq!(start[0]["dd_archive"]["files_done"], 0);

        src.run_with_store(&store).await.unwrap();

        let end = src.cursor().unwrap();
        assert_eq!(
            end[0]["dd_archive"]["last_key"],
            "dd/2026/06/19/00/01_second.json.gz"
        );
        assert_eq!(end[0]["dd_archive"]["files_done"], 2);
    }

    #[tokio::test]
    async fn resume_blob_skips_processed_keys() {
        let store = seed_store();
        // Resume blob says the first file is already done; only file 2 remains.
        let resume = Some(vec![serde_json::json!({
            "dd_archive": {
                "last_key": "dd/2026/06/19/00/00_first.json.gz",
                "files_done": 1,
            }
        })]);
        let mut src = new_source(resume);
        let docs = src.run_with_store(&store).await.unwrap();

        // Only the single event from the second file is emitted.
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "dd/2026/06/19/00/01_second.json.gz#0");

        // files_done continues from the resumed count (1 → 2).
        let end = src.cursor().unwrap();
        assert_eq!(end[0]["dd_archive"]["files_done"], 2);
        assert_eq!(
            end[0]["dd_archive"]["last_key"],
            "dd/2026/06/19/00/01_second.json.gz"
        );
    }

    #[tokio::test]
    async fn next_batch_returns_one_file_per_call() {
        let store = seed_store();
        let mut src = new_source(None);
        src.open_with_store(&store).await.unwrap();

        let first = src.next_batch_with_store(&store).await.unwrap().unwrap();
        assert_eq!(first.len(), 2); // file 1
        let second = src.next_batch_with_store(&store).await.unwrap().unwrap();
        assert_eq!(second.len(), 1); // file 2
        assert!(src.next_batch_with_store(&store).await.unwrap().is_none());
    }

    /// One gzip NDJSON file per hour bucket 00..=02 under prefix `dd/` (which
    /// does NOT include the date path, so the YYYY/MM/DD/HH segments survive the
    /// prefix strip), plus one key with an unparseable date segment.
    fn seed_hourly_store() -> FakeStore {
        let mut objects = BTreeMap::new();
        objects.insert(
            "dd/2026/06/19/00/00_h0.json.gz".to_string(),
            gzip("{\"host\":\"h0\"}\n"),
        );
        objects.insert(
            "dd/2026/06/19/01/00_h1.json.gz".to_string(),
            gzip("{\"host\":\"h1\"}\n"),
        );
        objects.insert(
            "dd/2026/06/19/02/00_h2.json.gz".to_string(),
            gzip("{\"host\":\"h2\"}\n"),
        );
        // Unparseable date shape (no YYYY/MM/DD/HH): must always be kept.
        objects.insert(
            "dd/2026/06/19/weird/00_x.json.gz".to_string(),
            gzip("{\"host\":\"hx\"}\n"),
        );
        FakeStore { objects }
    }

    fn new_source_with_range(from: Option<&str>, to: Option<&str>) -> DatadogArchiveSource {
        DatadogArchiveSource::new(
            "my-bucket",
            "dd/",
            None,
            from.map(|s| s.to_string()),
            to.map(|s| s.to_string()),
            Compression::Auto,
            None,
        )
        .unwrap()
    }

    fn host_of(doc: &Document) -> String {
        doc.body["host"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn from_filters_out_earlier_hours() {
        let store = seed_hourly_store();
        // Lower bound at hour 01: drop hour 00, keep 01, 02, and the unparseable.
        let mut src = new_source_with_range(Some("2026-06-19T01:30:00Z"), None);
        let docs = src.run_with_store(&store).await.unwrap();
        let hosts: Vec<String> = docs.iter().map(host_of).collect();
        assert_eq!(hosts, vec!["h1", "h2", "hx"]);
    }

    #[tokio::test]
    async fn to_filters_out_later_hours() {
        let store = seed_hourly_store();
        // Upper bound at hour 01: keep 00, 01, and the unparseable; drop 02.
        let mut src = new_source_with_range(None, Some("2026-06-19T01:15:00Z"));
        let docs = src.run_with_store(&store).await.unwrap();
        let hosts: Vec<String> = docs.iter().map(host_of).collect();
        assert_eq!(hosts, vec!["h0", "h1", "hx"]);
    }

    #[tokio::test]
    async fn from_and_to_bound_a_single_hour() {
        let store = seed_hourly_store();
        // Both bounds inside hour 01: only hour 01 plus the unparseable remain.
        let mut src =
            new_source_with_range(Some("2026-06-19T01:00:00Z"), Some("2026-06-19T01:59:59Z"));
        let docs = src.run_with_store(&store).await.unwrap();
        let hosts: Vec<String> = docs.iter().map(host_of).collect();
        assert_eq!(hosts, vec!["h1", "hx"]);
    }

    #[tokio::test]
    async fn unparseable_date_key_is_kept() {
        let store = seed_hourly_store();
        // A tight range that excludes every parseable hour still keeps the
        // key whose date can't be parsed (conservative: never silently drop).
        let mut src =
            new_source_with_range(Some("2026-06-20T00:00:00Z"), Some("2026-06-20T23:00:00Z"));
        let docs = src.run_with_store(&store).await.unwrap();
        let hosts: Vec<String> = docs.iter().map(host_of).collect();
        assert_eq!(hosts, vec!["hx"]);
    }

    #[tokio::test]
    async fn fixed_compression_overrides_suffix_inference() {
        // Key has no recognizable suffix; Auto would fail, Fixed(Gzip) works.
        let mut objects = BTreeMap::new();
        objects.insert(
            "dd/2026/06/19/00/plainname".to_string(),
            gzip("{\"host\":\"h9\"}\n"),
        );
        let store = FakeStore { objects };

        let mut src = DatadogArchiveSource::new(
            "my-bucket",
            "dd/2026/06/19/",
            None,
            None,
            None,
            Compression::Fixed(decompress::Codec::Gzip),
            None,
        )
        .unwrap();
        let docs = src.run_with_store(&store).await.unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].body, serde_json::json!({"host":"h9"}));
    }

    #[tokio::test]
    async fn cached_store_is_reused_across_next_batch_calls() {
        // Simulate what the per-cloud `open_*` arms do: run the listing, then
        // cache the SAME store instance in `self.store`. Driving the cloud-
        // agnostic `next_batch_cached` must reuse that cached store for every
        // file (no per-batch rebuild) and leave it in place afterwards.
        let store = seed_store();
        let mut src = new_source(None);
        src.open_with_store(&store).await.unwrap();
        src.store = Some(Box::new(store));

        let mut all = Vec::new();
        while let Some(batch) = src.next_batch_cached().await.unwrap() {
            all.extend(batch);
        }
        // Same 3 events as a full run, served entirely by the cached store.
        assert_eq!(all.len(), 3);
        // The box is returned to the field once the stream is exhausted.
        assert!(src.store.is_some());
    }
}

// ---------------------------------------------------------------------------
// Metrics test: drive the pipeline under a local Prometheus recorder and assert
// the archive counters are actually emitted. Pinned to `datadog-s3` so it runs
// in the lean feature-gated test job; the metrics macros are no-ops in the other
// pipeline tests (no global recorder), so those are unaffected.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "datadog-s3"))]
mod metrics_tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression as GzLevel};
    use metrics_exporter_prometheus::PrometheusBuilder;
    use std::collections::BTreeMap;
    use std::io::Write;

    /// Minimal in-memory store (self-contained so this module doesn't depend on
    /// the sibling `tests` module's private fixtures).
    struct FakeStore {
        objects: BTreeMap<String, Vec<u8>>,
    }

    #[async_trait]
    impl ObjectStore for FakeStore {
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            Ok(self
                .objects
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        async fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.objects
                .get(key)
                .cloned()
                .ok_or_else(|| EsiftError::Source(format!("no such key {key}")))
        }
    }

    fn gzip(ndjson: &str) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), GzLevel::default());
        enc.write_all(ndjson.as_bytes()).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn pipeline_run_increments_files_total() {
        // Build a local recorder so the otherwise-no-op `metrics!` macros record
        // into a handle we can render, without installing a process-global one.
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        // `with_local_recorder` sets the recorder for this thread for the
        // duration of the (synchronous) closure, so run the async pipeline to
        // completion on a current-thread runtime created inside it.
        metrics::with_local_recorder(&recorder, || {
            let mut objects = BTreeMap::new();
            objects.insert(
                "dd/2026/06/19/00/00_a.json.gz".to_string(),
                gzip("{\"host\":\"h1\"}\n"),
            );
            objects.insert(
                "dd/2026/06/19/00/01_b.json.gz".to_string(),
                gzip("{\"host\":\"h2\"}\n"),
            );
            let store = FakeStore { objects };
            let mut src = DatadogArchiveSource::new(
                "my-bucket",
                "dd/2026/06/19/",
                Some("us-east-1".to_string()),
                None,
                None,
                Compression::Auto,
                None,
            )
            .unwrap();
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(src.run_with_store(&store))
                .unwrap();
        });

        let rendered = handle.render();
        // Two objects, both downloaded → files_total counter is emitted (==2).
        assert!(
            rendered.contains("esift_archive_files_total"),
            "expected esift_archive_files_total in:\n{rendered}"
        );
        assert!(
            rendered.contains("cloud=\"s3\""),
            "expected cloud=\"s3\" label in:\n{rendered}"
        );
    }
}

// ---------------------------------------------------------------------------
// Cloud-selector tests: these need no backend feature, so they always compile
// and run (including in the default/lean build).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod cloud_tests {
    use super::*;

    #[test]
    fn parses_known_providers_case_insensitively() {
        assert_eq!(CloudProvider::parse("s3").unwrap(), CloudProvider::S3);
        assert_eq!(CloudProvider::parse("S3").unwrap(), CloudProvider::S3);
        assert_eq!(CloudProvider::parse("gcs").unwrap(), CloudProvider::Gcs);
        assert_eq!(CloudProvider::parse("GCS").unwrap(), CloudProvider::Gcs);
        assert_eq!(CloudProvider::parse("azure").unwrap(), CloudProvider::Azure);
        assert_eq!(
            CloudProvider::parse(" Azure ").unwrap(),
            CloudProvider::Azure
        );
    }

    #[test]
    fn empty_and_default_are_s3() {
        assert_eq!(CloudProvider::parse("").unwrap(), CloudProvider::S3);
        assert_eq!(CloudProvider::default(), CloudProvider::S3);
    }

    #[test]
    fn unknown_provider_errors() {
        let err = CloudProvider::parse("gcp").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("gcp"),
            "error should name the bad value: {msg}"
        );
        assert!(
            msg.contains("s3") && msg.contains("gcs") && msg.contains("azure"),
            "error should list the valid values: {msg}"
        );
    }

    #[test]
    fn as_str_roundtrips() {
        for c in [CloudProvider::S3, CloudProvider::Gcs, CloudProvider::Azure] {
            assert_eq!(CloudProvider::parse(c.as_str()).unwrap(), c);
        }
    }

    #[test]
    fn with_cloud_is_reflected_in_description() {
        let src = DatadogArchiveSource::new("b", "p", None, None, None, Compression::Auto, None)
            .unwrap()
            .with_cloud(CloudProvider::Gcs);
        assert!(src.description().contains("cloud=gcs"));
    }

    /// Selecting a cloud whose backend feature is NOT compiled in must yield the
    /// clear "build with --features datadog-<cloud>" error from `open()`. Under
    /// `--all-features` every backend is present, so there is no disabled cloud
    /// to assert against; in that case the test is a no-op (still compiles).
    #[tokio::test]
    async fn disabled_cloud_open_errors_with_feature_hint() {
        // Collect every provider whose backend feature is OFF in this build.
        // Under --all-features no push compiles, leaving the vec empty.
        #[allow(unused_mut)]
        let mut disabled: Vec<CloudProvider> = Vec::new();
        #[cfg(not(feature = "datadog-s3"))]
        disabled.push(CloudProvider::S3);
        #[cfg(not(feature = "datadog-gcs"))]
        disabled.push(CloudProvider::Gcs);
        #[cfg(not(feature = "datadog-azure"))]
        disabled.push(CloudProvider::Azure);

        let Some(&cloud) = disabled.first() else {
            // All backends compiled in (e.g. --all-features): nothing to assert.
            return;
        };

        let mut src = DatadogArchiveSource::new(
            "bucket",
            "prefix/",
            None,
            None,
            None,
            Compression::Auto,
            None,
        )
        .unwrap()
        .with_cloud(cloud);

        let err = src.open().await.unwrap_err();
        let msg = err.to_string();
        let expected = format!("--features datadog-{}", cloud.as_str());
        assert!(
            msg.contains(&expected),
            "expected feature hint '{expected}' in error, got: {msg}"
        );
    }
}
