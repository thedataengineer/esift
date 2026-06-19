//! Datadog archive source (Path 1): read compressed-JSON logs straight from
//! object storage. Requires the `datadog-s3` feature.
//!
//! Lane 1 implementation: lists archive objects under a prefix, downloads each,
//! hands the bytes to [`super::decompress`], splits the resulting NDJSON, maps
//! every line through [`super::flatten_archive`], and emits one
//! [`Document`] per line. One object is processed per `next_batch` call so the
//! caller checkpoints at file granularity; the resume blob in [`Source::cursor`]
//! records the last fully processed key so a later run skips it.
//!
//! S3 access is hidden behind the private [`ObjectStore`] seam so the listing /
//! download / decode / flatten pipeline is unit-testable against an in-memory
//! fake with no real S3 or LocalStack.

use super::decompress;
use crate::error::{EsiftError, Result};
use crate::source::Source;
use crate::Document;
use async_trait::async_trait;
use serde_json::Value;

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
    // Read only by the `datadog-s3` impl; without the feature the fallback impl
    // ignores them, so the field is intentionally unused there.
    #[cfg_attr(not(feature = "datadog-s3"), allow(dead_code))]
    region: Option<String>,
    #[allow(dead_code)]
    from: Option<String>,
    #[allow(dead_code)]
    to: Option<String>,
    #[cfg_attr(not(feature = "datadog-s3"), allow(dead_code))]
    compression: Compression,
    /// Opaque resume blob from a prior checkpoint cursor; decoded in `open`.
    #[cfg_attr(not(feature = "datadog-s3"), allow(dead_code))]
    resume_after: Option<Vec<Value>>,
    /// Runtime listing/position state, populated by `open`. Only meaningful with
    /// the `datadog-s3` feature; the fallback impl never reads it.
    #[cfg(feature = "datadog-s3")]
    state: ArchiveState,
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
            region,
            from,
            to,
            compression,
            resume_after,
            #[cfg(feature = "datadog-s3")]
            state: ArchiveState::default(),
        })
    }
}

// ---------------------------------------------------------------------------
// Real implementation (datadog-s3)
// ---------------------------------------------------------------------------

/// Mutable listing/position state for an in-progress archive extraction.
#[cfg(feature = "datadog-s3")]
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
/// needs. Implemented by [`S3ObjectStore`] in production and by an in-memory
/// fake in tests, so the extraction pipeline is exercised without real S3.
#[cfg(feature = "datadog-s3")]
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

#[cfg(feature = "datadog-s3")]
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

    /// List + sort + apply resume, populating `self.state`.
    async fn open_with_store(&mut self, store: &dyn ObjectStore) -> Result<()> {
        let mut keys = store.list(&self.prefix).await?;
        keys.sort();

        let (last_key, files_done) = self.decode_resume();
        if let Some(anchor) = &last_key {
            keys.retain(|k| k > anchor);
        }

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

        let raw = store.get(&key).await?;
        let decoded = decompress::decompress(&raw, codec)?;
        let text = String::from_utf8(decoded)
            .map_err(|e| EsiftError::Source(format!("archive {key} is not valid UTF-8: {e}")))?;

        let mut docs = Vec::new();
        for (n, line) in text.split('\n').enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(line)
                .map_err(|e| EsiftError::Source(format!("invalid JSON in {key} line {n}: {e}")))?;
            let body = super::flatten_archive::flatten(value);
            docs.push(Document::new("datadog", format!("{key}#{n}"), body));
        }

        self.state.pos += 1;
        self.state.files_done += 1;
        self.state.last_key = Some(key);
        Ok(Some(docs))
    }
}

#[cfg(feature = "datadog-s3")]
#[async_trait]
impl Source for DatadogArchiveSource {
    async fn open(&mut self) -> Result<()> {
        let store = S3ObjectStore::new(self.bucket.clone(), self.region.clone()).await;
        self.open_with_store(&store).await
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        // A fresh client per batch mirrors `dest/s3.rs`; the listing already
        // lives in `self.state`, so each call only needs `get_object`.
        let store = S3ObjectStore::new(self.bucket.clone(), self.region.clone()).await;
        self.next_batch_with_store(&store).await
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "Datadog archive bucket={} prefix={}",
            self.bucket, self.prefix
        )
    }

    fn cursor(&self) -> Option<Vec<Value>> {
        Some(vec![serde_json::json!({
            "dd_archive": {
                "last_key": self.state.last_key,
                "files_done": self.state.files_done,
            }
        })])
    }
}

// ---------------------------------------------------------------------------
// Fallback (no datadog-s3 feature): clean build-with-feature error.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "datadog-s3"))]
#[async_trait]
impl Source for DatadogArchiveSource {
    async fn open(&mut self) -> Result<()> {
        Err(EsiftError::Source(
            "Datadog archive source requires building with --features datadog-s3".into(),
        ))
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<Document>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "Datadog archive bucket={} prefix={}",
            self.bucket, self.prefix
        )
    }
}

// ---------------------------------------------------------------------------
// Unit tests: fake object store, no real S3 / LocalStack.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "datadog-s3"))]
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
}
