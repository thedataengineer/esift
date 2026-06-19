//! End-to-end test for the Datadog archive source (Path 1) against a real S3
//! API, emulated locally by LocalStack.
//!
//! What it does, end to end:
//!   1. Connects an `aws-sdk-s3` client to LocalStack on `http://localhost:4566`
//!      with dummy credentials and path-style addressing.
//!   2. Creates a bucket and seeds Datadog-style archive objects: NDJSON with
//!      several log lines per file, compressed as `.json.gz` and `.json.zst`,
//!      laid out under a dated prefix (`dd/logs/YYYY/MM/DD/HH/MM_*.json.<codec>`).
//!   3. Runs `DatadogArchiveSource` to completion through the `Source` trait and
//!      asserts the total document count.
//!   4. Re-runs from the cursor captured partway through and asserts the resumed
//!      run only processes the files that were left.
//!
//! This requires external infrastructure (LocalStack) and the real Lane 1
//! archive driver, so the test is `#[ignore]`d: it is built in CI (`--no-run`)
//! to guard the public API, and run manually or in an extended job. In this
//! worktree the archive `Source` impl is still the foundation stub (returns
//! "not yet implemented"), so the assertions below are written against the
//! intended behaviour and will only pass once Lane 1 lands.
//!
//! Run it manually with:
//!   docker compose -f docker/localstack.yml up -d
//!   cargo test -p esift-core --features datadog-s3 -- --ignored

#![cfg(feature = "datadog-s3")]

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use esift_core::source::datadog::archive::{Compression, DatadogArchiveSource};
use esift_core::source::datadog::decompress::Codec;
use esift_core::source::Source;

/// LocalStack edge endpoint (see `docker/localstack.yml`).
const ENDPOINT: &str = "http://localhost:4566";
/// LocalStack ignores credential values but the SDK still requires some.
const DUMMY_ACCESS_KEY: &str = "test";
const DUMMY_SECRET_KEY: &str = "test";
const REGION: &str = "us-east-1";

/// Bucket holding the seeded archive objects. Unique per run so repeated local
/// runs against the same LocalStack instance don't collide.
fn bucket_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("esift-dd-archive-e2e-{nanos}")
}

/// The prefix under which all seeded objects live (the `dd_prefix` parameter).
const PREFIX: &str = "dd/logs/";

/// One seeded archive object: its full key and the log bodies it should yield.
struct SeedFile {
    key: String,
    lines: Vec<serde_json::Value>,
}

/// The archive objects we seed, in ascending key order. Two `.json.gz` and one
/// `.json.zst`, several log lines each, under a dated Datadog-style layout.
fn seed_files() -> Vec<SeedFile> {
    vec![
        SeedFile {
            key: "dd/logs/2025/01/01/00/00_test.json.gz".to_string(),
            lines: vec![
                serde_json::json!({ "message": "a", "service": "api", "status": "info" }),
                serde_json::json!({ "message": "b", "service": "api", "status": "warn" }),
                serde_json::json!({ "message": "c", "service": "api", "status": "error" }),
            ],
        },
        SeedFile {
            key: "dd/logs/2025/01/01/00/30_test.json.gz".to_string(),
            lines: vec![
                serde_json::json!({ "message": "d", "service": "web", "status": "info" }),
                serde_json::json!({ "message": "e", "service": "web", "status": "info" }),
            ],
        },
        SeedFile {
            key: "dd/logs/2025/01/01/01/00_test.json.zst".to_string(),
            lines: vec![
                serde_json::json!({ "message": "f", "service": "worker", "status": "info" }),
                serde_json::json!({ "message": "g", "service": "worker", "status": "debug" }),
                serde_json::json!({ "message": "h", "service": "worker", "status": "info" }),
                serde_json::json!({ "message": "i", "service": "worker", "status": "info" }),
            ],
        },
    ]
}

/// Serialize log lines to NDJSON and compress with the codec the key implies.
fn encode(lines: &[serde_json::Value], codec: Codec) -> Vec<u8> {
    let mut ndjson = Vec::new();
    for line in lines {
        ndjson.extend_from_slice(serde_json::to_string(line).unwrap().as_bytes());
        ndjson.push(b'\n');
    }
    match codec {
        Codec::Gzip => {
            use flate2::{write::GzEncoder, Compression as GzLevel};
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), GzLevel::default());
            enc.write_all(&ndjson).unwrap();
            enc.finish().unwrap()
        }
        Codec::Zstd => zstd::stream::encode_all(&ndjson[..], 0).unwrap(),
    }
}

/// Build an S3 client pointed at LocalStack: endpoint override, dummy static
/// credentials, and force path-style addressing (LocalStack doesn't do the
/// virtual-host `bucket.host` form).
async fn localstack_client() -> Client {
    let creds = Credentials::new(
        DUMMY_ACCESS_KEY,
        DUMMY_SECRET_KEY,
        None,
        None,
        "esift-localstack",
    );
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(REGION))
        .endpoint_url(ENDPOINT)
        .credentials_provider(creds)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
}

/// Create the bucket and upload every seed object.
async fn seed_bucket(client: &Client, bucket: &str, files: &[SeedFile]) {
    client.create_bucket().bucket(bucket).send().await.unwrap();

    for file in files {
        let codec = Codec::from_key(&file.key).expect("seed key must have a known codec suffix");
        let body = encode(&file.lines, codec);
        client
            .put_object()
            .bucket(bucket)
            .key(&file.key)
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();
    }
}

/// Drive a `Source` from `open()` through repeated `next_batch()` to `close()`,
/// collecting every document body. Returns the documents and the final cursor.
async fn drain<S: Source>(
    source: &mut S,
) -> (Vec<serde_json::Value>, Option<Vec<serde_json::Value>>) {
    source.open().await.expect("source open failed");
    let mut docs = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("next_batch failed") {
        for doc in batch {
            docs.push(doc.body);
        }
    }
    let cursor = source.cursor();
    source.close().await.expect("source close failed");
    (docs, cursor)
}

/// Like `drain`, but stops as soon as `min_docs` documents have been seen,
/// returning what was collected and the cursor at that point. Used to simulate
/// an interrupted run whose checkpoint a later run resumes from.
async fn drain_until<S: Source>(
    source: &mut S,
    min_docs: usize,
) -> (Vec<serde_json::Value>, Option<Vec<serde_json::Value>>) {
    source.open().await.expect("source open failed");
    let mut docs = Vec::new();
    while docs.len() < min_docs {
        match source.next_batch().await.expect("next_batch failed") {
            Some(batch) => {
                for doc in batch {
                    docs.push(doc.body);
                }
            }
            None => break,
        }
    }
    let cursor = source.cursor();
    source.close().await.expect("source close failed");
    (docs, cursor)
}

#[tokio::test]
#[ignore = "requires LocalStack (docker/localstack.yml) and the archive driver; run manually or in an extended CI job"]
async fn archive_full_extraction_counts_all_documents() {
    let client = localstack_client().await;
    let bucket = bucket_name();
    let files = seed_files();
    seed_bucket(&client, &bucket, &files).await;

    let expected_total: usize = files.iter().map(|f| f.lines.len()).sum();

    let mut source = DatadogArchiveSource::new(
        bucket.clone(),
        PREFIX,
        Some(REGION.to_string()),
        None,
        None,
        Compression::Auto,
        None,
    )
    .expect("source construction failed");

    let (docs, _cursor) = drain(&mut source).await;

    assert_eq!(
        docs.len(),
        expected_total,
        "expected one document per seeded NDJSON line across all archive files",
    );
}

#[tokio::test]
#[ignore = "requires LocalStack (docker/localstack.yml) and the archive driver; run manually or in an extended CI job"]
async fn archive_resume_processes_only_remaining_files() {
    let client = localstack_client().await;
    let bucket = bucket_name();
    let files = seed_files();
    seed_bucket(&client, &bucket, &files).await;

    let total: usize = files.iter().map(|f| f.lines.len()).sum();
    // Stop after the first file's worth of lines so resume has work left.
    let first_file_lines = files[0].lines.len();

    // First (interrupted) run: stop once the first file is consumed, capture the
    // resume cursor the loop would have checkpointed.
    let mut first = DatadogArchiveSource::new(
        bucket.clone(),
        PREFIX,
        Some(REGION.to_string()),
        None,
        None,
        Compression::Auto,
        None,
    )
    .expect("source construction failed");
    let (first_docs, cursor) = drain_until(&mut first, first_file_lines).await;

    assert!(
        !first_docs.is_empty(),
        "interrupted run should have produced some documents",
    );
    let cursor = cursor.expect("archive source must expose a resume cursor");

    // Second (resumed) run: constructed from the prior cursor. It must skip the
    // already-processed file(s) and only emit the remaining documents.
    let mut resumed = DatadogArchiveSource::new(
        bucket.clone(),
        PREFIX,
        Some(REGION.to_string()),
        None,
        None,
        Compression::Auto,
        Some(cursor),
    )
    .expect("resumed source construction failed");
    let (resumed_docs, _) = drain(&mut resumed).await;

    assert_eq!(
        resumed_docs.len(),
        total - first_docs.len(),
        "resumed run should process exactly the documents not seen by the first run",
    );
    assert!(
        resumed_docs.len() < total,
        "resumed run must process fewer than all documents (it skipped completed files)",
    );
}
