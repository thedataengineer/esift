[![Build](https://img.shields.io/github/actions/workflow/status/thedataengineer/esift/ci.yml?branch=main)](https://github.com/thedataengineer/esift/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

# esift

Extract data from Elasticsearch-compatible sources and re-ingest it anywhere.

Most observability platforms assume logs are short-lived. esift is for the cases where they aren't: usage pattern analysis, feature adoption tracking, cost attribution, anomaly detection on historical data. It reads from any OpenSearch or Elasticsearch cluster using Point-in-Time pagination and writes to a pluggable destination.

---

## Installation

### From source

```bash
git clone https://github.com/thedataengineer/esift
cd esift
cargo build --release
# Or with AWS SigV4 support:
cargo build --release --features aws
# binary is at ./target/release/esift
```

Requires Rust 1.75 or later. Install via [rustup](https://rustup.rs).

---

## Usage

### Inspect before committing

Dump an index to stdout as NDJSON. Pipe to `jq` for readable output.

```bash
esift extract \
  --source-url http://localhost:9200 \
  --source-index "nginx-logs-*" \
  --dest stdout | jq .
```

### Extract to OpenObserve

```bash
esift extract \
  --source-url http://localhost:9200 \
  --source-index "nginx-logs-*" \
  --query '{"range":{"@timestamp":{"gte":"2025-01-01","lte":"2025-06-01"}}}' \
  --dest openobserve \
  --dest-url http://localhost:5080 \
  --dest-org default \
  --dest-stream api_usage \
  --dest-username root@example.com \
  --dest-password Complexpass#123 \
  --checkpoint ./progress.json
```

### Run from a config file

```bash
cp config/example.toml esift.toml
# edit esift.toml
esift run
```

---

## Flags

| Flag | Default | Description |
|---|---|---|
| `--source-url` | required | Base URL of the OpenSearch/ES cluster |
| `--source-index` | required | Index name or pattern (e.g. `nginx-logs-*`) |
| `--query` | `match_all` | Query DSL as a JSON string |
| `--batch-size` | `500` | Documents per request |
| `--source-auth-type` | — | Source auth type: `basic`, `aws-sigv4`, `none` (env: `ESIFT_SOURCE_AUTH_TYPE`) |
| `--source-aws-region` | — | AWS region for SigV4 signing (env: `ESIFT_SOURCE_AWS_REGION`) |
| `--source-username` | — | Username for basic auth (env: `ESIFT_SOURCE_USERNAME`) |
| `--source-password` | — | Password for basic auth (env: `ESIFT_SOURCE_PASSWORD`) |
| `--dest` | `stdout` | Destination: `stdout` or `openobserve` |
| `--dest-url` | — | OpenObserve base URL |
| `--dest-org` | `default` | OpenObserve organization |
| `--dest-stream` | — | OpenObserve stream name |
| `--dest-username` | — | Auth username (env: `ESIFT_DEST_USERNAME`) |
| `--dest-password` | — | Auth password (env: `ESIFT_DEST_PASSWORD`) |
| `--checkpoint` | `./esift-checkpoint.json` | Path to resumable checkpoint file |

---

## Config file format

```toml
[source]
url = "http://localhost:9200"
index = "nginx-logs-*"
batch_size = 500
query = '{"match_all": {}}'
# username = "admin"
# password = "changeme"
# auth_type = "aws-sigv4" # basic, aws-sigv4, none
# aws_region = "us-east-1"

[destination]
type = "openobserve"
url = "http://localhost:5080"
org = "default"
stream = "migrated_logs"
username = "root@example.com"
password = "Complexpass#123"

checkpoint_path = "./esift-checkpoint.json"
```

---

## Resumability

Every successful batch writes a checkpoint file atomically. If the process is interrupted, restart with the same `--checkpoint` path and extraction resumes from the last confirmed cursor position. Delete the checkpoint file to start over.

---

## Local development

```bash
# Start OpenSearch + OpenObserve
docker compose -f docker/docker-compose.yml up -d

# Seed test data
curl -X POST "http://localhost:9200/test-logs/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index":{"_id":"1"}}
{"@timestamp":"2025-01-15T10:00:00Z","message":"user login","user":"u001"}
'

# Run
cargo run -- extract --source-url http://localhost:9200 --source-index test-logs --dest stdout
```

---

## Datadog source

esift can extract log events directly from Datadog using two paths. Build with the matching feature flag:

```bash
cargo build --release --features datadog-s3   # Archive source (Path 1)
cargo build --release --features datadog-api  # API source (Path 2)
```

### datadog-archive (Path 1)

Reads the compressed-JSON archive files that Datadog writes to your S3 bucket via Log Archives. No rate limits, unlimited history, file-level resume.

```toml
[source]
kind = "datadog-archive"
dd_bucket      = "my-log-archive-bucket"
dd_prefix      = "datadog/logs"
dd_region      = "us-east-1"
dd_compression = "zstd"          # zstd (default) or gzip
dd_from        = "2025-01-01T00:00:00Z"
dd_to          = "2025-02-01T00:00:00Z"
```

### datadog-api (Path 2)

Calls `POST /api/v2/logs/events/search`, chunked into time windows and cursor-paginated. Limited to Datadog's live retention window (typically 15 days).

```toml
[source]
kind              = "datadog-api"
dd_site           = "datadoghq.com"
dd_api_key        = "env:DD_API_KEY"
dd_app_key        = "env:DD_APP_KEY"
dd_query          = "service:my-app status:error"
dd_from           = "2025-06-01T00:00:00Z"
dd_to             = "2025-06-15T00:00:00Z"
dd_window_minutes = 60
```

### Datadog config fields

| Field | Source kind | Description |
|---|---|---|
| `dd_bucket` | `datadog-archive` | S3 bucket name where Datadog writes archive files |
| `dd_prefix` | `datadog-archive` | Key prefix within the bucket |
| `dd_region` | `datadog-archive` | AWS region of the bucket |
| `dd_compression` | `datadog-archive` | Codec: `zstd` (default) or `gzip` |
| `dd_site` | `datadog-api` | Regional site: `datadoghq.com`, `datadoghq.eu`, `us3.datadoghq.com`, `us5.datadoghq.com`, `ap1.datadoghq.com` |
| `dd_api_key` | `datadog-api` | Datadog API key — literal, `env:VAR`, or `file:PATH` |
| `dd_app_key` | `datadog-api` | Datadog Application key — literal, `env:VAR`, or `file:PATH` |
| `dd_query` | `datadog-api` | Datadog log search query (default: all logs) |
| `dd_from` | both | Start of time range (RFC 3339) |
| `dd_to` | both | End of time range (RFC 3339) |
| `dd_window_minutes` | `datadog-api` | Time-window chunk size in minutes (default: `60`) |

See [DATADOG.md](DATADOG.md) for a full guide including path comparison, regional sites, secret references, and troubleshooting.

---

## Destinations

| `--dest` | Description |
|---|---|
| `stdout` | NDJSON to stdout. Pipe to `jq` for inspection. |
| `openobserve` | OpenObserve bulk ingest API (`/api/{org}/_bulk`) |

Planned: S3/Parquet, ClickHouse, local NDJSON file.

---

## Compatibility

| Source | Status |
|---|---|
| OpenSearch 2.4+ | Tested |
| Elasticsearch 7.10+ | Supported (auto-detected) |
| AWS OpenSearch Service | Supported (basic auth, IAM/SigV4 auth, or open access domains) |
| Datadog Log Archives (S3) | Supported (`--features datadog-s3`) |
| Datadog Logs API | Supported (`--features datadog-api`) |

---

## Roadmap

- [x] AWS SigV4 auth for managed OpenSearch domains
- [ ] S3/Parquet destination
- [ ] ClickHouse HTTP destination
- [ ] Per-job checkpoint isolation (no shared checkpoint file across runs)
- [ ] Field mapping via config (rename, drop, set timestamp)
- [ ] Pre-built binaries via GitHub Releases

---

## License

Apache 2.0
