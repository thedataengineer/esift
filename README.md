# esift

[![Build](https://img.shields.io/github/actions/workflow/status/thekarteek/esift/ci.yml?branch=main)](https://github.com/thekarteek/esift/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

Extract data from Elasticsearch-compatible sources and re-ingest it anywhere.

Most observability platforms assume logs are short-lived. esift is for the cases where they aren't: usage pattern analysis, feature adoption tracking, cost attribution, anomaly detection on historical data. It reads from any OpenSearch or Elasticsearch cluster using Point-in-Time pagination and writes to a pluggable destination.

---

## Installation

### From source

```bash
git clone https://github.com/thekarteek/esift
cd esift
cargo build --release
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
| AWS OpenSearch Service | Supported (basic auth / open access domains) |

AWS OpenSearch Service with IAM/SigV4 auth is on the roadmap.

---

## Roadmap

- [ ] AWS SigV4 auth for managed OpenSearch domains
- [ ] S3/Parquet destination
- [ ] ClickHouse HTTP destination  
- [ ] Per-job checkpoint isolation (no shared checkpoint file across runs)
- [ ] Field mapping via config (rename, drop, set timestamp)
- [ ] Pre-built binaries via GitHub Releases

---

## License

Apache 2.0
