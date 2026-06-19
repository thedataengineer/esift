# Datadog Source — User Guide

esift can read log events from Datadog in two ways:

- **Path 1 — Archive source** (`datadog-archive`): reads compressed-JSON log archives that Datadog writes to your S3 bucket via the Log Archives feature. No rate limits, full history back to whenever you enabled archiving, and file-level resumability.
- **Path 2 — API source** (`datadog-api`): calls `POST /api/v2/logs/events/search` with cursor pagination and time-window chunking. Limited to Datadog's live retention window (typically 15 days) but works without any archive infrastructure.

---

## Path comparison

| Property | Archive (Path 1) | API (Path 2) |
|---|---|---|
| **Rate limits** | None (S3 reads only) | Yes — Datadog enforces per-org request quotas; esift honours `X-RateLimit-Reset` and backs off automatically |
| **Max history** | Unlimited (all archived data) | Live retention window only (typically 15 days) |
| **Auth required** | AWS credentials for S3 + Datadog archive config | Datadog API key + Application key |
| **Throughput** | High — parallel S3 object downloads possible | Moderate — bounded by Datadog API quotas |
| **Resumability** | File-level: resumes from the last completed S3 object key | Window-level: resumes from the last completed time-window cursor |
| **Setup dependency** | Datadog Log Archives must be enabled and pointing at your S3 bucket | No extra Datadog setup beyond API credentials |
| **Freshness** | Lag of up to ~15 min (Datadog flushes archive files periodically) | Near-real-time (live index) |

---

## When to use which path

Use **Archive (Path 1)** when:
- You need data older than your Datadog retention window.
- You are running a large bulk backfill (gigabytes to terabytes of logs).
- You need reliable, resumable extraction that can survive a restart mid-run.
- You already have Datadog Log Archives configured to an S3 bucket.

Use **API (Path 2)** when:
- You only need recent data (within the retention window, typically 15 days).
- You have not set up Log Archives.
- You need to quickly pull a targeted query result and don't have an S3 archive to work from.

---

## Building with Datadog support

Datadog source support is behind feature flags. Build with the flag(s) you need:

```bash
# Archive source (reads from S3)
cargo build --release --features datadog-s3

# API source (reads from Datadog API)
cargo build --release --features datadog-api

# Both paths
cargo build --release --features datadog-s3,datadog-api
```

Without these flags the binary still works for all other sources (opensearch, file). Attempting to use a Datadog source kind without the matching feature flag prints a clear error message directing you to rebuild with the right flag.

---

## Path 1 — Archive source setup

### Prerequisites

1. In your Datadog account, go to **Logs > Configuration > Log Archives** and configure an archive to an S3 bucket you control.
2. Grant the IAM role or user that esift runs as `s3:GetObject` and `s3:ListBucket` on that bucket.
3. AWS credentials available in the standard locations (env vars, `~/.aws/credentials`, EC2/ECS instance profile, etc.).

### Config file example

```toml
[source]
kind = "datadog-archive"
dd_bucket = "my-log-archive-bucket"
dd_prefix = "datadog/logs"      # prefix Datadog writes under; omit trailing slash
dd_region = "us-east-1"
dd_compression = "zstd"         # "zstd" (default) or "gzip"
dd_from = "2025-01-01T00:00:00Z"
dd_to   = "2025-02-01T00:00:00Z"
```

### Config fields

| Field | Required | Default | Description |
|---|---|---|---|
| `dd_bucket` | Yes | — | S3 bucket name where Datadog writes archive files |
| `dd_prefix` | No | `""` | Key prefix within the bucket (no trailing slash) |
| `dd_region` | No | AWS SDK default | AWS region of the bucket |
| `dd_compression` | No | `zstd` | Compression codec: `zstd` or `gzip` |
| `dd_from` | Yes | — | Start of the time range (RFC 3339 / ISO 8601) |
| `dd_to` | Yes | — | End of the time range (RFC 3339 / ISO 8601) |

### How resumability works

esift writes a checkpoint after each S3 object (archive file) is fully processed. The checkpoint records the last completed S3 key. On restart, esift skips all keys up to and including the last checkpointed key and resumes from the next one.

Delete the checkpoint file to start the extraction over from the beginning.

---

## Path 2 — API source setup

### Prerequisites

1. In your Datadog account, create an **API key** (Organization Settings > API Keys) and an **Application key** (Organization Settings > Application Keys) with the `logs_read_data` scope.
2. Store the keys somewhere safe; see the secret reference section below.

### Config file example

```toml
[source]
kind = "datadog-api"
dd_site           = "datadoghq.com"
dd_api_key        = "env:DD_API_KEY"        # or "file:/run/secrets/dd_api_key"
dd_app_key        = "env:DD_APP_KEY"
dd_query          = "service:my-app status:error"
dd_from           = "2025-06-01T00:00:00Z"
dd_to             = "2025-06-15T00:00:00Z"
dd_window_minutes = 60                      # chunk [from,to] into 60-minute windows
```

### Config fields

| Field | Required | Default | Description |
|---|---|---|---|
| `dd_site` | No | `datadoghq.com` | Datadog regional site (see table below) |
| `dd_api_key` | Yes | — | Datadog API key (literal, `env:VAR`, or `file:PATH`) |
| `dd_app_key` | Yes | — | Datadog Application key (literal, `env:VAR`, or `file:PATH`) |
| `dd_query` | No | `""` (all logs) | Datadog log search query |
| `dd_from` | Yes | — | Start of the time range (RFC 3339 / ISO 8601) |
| `dd_to` | Yes | — | End of the time range (RFC 3339 / ISO 8601) |
| `dd_window_minutes` | No | `60` | Size of each time-window chunk in minutes |

### Supported regional sites

| `dd_site` value | Region |
|---|---|
| `datadoghq.com` | US1 (default) |
| `us3.datadoghq.com` | US3 |
| `us5.datadoghq.com` | US5 |
| `datadoghq.eu` | EU1 |
| `ap1.datadoghq.com` | AP1 (Japan) |

### How resumability works

esift chunks the `[dd_from, dd_to]` range into windows of `dd_window_minutes` minutes. It paginates each window to exhaustion before moving on. The checkpoint records the window boundaries and the cursor position within the current window.

On restart, esift skips fully completed windows and resumes from the saved cursor inside the partially completed window. No events are duplicated or skipped at a resume boundary.

### Rate limiting

When the Datadog API returns HTTP 429, esift reads the `X-RateLimit-Reset` response header, sleeps until that instant (plus a small jitter), and then retries. This is logged at the WARN level so you can see when throttling occurs.

---

## Secret references for credentials

The `dd_api_key` and `dd_app_key` fields support secret indirection — you do not have to write credentials directly in the config file:

| Format | Behaviour |
|---|---|
| `"literal-value"` | Used as-is |
| `"env:VAR_NAME"` | Read from environment variable `VAR_NAME` at startup |
| `"file:/path/to/secret"` | Read from the file at that path; leading/trailing whitespace stripped |

Example using environment variables:

```bash
export DD_API_KEY="abc123..."
export DD_APP_KEY="def456..."
esift run --config esift.toml
```

Example using Docker secrets or similar file mounts:

```toml
dd_api_key = "file:/run/secrets/dd_api_key"
dd_app_key = "file:/run/secrets/dd_app_key"
```

---

## Compression codecs (Archive path)

| `dd_compression` | Notes |
|---|---|
| `zstd` | Default. Datadog writes `.json.zst` files when zstd is selected in the archive config. |
| `gzip` | Datadog writes `.json.gz` files when gzip is selected. |

The codec is set once per extraction job and must match what Datadog was configured to write. If the codec does not match the file content, extraction fails with a decompression error on the first archive object.

---

## Troubleshooting

**"Build with `--features datadog-s3`" error at startup**
Rebuild esift with the matching feature flag (see the build section above).

**Archive extraction finds no files**
Check that `dd_bucket`, `dd_prefix`, `dd_region`, and the `[dd_from, dd_to]` range are correct. Datadog structures archive keys as `{prefix}/YYYY/MM/DD/HH/`. Verify with `aws s3 ls s3://{bucket}/{prefix}/` that files exist.

**API extraction returns no results**
Verify that `dd_from`/`dd_to` falls within your Datadog retention window. Check that `dd_query` is valid Datadog search syntax. Confirm the API key has `logs_read_data` scope.

**Rate limit warnings**
Increase `dd_window_minutes` to reduce the number of API requests. esift will still honour rate-limit headers and retry; the warning is informational.
