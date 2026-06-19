# Datadog Source — User Guide

esift can read log events from Datadog in two ways:

- **Path 1 — Archive source** (`datadog-archive`): reads compressed-JSON log archives that Datadog writes to object storage via the Log Archives feature. Supports **Amazon S3**, **Google Cloud Storage**, and **Azure Blob Storage** (selected with `dd_cloud`). No rate limits, full history back to whenever you enabled archiving, and file-level resumability.
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
# Archive source on Amazon S3
cargo build --release --features datadog-s3

# Archive source on Google Cloud Storage
cargo build --release --features datadog-gcs

# Archive source on Azure Blob Storage
cargo build --release --features datadog-azure

# API source (reads from Datadog API)
cargo build --release --features datadog-api

# Everything
cargo build --release --features datadog-s3,datadog-gcs,datadog-azure,datadog-api
```

Each archive cloud is behind its own feature so the default (and lean per-cloud) builds never pull another cloud's SDK. Without any of these flags the binary still works for all other sources (opensearch, file). Attempting to use a Datadog source kind — or an archive `dd_cloud` — without the matching feature flag prints a clear error message directing you to rebuild with the right flag (e.g. `… requires building with --features datadog-gcs`).

---

## Path 1 — Archive source setup

The archive source reads from one of three object stores, chosen with `dd_cloud`:

| `dd_cloud` | Backend | Build feature | `dd_bucket` means |
|---|---|---|---|
| `s3` (default) | Amazon S3 (or S3-compatible) | `datadog-s3` | S3 bucket name |
| `gcs` | Google Cloud Storage | `datadog-gcs` | GCS bucket name |
| `azure` | Azure Blob Storage | `datadog-azure` | Azure container name |

`dd_region` applies to S3 only; GCS and Azure ignore it. All clouds share the same `dd_prefix`, `dd_compression`, `dd_from`/`dd_to`, resume, and flatten behaviour — only listing/download auth differs.

### Common config fields

| Field | Required | Default | Description |
|---|---|---|---|
| `dd_cloud` | No | `s3` | Object store: `s3`, `gcs`, or `azure` |
| `dd_bucket` | Yes | — | S3 bucket / GCS bucket / Azure container holding the archive files |
| `dd_prefix` | No | `""` | Key prefix within the bucket (no trailing slash) |
| `dd_region` | No | AWS SDK default | AWS region of the bucket (**S3 only**) |
| `dd_compression` | No | `zstd` | Compression codec: `zstd` or `gzip` |
| `dd_from` | Yes | — | Start of the time range (RFC 3339 / ISO 8601) |
| `dd_to` | Yes | — | End of the time range (RFC 3339 / ISO 8601) |

CLI equivalents on `esift extract`: `--source-dd-cloud`, `--source-dd-bucket`, `--source-dd-prefix`, `--source-dd-region`, `--source-dd-compression`, `--source-dd-from`, `--source-dd-to`.

### Amazon S3 (`dd_cloud = "s3"`)

**Prerequisites**

1. In Datadog, **Logs > Configuration > Log Archives**, configure an archive to an S3 bucket you control.
2. Grant the IAM role/user esift runs as `s3:GetObject` and `s3:ListBucket` on that bucket.
3. AWS credentials in the standard locations (env vars, `~/.aws/credentials`, EC2/ECS instance profile, etc.). Authentication uses the AWS SDK default credential chain.

**Config example**

```toml
[source]
kind = "datadog-archive"
dd_cloud = "s3"                 # optional; s3 is the default
dd_bucket = "my-log-archive-bucket"
dd_prefix = "datadog/logs"      # prefix Datadog writes under; omit trailing slash
dd_region = "us-east-1"
dd_compression = "zstd"         # "zstd" (default) or "gzip"
dd_from = "2025-01-01T00:00:00Z"
dd_to   = "2025-02-01T00:00:00Z"
```

### Google Cloud Storage (`dd_cloud = "gcs"`)

**Prerequisites**

1. In Datadog, configure a Log Archive to a GCS bucket you control.
2. Grant the service account esift runs as `storage.objects.get` and `storage.objects.list` on that bucket (e.g. the `roles/storage.objectViewer` role).
3. Authentication uses **Application Default Credentials** — the Google SDK's default chain.

**Auth environment**

| Variable | Purpose |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to a service-account JSON key file (most common for servers). |
| *(none)* | On GCE/GKE/Cloud Run, the attached service account is used automatically via the metadata server. `gcloud auth application-default login` also works for local runs. |

**Config example**

```toml
[source]
kind = "datadog-archive"
dd_cloud = "gcs"
dd_bucket = "my-dd-archive-gcs-bucket"   # GCS bucket name
dd_prefix = "datadog/logs"
dd_compression = "zstd"
dd_from = "2025-01-01T00:00:00Z"
dd_to   = "2025-02-01T00:00:00Z"
# dd_region is ignored for GCS.
```

### Azure Blob Storage (`dd_cloud = "azure"`)

**Prerequisites**

1. In Datadog, configure a Log Archive to an Azure storage container you control.
2. Grant the identity esift runs as read + list on that container (e.g. the **Storage Blob Data Reader** role).
3. Set `dd_bucket` to the **container name**.

**Auth environment**

The endpoint is built as `https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{dd_bucket}`.

| Variable | Purpose |
|---|---|
| `AZURE_STORAGE_ACCOUNT` | **Required.** Storage account name used to build the blob endpoint. |
| `AZURE_STORAGE_BLOB_ENDPOINT` | Optional. Full base endpoint override (sovereign clouds or an emulator like Azurite); the container name is appended. |
| `AZURE_FEDERATED_TOKEN_FILE` | **Checked first.** If set, a **Workload Identity** credential is used (AKS federated tokens; also reads `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`) — the recommended production path on AKS. |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` | Otherwise, if all three are set, a service-principal credential is used (non-interactive — recommended for servers and CI). |
| *(none of the above)* | Otherwise falls back to developer-tools credentials (`az login` / `azd auth login`) for local use. |

> Note: `azure_identity` 1.0 ships no `DefaultAzureCredential` (nor a public credential-chain type), so esift selects one of the above deterministically by environment. Managed Identity (IMDS) has no clean env marker to auto-select on and is a follow-up — on an Azure VM/managed-identity host today, log in via the Azure CLI or set the service-principal variables. The GA `azure_storage_blob` 1.x client authenticates with an Entra ID token credential (`Arc<dyn TokenCredential>`) and does not accept a storage connection string.

**Config example**

```toml
[source]
kind = "datadog-archive"
dd_cloud = "azure"
dd_bucket = "dd-archive-container"       # Azure container name
dd_prefix = "datadog/logs"
dd_compression = "zstd"
dd_from = "2025-01-01T00:00:00Z"
dd_to   = "2025-02-01T00:00:00Z"
# Plus environment:
#   AZURE_STORAGE_ACCOUNT=mystorageacct
#   AZURE_TENANT_ID=... AZURE_CLIENT_ID=... AZURE_CLIENT_SECRET=...   (service principal)
```

### How resumability works

esift writes a checkpoint after each object (archive file) is fully processed. The checkpoint records the last completed object key. On restart, esift skips all keys up to and including the last checkpointed key and resumes from the next one. This is identical across S3, GCS, and Azure.

Delete the checkpoint file to start the extraction over from the beginning.

### Manual testing against cloud emulators

The shared listing/decode/flatten pipeline is unit-tested with an in-memory fake store (no cloud needed). To exercise the real cloud `list`/`get` code paths locally:

- **S3** — LocalStack: see `crates/esift-core/tests/datadog_archive_e2e.rs` and `docker/localstack.yml`, then `cargo test -p esift-core --features datadog-s3 -- --ignored`.
- **GCS** — [`fake-gcs-server`](https://github.com/fsouza/fake-gcs-server): run it, seed a bucket with `.json.gz`/`.json.zst` objects under a dated prefix, point ADC/endpoint at it, and run an extract with `dd_cloud = "gcs"`.
- **Azure** — [Azurite](https://github.com/Azure/Azurite): run it, create a container, seed blobs, set `AZURE_STORAGE_BLOB_ENDPOINT` to the Azurite blob endpoint plus account, and run an extract with `dd_cloud = "azure"`.

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

**"requires building with `--features datadog-<cloud>`" error at startup**
You selected an archive `dd_cloud` (or a Datadog source kind) whose backend wasn't compiled in. Rebuild esift with the matching feature flag — `datadog-s3`, `datadog-gcs`, `datadog-azure`, or `datadog-api` (see the build section above).

**Archive extraction finds no files**
Check that `dd_bucket`, `dd_prefix`, the cloud (`dd_cloud`), and the `[dd_from, dd_to]` range are correct (and `dd_region` for S3). Datadog structures archive keys as `{prefix}/YYYY/MM/DD/HH/`. Verify the objects exist: `aws s3 ls s3://{bucket}/{prefix}/` (S3), `gcloud storage ls gs://{bucket}/{prefix}/` (GCS), or `az storage blob list -c {container} --prefix {prefix}` (Azure).

**Azure: "requires AZURE_STORAGE_ACCOUNT" or auth failures**
Set `AZURE_STORAGE_ACCOUNT` to the storage account name. For non-interactive auth set `AZURE_TENANT_ID`/`AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET`; otherwise log in with `az login`. Confirm the identity has **Storage Blob Data Reader** on the container.

**API extraction returns no results**
Verify that `dd_from`/`dd_to` falls within your Datadog retention window. Check that `dd_query` is valid Datadog search syntax. Confirm the API key has `logs_read_data` scope.

**Rate limit warnings**
Increase `dd_window_minutes` to reduce the number of API requests. esift will still honour rate-limit headers and retry; the warning is informational.
