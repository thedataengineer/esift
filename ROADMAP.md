# esift roadmap

Scope: hardening the OpenObserve `_bulk` sink. esift's niche is resumable historical backfill from Elasticsearch/OpenSearch into OpenObserve. Live-tail shippers (Vector, Fluent Bit, OpenTelemetry Collector) own the streaming case and are out of scope here.

The sink is structured as independent seams under `crates/esift-core/src/dest/openobserve/` (one file per concern), each wired by `mod.rs`. A roadmap item is implemented by filling in one seam; items do not touch each other.

## P0 — data integrity

| Item | Seam | Status |
|---|---|---|
| Bulk partial-failure accounting | `response.rs` | Done |
| Retry with backoff on 429 / 5xx | `http/retry.rs` | Done |

Partial-failure accounting is the priority. Before this work the sink returned the submitted document count and only logged when OpenObserve set `errors:true`, so rejected documents were counted as written and the checkpoint advanced past them. That is silent data loss: a run can report a clean migration while dropping documents the server refused. The fix parses the `items[]` array, counts real successes, and routes rejects to the dead-letter sink.

## P1 — throughput and flexibility

| Item | Seam | Status |
|---|---|---|
| gzip request compression | `transport.rs` | Done |
| Byte-size batch cap | `build.rs` | Done |
| Concurrent in-flight requests | `pipeline.rs` | Done |
| Per-document stream routing | `routing.rs` | Done |
| `_timestamp` derivation | `timestamp.rs` | Done |

## P2 — operations and security

| Item | Seam | Status |
|---|---|---|
| Token auth | `auth.rs` | Done |
| Secret sourcing (`env:` / `file:`) | cli `secret.rs` | Done |
| TLS controls (custom CA, mTLS, insecure) | `tls.rs` | Done |
| Dead-letter sink for rejected docs | `deadletter.rs` | Done |
| Throughput / reject metrics | `metrics.rs` | Done |

## Configuration

All sink tuning lives in `OpenObserveOptions` (`dest/openobserve/config.rs`), surfaced as an optional `[destination.options]` table. Every field defaults to off, so existing configs keep working unchanged.

## Later / out of scope

Not part of this roadmap; tracked for future consideration:

- Additional sinks (file/NDJSON, S3, Kafka).
- Wiring the transform pipeline through config (today the CLI uses the identity transform).
- Additional sources beyond OpenSearch/Elasticsearch.
- Selectable ingestion API (`_json` / `_multi`) alongside `_bulk`.
