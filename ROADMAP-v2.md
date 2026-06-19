# esift v2 — competitive roadmap

Closes the gaps surfaced comparing esift against Vector, Logstash, and Data Prepper: weak transforms, narrow connector breadth, single-process scale, and no metrics export. Delivered as five independent lanes on top of a foundation that pre-wires every seam, so the lanes touch disjoint files.

## Foundation (landed first)

Pure scaffolding; default behavior unchanged. Pre-wires, with stubs each lane fills in:

- `Transform` enum moved to `esift-core` (`transform.rs`) to break the core/transform dependency cycle, so config can reference transforms.
- `EsiftConfig.transforms` and `EsiftConfig.metrics_addr`; `Transformer::new(cfg.transforms)` wired in the config path.
- `DestConfig::File` and `DestConfig::S3` variants + dispatch, with stub `dest/file.rs` and `dest/s3.rs`.
- `SourceConfig.kind` / `path` / `slices` (url/index now optional) + source dispatch, with stub `source/file.rs`.
- `OpenSearchSource::with_slices`.
- CLI `--slices` / `--metrics-addr`, a shared `metrics` handle updated by the extraction loop, and a stub `metrics_server`.

## Lanes (one file each, one PR each)

| # | Lane | Gap closed | Owns | New dep |
|---|---|---|---|---|
| 1 | Config-driven transforms | transforms (vs VRL/Bloblang) | `esift-core/transform.rs` + `esift-transform/mapping.rs` | — |
| 2 | File + S3 sinks | sink breadth | `dest/file.rs`, `dest/s3.rs` (+ Cargo) | `aws-sdk-s3` (feature `s3`) |
| 3 | NDJSON source | source breadth + dead-letter replay | `source/file.rs` | — |
| 4 | Sliced parallel extraction | scale | `source/opensearch.rs` | — |
| 5 | Prometheus `/metrics` endpoint | observability | `esift-cli/metrics_server.rs` (+ Cargo) | small http server |

Every lane depends only on the foundation; no lane-to-lane file overlap. Only lanes 2 and 5 touch a `Cargo.toml`, in different crates.

## Out of scope (deferred)

Live/follow tail mode; distributed multi-node execution; a full expression-language transform engine.
