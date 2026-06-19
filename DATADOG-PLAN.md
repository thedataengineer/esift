# esift — Datadog source: parallel execution plan

Turns the *"Datadog as a Source"* strategy doc into work that **5–6 agents can build
concurrently**. It reuses the model already proven in `ROADMAP-v2.md`: a **foundation**
PR pre-wires every shared seam with compiling stubs, then each **lane** fills in one
file (or a disjoint set) and merges independently. No two lanes touch the same file, so
they merge in any order without conflict.

Two extraction paths from the strategy doc are preserved:

- **Path 1 — Archive source**: read Datadog's compressed-JSON log archives directly from
  object storage (S3 first). No rate limits, unlimited history, file-level resume. Built first.
- **Path 2 — API source**: `POST /api/v2/logs/events/search`, cursor-paginated, time-window
  chunked, 429-backoff, regional base URLs. Live-retention window only.

---

## Adaptation decisions (where the codebase changes the doc)

These are deliberate departures from the strategy doc, chosen to fit esift's existing
architecture and to keep the foundation small.

| Topic | Doc proposal | Plan decision | Why |
|---|---|---|---|
| Resume state | Add a new `Checkpoint.source_state: serde_json::Value` field + loop wiring | **Reuse the existing `Source::cursor() -> Option<Vec<Value>>` channel**; each Datadog source self-encodes its resume blob into that opaque `Vec<Value>`. | `source/opensearch.rs` already does exactly this for sliced resume (`__esift_slices` marker). Zero change to `checkpoint.rs` or `main.rs`'s loop; old checkpoints still load. |
| Module layout | One file per path (`datadog_archive.rs`, `datadog_api.rs`) | A `source/datadog/` module with **separate impl and parsing files** so impl and helpers are different lanes. | Lets parsing (decompress / flatten) be built and tested in parallel with the source drivers. |
| Feature flags | `datadog-s3`, `datadog-gcs`, `datadog-api` | Declare `datadog-s3` (+ new optional `zstd` dep) and `datadog-api` **in the foundation**; GCS/Azure deferred. | Both source lanes reference the `#[cfg]` gates, so the flags must exist before the lanes — declaring them twice would collide on the `[features]` table. Mirrors existing `s3`/`aws`. |
| Flatten location | "belongs in the source layer" | Pure functions in their own files (`flatten_archive.rs`, `flatten_api.rs`), called by the source drivers. | Keeps flattening unit-testable on JSON fixtures with no S3/HTTP, and lets it be a separate lane. |

---

## Foundation (lands first — one PR, pure scaffolding)

Default behaviour unchanged: with no Datadog feature enabled there are **zero** new deps in
the tree, and the new source kinds return a clean *"build with `--features …`"* error rather
than panicking — the same fallback pattern as `dest/s3.rs` today.

The foundation **owns every shared file** so no lane has to touch one:

| File | Change |
|---|---|
| `crates/esift-core/src/config.rs` | Add optional, `#[serde(default)]` `dd_*` fields to `SourceConfig` (`dd_bucket`, `dd_prefix`, `dd_region`, `dd_compression`, `dd_cloud`, `dd_site`, `dd_api_key`, `dd_app_key`, `dd_query`, `dd_from`, `dd_to`, `dd_window_minutes`). All `Option`, all default-off → opensearch/file configs unaffected. |
| `crates/esift-core/src/source/mod.rs` | `pub mod datadog;` |
| `crates/esift-core/src/source/datadog/mod.rs` *(new)* | Declares all submodules: `archive`, `api`, `decompress`, `flatten_archive`, `flatten_api`, `site`. |
| `…/source/datadog/archive.rs` *(new, stub)* | `DatadogArchiveSource` + fixed ctor `new(bucket, prefix, region, from, to, compression, resume_after)`; feature-gated `Source` impl that returns *not-yet-implemented*, plus a `#[cfg(not(feature="datadog-s3"))]` fallback impl. |
| `…/source/datadog/api.rs` *(new, stub)* | `DatadogApiSource` + ctor `new(site, api_key, app_key, query, from, to, window_minutes, resume_after)`; same gate/fallback shape. |
| `…/source/datadog/decompress.rs` *(new, stub)* | `pub enum Codec { Zstd, Gzip }` + `pub fn decompress(bytes:&[u8], codec:Codec) -> Result<Vec<u8>>` (stub `Err`). |
| `…/source/datadog/flatten_archive.rs` *(new, stub)* | `pub fn flatten(event: Value) -> Value` (stub: identity). |
| `…/source/datadog/flatten_api.rs` *(new, stub)* | `pub fn flatten(event: Value) -> Value` (stub: identity). |
| `…/source/datadog/site.rs` *(new, stub)* | `pub fn base_url(site:&str) -> Result<String>` for the 5 regional sites (stub: US1 only). |
| `crates/esift-cli/src/main.rs` | `build_source` dispatch arms for `datadog-archive` / `datadog-api` (map `cfg.dd_*` → ctors); `--source-dd-*` flags on the `Extract` command mapping to the same ctors. Secrets (`dd_api_key`/`dd_app_key`) resolved via the existing `secret::resolve` (`env:`/`file:`). |
| `crates/esift-core/Cargo.toml` | Features `datadog-s3 = ["dep:aws-sdk-s3","dep:aws-config","dep:zstd"]`, `datadog-api = []`; add optional `zstd`. |
| `crates/esift-cli/Cargo.toml` | Propagate: `datadog-s3 = ["esift-core/datadog-s3"]`, `datadog-api = ["esift-core/datadog-api"]`. |

**Done when:** `cargo check --all-features`, `cargo check` (no features), `cargo clippy
--all-features -- -D warnings`, and `cargo test --all` all pass; running with
`--source-type datadog-archive` (no feature) prints the build-with-feature error.

---

## Lanes (each = disjoint files, one PR, runnable in parallel)

| # | Lane | Owns (new/edited files) | Consumes (foundation seams) | New dep | Tests |
|---|---|---|---|---|---|
| 1 | **Archive source driver** | `source/datadog/archive.rs` | `decompress`, `flatten_archive`, `dd_*` ctor, `datadog-s3` gate | — (uses `aws-sdk-s3` already gated) | in-file unit (list → iterate → checkpoint) over pre-decompressed NDJSON |
| 2 | **API source driver** | `source/datadog/api.rs` | `flatten_api`, `site::base_url`, `http/retry.rs`, `dd_*` ctor, `datadog-api` gate | — | in-file unit (cursor pagination, window advance, 429 backoff) via `wiremock` |
| 3 | **Archive parsing** | `source/datadog/decompress.rs`, `source/datadog/flatten_archive.rs` | — | `zstd` (decl. in foundation) | in-file unit on `.zst`/`.gz` byte fixtures + archive-shape JSON fixtures |
| 4 | **API parsing + regions** | `source/datadog/flatten_api.rs`, `source/datadog/site.rs` | — | — | in-file unit: double-nested `attributes.attributes` flatten; all 5 site URLs |
| 5 | **Verification & CI** | `tests/datadog_archive_e2e.rs`, `tests/datadog_api_e2e.rs`, `docker/localstack.yml`, `.github/workflows/ci.yml` | full source public APIs | — | LocalStack S3 archive E2E; `wiremock` API cross-window E2E (no dup/missing); CI "lean build" job asserting zero Datadog deps by default |
| 6 | **Docs & examples** | `README.md`, `config/example.toml`, `DATADOG.md` | — | — | doc-only |

> **Collapse to 5 lanes:** fold Lane 6 (Docs) into Lane 5 (Verification), or fold Lane 3
> (Archive parsing) into Lane 1. The split above is the 6-lane maximum-parallelism layout.

### Why the lanes are truly independent

The drivers (1, 2) call the parsing helpers (3, 4) **through the foundation's fixed
signatures**, which already compile as identity/stub. So:

- Lanes 1 & 2 unit-test their *own* logic (S3 listing + checkpoint; pagination + backoff)
  against the stub/identity helpers — green **regardless of when 3 & 4 merge**.
- Lanes 3 & 4 unit-test parsing on byte/JSON fixtures with **no S3 or HTTP**.
- Lane 5's E2E exercises the *real* `.zst` → flatten → Document path end-to-end and is the
  single place where everything is wired together against LocalStack / `wiremock`.

When a driver lane and its helper lane both merge, the real behaviour lights up
automatically — different files, no merge conflict.

---

## Per-lane detail

### Lane 1 — Archive source driver (`source/datadog/archive.rs`, `datadog-s3`)
List objects under `{prefix}/YYYY/MM/DD/HH/` within `[from, to]` (page the S3 `list_objects_v2`
continuation token), download each key, hand bytes to `decompress::decompress`, split NDJSON,
map each line through `flatten_archive::flatten`, emit `Document`s. Resume blob in `cursor()`:
`{"dd_archive":{"last_key":"…","files_done":N}}`; on construct, decode it from `resume_after`
and skip keys ≤ `last_key`. Reuse the `aws_config::defaults()` + `aws_sdk_s3::Client` pattern
from `dest/s3.rs`. **Done:** unit test seeds a fake key list + NDJSON bytes, asserts
Document count, ids, and that a mid-run resume skips processed keys.

### Lane 2 — API source driver (`source/datadog/api.rs`, `datadog-api`)
`reqwest` client (pattern from `opensearch.rs`); headers `DD-API-KEY` / `DD-APPLICATION-KEY`;
base URL from `site::base_url`. Chunk `[from,to]` into `window_minutes` windows; paginate each
to exhaustion via `meta.page.after` → `page.cursor`, re-sending `from`/`to` each request.
On HTTP 429, read `X-RateLimit-Reset` and sleep to that instant + jitter (map to
`EsiftError::Transient` so `http/retry.rs` governs retries; log at WARN with the wait). Flatten
each event via `flatten_api::flatten`. Resume blob in `cursor()`:
`{"dd_api":{"win_from":"…","win_to":"…","after":"…|null","windows_done":N}}`. **Done:**
`wiremock` tests for two-page pagination, window advance across a boundary, and a 429-then-200
sequence honouring the reset header.

### Lane 3 — Archive parsing (`decompress.rs` + `flatten_archive.rs`)
`decompress`: zstd via the `zstd` crate, gzip via `flate2` (already a dep); `Codec` chosen by
caller from the key suffix (`.json.zst` / `.json.gz`). `flatten_archive::flatten`: merge the
archive event's top-level metadata with its attribute map into one flat object (archive shape
differs from the API shape — its own logic). **Done:** round-trip a known zstd and gzip blob;
flatten a captured archive sample to the expected flat JSON.

### Lane 4 — API parsing + regions (`flatten_api.rs` + `site.rs`)
`flatten_api::flatten`: collapse the **double-nested** `data[].attributes.attributes`, merging
top-level `timestamp`/`service`/`host`/`status`/`tags` with the inner map. `site::base_url`:
map `datadoghq.com|datadoghq.eu|us3|us5|ap1` → the five `https://api.…` URLs; unknown site →
`EsiftError::Config`. **Done:** flatten a captured API sample; assert all five URLs + the error.

### Lane 5 — Verification & CI
LocalStack S3 (free) seeded with Datadog-format archive files for a full archive extraction;
`wiremock` API run proving no duplicate/missing docs across window boundaries. Add a CI **lean
build** job: `cargo check -p esift-core` (default features) + assert `aws-sdk-s3`/`zstd` absent
from `cargo tree`, complementing the existing `--all-features` jobs. **Done:** both E2E tests
pass locally; new CI job green.

### Lane 6 — Docs & examples
`README.md`: Datadog source section + `--source-dd-*` flag table + compatibility row.
`config/example.toml`: commented `datadog-archive` and `datadog-api` blocks. `DATADOG.md`:
the path comparison table and the resolved open questions below.

---

## Disjoint-file matrix (merge-safe proof)

| File | Owner |
|---|---|
| `config.rs`, `main.rs`, `source/mod.rs`, `source/datadog/mod.rs`, `Cargo.toml` (×2) | **Foundation only** |
| `source/datadog/archive.rs` | Lane 1 |
| `source/datadog/api.rs` | Lane 2 |
| `source/datadog/decompress.rs`, `flatten_archive.rs` | Lane 3 |
| `source/datadog/flatten_api.rs`, `site.rs` | Lane 4 |
| `tests/datadog_*_e2e.rs`, `docker/localstack.yml`, `.github/workflows/ci.yml` | Lane 5 |
| `README.md`, `config/example.toml`, `DATADOG.md` | Lane 6 |

No file appears twice → lanes merge in any order.

---

## Verification → existing CI jobs

The repo's CI runs four jobs (`check`, `fmt`, `clippy`, `test`), all with `--all-features`.
Every lane must keep all four green; Lane 5 adds the no-feature "lean build" assertion the
strategy doc asks for.

- `check` — `cargo check --all-targets --all-features` (datadog-s3 + datadog-api co-compile).
- `fmt` — `cargo fmt --all -- --check`.
- `clippy` — `cargo clippy --all-targets --all-features -- -D warnings`.
- `test` — `cargo test --all --all-features` (all in-file + E2E tests).
- **new (Lane 5)** — lean build proves zero Datadog deps in the default tree.

---

## Open questions (gate the work — defaults chosen so lanes aren't blocked)

From the strategy doc, with a default so implementation can start; revisit if a stakeholder
disagrees.

1. **Compression in customer archives (zstd/gzip/both)** → support **both** from day one
   (Lane 3), auto-selecting `Codec` by key suffix. Test zstd first.
2. **GCS / Azure archive destinations** → **S3 only** for the initial implementation; the
   `dd_cloud` field reserves the seam for later without scope creep.
3. **Archive prefix structure** → always a **parameter** (`dd_prefix`), never assumed.
4. **Datadog test account for Path 2** → not needed to *land* Lane 2 (mock-driven `wiremock`
   tests); only the manual real-org check needs an org. Tracked, not blocking.

---

## Execution model for 5–6 agents

1. **Foundation merges first** (one agent). After it, everything compiles, Datadog kinds error
   cleanly, behaviour is unchanged.
2. **Lanes 1–6 run concurrently**, each on its own branch/PR over disjoint files. Order of
   completion does not matter.
3. Drivers (1, 2) are green on stub helpers; helpers (3, 4) are green on fixtures; full-fidelity
   behaviour is verified once both halves merge and by Lane 5's E2E on `main`.
