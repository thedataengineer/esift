//! Datadog log sources.
//!
//! Two extraction paths, matching the strategy in `DATADOG-PLAN.md`:
//!
//! - [`archive`] (Path 1): read Datadog's compressed-JSON log archives directly
//!   from object storage. No rate limits, history bounded only by retention of
//!   the bucket, file-level resume. Requires the `datadog-s3` feature.
//! - [`api`] (Path 2): the Logs Search API (`POST /api/v2/logs/events/search`),
//!   cursor-paginated and time-window chunked, bounded by Datadog's live
//!   retention window. Requires the `datadog-api` feature.
//!
//! The source drivers call the pure parsing helpers ([`decompress`],
//! [`flatten_archive`], [`flatten_api`], [`site`]) through fixed signatures, so
//! each concern is independently testable.

pub mod api;
pub mod archive;
pub mod decompress;
pub mod flatten_api;
pub mod flatten_archive;
pub mod site;
