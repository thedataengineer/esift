//! Apply authentication to an outgoing bulk request.
//!
//! Foundation stub: HTTP basic auth, as before. Lane 8 adds token-header auth
//! (used when `options.token` is set) and sources secrets from env/file.

use super::SinkContext;

/// Attach credentials to the request builder.
pub(crate) fn apply(
    builder: reqwest::RequestBuilder,
    ctx: &SinkContext,
) -> reqwest::RequestBuilder {
    builder.basic_auth(&ctx.username, Some(&ctx.password))
}
