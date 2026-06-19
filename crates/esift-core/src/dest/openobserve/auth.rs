//! Apply authentication to an outgoing bulk request.
//!
//! When `options.token` is set, the request carries a bearer token; otherwise
//! it falls back to HTTP basic auth with the configured username/password.

use super::SinkContext;

/// Attach credentials to the request builder.
pub(crate) fn apply(
    builder: reqwest::RequestBuilder,
    ctx: &SinkContext,
) -> reqwest::RequestBuilder {
    match &ctx.options.token {
        Some(token) => builder.bearer_auth(token),
        None => builder.basic_auth(&ctx.username, Some(&ctx.password)),
    }
}
