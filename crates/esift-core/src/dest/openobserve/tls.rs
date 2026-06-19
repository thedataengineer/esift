//! Build the reqwest client with the configured TLS options.
//!
//! Foundation stub: a default client (system trust store, verification on).
//! Lane 9 wires a custom CA, client certificate (mTLS), and the `insecure`
//! escape hatch.

use super::config::TlsOptions;
use crate::error::Result;

/// Construct the HTTP client used for all bulk requests.
pub(crate) fn build_client(_tls: &TlsOptions) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().build()?)
}
