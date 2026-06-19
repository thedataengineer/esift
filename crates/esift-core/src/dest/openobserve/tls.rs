//! Build the reqwest client with the configured TLS options.
//!
//! Wires a custom CA certificate and the `insecure` escape hatch. Client
//! certificate (mTLS) is out of scope here; `client_cert`/`client_key` stay
//! unused until a later lane.

use super::config::TlsOptions;
use crate::error::{EsiftError, Result};

/// Construct the HTTP client used for all bulk requests.
pub(crate) fn build_client(tls: &TlsOptions) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();

    if tls.insecure {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(path) = &tls.ca_cert {
        let bytes = std::fs::read(path).map_err(|e| {
            EsiftError::Config(format!(
                "failed to read CA certificate {}: {e}",
                path.display()
            ))
        })?;
        let cert = reqwest::Certificate::from_pem(&bytes).map_err(|e| {
            EsiftError::Config(format!("invalid CA certificate {}: {e}", path.display()))
        })?;
        builder = builder.add_root_certificate(cert);
    }

    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_build_ok() {
        assert!(build_client(&TlsOptions::default()).is_ok());
    }

    #[test]
    fn insecure_builds_ok() {
        let tls = TlsOptions {
            insecure: true,
            ..Default::default()
        };
        assert!(build_client(&tls).is_ok());
    }
}
