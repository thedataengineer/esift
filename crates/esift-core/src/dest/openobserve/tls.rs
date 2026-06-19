//! Build the reqwest client with the configured TLS options.
//!
//! Wires a custom CA certificate, mutual TLS (client certificate + key), and
//! the `insecure` escape hatch.

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

    // Mutual TLS: present a client certificate. Both halves are required.
    match (&tls.client_cert, &tls.client_key) {
        (Some(cert_path), Some(key_path)) => {
            let mut cert = std::fs::read(cert_path).map_err(|e| {
                EsiftError::Config(format!(
                    "failed to read client certificate {}: {e}",
                    cert_path.display()
                ))
            })?;
            let key = std::fs::read(key_path).map_err(|e| {
                EsiftError::Config(format!(
                    "failed to read client key {}: {e}",
                    key_path.display()
                ))
            })?;
            // reqwest's rustls identity parses a single PEM buffer carrying the
            // certificate chain followed by the private key.
            cert.push(b'\n');
            cert.extend_from_slice(&key);
            let identity = reqwest::Identity::from_pem(&cert).map_err(|e| {
                EsiftError::Config(format!("invalid client certificate/key for mTLS: {e}"))
            })?;
            builder = builder.identity(identity);
        }
        (None, None) => {}
        _ => {
            return Err(EsiftError::Config(
                "client_cert and client_key must both be set for mTLS".to_string(),
            ));
        }
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

    #[test]
    fn client_cert_without_key_errors() {
        let tls = TlsOptions {
            client_cert: Some("/nonexistent/cert.pem".into()),
            ..Default::default()
        };
        let err = build_client(&tls).unwrap_err();
        assert!(
            err.to_string().contains("must both be set"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn invalid_client_identity_errors() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, b"not a real certificate").unwrap();
        std::fs::write(&key, b"not a real key").unwrap();

        let tls = TlsOptions {
            client_cert: Some(cert),
            client_key: Some(key),
            ..Default::default()
        };
        assert!(build_client(&tls).is_err());
    }
}
