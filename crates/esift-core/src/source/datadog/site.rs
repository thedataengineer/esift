//! Map a Datadog site identifier to its Logs API base URL.

use crate::error::{EsiftError, Result};

/// Base URL for the Datadog API at `site`. Accepts the common site identifiers
/// and treats the empty string as US1 (`datadoghq.com`).
pub fn base_url(site: &str) -> Result<String> {
    let url = match site {
        "" | "datadoghq.com" | "us1.datadoghq.com" | "us" | "us1" => "https://api.datadoghq.com",
        "datadoghq.eu" | "eu" | "eu1" => "https://api.datadoghq.eu",
        "us3.datadoghq.com" | "us3" => "https://api.us3.datadoghq.com",
        "us5.datadoghq.com" | "us5" => "https://api.us5.datadoghq.com",
        "ap1.datadoghq.com" | "ap1" => "https://api.ap1.datadoghq.com",
        other => {
            return Err(EsiftError::Config(format!(
                "Unknown Datadog site '{other}'. Use datadoghq.com, datadoghq.eu, \
                 us3.datadoghq.com, us5.datadoghq.com, or ap1.datadoghq.com."
            )))
        }
    };
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_all_known_sites() {
        assert_eq!(base_url("").unwrap(), "https://api.datadoghq.com");
        assert_eq!(
            base_url("datadoghq.com").unwrap(),
            "https://api.datadoghq.com"
        );
        assert_eq!(
            base_url("datadoghq.eu").unwrap(),
            "https://api.datadoghq.eu"
        );
        assert_eq!(
            base_url("us3.datadoghq.com").unwrap(),
            "https://api.us3.datadoghq.com"
        );
        assert_eq!(
            base_url("us5.datadoghq.com").unwrap(),
            "https://api.us5.datadoghq.com"
        );
        assert_eq!(
            base_url("ap1.datadoghq.com").unwrap(),
            "https://api.ap1.datadoghq.com"
        );
    }

    #[test]
    fn unknown_site_errors() {
        assert!(base_url("example.com").is_err());
    }
}
