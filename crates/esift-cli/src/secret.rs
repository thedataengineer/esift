//! Resolve secret values from indirection sources so credentials need not be
//! written inline in config files or shell history.
//!
//! A secret reference is one of:
//!   - `env:NAME`  read environment variable `NAME`
//!   - `file:PATH` read `PATH` and trim surrounding whitespace
//!   - anything else is used verbatim (a literal secret)

use anyhow::{Context, Result};

/// Resolve a single secret reference to its value.
pub fn resolve(raw: &str) -> Result<String> {
    if let Some(name) = raw.strip_prefix("env:") {
        std::env::var(name)
            .with_context(|| format!("secret environment variable '{name}' is not set"))
    } else if let Some(path) = raw.strip_prefix("file:") {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read secret file '{path}'"))?;
        Ok(contents.trim().to_string())
    } else {
        Ok(raw.to_string())
    }
}

/// Resolve an optional secret, leaving `None` untouched.
pub fn resolve_opt(raw: Option<String>) -> Result<Option<String>> {
    raw.map(|v| resolve(&v)).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_passes_through() {
        assert_eq!(resolve("plain-secret").unwrap(), "plain-secret");
    }

    #[test]
    fn env_reference_reads_variable() {
        std::env::set_var("ESIFT_TEST_SECRET_ENV", "from-env");
        assert_eq!(resolve("env:ESIFT_TEST_SECRET_ENV").unwrap(), "from-env");
        std::env::remove_var("ESIFT_TEST_SECRET_ENV");
    }

    #[test]
    fn missing_env_reference_errors() {
        std::env::remove_var("ESIFT_TEST_SECRET_ABSENT");
        assert!(resolve("env:ESIFT_TEST_SECRET_ABSENT").is_err());
    }

    #[test]
    fn file_reference_reads_and_trims() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "  secret-in-file\n").unwrap();
        let reference = format!("file:{}", path.display());
        assert_eq!(resolve(&reference).unwrap(), "secret-in-file");
    }

    #[test]
    fn resolve_opt_preserves_none() {
        assert_eq!(resolve_opt(None).unwrap(), None);
        assert_eq!(
            resolve_opt(Some("literal".to_string())).unwrap(),
            Some("literal".to_string())
        );
    }
}
