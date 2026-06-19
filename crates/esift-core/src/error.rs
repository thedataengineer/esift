use thiserror::Error;

#[derive(Error, Debug)]
pub enum EsiftError {
    #[error("Source error: {0}")]
    Source(String),

    #[error("Destination error: {0}")]
    Destination(String),

    #[error("Transient error: {0}")]
    Transient(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

// Alias so callers write Result<T> instead of Result<T, EsiftError>
pub type Result<T> = std::result::Result<T, EsiftError>;
