use crate::error::{EsiftError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Resumable extraction state persisted to disk after each successful batch.
///
/// Write pattern: serialize to a .tmp file, then atomically rename over the
/// real checkpoint file. On macOS/Linux, rename(2) is atomic, so a crash
/// mid-write leaves the previous checkpoint intact rather than a corrupt file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    /// The search_after cursor from the last successful batch.
    /// None means start from the beginning.
    pub search_after: Option<Vec<serde_json::Value>>,
    /// Total documents successfully written so far.
    pub docs_written: u64,
    /// Total batches completed.
    pub batches_completed: u64,
}

impl Checkpoint {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            debug!("No checkpoint at {:?}, starting fresh", path);
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(path)
            .map_err(|e| EsiftError::Checkpoint(format!("Failed to read checkpoint: {}", e)))?;

        let checkpoint: Self = serde_json::from_str(&content)
            .map_err(|e| EsiftError::Checkpoint(format!("Failed to parse checkpoint: {}", e)))?;

        info!(
            "Resuming: {} docs written, {} batches completed",
            checkpoint.docs_written, checkpoint.batches_completed
        );

        Ok(checkpoint)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");

        let content = serde_json::to_string_pretty(self)
            .map_err(|e| EsiftError::Checkpoint(format!("Serialize failed: {}", e)))?;

        std::fs::write(&tmp, content)
            .map_err(|e| EsiftError::Checkpoint(format!("Write tmp failed: {}", e)))?;

        std::fs::rename(&tmp, path)
            .map_err(|e| EsiftError::Checkpoint(format!("Atomic rename failed: {}", e)))?;

        debug!("Checkpoint saved ({} docs total)", self.docs_written);
        Ok(())
    }

    pub fn record_batch(
        &mut self,
        docs_written: usize,
        search_after: Option<Vec<serde_json::Value>>,
    ) {
        self.docs_written += docs_written as u64;
        self.batches_completed += 1;
        self.search_after = search_after;
    }
}

pub struct CheckpointManager {
    path: PathBuf,
    pub state: Checkpoint,
}

impl CheckpointManager {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let state = Checkpoint::load(&path)?;
        Ok(Self { path, state })
    }

    pub fn save(&self) -> Result<()> {
        self.state.save(&self.path)
    }
}
