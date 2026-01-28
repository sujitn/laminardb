//! Configuration for `LaminarDB`.

use std::path::PathBuf;

use laminar_core::streaming::BackpressureStrategy;

/// Configuration for a `LaminarDB` instance.
#[derive(Debug, Clone)]
pub struct LaminarConfig {
    /// Default buffer size for streaming channels.
    pub default_buffer_size: usize,
    /// Default backpressure strategy.
    pub default_backpressure: BackpressureStrategy,
    /// Storage directory for WAL and checkpoints (`None` = in-memory only).
    pub storage_dir: Option<PathBuf>,
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
        }
    }
}
