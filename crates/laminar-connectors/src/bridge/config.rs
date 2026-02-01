//! Configuration for the connector bridge runtime.

/// Configuration for [`ConnectorBridgeRuntime`](super::ConnectorBridgeRuntime).
#[derive(Debug, Clone)]
pub struct BridgeRuntimeConfig {
    /// Maximum number of records to poll from each source per cycle.
    pub max_batch_size: usize,
    /// Checkpoint interval in milliseconds (0 = disabled).
    pub checkpoint_interval_ms: u64,
    /// Whether to enable exactly-once delivery semantics for sinks
    /// that support it.
    pub enable_exactly_once: bool,
    /// Whether to enable changelog propagation in the DAG.
    pub enable_changelog: bool,
    /// Maximum number of inflight batches per source before
    /// backpressure is applied.
    pub max_inflight_batches: usize,
}

impl Default for BridgeRuntimeConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1024,
            checkpoint_interval_ms: 30_000,
            enable_exactly_once: false,
            enable_changelog: false,
            max_inflight_batches: 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BridgeRuntimeConfig::default();
        assert_eq!(config.max_batch_size, 1024);
        assert_eq!(config.checkpoint_interval_ms, 30_000);
        assert!(!config.enable_exactly_once);
        assert!(!config.enable_changelog);
        assert_eq!(config.max_inflight_batches, 8);
    }

    #[test]
    fn test_custom_config() {
        let config = BridgeRuntimeConfig {
            max_batch_size: 4096,
            checkpoint_interval_ms: 10_000,
            enable_exactly_once: true,
            enable_changelog: true,
            max_inflight_batches: 16,
        };
        assert_eq!(config.max_batch_size, 4096);
        assert!(config.enable_exactly_once);
    }
}
