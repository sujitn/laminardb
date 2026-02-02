//! Lakehouse connectors (Delta Lake, Iceberg).
//!
//! Writes Arrow `RecordBatch` data to lakehouse table formats with
//! ACID transactions and exactly-once semantics.
//!
//! # Architecture
//!
//! ```text
//! Ring 0 (Hot Path):  SPSC push only (~5ns, zero sink code)
//! Ring 1 (Background): Batch buffering -> Parquet writes -> transaction commits
//! Ring 2 (Control):    Schema management, configuration, health checks
//! ```
//!
//! # Module Structure
//!
//! - [`delta`] - `DeltaLakeSink` implementing `SinkConnector`
//! - [`delta_config`] - Configuration and enums
//! - [`delta_metrics`] - Lock-free atomic metrics
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig, DeltaWriteMode};
//!
//! let config = DeltaLakeSinkConfig {
//!     table_path: "s3://data-lake/trades/".to_string(),
//!     write_mode: DeltaWriteMode::Append,
//!     partition_columns: vec!["trade_date".to_string()],
//!     ..Default::default()
//! };
//!
//! let sink = DeltaLakeSink::new(config);
//! ```

pub mod delta;
pub mod delta_config;
pub mod delta_metrics;

// Re-export primary types at module level.
pub use delta::DeltaLakeSink;
pub use delta_config::{
    CompactionConfig, DeliveryGuarantee, DeltaLakeSinkConfig, DeltaWriteMode,
};
pub use delta_metrics::DeltaLakeSinkMetrics;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the Delta Lake sink connector with the given registry.
pub fn register_delta_lake_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "delta-lake".to_string(),
        display_name: "Delta Lake Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: delta_lake_config_keys(),
    };

    registry.register_sink(
        "delta-lake",
        info,
        Arc::new(|| {
            Box::new(DeltaLakeSink::new(DeltaLakeSinkConfig::default()))
        }),
    );
}

#[allow(clippy::too_many_lines)]
fn delta_lake_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required(
            "table.path",
            "Path to Delta Lake table (local, s3://, az://, gs://)",
        ),
        ConfigKeySpec::optional(
            "partition.columns",
            "Comma-separated partition column names",
            "",
        ),
        ConfigKeySpec::optional(
            "target.file.size",
            "Target Parquet file size in bytes",
            "134217728",
        ),
        ConfigKeySpec::optional(
            "max.buffer.records",
            "Maximum records to buffer before flushing",
            "100000",
        ),
        ConfigKeySpec::optional(
            "max.buffer.duration.ms",
            "Maximum time to buffer before flushing (ms)",
            "60000",
        ),
        ConfigKeySpec::optional(
            "checkpoint.interval",
            "Create Delta checkpoint every N commits",
            "10",
        ),
        ConfigKeySpec::optional(
            "schema.evolution",
            "Enable automatic schema evolution (additive columns)",
            "false",
        ),
        ConfigKeySpec::optional(
            "write.mode",
            "Write mode: append, overwrite, upsert",
            "append",
        ),
        ConfigKeySpec::optional(
            "merge.key.columns",
            "Key columns for upsert MERGE (required for upsert mode)",
            "",
        ),
        ConfigKeySpec::optional(
            "delivery.guarantee",
            "exactly-once or at-least-once",
            "exactly-once",
        ),
        ConfigKeySpec::optional(
            "compaction.enabled",
            "Enable background OPTIMIZE compaction",
            "true",
        ),
        ConfigKeySpec::optional(
            "compaction.z-order.columns",
            "Columns for Z-ORDER clustering",
            "",
        ),
        ConfigKeySpec::optional(
            "compaction.min-files",
            "Minimum files before triggering compaction",
            "10",
        ),
        ConfigKeySpec::optional(
            "vacuum.retention.hours",
            "Hours to retain old files during VACUUM",
            "168",
        ),
        ConfigKeySpec::optional(
            "writer.id",
            "Writer ID for exactly-once deduplication (auto UUID if not set)",
            "",
        ),
        // ── Cloud storage credentials (resolved via StorageCredentialResolver) ──
        ConfigKeySpec::optional(
            "storage.aws_access_key_id",
            "AWS access key ID (falls back to AWS_ACCESS_KEY_ID env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_secret_access_key",
            "AWS secret access key (falls back to AWS_SECRET_ACCESS_KEY env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_region",
            "AWS region for S3 paths (falls back to AWS_REGION env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_session_token",
            "AWS session token for temporary credentials (falls back to AWS_SESSION_TOKEN)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_endpoint",
            "Custom S3 endpoint (MinIO, LocalStack; falls back to AWS_ENDPOINT_URL)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_profile",
            "AWS profile name (falls back to AWS_PROFILE env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_account_name",
            "Azure storage account name (falls back to AZURE_STORAGE_ACCOUNT_NAME)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_account_key",
            "Azure storage account key (falls back to AZURE_STORAGE_ACCOUNT_KEY)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_sas_token",
            "Azure SAS token (falls back to AZURE_STORAGE_SAS_TOKEN)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_client_id",
            "Azure client ID for service principal auth (falls back to AZURE_CLIENT_ID)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.google_service_account_path",
            "Path to GCS service account JSON (falls back to GOOGLE_APPLICATION_CREDENTIALS)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.google_service_account_key",
            "Inline GCS service account JSON (falls back to GOOGLE_SERVICE_ACCOUNT_KEY)",
            "",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_delta_lake_sink() {
        let registry = ConnectorRegistry::new();
        register_delta_lake_sink(&registry);

        let info = registry.sink_info("delta-lake");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "delta-lake");
        assert!(info.is_sink);
        assert!(!info.is_source);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_config_keys_required() {
        let keys = delta_lake_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"table.path"));
        assert_eq!(required.len(), 1);
    }

    #[test]
    fn test_config_keys_include_cloud_storage() {
        let keys = delta_lake_config_keys();
        let key_names: Vec<&str> = keys.iter().map(|k| k.key.as_str()).collect();
        assert!(key_names.contains(&"storage.aws_access_key_id"));
        assert!(key_names.contains(&"storage.aws_secret_access_key"));
        assert!(key_names.contains(&"storage.aws_region"));
        assert!(key_names.contains(&"storage.azure_storage_account_name"));
        assert!(key_names.contains(&"storage.azure_storage_account_key"));
        assert!(key_names.contains(&"storage.google_service_account_path"));
    }

    #[test]
    fn test_config_keys_optional_present() {
        let keys = delta_lake_config_keys();
        let optional: Vec<&str> = keys
            .iter()
            .filter(|k| !k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(optional.contains(&"partition.columns"));
        assert!(optional.contains(&"target.file.size"));
        assert!(optional.contains(&"write.mode"));
        assert!(optional.contains(&"delivery.guarantee"));
        assert!(optional.contains(&"merge.key.columns"));
        assert!(optional.contains(&"schema.evolution"));
        assert!(optional.contains(&"compaction.enabled"));
        assert!(optional.contains(&"compaction.z-order.columns"));
        assert!(optional.contains(&"vacuum.retention.hours"));
        assert!(optional.contains(&"writer.id"));
    }

    #[test]
    fn test_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_delta_lake_sink(&registry);

        let config = crate::config::ConnectorConfig::new("delta-lake");
        let sink = registry.create_sink(&config);
        assert!(sink.is_ok());
    }
}
