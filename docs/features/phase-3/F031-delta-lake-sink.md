# F031: Delta Lake Sink

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0 (Foundation for lakehouse integration)
**Estimated Complexity:** Large (1-2 weeks)
**Prerequisites:** F023 (Exactly-Once Sinks), F034 (Connector SDK), F063 (Changelog/Retraction)

---

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F031 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F023 (Exactly-Once Sinks), F034 (Connector SDK), F063 (Changelog/Retraction) |
| **Blocks** | F061 (Historical Backfill) |
| **Owner** | TBD |

---

## Executive Summary

This specification defines the Delta Lake Sink connector: a high-performance, exactly-once streaming writer that materializes Arrow RecordBatch data from LaminarDB's streaming pipeline into Delta Lake tables. The sink writes Parquet files via zero-copy Arrow-to-Parquet conversion, maintains the Delta Lake transaction log (`_delta_log/`) for ACID semantics, and maps LaminarDB checkpoint epochs to Delta Lake versions for exactly-once delivery. It supports time-based partitioning, schema evolution, CDC/upsert mode via MERGE operations, background compaction (OPTIMIZE/Z-ORDER), and multi-cloud storage backends (local, S3, Azure ADLS, GCS).

The Delta Lake Sink runs entirely in Ring 1 (background I/O), with zero impact on Ring 0 hot path latency. It implements the `SinkConnector` trait from F034 and integrates with the `ExactlyOnceSinkAdapter` from F023 for transactional epoch-based commits.

---

## 1. Problem Statement

### Current Limitation

LaminarDB's streaming engine processes events at sub-microsecond latency through its three-ring architecture, but there is no way to persist streaming results to a lakehouse format. The `laminar-storage` crate contains only WAL and checkpoint infrastructure. The `lakehouse.rs` module is a placeholder with no implementation. Without a Delta Lake sink, users cannot:

- Archive streaming results for batch analytics (Spark, Trino, Presto)
- Build unified real-time and historical query pipelines
- Maintain ACID-compliant data lakes from streaming sources
- Feed downstream data platforms with exactly-once semantics

### Required Capability

A Delta Lake Sink connector that:

1. Writes Arrow RecordBatch data to Parquet files with zero-copy conversion
2. Maintains the Delta Lake transaction log for ACID commit semantics
3. Provides exactly-once delivery via epoch-to-Delta-version mapping
4. Supports time-based partitioning for query performance
5. Handles schema evolution (additive column changes, type widening)
6. Supports CDC/upsert mode for changelog streams (F063 Z-sets)
7. Runs background compaction (OPTIMIZE, Z-ORDER) without blocking ingestion
8. Works with local filesystem, S3, Azure ADLS, and GCS storage backends

### Use Case Diagram

```
+------------------------------------------------------------------+
|                      Delta Lake Sink Architecture                  |
|                                                                    |
|  LaminarDB Pipeline            Delta Lake Sink         Storage     |
|  +------------------+     +---------------------+                  |
|  |  Source (Kafka)   |     |                     |     +---------+ |
|  |       |           |     | RecordBatchWriter   |---->| Parquet | |
|  |  [Operators]      |     |  (Arrow zero-copy)  |     | Files   | |
|  |       |           |     |                     |     +---------+ |
|  |  Subscription     |---->| EpochTracker        |                  |
|  |  (Ring 0 -> 1)    |     |  (epoch -> version) |     +---------+ |
|  +------------------+     |                     |---->|_delta_log| |
|                            | PartitionWriter    |     | /        | |
|                            |  (time partitions) |     +---------+ |
|  Ring 0 (Hot Path)         |                     |                  |
|  <500ns, no I/O            | CompactionManager  |     +---------+ |
|                            |  (OPTIMIZE/ZORDER) |---->| Cloud    | |
|  Ring 1 (Background)       +---------------------+     | Storage  | |
|  Async I/O, Delta writes                               | S3/ADLS  | |
|                                                         | GCS/Local| |
|  Ring 2 (Control Plane)                                +---------+ |
|  Schema management, config                                         |
+------------------------------------------------------------------+
```

---

## 2. Design Principles

### 2.1 Core Principles (Aligned with LaminarDB Philosophy)

| Principle | Application |
|-----------|-------------|
| **Three-ring separation** | All Delta Lake I/O runs in Ring 1; schema management in Ring 2; Ring 0 untouched |
| **Zero-allocation hot path** | Sink receives data via SPSC channel subscription; no allocations in Ring 0 |
| **Arrow-native** | RecordBatch passes directly to Parquet writer; zero serialization overhead |
| **Exactly-once via epochs** | Epoch-to-Delta-version mapping guarantees no duplicates on recovery |
| **SQL-first configuration** | `CREATE SINK TABLE ... WITH (connector = 'delta-lake', ...)` DDL |
| **No custom types unless benchmarks demand** | Reuses `SinkConnector` trait, `RecordBatch`, `ConnectorConfig` |

### 2.2 Industry-Informed Patterns (2025-2026 Research)

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **Delta Lake Protocol** | Delta Lake 3.x (Databricks) | Implement protocol via `deltalake` (delta-rs) Rust crate |
| **Exactly-once lakehouse writes** | Flink Delta Lake Connector | Epoch-to-version mapping with idempotent commits |
| **Arrow-to-Parquet zero-copy** | Apache Arrow Rust ecosystem | `arrow::parquet::ArrowWriter` direct from RecordBatch |
| **Partition pruning** | Delta Lake partition columns | Time-based partitioning (date/hour) for query pushdown |
| **Schema evolution** | Delta Lake column mapping | Additive columns, type widening via merge schema |
| **CDC MERGE operations** | Debezium + Delta Lake | F063 Z-set changelog to Delta Lake MERGE for upserts |
| **Z-ORDER compaction** | Databricks OPTIMIZE | Background compaction for multi-dimensional query performance |
| **Multi-cloud object store** | `object_store` Rust crate | S3, Azure ADLS, GCS via unified `ObjectStore` trait |

---

## 3. Architecture

### 3.1 Core Data Structures

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use deltalake::kernel::{Action, Add, Metadata, Protocol, Remove};
use deltalake::operations::write::WriteBuilder;
use deltalake::DeltaTable;

use crate::connector::{
    ConnectorConfig, ConnectorError, ConnectorInfo, ConnectorState,
    HealthStatus, SinkConnector, SinkConnectorCapabilities, WriteResult,
};
use crate::metrics::ConnectorMetrics;

/// Configuration for the Delta Lake sink connector.
#[derive(Debug, Clone)]
pub struct DeltaLakeSinkConfig {
    /// Path to the Delta Lake table (local, s3://, az://, gs://).
    pub table_path: String,
    /// Columns to partition by (e.g., ["trade_date", "hour"]).
    pub partition_columns: Vec<String>,
    /// Target Parquet file size in bytes (default: 128 MB).
    pub target_file_size: usize,
    /// Maximum number of records to buffer before flushing to Parquet.
    pub max_buffer_records: usize,
    /// Maximum time to buffer records before flushing (even if below target size).
    pub max_buffer_duration: Duration,
    /// Delta Lake checkpoint interval (create checkpoint every N commits).
    pub checkpoint_interval: u64,
    /// Whether to enable schema evolution (auto-merge new columns).
    pub schema_evolution: bool,
    /// Write mode: Append, Overwrite, or Upsert (CDC merge).
    pub write_mode: DeltaWriteMode,
    /// Key columns for upsert/merge operations (required for Upsert mode).
    pub merge_key_columns: Vec<String>,
    /// Storage options (S3 credentials, Azure keys, etc.).
    pub storage_options: HashMap<String, String>,
    /// Compaction configuration.
    pub compaction: CompactionConfig,
    /// Vacuum retention period for old files.
    pub vacuum_retention: Duration,
    /// Delivery guarantee: AtLeastOnce or ExactlyOnce.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Writer ID for exactly-once deduplication.
    pub writer_id: String,
}

impl Default for DeltaLakeSinkConfig {
    fn default() -> Self {
        Self {
            table_path: String::new(),
            partition_columns: Vec::new(),
            target_file_size: 128 * 1024 * 1024, // 128 MB
            max_buffer_records: 100_000,
            max_buffer_duration: Duration::from_secs(60),
            checkpoint_interval: 10,
            schema_evolution: false,
            write_mode: DeltaWriteMode::Append,
            merge_key_columns: Vec::new(),
            storage_options: HashMap::new(),
            compaction: CompactionConfig::default(),
            vacuum_retention: Duration::from_secs(7 * 24 * 3600), // 7 days
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
            writer_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

/// Delta Lake write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaWriteMode {
    /// Append-only: all records are inserts. Most efficient for immutable streams.
    Append,
    /// Overwrite: replace partition contents. Used for batch-style recomputation.
    Overwrite,
    /// Upsert/Merge: CDC-style insert/update/delete via MERGE statement.
    /// Requires `merge_key_columns` to be set. Integrates with F063 Z-sets.
    Upsert,
}

/// Delivery guarantee level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// May produce duplicates on recovery. Simpler, slightly higher throughput.
    AtLeastOnce,
    /// No duplicates. Uses epoch-to-Delta-version mapping.
    ExactlyOnce,
}

/// Configuration for background compaction.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Whether to run automatic compaction.
    pub enabled: bool,
    /// Minimum number of files before triggering compaction.
    pub min_files_for_compaction: usize,
    /// Target file size after compaction.
    pub target_file_size: usize,
    /// Columns for Z-ORDER clustering (optional).
    pub z_order_columns: Vec<String>,
    /// How often to check if compaction is needed.
    pub check_interval: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_files_for_compaction: 10,
            target_file_size: 128 * 1024 * 1024, // 128 MB
            z_order_columns: Vec::new(),
            check_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}
```

### 3.2 Delta Lake Sink Implementation

```rust
/// Delta Lake Sink Connector.
///
/// Writes Arrow RecordBatch data to a Delta Lake table with ACID transactions
/// and exactly-once semantics. Implements the `SinkConnector` trait from F034.
///
/// # Lifecycle
///
/// ```text
/// new() -> open() -> [begin_epoch() -> write_batch()* -> commit_epoch()] -> close()
///                          |                                    |
///                          +--- rollback_epoch() (on failure) --+
/// ```
///
/// # Exactly-Once Semantics
///
/// Each LaminarDB epoch maps to exactly one Delta Lake transaction (version).
/// On recovery, the sink checks `_delta_log/` for the last committed epoch
/// via `txn` (application transaction) metadata. If an epoch was already
/// committed, it is skipped (idempotent commit).
///
/// # Ring Architecture
///
/// All Delta Lake I/O runs in Ring 1 (background async I/O).
/// Ring 0 pushes RecordBatch data via SPSC subscription channel.
/// Ring 2 manages configuration and schema discovery.
pub struct DeltaLakeSink {
    /// The delta-rs DeltaTable handle.
    table: Option<DeltaTable>,
    /// Connector configuration.
    config: DeltaLakeSinkConfig,
    /// Arrow schema of the table.
    schema: Option<SchemaRef>,
    /// Current connector state.
    state: ConnectorState,
    /// Current epoch being written.
    current_epoch: u64,
    /// Last successfully committed epoch.
    last_committed_epoch: u64,
    /// RecordBatch buffer for the current epoch.
    batch_buffer: Vec<RecordBatch>,
    /// Total rows buffered in current epoch.
    buffered_rows: usize,
    /// Total bytes buffered (estimated) in current epoch.
    buffered_bytes: u64,
    /// Parquet files written in the current epoch (pending commit).
    pending_files: Vec<Add>,
    /// Background compaction manager.
    compaction_manager: Option<CompactionManager>,
    /// Metrics for this connector.
    metrics: DeltaLakeSinkMetrics,
    /// Time when the current buffer started accumulating.
    buffer_start_time: Option<Instant>,
}

impl DeltaLakeSink {
    /// Creates a new Delta Lake sink with the given configuration.
    pub fn new(config: DeltaLakeSinkConfig) -> Self {
        Self {
            table: None,
            config,
            schema: None,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            batch_buffer: Vec::new(),
            buffered_rows: 0,
            buffered_bytes: 0,
            pending_files: Vec::new(),
            compaction_manager: None,
            metrics: DeltaLakeSinkMetrics::default(),
            buffer_start_time: None,
        }
    }

    /// Creates a Delta Lake sink from SQL connector config.
    pub fn from_connector_config(
        connector_config: &ConnectorConfig,
    ) -> Result<Self, ConnectorError> {
        let table_path = connector_config.require("table.path")?;

        let partition_columns: Vec<String> = connector_config
            .options
            .get("partition.columns")
            .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
            .unwrap_or_default();

        let target_file_size: usize = connector_config
            .get("target.file.size")
            .unwrap_or(128 * 1024 * 1024);

        let checkpoint_interval: u64 = connector_config
            .get("checkpoint.interval")
            .unwrap_or(10);

        let write_mode = match connector_config
            .options
            .get("write.mode")
            .map(|s| s.as_str())
        {
            Some("append") | None => DeltaWriteMode::Append,
            Some("overwrite") => DeltaWriteMode::Overwrite,
            Some("upsert") | Some("merge") => DeltaWriteMode::Upsert,
            Some(other) => {
                return Err(ConnectorError::Config(
                    format!("Unknown write mode: '{other}'. Expected: append, overwrite, upsert"),
                ));
            }
        };

        let merge_key_columns: Vec<String> = connector_config
            .options
            .get("merge.key.columns")
            .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
            .unwrap_or_default();

        if write_mode == DeltaWriteMode::Upsert && merge_key_columns.is_empty() {
            return Err(ConnectorError::Config(
                "Upsert mode requires 'merge.key.columns' to be specified".into(),
            ));
        }

        let delivery_guarantee = match connector_config
            .options
            .get("delivery.guarantee")
            .map(|s| s.as_str())
        {
            Some("exactly-once") | None => DeliveryGuarantee::ExactlyOnce,
            Some("at-least-once") => DeliveryGuarantee::AtLeastOnce,
            Some(other) => {
                return Err(ConnectorError::Config(
                    format!("Unknown delivery guarantee: '{other}'"),
                ));
            }
        };

        let z_order_columns: Vec<String> = connector_config
            .options
            .get("compaction.z-order.columns")
            .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
            .unwrap_or_default();

        // Collect storage-prefixed options (e.g., "storage.aws_access_key_id")
        let storage_options: HashMap<String, String> = connector_config
            .options
            .iter()
            .filter(|(k, _)| k.starts_with("storage."))
            .map(|(k, v)| (k.strip_prefix("storage.").unwrap().to_string(), v.clone()))
            .collect();

        let config = DeltaLakeSinkConfig {
            table_path: table_path.to_string(),
            partition_columns,
            target_file_size,
            checkpoint_interval,
            write_mode,
            merge_key_columns,
            storage_options,
            delivery_guarantee,
            compaction: CompactionConfig {
                z_order_columns,
                ..CompactionConfig::default()
            },
            ..DeltaLakeSinkConfig::default()
        };

        Ok(Self::new(config))
    }

    /// Flushes the current buffer to Parquet files.
    ///
    /// Converts buffered RecordBatch data to Parquet format using Arrow's
    /// zero-copy writer and stages the files for Delta Lake commit.
    async fn flush_buffer(&mut self) -> Result<(), ConnectorError> {
        if self.batch_buffer.is_empty() {
            return Ok(());
        }

        let table = self.table.as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            })?;

        let batches = std::mem::take(&mut self.batch_buffer);
        let rows_flushed = self.buffered_rows;
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;

        let flush_start = Instant::now();

        // Use delta-rs WriteBuilder for Parquet file creation.
        // This handles partitioning, file naming, and Parquet encoding.
        let mut write_builder = WriteBuilder::new(
            table.log_store().clone(),
            table.state.clone(),
        )
        .with_input_batches(batches.into_iter());

        if !self.config.partition_columns.is_empty() {
            write_builder = write_builder
                .with_partition_columns(self.config.partition_columns.clone());
        }

        // Execute the write (produces Parquet files, returns Add actions).
        let add_actions = write_builder
            .write_parquet_files()
            .await
            .map_err(|e| ConnectorError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            ))?;

        // Stage files for commit (committed in commit_epoch).
        self.pending_files.extend(add_actions);

        // Update metrics.
        let flush_duration = flush_start.elapsed();
        self.metrics.flush_count += 1;
        self.metrics.rows_flushed += rows_flushed as u64;
        self.metrics.bytes_written += self.pending_files.iter()
            .map(|a| a.size as u64)
            .sum::<u64>();
        self.metrics.last_flush_duration = flush_duration;

        tracing::debug!(
            rows = rows_flushed,
            files = self.pending_files.len(),
            duration_ms = flush_duration.as_millis(),
            "Delta Lake: flushed buffer to Parquet"
        );

        Ok(())
    }

    /// Commits pending Parquet files as a Delta Lake transaction.
    ///
    /// This creates a new Delta Lake version with the Add actions
    /// and includes epoch metadata for exactly-once tracking.
    async fn commit_delta_transaction(
        &mut self,
        epoch: u64,
    ) -> Result<u64, ConnectorError> {
        let table = self.table.as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            })?;

        if self.pending_files.is_empty() {
            tracing::debug!(epoch, "Delta Lake: no files to commit for epoch");
            return Ok(table.version());
        }

        let commit_start = Instant::now();

        // Build commit actions.
        let mut actions: Vec<Action> = self.pending_files
            .drain(..)
            .map(Action::Add)
            .collect();

        // Add application transaction metadata for exactly-once.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            actions.push(Action::Txn(deltalake::kernel::Txn {
                app_id: self.config.writer_id.clone(),
                version: epoch as i64,
                last_updated: Some(chrono::Utc::now().timestamp_millis()),
            }));
        }

        // Commit to Delta Lake transaction log.
        let version = deltalake::operations::transaction::commit(
            table.log_store().as_ref(),
            &actions,
            deltalake::protocol::DeltaOperation::Write {
                mode: match self.config.write_mode {
                    DeltaWriteMode::Append => deltalake::protocol::SaveMode::Append,
                    DeltaWriteMode::Overwrite => deltalake::protocol::SaveMode::Overwrite,
                    DeltaWriteMode::Upsert => deltalake::protocol::SaveMode::Append,
                },
                partition_by: if self.config.partition_columns.is_empty() {
                    None
                } else {
                    Some(self.config.partition_columns.clone())
                },
                predicate: None,
            },
            table.state.as_ref(),
            None,
        )
        .await
        .map_err(|e| ConnectorError::Transaction(
            format!("Delta Lake commit failed for epoch {epoch}: {e}"),
        ))?;

        // Update table state to reflect new version.
        table.update().await.map_err(|e| ConnectorError::Io(
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
        ))?;

        let commit_duration = commit_start.elapsed();
        self.metrics.commits += 1;
        self.metrics.last_commit_duration = commit_duration;
        self.metrics.last_delta_version = version;

        tracing::info!(
            epoch,
            version,
            duration_ms = commit_duration.as_millis(),
            "Delta Lake: committed transaction"
        );

        // Create Delta Lake checkpoint if interval reached.
        if version % self.config.checkpoint_interval as i64 == 0 {
            self.create_delta_checkpoint().await?;
        }

        Ok(version as u64)
    }

    /// Creates a Delta Lake checkpoint (_delta_log/_last_checkpoint).
    ///
    /// Delta Lake checkpoints are Parquet files that consolidate the
    /// transaction log for faster reads. This is different from LaminarDB's
    /// internal checkpointing (F022).
    async fn create_delta_checkpoint(&self) -> Result<(), ConnectorError> {
        let table = self.table.as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            })?;

        deltalake::checkpoints::create_checkpoint(table)
            .await
            .map_err(|e| ConnectorError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            ))?;

        tracing::info!(
            version = table.version(),
            "Delta Lake: created checkpoint"
        );

        Ok(())
    }

    /// Resolves the last committed epoch from Delta Lake transaction log.
    ///
    /// Used on recovery to determine which epoch to resume from.
    async fn resolve_last_committed_epoch(
        table: &DeltaTable,
        writer_id: &str,
    ) -> u64 {
        // Read application transaction metadata from Delta Lake log.
        // The `txn` action stores (app_id, version) pairs.
        match table
            .state
            .as_ref()
            .app_transaction_version()
            .get(writer_id)
        {
            Some(&version) => version as u64,
            None => 0,
        }
    }

    /// Checks if a buffer flush is needed based on size or time thresholds.
    fn should_flush(&self) -> bool {
        if self.buffered_rows >= self.config.max_buffer_records {
            return true;
        }
        if self.buffered_bytes >= self.config.target_file_size as u64 {
            return true;
        }
        if let Some(start) = self.buffer_start_time {
            if start.elapsed() >= self.config.max_buffer_duration {
                return true;
            }
        }
        false
    }

    /// Estimates the byte size of a RecordBatch.
    fn estimate_batch_size(batch: &RecordBatch) -> u64 {
        batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size() as u64)
            .sum()
    }
}
```

### 3.3 SinkConnector Trait Implementation

```rust
#[async_trait]
impl SinkConnector for DeltaLakeSink {
    fn info(&self) -> ConnectorInfo {
        ConnectorInfo {
            connector_type: "delta-lake".to_string(),
            description: "Delta Lake table sink with ACID transactions".to_string(),
            config_keys: vec![
                ConfigKeySpec {
                    name: "table.path".into(),
                    description: "Path to Delta Lake table (local, s3://, az://, gs://)".into(),
                    required: true,
                    default: None,
                    example: Some("s3://data-lake/trades/".into()),
                },
                ConfigKeySpec {
                    name: "partition.columns".into(),
                    description: "Comma-separated partition column names".into(),
                    required: false,
                    default: None,
                    example: Some("trade_date,hour".into()),
                },
                ConfigKeySpec {
                    name: "target.file.size".into(),
                    description: "Target Parquet file size in bytes".into(),
                    required: false,
                    default: Some("134217728".into()), // 128 MB
                    example: Some("134217728".into()),
                },
                ConfigKeySpec {
                    name: "checkpoint.interval".into(),
                    description: "Create Delta checkpoint every N commits".into(),
                    required: false,
                    default: Some("10".into()),
                    example: Some("10".into()),
                },
                ConfigKeySpec {
                    name: "write.mode".into(),
                    description: "Write mode: append, overwrite, upsert".into(),
                    required: false,
                    default: Some("append".into()),
                    example: Some("append".into()),
                },
                ConfigKeySpec {
                    name: "delivery.guarantee".into(),
                    description: "Delivery guarantee: exactly-once, at-least-once".into(),
                    required: false,
                    default: Some("exactly-once".into()),
                    example: Some("exactly-once".into()),
                },
            ],
            supported_formats: vec!["arrow".to_string(), "parquet".to_string()],
        }
    }

    fn state(&self) -> ConnectorState {
        self.state
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        // Open or create the Delta Lake table.
        let table = deltalake::open_table_with_storage_options(
            &self.config.table_path,
            self.config.storage_options.clone(),
        )
        .await
        .or_else(|_| async {
            // Table does not exist; will be created on first write.
            // Use DeltaTableBuilder for lazy creation.
            deltalake::DeltaTableBuilder::from_uri(&self.config.table_path)
                .with_storage_options(self.config.storage_options.clone())
                .build()
        })
        .map_err(|e| ConnectorError::Connection(
            format!("Failed to open Delta Lake table at '{}': {e}", self.config.table_path),
        ))?;

        // Resolve last committed epoch for exactly-once recovery.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            self.last_committed_epoch =
                Self::resolve_last_committed_epoch(&table, &self.config.writer_id).await;
            tracing::info!(
                last_epoch = self.last_committed_epoch,
                writer_id = %self.config.writer_id,
                "Delta Lake: resolved last committed epoch"
            );
        }

        // Store schema from existing table or defer to first write.
        if table.version() >= 0 {
            self.schema = Some(Arc::new(
                table.schema()
                    .map_err(|e| ConnectorError::Config(
                        format!("Failed to read table schema: {e}"),
                    ))?
                    .try_into()
                    .map_err(|e: arrow::error::ArrowError| {
                        ConnectorError::Config(format!("Schema conversion error: {e}"))
                    })?,
            ));
        }

        // Start background compaction manager if enabled.
        if self.config.compaction.enabled {
            self.compaction_manager = Some(CompactionManager::new(
                self.config.table_path.clone(),
                self.config.compaction.clone(),
                self.config.storage_options.clone(),
            ));
        }

        self.table = Some(table);
        self.state = ConnectorState::Running;

        tracing::info!(
            table_path = %self.config.table_path,
            "Delta Lake sink: opened successfully"
        );

        Ok(())
    }

    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            });
        }

        let num_rows = batch.num_rows();
        let estimated_bytes = Self::estimate_batch_size(batch);

        // Handle schema on first write.
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }

        // Handle schema evolution (additive columns).
        if self.config.schema_evolution {
            if let Some(ref existing_schema) = self.schema {
                if existing_schema.as_ref() != batch.schema().as_ref() {
                    self.evolve_schema(batch.schema()).await?;
                }
            }
        }

        // Buffer the batch.
        if self.buffer_start_time.is_none() {
            self.buffer_start_time = Some(Instant::now());
        }
        self.batch_buffer.push(batch.clone());
        self.buffered_rows += num_rows;
        self.buffered_bytes += estimated_bytes;

        // Flush if buffer threshold reached.
        if self.should_flush() {
            self.flush_buffer().await?;
        }

        Ok(WriteResult {
            records_written: num_rows,
            bytes_written: estimated_bytes,
        })
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // For exactly-once, skip epochs already committed.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            tracing::warn!(
                epoch,
                last_committed = self.last_committed_epoch,
                "Delta Lake: skipping already-committed epoch"
            );
            return Ok(());
        }

        self.current_epoch = epoch;
        self.batch_buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_files.clear();
        self.buffer_start_time = None;

        tracing::debug!(epoch, "Delta Lake: began epoch");
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        // Flush any remaining buffered data.
        self.flush_buffer().await?;

        // Commit all pending files as a single Delta Lake transaction.
        let version = self.commit_delta_transaction(epoch).await?;

        self.last_committed_epoch = epoch;

        // Trigger background compaction check.
        if let Some(ref mut manager) = self.compaction_manager {
            manager.maybe_compact().await;
        }

        tracing::info!(
            epoch,
            delta_version = version,
            "Delta Lake: committed epoch"
        );

        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard buffered data and pending files.
        self.batch_buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_files.clear();
        self.buffer_start_time = None;

        tracing::warn!(epoch, "Delta Lake: rolled back epoch");
        Ok(())
    }

    fn last_committed_epoch(&self) -> u64 {
        self.last_committed_epoch
    }

    async fn health_check(&self) -> Result<HealthStatus, ConnectorError> {
        let mut details = HashMap::new();
        details.insert("table_path".into(), self.config.table_path.clone());

        if let Some(ref table) = self.table {
            details.insert("version".into(), table.version().to_string());
            details.insert("last_committed_epoch".into(), self.last_committed_epoch.to_string());

            Ok(HealthStatus {
                healthy: true,
                message: format!(
                    "Delta Lake table at version {}, epoch {}",
                    table.version(),
                    self.last_committed_epoch,
                ),
                last_success: Some(self.metrics.last_commit_duration),
                details,
            })
        } else {
            Ok(HealthStatus {
                healthy: false,
                message: "Delta Lake table not yet opened".into(),
                last_success: None,
                details,
            })
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Flush any remaining data.
        if !self.batch_buffer.is_empty() {
            self.flush_buffer().await?;
            if !self.pending_files.is_empty() {
                self.commit_delta_transaction(self.current_epoch).await?;
            }
        }

        // Stop background compaction.
        if let Some(ref mut manager) = self.compaction_manager {
            manager.stop().await;
        }

        self.state = ConnectorState::Stopped;
        tracing::info!("Delta Lake sink: closed");
        Ok(())
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.metrics.rows_flushed,
            bytes_total: self.metrics.bytes_written,
            errors_total: self.metrics.errors,
            records_per_sec: self.metrics.records_per_sec(),
            avg_batch_latency_us: self.metrics.last_flush_duration.as_micros() as u64,
            p99_batch_latency_us: 0, // TODO: histogram
            lag: None,
            custom: {
                let mut m = HashMap::new();
                m.insert("delta_version".into(), self.metrics.last_delta_version as f64);
                m.insert("commits".into(), self.metrics.commits as f64);
                m.insert("flush_count".into(), self.metrics.flush_count as f64);
                m.insert("pending_files".into(), self.pending_files.len() as f64);
                m
            },
        }
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        SinkConnectorCapabilities {
            transactional: true,
            idempotent: self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce,
            partitioned: !self.config.partition_columns.is_empty(),
            schema_evolution: self.config.schema_evolution,
            two_phase_commit: false, // Delta Lake uses OCC, not 2PC
        }
    }
}
```

### 3.4 CDC / Upsert Mode (F063 Integration)

```rust
/// Handles CDC/upsert operations by converting F063 Z-set changelog
/// records into Delta Lake MERGE operations.
///
/// When `write_mode = Upsert`, the sink interprets changelog records:
/// - `CdcOperation::Insert` / weight +1 -> INSERT
/// - `CdcOperation::Delete` / weight -1 -> DELETE
/// - `CdcOperation::UpdateBefore` + `UpdateAfter` -> UPDATE
///
/// Uses Delta Lake's MERGE INTO semantics:
/// ```sql
/// MERGE INTO target USING source
/// ON target.key = source.key
/// WHEN MATCHED AND source._op = 'D' THEN DELETE
/// WHEN MATCHED THEN UPDATE SET *
/// WHEN NOT MATCHED THEN INSERT *
/// ```
impl DeltaLakeSink {
    /// Executes a MERGE operation for CDC/upsert writes.
    ///
    /// Converts buffered batches with CDC operation columns into
    /// a Delta Lake merge operation.
    async fn execute_merge(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<u64, ConnectorError> {
        let table = self.table.as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            })?;

        // Build merge predicate from key columns.
        let merge_predicate = self.config.merge_key_columns
            .iter()
            .map(|col| format!("target.{col} = source.{col}"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let merge = deltalake::operations::merge::MergeBuilder::new(
            table.log_store().clone(),
            table.state.clone(),
            merge_predicate,
            batches,
        )
        .with_source_alias("source")
        .with_target_alias("target")
        // When matched and operation is Delete -> DELETE
        .when_matched_delete(|delete| {
            delete.predicate("source._op = 'D' OR source._op = '-D'")
        })
        // When matched and operation is Update -> UPDATE SET *
        .when_matched_update(|update| {
            update.predicate(
                "source._op = 'U' OR source._op = '+U' OR source._op = 'c'",
            )
            .update_all()
        })
        // When not matched -> INSERT *
        .when_not_matched_insert(|insert| {
            insert
                .predicate("source._op != 'D' AND source._op != '-D'")
                .insert_all()
        });

        let (table_new, merge_metrics) = merge
            .await
            .map_err(|e| ConnectorError::Transaction(
                format!("Delta Lake MERGE failed: {e}"),
            ))?;

        let version = table_new.version();
        *table = table_new;

        tracing::info!(
            version,
            inserted = merge_metrics.num_target_rows_inserted,
            updated = merge_metrics.num_target_rows_updated,
            deleted = merge_metrics.num_target_rows_deleted,
            "Delta Lake: MERGE completed"
        );

        Ok(version as u64)
    }

    /// Evolves the Delta Lake table schema to accommodate new columns.
    ///
    /// Only supports additive changes (new nullable columns) and
    /// type widening (e.g., Int32 -> Int64, Float32 -> Float64).
    async fn evolve_schema(
        &mut self,
        new_schema: SchemaRef,
    ) -> Result<(), ConnectorError> {
        let table = self.table.as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            })?;

        // Merge schemas: existing + new columns.
        let merged = arrow::datatypes::Schema::try_merge(vec![
            self.schema.as_ref().unwrap().as_ref().clone(),
            new_schema.as_ref().clone(),
        ])
        .map_err(|e| ConnectorError::SchemaMismatch {
            expected: format!("{:?}", self.schema),
            actual: format!("{e}"),
        })?;

        // Update Delta Lake table metadata with new schema.
        let metadata = Metadata {
            schema_string: serde_json::to_string(&merged)
                .map_err(|e| ConnectorError::Config(format!("Schema serialization error: {e}")))?,
            partition_columns: self.config.partition_columns.clone(),
            ..table.metadata()
                .map_err(|e| ConnectorError::Config(e.to_string()))?
                .clone()
        };

        // Commit schema change as a metadata-only transaction.
        deltalake::operations::transaction::commit(
            table.log_store().as_ref(),
            &[Action::Metadata(metadata)],
            deltalake::protocol::DeltaOperation::SetTableProperties {},
            table.state.as_ref(),
            None,
        )
        .await
        .map_err(|e| ConnectorError::Transaction(
            format!("Schema evolution failed: {e}"),
        ))?;

        self.schema = Some(Arc::new(merged));

        tracing::info!("Delta Lake: schema evolved successfully");
        Ok(())
    }
}
```

### 3.5 Background Compaction Manager

```rust
/// Manages background compaction of Delta Lake tables.
///
/// Runs OPTIMIZE (bin-packing) and optional Z-ORDER clustering
/// without blocking the streaming write path.
pub struct CompactionManager {
    /// Table path.
    table_path: String,
    /// Compaction configuration.
    config: CompactionConfig,
    /// Storage options.
    storage_options: HashMap<String, String>,
    /// Handle to the background compaction task.
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Shutdown signal.
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl CompactionManager {
    /// Creates a new compaction manager.
    pub fn new(
        table_path: String,
        config: CompactionConfig,
        storage_options: HashMap<String, String>,
    ) -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            table_path,
            config,
            storage_options,
            task_handle: None,
            shutdown: shutdown_tx,
        }
    }

    /// Checks if compaction is needed and triggers it asynchronously.
    pub async fn maybe_compact(&mut self) {
        if self.task_handle.is_some() {
            return; // Already running.
        }

        let table_path = self.table_path.clone();
        let config = self.config.clone();
        let storage_options = self.storage_options.clone();
        let mut shutdown_rx = self.shutdown.subscribe();

        self.task_handle = Some(tokio::spawn(async move {
            // Open table for compaction.
            let table = match deltalake::open_table_with_storage_options(
                &table_path,
                storage_options,
            ).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!(error = %e, "Compaction: failed to open table");
                    return;
                }
            };

            // Check file count.
            let file_count = table.get_files_count();
            if file_count < config.min_files_for_compaction {
                tracing::debug!(
                    file_count,
                    threshold = config.min_files_for_compaction,
                    "Compaction: not enough files, skipping"
                );
                return;
            }

            // Run OPTIMIZE.
            tracing::info!(file_count, "Compaction: starting OPTIMIZE");
            let optimize = deltalake::operations::optimize::OptimizeBuilder::new(
                table.log_store().clone(),
                table.state.clone(),
            )
            .with_target_size(config.target_file_size as i64);

            // Add Z-ORDER if configured.
            let optimize = if !config.z_order_columns.is_empty() {
                optimize.with_type(
                    deltalake::operations::optimize::OptimizeType::ZOrder(
                        config.z_order_columns.clone(),
                    ),
                )
            } else {
                optimize
            };

            match optimize.await {
                Ok((_, metrics)) => {
                    tracing::info!(
                        files_added = metrics.num_files_added,
                        files_removed = metrics.num_files_removed,
                        "Compaction: OPTIMIZE completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "Compaction: OPTIMIZE failed");
                }
            }
        }));
    }

    /// Runs VACUUM to delete old files beyond the retention period.
    pub async fn vacuum(
        table_path: &str,
        retention_hours: u64,
        storage_options: &HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let table = deltalake::open_table_with_storage_options(
            table_path,
            storage_options.clone(),
        )
        .await
        .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        deltalake::operations::vacuum::VacuumBuilder::new(
            table.log_store().clone(),
            table.state.clone(),
        )
        .with_retention_period(chrono::Duration::hours(retention_hours as i64))
        .with_enforce_retention_duration(true)
        .await
        .map_err(|e| ConnectorError::Io(
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
        ))?;

        tracing::info!(
            retention_hours,
            "Delta Lake: VACUUM completed"
        );

        Ok(())
    }

    /// Stops the compaction manager.
    pub async fn stop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}
```

### 3.6 Metrics

```rust
/// Metrics for the Delta Lake sink connector.
#[derive(Debug, Default)]
pub struct DeltaLakeSinkMetrics {
    /// Total rows flushed to Parquet files.
    pub rows_flushed: u64,
    /// Total bytes written to storage.
    pub bytes_written: u64,
    /// Total number of Parquet flush operations.
    pub flush_count: u64,
    /// Total number of Delta Lake commits.
    pub commits: u64,
    /// Total errors encountered.
    pub errors: u64,
    /// Duration of the last flush operation.
    pub last_flush_duration: Duration,
    /// Duration of the last commit operation.
    pub last_commit_duration: Duration,
    /// Last Delta Lake table version.
    pub last_delta_version: u64,
    /// Start time for throughput calculation.
    pub start_time: Option<Instant>,
}

impl DeltaLakeSinkMetrics {
    /// Returns current records-per-second throughput.
    pub fn records_per_sec(&self) -> f64 {
        match self.start_time {
            Some(start) => {
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    self.rows_flushed as f64 / elapsed
                } else {
                    0.0
                }
            }
            None => 0.0,
        }
    }
}
```

### 3.7 Three-Ring Integration

| Ring | Delta Lake Sink Responsibilities |
|------|----------------------------------|
| **Ring 0 (Hot Path)** | No Delta Lake code runs here. Data arrives via SPSC subscription channel. |
| **Ring 1 (Background)** | `DeltaLakeSink.write_batch()` buffers RecordBatch. `flush_buffer()` writes Parquet files. `commit_delta_transaction()` commits to Delta log. `CompactionManager.maybe_compact()` runs OPTIMIZE. `vacuum()` cleans old files. |
| **Ring 2 (Control Plane)** | Schema discovery and evolution. Configuration parsing from SQL DDL. Health checks. Metrics export. |

---

## 4. Latency Considerations

### 4.1 Hot Path Budget

The Delta Lake Sink runs entirely in Ring 1. Ring 0 is unaffected:

| Component | Ring | Latency | Notes |
|-----------|------|---------|-------|
| SPSC subscription poll (Ring 0 -> Ring 1) | Ring 0 | ~5ns | Lock-free, existing implementation |
| RecordBatch buffer append | Ring 1 | ~100ns | Vec push, no copy |
| Parquet file write (flush) | Ring 1 | 10-500ms | Depends on batch size, storage backend |
| Delta Lake commit (transaction log) | Ring 1 | 5-50ms | Depends on storage backend latency |
| S3 PUT (Parquet file upload) | Ring 1 | 50-200ms | Network I/O to cloud storage |
| OPTIMIZE compaction | Ring 1 | 1-60s | Background, does not block writes |
| VACUUM cleanup | Ring 1 | 1-30s | Background, does not block writes |
| **Ring 0 impact** | **Ring 0** | **~5ns** | **SPSC channel read only** |

### 4.2 What Stays OFF the Hot Path

- All Parquet file writing
- All Delta Lake transaction log operations
- Schema validation and evolution
- Storage backend I/O (local/S3/ADLS/GCS)
- Compaction and vacuum operations
- Metrics collection and health checks

---

## 5. SQL Integration

### 5.1 CREATE SINK Syntax

```sql
-- Basic append-only Delta Lake sink
CREATE SINK TABLE trade_archive
FROM processed_trades
WITH (
    connector = 'delta-lake',
    'table.path' = 's3://data-lake/trades/',
    'partition.columns' = 'trade_date',
    'target.file.size' = '134217728',
    'checkpoint.interval' = '10',
    'delivery.guarantee' = 'exactly-once'
);

-- Partitioned sink with multiple columns
CREATE SINK TABLE sensor_data_archive
FROM sensor_events
WITH (
    connector = 'delta-lake',
    'table.path' = '/data/warehouse/sensors/',
    'partition.columns' = 'region,event_date',
    'target.file.size' = '67108864',
    'write.mode' = 'append',
    'delivery.guarantee' = 'exactly-once'
);

-- CDC/Upsert sink from changelog stream (F063 integration)
CREATE SINK TABLE customer_snapshot
FROM customer_changes
WITH (
    connector = 'delta-lake',
    'table.path' = 's3://data-lake/customers/',
    'write.mode' = 'upsert',
    'merge.key.columns' = 'customer_id',
    'delivery.guarantee' = 'exactly-once',
    'storage.aws_access_key_id' = '...',
    'storage.aws_secret_access_key' = '...',
    'storage.aws_region' = 'us-east-1'
);

-- Sink with Z-ORDER compaction for multi-dimensional queries
CREATE SINK TABLE order_analytics
FROM enriched_orders
WITH (
    connector = 'delta-lake',
    'table.path' = 'az://warehouse/orders/',
    'partition.columns' = 'order_date',
    'compaction.z-order.columns' = 'customer_id,product_id',
    'target.file.size' = '268435456',
    'delivery.guarantee' = 'exactly-once',
    'storage.azure_storage_account_name' = '...',
    'storage.azure_storage_account_key' = '...'
);

-- Schema evolution enabled
CREATE SINK TABLE event_log
FROM app_events
WITH (
    connector = 'delta-lake',
    'table.path' = 'gs://analytics/events/',
    'schema.evolution' = 'true',
    'write.mode' = 'append',
    'delivery.guarantee' = 'at-least-once'
);
```

### 5.2 DDL Resolution Flow

```
SQL Statement                    ConnectorRegistry              Delta Lake Sink
     |                                |                              |
     | CREATE SINK TABLE ... WITH     |                              |
     | (connector = 'delta-lake', ..) |                              |
     |------------------------------->|                              |
     |                                | lookup("delta-lake")         |
     |                                |----------------------------->|
     |                                |                              |
     |                                | DeltaLakeSink::from_config() |
     |                                |<-----------------------------|
     |                                |                              |
     | SinkRunner started in Ring 1   |                              |
     |<-------------------------------|                              |
     |                                |                              |
     | Subscription channel created   |                              |
     | (Ring 0 -> Ring 1 SPSC)        |                              |
```

---

## 6. Rust API Examples

### 6.1 Programmatic Usage

```rust
use laminar_connectors::lakehouse::delta::DeltaLakeSink;
use laminar_connectors::lakehouse::delta::{DeltaLakeSinkConfig, DeltaWriteMode};
use laminar_connectors::connector::SinkConnector;

// Create Delta Lake sink with programmatic config.
let config = DeltaLakeSinkConfig {
    table_path: "s3://my-bucket/trades/".to_string(),
    partition_columns: vec!["trade_date".to_string()],
    target_file_size: 128 * 1024 * 1024,
    write_mode: DeltaWriteMode::Append,
    delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
    storage_options: HashMap::from([
        ("aws_access_key_id".into(), "AKID...".into()),
        ("aws_secret_access_key".into(), "SECRET...".into()),
        ("aws_region".into(), "us-east-1".into()),
    ]),
    ..Default::default()
};

let mut sink = DeltaLakeSink::new(config);

// Open the sink (connects to storage, resolves last epoch).
sink.open(&connector_config).await?;

// Write data in epoch-based transactions.
sink.begin_epoch(1).await?;
sink.write_batch(&record_batch_1).await?;
sink.write_batch(&record_batch_2).await?;
sink.commit_epoch(1).await?;

// On recovery, epoch 1 is skipped (idempotent).
sink.begin_epoch(1).await?;  // No-op: already committed

// Close the sink.
sink.close().await?;
```

### 6.2 Usage with ConnectorRuntime (F034)

```rust
use laminar_connectors::runtime::ConnectorRuntime;
use laminar_connectors::lakehouse::delta::DeltaLakeSink;

let mut runtime = ConnectorRuntime::new();

// Register Delta Lake sink factory.
runtime.registry().register_sink("delta-lake", |config| {
    Ok(Box::new(DeltaLakeSink::from_connector_config(config)?))
});

// Start sink from SQL definition.
let sink_handle = runtime.start_sink(
    "trade_archive",
    Box::new(DeltaLakeSink::from_connector_config(&config)?),
    Box::new(ArrowPassthroughSerializer), // No serialization needed
    connector_config,
).await?;

// The SinkRunner in Ring 1 now polls the subscription and
// writes to Delta Lake automatically.
```

### 6.3 CDC/Upsert Example

```rust
use laminar_core::operator::changelog::{ChangelogRecord, CdcOperation};

// Changelog stream produces Z-set records (F063).
let changelog_batch = create_changelog_batch(vec![
    ChangelogRecord::insert(customer_row_1, timestamp),
    ChangelogRecord::delete(customer_row_2, timestamp),
]);

// Delta Lake sink in upsert mode converts to MERGE.
let config = DeltaLakeSinkConfig {
    write_mode: DeltaWriteMode::Upsert,
    merge_key_columns: vec!["customer_id".to_string()],
    ..Default::default()
};

let mut sink = DeltaLakeSink::new(config);
sink.open(&connector_config).await?;

sink.begin_epoch(1).await?;
sink.write_batch(&changelog_batch).await?;
sink.commit_epoch(1).await?;
// MERGE INTO customers USING source ON customer_id
// WHEN MATCHED AND _op = 'D' THEN DELETE
// WHEN MATCHED THEN UPDATE SET *
// WHEN NOT MATCHED THEN INSERT *
```

---

## 7. Configuration Reference

### 7.1 Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'delta-lake'` |
| `table.path` | String | Path to Delta Lake table. Supports `s3://`, `az://`, `gs://`, or local path |

### 7.2 Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `partition.columns` | String (CSV) | (none) | Comma-separated partition column names |
| `target.file.size` | Integer | `134217728` (128 MB) | Target Parquet file size in bytes |
| `max.buffer.records` | Integer | `100000` | Maximum records to buffer before flushing |
| `max.buffer.duration` | String | `60s` | Maximum time to buffer before flushing |
| `checkpoint.interval` | Integer | `10` | Create Delta checkpoint every N commits |
| `write.mode` | String | `append` | Write mode: `append`, `overwrite`, `upsert` |
| `merge.key.columns` | String (CSV) | (none) | Key columns for upsert MERGE (required for upsert mode) |
| `delivery.guarantee` | String | `exactly-once` | Delivery guarantee: `exactly-once`, `at-least-once` |
| `schema.evolution` | Boolean | `false` | Enable automatic schema evolution |
| `compaction.enabled` | Boolean | `true` | Enable background OPTIMIZE compaction |
| `compaction.z-order.columns` | String (CSV) | (none) | Columns for Z-ORDER clustering |
| `compaction.min-files` | Integer | `10` | Minimum files before triggering compaction |
| `vacuum.retention.hours` | Integer | `168` (7 days) | Hours to retain old files |
| `writer.id` | String | (auto UUID) | Writer ID for exactly-once deduplication |

### 7.3 Storage Backend Options

All storage options are prefixed with `storage.` in the SQL WITH clause.

**Amazon S3:**

| Option | Description |
|--------|-------------|
| `storage.aws_access_key_id` | AWS access key ID |
| `storage.aws_secret_access_key` | AWS secret access key |
| `storage.aws_region` | AWS region (e.g., `us-east-1`) |
| `storage.aws_endpoint` | Custom S3 endpoint (for MinIO, etc.) |
| `storage.aws_s3_allow_unsafe_rename` | Allow non-atomic renames (for MinIO) |

**Azure Data Lake Storage:**

| Option | Description |
|--------|-------------|
| `storage.azure_storage_account_name` | Azure storage account name |
| `storage.azure_storage_account_key` | Azure storage account key |
| `storage.azure_storage_sas_token` | Azure SAS token (alternative to key) |

**Google Cloud Storage:**

| Option | Description |
|--------|-------------|
| `storage.google_service_account_path` | Path to service account JSON key file |
| `storage.google_service_account_key` | Inline service account JSON key |

---

## 8. Exactly-Once Semantics Deep Dive

### 8.1 Epoch-to-Version Mapping

```
LaminarDB Epochs              Delta Lake Versions
+---+---+---+---+---+         +---+---+---+---+---+
| 1 | 2 | 3 | 4 | 5 |  --->  | 0 | 1 | 2 | 3 | 4 |
+---+---+---+---+---+         +---+---+---+---+---+
                                         |
                               _delta_log/
                               00000000000000000000.json  (version 0: create table)
                               00000000000000000001.json  (version 1: epoch 1 data)
                               00000000000000000002.json  (version 2: epoch 2 data)
                               00000000000000000003.json  (version 3: epoch 3 data)
                               00000000000000000004.json  (version 4: epoch 4 data)
```

### 8.2 Recovery Protocol

```
1. Sink opens Delta Lake table
2. Read _delta_log for txn action with matching writer_id
3. last_committed_epoch = txn.version (e.g., epoch 3)
4. On begin_epoch(3): skip (already committed)
5. On begin_epoch(4): proceed normally
6. On commit_epoch(4): write txn action with version=4
```

### 8.3 Idempotent Commit

Delta Lake's `txn` action provides application-level transaction tracking.
Each commit includes:

```json
{
  "txn": {
    "appId": "laminardb-writer-abc123",
    "version": 4,
    "lastUpdated": 1706140800000
  }
}
```

On recovery, if `txn.version >= epoch`, the epoch is skipped. This guarantees exactly-once even if the process crashes after writing Parquet files but before the LaminarDB checkpoint completes.

---

## 9. Implementation Roadmap

### Phase 1: Core Sink and Append Mode (3-4 days)

- [ ] Define `DeltaLakeSinkConfig` and `DeltaWriteMode`
- [ ] Implement `DeltaLakeSink` struct with RecordBatch buffering
- [ ] Implement `SinkConnector` trait (open, write_batch, close)
- [ ] Implement `flush_buffer()` with Arrow-to-Parquet conversion
- [ ] Implement `commit_delta_transaction()` with Delta Lake protocol
- [ ] Local filesystem storage backend working
- [ ] Unit tests for config parsing and basic write flow
- [ ] Integration test: write batches and verify Parquet files

### Phase 2: Exactly-Once and Epoch Management (2-3 days)

- [ ] Implement `begin_epoch()` / `commit_epoch()` / `rollback_epoch()`
- [ ] Implement epoch-to-Delta-version mapping via `txn` actions
- [ ] Implement `resolve_last_committed_epoch()` for recovery
- [ ] Implement idempotent commit (skip already-committed epochs)
- [ ] Integration test: crash-recovery with no duplicates
- [ ] Integration test: rollback discards pending files

### Phase 3: Partitioning and Schema Evolution (2-3 days)

- [ ] Implement time-based partitioning (date/hour columns)
- [ ] Implement `evolve_schema()` for additive column changes
- [ ] Implement type widening detection and schema merge
- [ ] Test: partitioned writes create correct directory structure
- [ ] Test: schema evolution adds nullable columns
- [ ] Test: type widening (Int32 -> Int64) succeeds

### Phase 4: CDC/Upsert Mode (2-3 days)

- [ ] Implement `execute_merge()` for F063 changelog integration
- [ ] Map `CdcOperation` to MERGE WHEN clauses
- [ ] Handle insert, update, delete operations in single merge
- [ ] Test: changelog stream with mixed insert/update/delete
- [ ] Test: upsert mode requires merge key columns
- [ ] Test: CDC envelope format compatibility

### Phase 5: Cloud Storage Backends (1-2 days)

- [ ] Configure S3 storage options (credentials, region, endpoint)
- [ ] Configure Azure ADLS storage options
- [ ] Configure GCS storage options
- [ ] Test: write to S3-compatible storage (MinIO for CI)
- [ ] Test: storage option validation and error messages

### Phase 6: Compaction and Maintenance (2-3 days)

- [ ] Implement `CompactionManager` with background OPTIMIZE
- [ ] Implement Z-ORDER compaction support
- [ ] Implement Delta Lake checkpoint creation
- [ ] Implement `vacuum()` for old file cleanup
- [ ] Test: compaction reduces file count
- [ ] Test: vacuum removes files beyond retention
- [ ] Test: compaction does not block streaming writes

### Phase 7: SQL Integration and Documentation (1-2 days)

- [ ] Register `delta-lake` connector in `ConnectorRegistry`
- [ ] Wire `CREATE SINK ... WITH (connector = 'delta-lake')` to factory
- [ ] Implement `from_connector_config()` with full option parsing
- [ ] SQL integration tests with full DDL flow
- [ ] API documentation with examples

---

## 10. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Ring 0 latency impact | < 5ns per event | Benchmark with/without Delta sink |
| Write throughput | > 200K records/sec | Benchmark with 1KB records to local storage |
| S3 write throughput | > 50K records/sec | Benchmark with 1KB records to S3 |
| Parquet file size | Within 10% of target | Verify file sizes on disk |
| Commit latency (local) | < 50ms | Benchmark commit_epoch() |
| Commit latency (S3) | < 500ms | Benchmark commit_epoch() to S3 |
| Recovery time | < 5s | Benchmark open() with epoch resolution |
| Exactly-once verification | 0 duplicates after crash | Recovery integration test |
| Schema evolution | Additive columns work | Integration test |
| CDC/Upsert correctness | Insert/Update/Delete all handled | Integration test with F063 changelog |
| Test coverage | > 80% | cargo tarpaulin |
| Test count | 25+ tests | cargo test |

---

## 11. Module Structure

```
crates/laminar-connectors/src/
├── lakehouse/
│   ├── mod.rs                # Module declarations, shared types
│   ├── delta.rs              # DeltaLakeSink, DeltaLakeSinkConfig
│   ├── delta_compaction.rs   # CompactionManager, VacuumBuilder
│   ├── delta_merge.rs        # CDC/upsert MERGE operations
│   ├── delta_metrics.rs      # DeltaLakeSinkMetrics
│   └── iceberg.rs            # IcebergSink (F032, future)
```

---

## 12. Dependencies

### Rust Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| `deltalake` | 0.21+ | Delta Lake protocol, transaction log, operations |
| `arrow` | 53+ | RecordBatch, Schema, Parquet integration |
| `parquet` | 53+ | Parquet file reading/writing |
| `object_store` | 0.11+ | Multi-cloud storage abstraction (S3, ADLS, GCS) |
| `tokio` | 1.x | Async runtime for Ring 1 I/O |
| `chrono` | 0.4 | Timestamp handling for partitioning |
| `uuid` | 1.x | Writer ID generation |
| `serde_json` | 1.x | Schema serialization for Delta metadata |
| `tracing` | 0.1 | Structured logging |

### Feature Flags

```toml
[features]
delta-lake = ["deltalake", "object_store"]
delta-lake-s3 = ["delta-lake", "deltalake/s3"]
delta-lake-azure = ["delta-lake", "deltalake/azure"]
delta-lake-gcs = ["delta-lake", "deltalake/gcs"]
```

---

## 13. References

1. **Delta Lake Protocol Specification** - Transaction log format, ACID semantics
   - https://github.com/delta-io/delta/blob/master/PROTOCOL.md

2. **delta-rs (Rust Delta Lake implementation)** - Native Rust Delta Lake client
   - https://github.com/delta-io/delta-rs
   - Used for all Delta Lake operations (read, write, optimize, vacuum)

3. **Apache Arrow Rust** - Zero-copy columnar data and Parquet integration
   - https://github.com/apache/arrow-rs
   - RecordBatch to Parquet writer for zero-copy conversion

4. **Flink Delta Lake Connector** - Exactly-once streaming writes to Delta Lake
   - https://github.com/delta-io/connectors/tree/master/flink
   - Epoch-to-version mapping pattern for exactly-once

5. **F023: Exactly-Once Sinks** - LaminarDB transactional sink framework
   - Provides `SinkConnector` epoch-based commit protocol

6. **F034: Connector SDK** - LaminarDB connector trait hierarchy
   - Defines `SinkConnector`, `ConnectorRuntime`, `ConnectorRegistry`

7. **F063: Changelog/Retraction** - Z-set CDC format for upsert support
   - `CdcOperation`, `ChangelogRecord<T>` types for MERGE operations

8. **F061: Historical Backfill** - Reads FROM Delta Lake tables
   - `DeltaLakeHistoricalSource` for unified live + historical queries

9. **Delta Lake OPTIMIZE and Z-ORDER** - Compaction and data layout optimization
   - https://docs.delta.io/latest/optimizations-oss.html

10. **Databricks Streaming to Delta Lake** - Production patterns for streaming Delta writes
    - Best practices for file size, checkpoint interval, and compaction

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Delta Lake** | Open-source storage layer that brings ACID transactions to data lakes (Parquet files + JSON transaction log) |
| **Transaction Log** | The `_delta_log/` directory containing JSON files that record all changes to the table |
| **Add Action** | A Delta Lake action that adds a new Parquet data file to the table |
| **Remove Action** | A Delta Lake action that marks a Parquet file as deleted (soft delete) |
| **Txn Action** | Application transaction metadata used for exactly-once deduplication |
| **Delta Version** | A monotonically increasing integer identifying each committed transaction |
| **Epoch** | A LaminarDB checkpoint boundary; maps 1:1 to Delta Lake versions for exactly-once |
| **OPTIMIZE** | Delta Lake operation that compacts small files into larger ones (bin-packing) |
| **Z-ORDER** | Multi-dimensional clustering that co-locates related data for faster queries |
| **VACUUM** | Delta Lake operation that deletes old files no longer referenced by any version |
| **Parquet** | Columnar storage format; Delta Lake stores data as Parquet files |
| **RecordBatch** | Arrow columnar data structure; the native data format in LaminarDB's pipeline |
| **SinkConnector** | F034 trait for writing data to external systems with epoch-based transactions |
| **Z-Set** | F063 mathematical model where records carry integer weights (+1 insert, -1 delete) |
| **MERGE** | SQL DML operation that combines INSERT, UPDATE, and DELETE in one statement |
| **OCC** | Optimistic Concurrency Control; Delta Lake's conflict resolution strategy |

---

## Appendix B: Competitive Comparison

| Feature | LaminarDB Delta Sink | Flink Delta Connector | Spark Structured Streaming | Kafka Connect Delta Sink |
|---------|---------------------|-----------------------|---------------------------|-------------------------|
| **Language** | Rust (native) | Java | Scala/Java | Java |
| **Arrow zero-copy** | Yes | No (Java objects) | Partial (Spark internal) | No |
| **Exactly-once** | Yes (epoch-to-version) | Yes (checkpoint-to-version) | Yes (idempotent writes) | At-least-once |
| **CDC/Upsert** | Yes (F063 Z-sets) | Yes (Flink CDC) | Yes (foreachBatch) | Limited |
| **Schema evolution** | Yes (additive) | Limited | Yes (merge schema) | No |
| **Z-ORDER compaction** | Yes (background) | No (manual) | Yes (OPTIMIZE) | No |
| **Embedded deployment** | Yes | No (cluster) | No (cluster) | No (cluster) |
| **Sub-microsecond pipeline** | Yes (Ring 0 unaffected) | No (~10ms) | No (~100ms) | No (~1s) |
| **Multi-cloud** | S3/ADLS/GCS/Local | S3/ADLS/GCS | S3/ADLS/GCS | S3 only |
| **Latency to commit** | 50-500ms | 1-10s | 1-60s | 10-60s |
| **Write throughput** | 200K+ rec/sec | 100K+ rec/sec | 500K+ rec/sec (batch) | 50K+ rec/sec |
| **Background compaction** | Automatic | Manual | Manual/Scheduled | No |
| **Vacuum support** | Built-in | External | Spark VACUUM | No |

### Key Differentiators

1. **Zero-copy Arrow-to-Parquet**: LaminarDB's native Arrow RecordBatch passes directly to the Parquet writer with no serialization overhead. Java-based systems require row-to-columnar conversion.

2. **Embedded + streaming**: LaminarDB's Delta Lake Sink runs as part of a single-binary embedded streaming database. No cluster infrastructure required.

3. **Sub-microsecond pipeline latency**: Because the Delta Lake Sink runs in Ring 1, the Ring 0 hot path remains at sub-microsecond latency even while writing to cloud storage.

4. **Native CDC/upsert via Z-sets**: F063 changelog records with Z-set weights map directly to Delta Lake MERGE operations, providing first-class CDC support without external tooling.

---

## Appendix C: Data Flow Diagram

```
                     LaminarDB Pipeline
                     ==================

  +---------+     +-----------+     +----------+     +-----------+
  | Source  |---->| Operators |---->| Ring 0   |---->|   SPSC    |
  | (Kafka) |     | (window,  |     | Emit     |     |  Channel  |
  |         |     |  join,    |     |          |     |           |
  +---------+     |  agg)     |     +----------+     +-----+-----+
                  +-----------+                            |
                                                           | Subscription
                                                           | (Ring 0 -> Ring 1)
                                                           v
                     +------------------------------------------+
                     |           DeltaLakeSink (Ring 1)         |
                     |                                          |
                     |  +------------------+                    |
                     |  | RecordBatch      |                    |
                     |  | Buffer           |                    |
                     |  +--------+---------+                    |
                     |           |                              |
                     |           | flush (target size reached)  |
                     |           v                              |
                     |  +------------------+                    |
                     |  | Arrow -> Parquet |  (zero-copy)       |
                     |  | Writer           |                    |
                     |  +--------+---------+                    |
                     |           |                              |
                     |           | Add actions staged           |
                     |           v                              |
                     |  +------------------+                    |
                     |  | Delta Lake       |  (commit_epoch)    |
                     |  | Transaction Log  |                    |
                     |  | (_delta_log/)    |                    |
                     |  +--------+---------+                    |
                     |           |                              |
                     |           | Background (async)           |
                     |           v                              |
                     |  +------------------+                    |
                     |  | Compaction       |  (OPTIMIZE)        |
                     |  | Manager          |  (Z-ORDER)         |
                     |  +------------------+                    |
                     +------------------------------------------+
                                 |
                                 v
                     +---------------------+
                     |   Storage Backend   |
                     |  Local / S3 / ADLS  |
                     |  / GCS              |
                     +---------------------+
                         |           |
                    +----+----+ +----+----+
                    | Parquet | |_delta_log|
                    | Files   | | JSON     |
                    +---------+ +----------+
```
