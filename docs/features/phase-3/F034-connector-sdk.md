# F034: Connector SDK

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P1 (Foundation for all Phase 3 connectors)
**Estimated Complexity:** Large
**Prerequisites:** F-STREAM-001 to F-STREAM-007 (Streaming API), F023 (Exactly-Once Sinks), F063 (Changelog/Retraction)

---

## Executive Summary

This specification defines the Connector SDK: a unified framework for building source and sink connectors that bridge external systems (Kafka, PostgreSQL, Delta Lake, etc.) to LaminarDB's in-memory streaming API. The SDK provides traits, runtime integration, serialization/deserialization, offset tracking, health monitoring, and schema discovery while maintaining LaminarDB's three-ring architecture and sub-500ns hot path guarantees.

---

## 1. Problem Statement

### Current Limitation

LaminarDB's Phase 3 streaming API (F-STREAM-001 to F-STREAM-007) provides high-performance in-memory channels between sources and sinks. However, there is no standardized way to:
- Connect external systems (Kafka, databases, file systems) to these channels
- Handle serialization/deserialization of external data formats
- Track offsets and provide exactly-once guarantees across system boundaries
- Discover schemas from external systems
- Monitor connector health and metrics

The `laminar-connectors` crate currently contains only:
- `lookup.rs`: `TableLoader` trait + `InMemoryTableLoader` (for lookup joins)
- `cdc.rs`: Empty placeholder module
- `lib.rs`: Feature-gated `kafka` module (not yet implemented)

### Required Capability

A Connector SDK that:
1. Defines standard traits for source and sink connectors
2. Provides a runtime that bridges connectors to the streaming API
3. Handles serialization/deserialization (JSON, Avro, Protobuf, raw bytes)
4. Integrates with the SQL DDL layer (`CREATE SOURCE/SINK WITH (connector = '...')`)
5. Supports exactly-once semantics via offset tracking and transactional commits
6. Provides testing utilities for connector developers

### Example Use Case

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Connector Architecture                        │
│                                                                      │
│  External Systems          Connector SDK           Streaming API     │
│  ┌─────────┐          ┌──────────────────┐     ┌───────────────┐    │
│  │  Kafka  │──────────│ SourceConnector  │────▶│  Source<T>    │    │
│  └─────────┘          │  + Deserializer  │     └───────────────┘    │
│                        │  + OffsetTracker │                          │
│  ┌─────────┐          └──────────────────┘                          │
│  │PostgreSQL│─────────│ CdcConnector     │────▶│  Source<T>    │    │
│  └─────────┘          └──────────────────┘     └───────────────┘    │
│                                                                      │
│  ┌─────────┐          ┌──────────────────┐     ┌───────────────┐    │
│  │Delta Lake│◀────────│ SinkConnector    │◀────│  Sink<T>     │    │
│  └─────────┘          │  + Serializer    │     └───────────────┘    │
│                        │  + TxnCoordinator│                          │
│                        └──────────────────┘                          │
│                                                                      │
│  Ring 2 (Control)    Ring 1 (Background I/O)   Ring 0 (Hot Path)    │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Design Principles

### 2.1 Core Principles (Aligned with LaminarDB Philosophy)

| Principle | Application |
|-----------|-------------|
| **Three-ring separation** | Connectors run in Ring 1 (async I/O), never touch Ring 0 |
| **Zero-allocation hot path** | Connector I/O is decoupled from Ring 0 via SPSC channels |
| **No custom types unless benchmarks demand** | Reuse existing `Source<T>`, `Sink<T>`, `ExactlyOnceSink` |
| **Automatic complexity derivation** | Connector type resolved from `WITH (connector = '...')` clause |
| **SQL-first** | All connectors configurable via `CREATE SOURCE/SINK` DDL |

### 2.2 Industry-Informed Patterns (2025-2026 Research)

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **Connector Framework** | Kafka Connect, Flink Connectors | Trait-based SDK with runtime bridge |
| **Schema Registry Integration** | Confluent Schema Registry | Optional schema discovery + validation |
| **Exactly-Once Source** | Flink Source API v2 | Offset checkpointing with epoch alignment |
| **Two-Phase Sink** | Flink TwoPhaseCommitSinkFunction | Extends existing F023 `ExactlyOnceSink` |
| **Pluggable Serialization** | Debezium, Flink Formats | `RecordDeserializer` / `RecordSerializer` traits |

---

## 3. Architecture

### 3.1 Connector Trait Hierarchy

```rust
/// Lifecycle state for connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    /// Created but not started.
    Created,
    /// Running and processing data.
    Running,
    /// Temporarily paused (e.g., backpressure).
    Paused,
    /// Stopped gracefully.
    Stopped,
    /// Failed with error.
    Failed,
}

/// Common connector metadata.
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    /// Unique connector type identifier (e.g., "kafka", "postgres-cdc").
    pub connector_type: String,
    /// Human-readable description.
    pub description: String,
    /// Supported configuration keys with descriptions.
    pub config_keys: Vec<ConfigKeySpec>,
    /// Supported serialization formats.
    pub supported_formats: Vec<String>,
}

/// Specification for a configuration key.
#[derive(Debug, Clone)]
pub struct ConfigKeySpec {
    /// Key name (e.g., "bootstrap.servers").
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Whether this key is required.
    pub required: bool,
    /// Default value, if any.
    pub default: Option<String>,
    /// Example value.
    pub example: Option<String>,
}
```

### 3.2 Source Connector Trait

```rust
/// Trait for source connectors that read from external systems.
///
/// Source connectors run in Ring 1 (background I/O) and push data
/// into Ring 0 via the streaming `Source<T>` API.
///
/// # Lifecycle
///
/// ```text
/// new() → open() → [poll_batch() loop] → close()
///                        ↑
///                  commit_offsets()
///                  (on checkpoint)
/// ```
///
/// # Thread Safety
///
/// Source connectors are `Send` but not required to be `Sync`.
/// Each connector instance runs on a single tokio task in Ring 1.
#[async_trait]
pub trait SourceConnector: Send {
    /// Returns metadata about this connector.
    fn info(&self) -> ConnectorInfo;

    /// Returns the current connector state.
    fn state(&self) -> ConnectorState;

    /// Opens the connector and prepares to read data.
    ///
    /// Called once before the first `poll_batch()`. Should establish
    /// connections, create consumer groups, etc.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Polls for the next batch of records from the external system.
    ///
    /// Returns `None` when no data is currently available (the runtime
    /// will call again after a configurable poll interval).
    ///
    /// Records are returned as Arrow `RecordBatch` for zero-copy
    /// integration with the streaming API.
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError>;

    /// Commits offsets up to the given checkpoint.
    ///
    /// Called by the runtime when a checkpoint completes successfully.
    /// The connector should persist offsets so that on recovery,
    /// `open()` can resume from the committed position.
    async fn commit_offsets(
        &mut self,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError>;

    /// Returns the current uncommitted offsets.
    ///
    /// Used by the checkpoint coordinator to include source offsets
    /// in the checkpoint metadata.
    fn current_offsets(&self) -> SourceCheckpoint;

    /// Discovers the schema of the external data source.
    ///
    /// Returns `None` if schema discovery is not supported
    /// (e.g., raw byte streams).
    async fn discover_schema(&self) -> Result<Option<SchemaRef>, ConnectorError>;

    /// Checks if the connector is healthy.
    async fn health_check(&self) -> Result<HealthStatus, ConnectorError>;

    /// Closes the connector and releases resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns connector-specific metrics.
    fn metrics(&self) -> ConnectorMetrics;
}

/// A batch of records from a source connector.
#[derive(Debug)]
pub struct SourceBatch {
    /// The data records as Arrow RecordBatch.
    pub records: RecordBatch,
    /// Offsets for each record (connector-specific).
    pub offsets: SourceCheckpoint,
    /// Event timestamps extracted from data (if available).
    pub event_times: Option<Vec<i64>>,
    /// Source partition information.
    pub partition: Option<PartitionInfo>,
}

/// Connector-specific offset information for checkpointing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    /// Connector-specific offset data.
    ///
    /// For Kafka: `{ "topic-partition": offset }`
    /// For PostgreSQL CDC: `{ "lsn": "0/1234567" }`
    /// For file sources: `{ "file": path, "offset": byte_offset }`
    pub offsets: HashMap<String, String>,
    /// Epoch when this checkpoint was created.
    pub epoch: u64,
    /// Timestamp when this checkpoint was created.
    pub timestamp: i64,
}

/// Health status of a connector.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the connector is healthy.
    pub healthy: bool,
    /// Human-readable status message.
    pub message: String,
    /// Time since last successful operation.
    pub last_success: Option<Duration>,
    /// Connection details.
    pub details: HashMap<String, String>,
}

/// Partition information for multi-partition sources.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition identifier.
    pub partition_id: String,
    /// Total number of partitions.
    pub total_partitions: usize,
}
```

### 3.3 Sink Connector Trait

```rust
/// Trait for sink connectors that write to external systems.
///
/// Sink connectors run in Ring 1 and receive data from the streaming
/// `Subscription<T>` API. They integrate with the exactly-once
/// framework (F023) for transactional writes.
///
/// # Lifecycle
///
/// ```text
/// new() → open() → [write_batch() loop] → close()
///                        ↑
///                  begin_epoch() / commit_epoch()
///                  (on checkpoint)
/// ```
#[async_trait]
pub trait SinkConnector: Send {
    /// Returns metadata about this connector.
    fn info(&self) -> ConnectorInfo;

    /// Returns the current connector state.
    fn state(&self) -> ConnectorState;

    /// Opens the connector and prepares to write data.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Writes a batch of records to the external system.
    ///
    /// Records are buffered internally until `commit_epoch()` is called.
    /// If the sink supports transactions (e.g., Kafka), records are
    /// written within the current transaction.
    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<WriteResult, ConnectorError>;

    /// Begins a new epoch (checkpoint boundary).
    ///
    /// For transactional sinks, this starts a new transaction.
    /// For idempotent sinks, this updates the epoch marker.
    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;

    /// Commits the current epoch.
    ///
    /// For transactional sinks, this commits the transaction.
    /// For idempotent sinks, this persists the epoch marker.
    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;

    /// Rolls back the current epoch.
    ///
    /// Called on checkpoint failure or recovery.
    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;

    /// Returns the last successfully committed epoch.
    fn last_committed_epoch(&self) -> u64;

    /// Checks if the connector is healthy.
    async fn health_check(&self) -> Result<HealthStatus, ConnectorError>;

    /// Closes the connector and releases resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns connector-specific metrics.
    fn metrics(&self) -> ConnectorMetrics;

    /// Returns the capabilities of this sink.
    fn capabilities(&self) -> SinkCapabilities;
}

/// Result of a write operation.
#[derive(Debug)]
pub struct WriteResult {
    /// Number of records written.
    pub records_written: usize,
    /// Bytes written.
    pub bytes_written: u64,
}

/// Capabilities of a sink connector.
#[derive(Debug, Clone, Default)]
pub struct SinkConnectorCapabilities {
    /// Supports transactions (begin/commit/rollback).
    pub transactional: bool,
    /// Supports idempotent writes (upsert by key).
    pub idempotent: bool,
    /// Supports partitioned writes.
    pub partitioned: bool,
    /// Supports schema evolution.
    pub schema_evolution: bool,
    /// Supports exactly-once via 2PC.
    pub two_phase_commit: bool,
}
```

### 3.4 Serialization Framework

```rust
/// Trait for deserializing external data to Arrow RecordBatch.
///
/// Deserializers convert raw bytes from external systems into
/// Arrow columnar format for processing in the streaming engine.
pub trait RecordDeserializer: Send + Sync {
    /// Returns the format name (e.g., "json", "avro", "protobuf").
    fn format(&self) -> &str;

    /// Deserializes raw bytes into an Arrow RecordBatch.
    ///
    /// # Arguments
    ///
    /// * `data` - Raw bytes from the external system
    /// * `schema` - Expected Arrow schema for validation
    ///
    /// # Returns
    ///
    /// A RecordBatch containing the deserialized records.
    fn deserialize(
        &self,
        data: &[u8],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError>;

    /// Deserializes a batch of raw byte records.
    ///
    /// More efficient than calling `deserialize()` per record
    /// because it can batch Arrow array construction.
    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError>;
}

/// Trait for serializing Arrow RecordBatch to external formats.
pub trait RecordSerializer: Send + Sync {
    /// Returns the format name (e.g., "json", "avro", "protobuf").
    fn format(&self) -> &str;

    /// Serializes a RecordBatch into raw bytes.
    ///
    /// Returns one byte vector per row in the batch.
    fn serialize(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<Vec<u8>>, SerdeError>;

    /// Serializes a RecordBatch into a single byte vector.
    ///
    /// Used for formats that support batch encoding (e.g., Avro container).
    fn serialize_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<u8>, SerdeError>;
}

/// Built-in serialization format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// JSON (one JSON object per record).
    Json,
    /// JSON with schema (includes field names).
    JsonSchema,
    /// Apache Avro with schema registry.
    Avro,
    /// Protocol Buffers.
    Protobuf,
    /// Raw bytes (pass-through).
    RawBytes,
    /// CSV format.
    Csv,
    /// Debezium CDC envelope format.
    DebeziumJson,
}

/// JSON deserializer implementation.
pub struct JsonDeserializer {
    /// Whether to use strict type checking.
    strict: bool,
    /// Timestamp format for parsing.
    timestamp_format: TimestampFormat,
}

/// Debezium CDC envelope deserializer.
///
/// Handles the standard Debezium CDC format:
/// ```json
/// {
///   "before": { ... },
///   "after": { ... },
///   "op": "c|u|d|r",
///   "ts_ms": 1234567890,
///   "source": { "connector": "postgresql", ... }
/// }
/// ```
pub struct DebeziumDeserializer {
    /// Whether to include before images for updates.
    include_before: bool,
}
```

### 3.5 Connector Runtime

```rust
/// The Connector Runtime bridges external connectors to LaminarDB's
/// streaming API. It manages connector lifecycle, threading, and
/// data flow between Ring 1 (I/O) and Ring 0 (processing).
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │                     ConnectorRuntime                           │
/// │                                                                │
/// │  ┌──────────────┐    SPSC Channel    ┌───────────────────┐    │
/// │  │SourceRunner  │──────────────────▶│ Source<ArrowRecord>│    │
/// │  │ (Ring 1 task) │   RecordBatch     │   (Ring 0)        │    │
/// │  │              │                    │                   │    │
/// │  │ poll_batch() │                    │                   │    │
/// │  │ deserialize()│                    │                   │    │
/// │  │ watermark()  │                    │                   │    │
/// │  └──────────────┘                    └───────────────────┘    │
/// │                                                                │
/// │  ┌──────────────┐    Subscription    ┌───────────────────┐    │
/// │  │ SinkRunner   │◀──────────────────│ Sink<ArrowRecord> │    │
/// │  │ (Ring 1 task) │   RecordBatch     │   (Ring 0)        │    │
/// │  │              │                    │                   │    │
/// │  │ serialize()  │                    │                   │    │
/// │  │ write_batch()│                    │                   │    │
/// │  │ commit()     │                    │                   │    │
/// │  └──────────────┘                    └───────────────────┘    │
/// └────────────────────────────────────────────────────────────────┘
/// ```
pub struct ConnectorRuntime {
    /// Registry of available connector factories.
    registry: ConnectorRegistry,
    /// Active source runners.
    source_runners: HashMap<String, SourceRunnerHandle>,
    /// Active sink runners.
    sink_runners: HashMap<String, SinkRunnerHandle>,
    /// Checkpoint coordinator reference.
    checkpoint_coordinator: Option<Arc<dyn CheckpointCoordinator>>,
    /// Runtime configuration.
    config: RuntimeConfig,
    /// Metrics collector.
    metrics: Arc<RuntimeMetrics>,
}

impl ConnectorRuntime {
    /// Creates a new connector runtime with default configuration.
    pub fn new() -> Self;

    /// Creates a runtime with custom configuration.
    pub fn with_config(config: RuntimeConfig) -> Self;

    /// Starts a source connector and returns the streaming Source handle.
    ///
    /// The connector runs as a tokio task in Ring 1, polling for data
    /// and pushing records into the returned `Source<ArrowRecord>`.
    pub async fn start_source(
        &mut self,
        name: &str,
        connector: Box<dyn SourceConnector>,
        deserializer: Box<dyn RecordDeserializer>,
        config: ConnectorConfig,
    ) -> Result<SourceHandle, ConnectorError>;

    /// Starts a sink connector and returns the streaming Sink handle.
    ///
    /// The connector runs as a tokio task in Ring 1, consuming records
    /// from the streaming `Subscription` and writing to the external system.
    pub async fn start_sink(
        &mut self,
        name: &str,
        connector: Box<dyn SinkConnector>,
        serializer: Box<dyn RecordSerializer>,
        config: ConnectorConfig,
    ) -> Result<SinkHandle, ConnectorError>;

    /// Stops a running source connector.
    pub async fn stop_source(&mut self, name: &str) -> Result<(), ConnectorError>;

    /// Stops a running sink connector.
    pub async fn stop_sink(&mut self, name: &str) -> Result<(), ConnectorError>;

    /// Triggers a checkpoint across all connectors.
    pub async fn checkpoint(&self, epoch: u64) -> Result<RuntimeCheckpoint, ConnectorError>;

    /// Restores all connectors from a checkpoint.
    pub async fn restore(
        &mut self,
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<(), ConnectorError>;

    /// Returns health status for all connectors.
    pub async fn health(&self) -> HashMap<String, HealthStatus>;

    /// Returns aggregate metrics.
    pub fn metrics(&self) -> &RuntimeMetrics;
}

/// Handle to a running source connector.
pub struct SourceHandle {
    /// The streaming Source for pushing data into Ring 0.
    pub source: Source<ArrowRecord>,
    /// The streaming Sink for subscribing to data.
    pub sink: streaming::Sink<ArrowRecord>,
    /// Schema of the source data.
    pub schema: SchemaRef,
    /// Connector name.
    pub name: String,
    /// Handle to the background task.
    task_handle: JoinHandle<Result<(), ConnectorError>>,
    /// Shutdown signal.
    shutdown: tokio::sync::watch::Sender<bool>,
}

/// Handle to a running sink connector.
pub struct SinkHandle {
    /// Connector name.
    pub name: String,
    /// Handle to the background task.
    task_handle: JoinHandle<Result<(), ConnectorError>>,
    /// Shutdown signal.
    shutdown: tokio::sync::watch::Sender<bool>,
}
```

### 3.6 Connector Registry

```rust
/// Factory function type for creating source connectors.
pub type SourceConnectorFactory =
    Box<dyn Fn(&ConnectorConfig) -> Result<Box<dyn SourceConnector>, ConnectorError> + Send + Sync>;

/// Factory function type for creating sink connectors.
pub type SinkConnectorFactory =
    Box<dyn Fn(&ConnectorConfig) -> Result<Box<dyn SinkConnector>, ConnectorError> + Send + Sync>;

/// Registry of available connector types.
///
/// The registry maps connector type names (from SQL `WITH (connector = '...')`)
/// to factory functions that create connector instances.
pub struct ConnectorRegistry {
    /// Source connector factories.
    sources: HashMap<String, SourceConnectorFactory>,
    /// Sink connector factories.
    sinks: HashMap<String, SinkConnectorFactory>,
    /// Deserializer factories by format name.
    deserializers: HashMap<String, Box<dyn Fn() -> Box<dyn RecordDeserializer> + Send + Sync>>,
    /// Serializer factories by format name.
    serializers: HashMap<String, Box<dyn Fn() -> Box<dyn RecordSerializer> + Send + Sync>>,
}

impl ConnectorRegistry {
    /// Creates a new registry with built-in connectors.
    pub fn new() -> Self {
        let mut registry = Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
            deserializers: HashMap::new(),
            serializers: HashMap::new(),
        };

        // Register built-in serializers
        registry.register_deserializer("json", || Box::new(JsonDeserializer::default()));
        registry.register_serializer("json", || Box::new(JsonSerializer::default()));
        registry.register_deserializer("debezium-json",
            || Box::new(DebeziumDeserializer::default()));
        registry.register_deserializer("csv", || Box::new(CsvDeserializer::default()));
        registry.register_serializer("csv", || Box::new(CsvSerializer::default()));

        registry
    }

    /// Registers a source connector factory.
    pub fn register_source(
        &mut self,
        connector_type: &str,
        factory: SourceConnectorFactory,
    );

    /// Registers a sink connector factory.
    pub fn register_sink(
        &mut self,
        connector_type: &str,
        factory: SinkConnectorFactory,
    );

    /// Registers a deserializer factory.
    pub fn register_deserializer<F>(
        &mut self,
        format: &str,
        factory: F,
    ) where F: Fn() -> Box<dyn RecordDeserializer> + Send + Sync + 'static;

    /// Registers a serializer factory.
    pub fn register_serializer<F>(
        &mut self,
        format: &str,
        factory: F,
    ) where F: Fn() -> Box<dyn RecordSerializer> + Send + Sync + 'static;

    /// Creates a source connector from a SQL `SourceDefinition`.
    pub fn create_source(
        &self,
        definition: &SourceDefinition,
    ) -> Result<(Box<dyn SourceConnector>, Box<dyn RecordDeserializer>), ConnectorError>;

    /// Creates a sink connector from a SQL `SinkDefinition`.
    pub fn create_sink(
        &self,
        definition: &SinkDefinition,
    ) -> Result<(Box<dyn SinkConnector>, Box<dyn RecordSerializer>), ConnectorError>;

    /// Lists all registered connector types.
    pub fn list_connectors(&self) -> Vec<ConnectorInfo>;
}
```

### 3.7 Three-Ring Integration

| Ring | Connector Responsibilities |
|------|---------------------------|
| **Ring 0 (Hot Path)** | Consume data from SPSC channels, process through operators, emit to sink channels. No connector code runs here. |
| **Ring 1 (Background)** | `SourceRunner` polls external systems, deserializes, pushes to channels. `SinkRunner` receives from subscriptions, serializes, writes to external systems. Checkpoint coordination. |
| **Ring 2 (Control Plane)** | Connector lifecycle management (start/stop/restart). Health monitoring. Schema discovery. Configuration changes. SQL DDL processing. |

---

## 4. SQL Integration

### 4.1 DDL Resolution

The Connector SDK integrates with the existing DDL translator (`laminar-sql/src/translator/streaming_ddl.rs`) by extending `SourceConfigOptions` to include connector-specific settings:

```rust
/// Extended source configuration with connector resolution.
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    /// Connector type (e.g., "kafka", "postgres-cdc", "delta-lake").
    pub connector_type: String,
    /// Data format (e.g., "json", "avro", "debezium-json").
    pub format: String,
    /// All key-value options from the WITH clause.
    pub options: HashMap<String, String>,
    /// Resolved Arrow schema.
    pub schema: Option<SchemaRef>,
    /// Watermark specification.
    pub watermark: Option<WatermarkSpec>,
    /// Channel configuration (buffer size, backpressure).
    pub channel: SourceConfigOptions,
}

impl ConnectorConfig {
    /// Creates a ConnectorConfig from SQL WITH clause options.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self, ConnectorError> {
        let connector_type = options
            .get("connector")
            .ok_or_else(|| ConnectorError::Config(
                "Missing required option 'connector'".into(),
            ))?
            .clone();

        let format = options
            .get("format")
            .cloned()
            .unwrap_or_else(|| "json".to_string());

        Ok(Self {
            connector_type,
            format,
            options: options.clone(),
            schema: None,
            watermark: None,
            channel: SourceConfigOptions::default(),
        })
    }

    /// Gets a typed option value.
    pub fn get<T: FromStr>(&self, key: &str) -> Option<T> {
        self.options.get(key).and_then(|v| v.parse().ok())
    }

    /// Gets a required option value.
    pub fn require(&self, key: &str) -> Result<&str, ConnectorError> {
        self.options
            .get(key)
            .map(|s| s.as_str())
            .ok_or_else(|| ConnectorError::Config(
                format!("Missing required option '{key}'"),
            ))
    }
}
```

### 4.2 SQL Syntax

```sql
-- Source with connector resolution
CREATE SOURCE TABLE orders (
    order_id BIGINT,
    customer_id VARCHAR,
    amount DECIMAL(10, 2),
    event_time TIMESTAMP,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',           -- Resolved via ConnectorRegistry
    topic = 'orders',
    'bootstrap.servers' = 'broker:9092',
    'group.id' = 'laminardb-orders',
    format = 'json',               -- Resolved via DeserializerRegistry
    'auto.offset.reset' = 'earliest'
);

-- Sink with connector resolution
CREATE SINK TABLE order_results
FROM processed_orders
WITH (
    connector = 'kafka',
    topic = 'order-results',
    'bootstrap.servers' = 'broker:9092',
    format = 'json',
    'delivery.guarantee' = 'exactly-once',
    'transaction.timeout.ms' = '60000'
);

-- CDC source
CREATE SOURCE TABLE users_cdc (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    _op VARCHAR,
    _ts TIMESTAMP
) WITH (
    connector = 'postgres-cdc',
    hostname = 'localhost',
    port = '5432',
    database = 'mydb',
    'schema.name' = 'public',
    'table.name' = 'users',
    'slot.name' = 'laminardb_slot',
    format = 'debezium-json'
);

-- Discover schema from external system
DESCRIBE CONNECTOR 'kafka' WITH (
    topic = 'orders',
    'bootstrap.servers' = 'broker:9092',
    format = 'avro',
    'schema.registry.url' = 'http://registry:8081'
);
```

---

## 5. Source Runner Architecture

### 5.1 Source Runner Loop

```rust
/// Background task that bridges a SourceConnector to a streaming Source.
struct SourceRunner {
    /// The source connector.
    connector: Box<dyn SourceConnector>,
    /// Deserializer for converting raw data to RecordBatch.
    deserializer: Box<dyn RecordDeserializer>,
    /// Streaming source for pushing data into Ring 0.
    source: Source<ArrowRecord>,
    /// Configuration.
    config: SourceRunnerConfig,
    /// Shutdown signal receiver.
    shutdown: tokio::sync::watch::Receiver<bool>,
    /// Metrics.
    metrics: Arc<SourceRunnerMetrics>,
}

impl SourceRunner {
    async fn run(&mut self) -> Result<(), ConnectorError> {
        self.connector.open(&self.config.connector_config).await?;

        loop {
            // Check shutdown
            if *self.shutdown.borrow() {
                break;
            }

            // Poll for data
            match self.connector.poll_batch(self.config.max_poll_records).await {
                Ok(Some(batch)) => {
                    // Push records into streaming source
                    self.source.push_arrow(batch.records)?;

                    // Emit watermarks from event times
                    if let Some(event_times) = &batch.event_times {
                        if let Some(&max_ts) = event_times.iter().max() {
                            let watermark = max_ts - self.config.max_out_of_orderness_ms;
                            self.source.watermark(watermark);
                        }
                    }

                    // Update metrics
                    self.metrics.records_polled.fetch_add(
                        batch.records.num_rows() as u64,
                        Ordering::Relaxed,
                    );
                }
                Ok(None) => {
                    // No data available, wait before polling again
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(e) => {
                    tracing::error!(connector = %self.config.name, error = %e,
                        "Source connector poll error");
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);

                    // Exponential backoff on errors
                    tokio::time::sleep(self.config.error_backoff()).await;
                }
            }
        }

        self.connector.close().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SourceRunnerConfig {
    /// Connector name.
    pub name: String,
    /// Maximum records per poll.
    pub max_poll_records: usize,
    /// Poll interval when no data available.
    pub poll_interval: Duration,
    /// Maximum out-of-orderness for watermarks.
    pub max_out_of_orderness_ms: i64,
    /// Connector-specific config.
    pub connector_config: ConnectorConfig,
    /// Maximum consecutive errors before stopping.
    pub max_consecutive_errors: usize,
    /// Base backoff duration for errors.
    pub error_backoff_base: Duration,
    /// Maximum backoff duration.
    pub error_backoff_max: Duration,
}

impl Default for SourceRunnerConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            max_poll_records: 1000,
            poll_interval: Duration::from_millis(10),
            max_out_of_orderness_ms: 5000,
            connector_config: ConnectorConfig::default(),
            max_consecutive_errors: 10,
            error_backoff_base: Duration::from_millis(100),
            error_backoff_max: Duration::from_secs(30),
        }
    }
}
```

### 5.2 Sink Runner Loop

```rust
/// Background task that bridges a streaming Subscription to a SinkConnector.
struct SinkRunner {
    /// The sink connector.
    connector: Box<dyn SinkConnector>,
    /// Serializer for converting RecordBatch to external format.
    serializer: Box<dyn RecordSerializer>,
    /// Subscription for receiving data from Ring 0.
    subscription: Subscription<ArrowRecord>,
    /// Configuration.
    config: SinkRunnerConfig,
    /// Shutdown signal receiver.
    shutdown: tokio::sync::watch::Receiver<bool>,
    /// Checkpoint signal receiver.
    checkpoint_rx: tokio::sync::mpsc::Receiver<CheckpointCommand>,
    /// Metrics.
    metrics: Arc<SinkRunnerMetrics>,
}

impl SinkRunner {
    async fn run(&mut self) -> Result<(), ConnectorError> {
        self.connector.open(&self.config.connector_config).await?;

        // Begin first epoch
        let mut current_epoch = self.connector.last_committed_epoch() + 1;
        self.connector.begin_epoch(current_epoch).await?;

        let mut batch_buffer: Vec<RecordBatch> = Vec::new();

        loop {
            tokio::select! {
                // Check shutdown
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow() {
                        // Commit current epoch before shutdown
                        self.flush_and_commit(&mut batch_buffer, current_epoch).await?;
                        break;
                    }
                }

                // Handle checkpoint commands
                Some(cmd) = self.checkpoint_rx.recv() => {
                    match cmd {
                        CheckpointCommand::Commit(epoch) => {
                            self.flush_and_commit(&mut batch_buffer, epoch).await?;
                            current_epoch = epoch + 1;
                            self.connector.begin_epoch(current_epoch).await?;
                        }
                        CheckpointCommand::Rollback(epoch) => {
                            batch_buffer.clear();
                            self.connector.rollback_epoch(epoch).await?;
                            current_epoch = epoch;
                            self.connector.begin_epoch(current_epoch).await?;
                        }
                    }
                }

                // Poll for records from streaming subscription
                record = self.poll_subscription() => {
                    if let Some(record) = record {
                        batch_buffer.push(record.to_record_batch());

                        // Flush when buffer is full
                        if batch_buffer.len() >= self.config.flush_batch_size {
                            self.flush_buffer(&mut batch_buffer).await?;
                        }
                    }
                }
            }
        }

        self.connector.close().await?;
        Ok(())
    }

    async fn flush_buffer(
        &mut self,
        buffer: &mut Vec<RecordBatch>,
    ) -> Result<(), ConnectorError> {
        for batch in buffer.drain(..) {
            self.connector.write_batch(&batch).await?;
            self.metrics.records_written.fetch_add(
                batch.num_rows() as u64,
                Ordering::Relaxed,
            );
        }
        Ok(())
    }

    async fn flush_and_commit(
        &mut self,
        buffer: &mut Vec<RecordBatch>,
        epoch: u64,
    ) -> Result<(), ConnectorError> {
        self.flush_buffer(buffer).await?;
        self.connector.commit_epoch(epoch).await?;
        self.metrics.epochs_committed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
```

---

## 6. Error Handling

```rust
/// Errors that can occur in the connector framework.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Configuration error (missing/invalid options).
    #[error("Configuration error: {0}")]
    Config(String),

    /// Connection to external system failed.
    #[error("Connection failed: {0}")]
    Connection(String),

    /// Authentication/authorization failure.
    #[error("Authentication failed: {0}")]
    Auth(String),

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serde(#[from] SerdeError),

    /// I/O error from external system.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Schema mismatch between expected and actual.
    #[error("Schema mismatch: expected {expected}, got {actual}")]
    SchemaMismatch {
        expected: String,
        actual: String,
    },

    /// Connector is not in the right state for the operation.
    #[error("Invalid state: connector is {state:?}, expected {expected:?}")]
    InvalidState {
        state: ConnectorState,
        expected: ConnectorState,
    },

    /// Transaction/commit failure.
    #[error("Transaction error: {0}")]
    Transaction(String),

    /// Timeout waiting for external system.
    #[error("Timeout after {0:?}")]
    Timeout(Duration),

    /// Connector-specific error.
    #[error("Connector error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// Serialization/deserialization errors.
#[derive(Debug, thiserror::Error)]
pub enum SerdeError {
    /// JSON parsing error.
    #[error("JSON error: {0}")]
    Json(String),

    /// Avro encoding/decoding error.
    #[error("Avro error: {0}")]
    Avro(String),

    /// Type conversion error.
    #[error("Type conversion: cannot convert {from} to {to}")]
    TypeConversion {
        from: String,
        to: String,
    },

    /// Missing required field.
    #[error("Missing field: {0}")]
    MissingField(String),
}
```

---

## 7. Metrics and Observability

```rust
/// Metrics for a source connector.
#[derive(Debug, Default)]
pub struct ConnectorMetrics {
    /// Total records processed.
    pub records_total: u64,
    /// Total bytes processed.
    pub bytes_total: u64,
    /// Total errors encountered.
    pub errors_total: u64,
    /// Current records per second.
    pub records_per_sec: f64,
    /// Average latency per batch (microseconds).
    pub avg_batch_latency_us: u64,
    /// P99 latency per batch (microseconds).
    pub p99_batch_latency_us: u64,
    /// Current lag (source only, connector-specific).
    pub lag: Option<u64>,
    /// Connector-specific metrics.
    pub custom: HashMap<String, f64>,
}

/// Aggregate runtime metrics.
#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    /// Total active source connectors.
    pub active_sources: AtomicU64,
    /// Total active sink connectors.
    pub active_sinks: AtomicU64,
    /// Total records ingested across all sources.
    pub total_records_ingested: AtomicU64,
    /// Total records written across all sinks.
    pub total_records_written: AtomicU64,
    /// Total connector errors.
    pub total_errors: AtomicU64,
    /// Total checkpoints completed.
    pub total_checkpoints: AtomicU64,
}
```

---

## 8. Testing Utilities

```rust
/// Test harness for connector developers.
///
/// Provides mock infrastructure for testing connectors without
/// requiring actual external systems.
pub mod testing {
    /// A mock source connector for testing.
    pub struct MockSourceConnector {
        /// Pre-configured batches to return.
        batches: VecDeque<SourceBatch>,
        /// Whether to simulate errors.
        error_after: Option<usize>,
        /// Committed offsets.
        committed: Vec<SourceCheckpoint>,
        /// State.
        state: ConnectorState,
    }

    impl MockSourceConnector {
        /// Creates a mock source with pre-configured data.
        pub fn with_data(batches: Vec<SourceBatch>) -> Self;

        /// Configures the mock to fail after N successful polls.
        pub fn fail_after(mut self, n: usize) -> Self;

        /// Returns all committed checkpoints.
        pub fn committed_checkpoints(&self) -> &[SourceCheckpoint];
    }

    /// A mock sink connector for testing.
    pub struct MockSinkConnector {
        /// Captured write batches.
        written: Vec<RecordBatch>,
        /// Committed epochs.
        committed_epochs: Vec<u64>,
        /// Whether to simulate commit failures.
        fail_commits: bool,
        /// State.
        state: ConnectorState,
    }

    impl MockSinkConnector {
        /// Creates a new mock sink.
        pub fn new() -> Self;

        /// Returns all written batches.
        pub fn written_batches(&self) -> &[RecordBatch];

        /// Returns all committed epochs.
        pub fn committed_epochs(&self) -> &[u64];
    }

    /// Runs a connector integration test with a mock runtime.
    pub async fn test_source_connector(
        connector: Box<dyn SourceConnector>,
        config: ConnectorConfig,
        expected_records: usize,
    ) -> Result<Vec<RecordBatch>, ConnectorError>;

    /// Runs a sink connector integration test.
    pub async fn test_sink_connector(
        connector: Box<dyn SinkConnector>,
        config: ConnectorConfig,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<u64>, ConnectorError>;
}
```

---

## 9. Configuration

```rust
/// Runtime configuration for the connector framework.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Default buffer size for source channels.
    pub default_buffer_size: usize,
    /// Default backpressure strategy.
    pub default_backpressure: BackpressureStrategy,
    /// Health check interval.
    pub health_check_interval: Duration,
    /// Maximum number of concurrent connectors.
    pub max_connectors: usize,
    /// Metrics reporting interval.
    pub metrics_interval: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            health_check_interval: Duration::from_secs(30),
            max_connectors: 64,
            metrics_interval: Duration::from_secs(10),
        }
    }
}
```

---

## 10. Sub-500ns Latency Considerations

### 10.1 Hot Path Budget

Connectors never run on the hot path. All connector I/O is decoupled from Ring 0 via SPSC channels:

| Component | Ring | Latency | Notes |
|-----------|------|---------|-------|
| SPSC channel push (Source→Ring 0) | Ring 0 | ~5ns | Lock-free, existing implementation |
| Subscription poll (Ring 0→Sink) | Ring 0 | ~5ns | Lock-free, existing implementation |
| Connector poll_batch() | Ring 1 | 1-100ms | External I/O, async |
| Deserialization | Ring 1 | 1-50μs | JSON/Avro parsing |
| Serialization | Ring 1 | 1-50μs | JSON/Avro encoding |
| External write | Ring 1 | 1-100ms | Network I/O |
| **Ring 0 impact** | **Ring 0** | **~10ns** | **Channel operations only** |

### 10.2 What Stays OFF the Hot Path

- All connector I/O (poll, write, commit)
- Serialization/deserialization
- Offset tracking and checkpointing
- Health checks and metrics collection
- Schema discovery and validation
- Connection management

---

## 11. Implementation Roadmap

### Phase 1: Core Traits and Types (3-4 days)
- [ ] Define `SourceConnector` trait
- [ ] Define `SinkConnector` trait
- [ ] Define `RecordDeserializer` / `RecordSerializer` traits
- [ ] Define `ConnectorConfig`, `SourceCheckpoint`, `HealthStatus`
- [ ] Define `ConnectorError` and `SerdeError`
- [ ] Unit tests for configuration parsing

### Phase 2: Serialization Framework (3-4 days)
- [ ] Implement `JsonDeserializer` / `JsonSerializer`
- [ ] Implement `DebeziumDeserializer` (for CDC sources)
- [ ] Implement `CsvDeserializer` / `CsvSerializer`
- [ ] Implement `RawBytesDeserializer` (pass-through)
- [ ] Tests for each format with edge cases

### Phase 3: Connector Runtime (4-5 days)
- [ ] Implement `ConnectorRuntime`
- [ ] Implement `SourceRunner` (Ring 1 task loop)
- [ ] Implement `SinkRunner` (Ring 1 task loop with checkpoint integration)
- [ ] Implement `ConnectorRegistry` (factory pattern)
- [ ] Integration with `SourceCatalog` (laminar-db)
- [ ] Integration tests for end-to-end source/sink flow

### Phase 4: SQL Integration (2-3 days)
- [ ] Extend `SourceDefinition` to carry connector config
- [ ] Wire `CREATE SOURCE WITH (connector = '...')` to `ConnectorRegistry`
- [ ] Wire `CREATE SINK WITH (connector = '...')` to `ConnectorRegistry`
- [ ] Implement `DESCRIBE CONNECTOR` command
- [ ] SQL integration tests

### Phase 5: Testing Utilities and Documentation (2-3 days)
- [ ] Implement `MockSourceConnector` and `MockSinkConnector`
- [ ] Implement `test_source_connector()` and `test_sink_connector()` harnesses
- [ ] API documentation with examples
- [ ] Connector developer guide

---

## 12. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Ring 0 latency impact | < 10ns per event | Benchmark with/without connector |
| Source throughput | > 500K records/sec/connector | Benchmark with mock connector |
| Sink throughput | > 300K records/sec/connector | Benchmark with mock connector |
| Checkpoint overhead | < 5% throughput impact | Benchmark during checkpointing |
| JSON deserialization | < 1μs per record | Micro-benchmark |
| Test coverage | > 80% | cargo tarpaulin |
| Test count | 30+ tests | cargo test |

---

## 13. Module Structure

```
crates/laminar-connectors/src/
├── lib.rs                    # Crate root, re-exports
├── connector.rs              # SourceConnector, SinkConnector traits
├── config.rs                 # ConnectorConfig, ConfigKeySpec
├── error.rs                  # ConnectorError, SerdeError
├── registry.rs               # ConnectorRegistry, factory pattern
├── runtime.rs                # ConnectorRuntime, SourceRunner, SinkRunner
├── checkpoint.rs             # SourceCheckpoint, RuntimeCheckpoint
├── health.rs                 # HealthStatus, health check utilities
├── metrics.rs                # ConnectorMetrics, RuntimeMetrics
├── serde/                    # Serialization framework
│   ├── mod.rs                # RecordDeserializer, RecordSerializer traits
│   ├── json.rs               # JSON serde
│   ├── debezium.rs           # Debezium CDC envelope
│   ├── csv.rs                # CSV serde
│   └── raw.rs                # Raw bytes pass-through
├── testing.rs                # MockSourceConnector, MockSinkConnector, harnesses
├── kafka/                    # Kafka connectors (F025, F026)
│   ├── mod.rs
│   ├── source.rs
│   └── sink.rs
├── cdc/                      # CDC connectors (F027, F028, F029)
│   ├── mod.rs
│   ├── postgres.rs
│   ├── mysql.rs
│   └── mongodb.rs
├── lakehouse/                # Lakehouse sinks (F031, F032)
│   ├── mod.rs
│   ├── delta.rs
│   └── iceberg.rs
├── file/                     # File connectors (F033)
│   ├── mod.rs
│   └── parquet.rs
└── lookup/                   # Lookup tables (existing, F030)
    ├── mod.rs                # TableLoader trait (existing)
    └── redis.rs              # Redis lookup (F030)
```

---

## 14. References

1. **Apache Flink Connector API** - Source/Sink interface design
   - Flink Source API v2 (FLIP-27)
   - TwoPhaseCommitSinkFunction pattern

2. **Kafka Connect** - Connector framework design
   - Source/Sink connector abstractions
   - Offset management and exactly-once

3. **Debezium** - CDC connector patterns
   - CDC event envelope format
   - Logical decoding integration

4. **Research Compendium Section 2** - Streaming Architecture Patterns
   - Actor model execution
   - Three-tier architecture

5. **Research Compendium Section 3** - Checkpointing & Fault Tolerance
   - Asynchronous barrier snapshots
   - Incremental checkpointing

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **SourceConnector** | Reads data from an external system and converts to Arrow RecordBatch |
| **SinkConnector** | Writes Arrow RecordBatch data to an external system |
| **ConnectorRuntime** | Manages connector lifecycle, bridges Ring 1 I/O to Ring 0 channels |
| **ConnectorRegistry** | Maps connector type names to factory functions |
| **RecordDeserializer** | Converts raw bytes to Arrow RecordBatch |
| **RecordSerializer** | Converts Arrow RecordBatch to raw bytes |
| **SourceCheckpoint** | Connector-specific offset data for resuming after failure |
| **Epoch** | Checkpoint boundary; sinks commit data per epoch |

---

## Appendix B: Comparison with Alternatives

| Feature | LaminarDB SDK | Kafka Connect | Flink Connectors | Debezium |
|---------|---------------|---------------|------------------|----------|
| Embedded deployment | Yes | No (requires cluster) | No (requires cluster) | No |
| Zero-copy Arrow | Yes | No (Java objects) | No (Java objects) | No |
| Sub-microsecond Ring 0 | Yes | N/A | No | N/A |
| Exactly-once | Yes (epoch-based) | Yes (Kafka txns) | Yes (barrier) | At-least-once |
| Schema discovery | Yes | Yes | Limited | Yes |
| Custom formats | Trait-based | Converter API | SerDe framework | Debezium format |
| Testing harness | Built-in mocks | Embedded Connect | MiniCluster | Debezium testing |
