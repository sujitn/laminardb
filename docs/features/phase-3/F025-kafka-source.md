# F025: Kafka Source Connector

## Feature Specification v2.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0
**Estimated Complexity:** Large (1-2 weeks)
**Prerequisites:** F034 (Connector SDK), F-STREAM-004 (Source API), F-STREAM-007 (SQL DDL), F064 (Per-Partition Watermarks), F023 (Exactly-Once Sinks), F063 (Changelog/Retraction)

---

## Executive Summary

This specification defines the Kafka Source Connector for LaminarDB: a high-performance, exactly-once Apache Kafka consumer that bridges Kafka topics into LaminarDB's in-memory streaming pipeline. The connector runs in Ring 1 (background I/O) and pushes deserialized Arrow `RecordBatch` data into Ring 0 via the Connector SDK's `SourceConnector` trait, supporting multi-partition consumption with per-partition watermarks (F064), consumer group coordination, multiple deserialization formats (JSON, Avro, Protobuf, Debezium CDC), optional Schema Registry integration, backpressure-driven partition pause/resume, and exactly-once semantics via checkpoint-coordinated offset commits.

The design draws from 2025-2026 best practices including Flink's Source API v2 (FLIP-27), Kafka's cooperative rebalance protocol (KIP-429), rdkafka's `StreamConsumer` for async consumption, and Confluent Schema Registry integration patterns.

---

## 1. Problem Statement

### Current Limitation

LaminarDB's streaming API (F-STREAM-001 to F-STREAM-007) provides high-performance in-memory channels between sources and sinks. The Connector SDK (F034) defines the `SourceConnector` trait for bridging external systems. However, no Kafka implementation exists yet. The `laminar-connectors` crate has:

- `lib.rs`: A feature-gated `kafka` module placeholder (`#[cfg(feature = "kafka")]`)
- `Cargo.toml`: `rdkafka = { version = "0.36", optional = true }` dependency
- No actual Kafka consumer, offset management, or deserialization logic

Without a Kafka source connector, LaminarDB cannot ingest data from the most widely deployed event streaming platform, blocking all real-world streaming pipeline use cases.

### Required Capability

A Kafka Source Connector that:
1. Consumes from one or more Kafka topics with multi-partition support
2. Integrates with LaminarDB's per-partition watermark system (F064)
3. Tracks offsets per topic-partition and commits on checkpoint for exactly-once semantics
4. Deserializes messages using pluggable formats (JSON, Avro, Protobuf, Debezium CDC)
5. Optionally integrates with Confluent Schema Registry for schema discovery and evolution
6. Handles consumer group rebalances gracefully (cooperative protocol)
7. Supports backpressure by pausing/resuming Kafka partitions
8. Provides health monitoring, lag tracking, and operational metrics
9. Is fully configurable via SQL `CREATE SOURCE TABLE ... WITH (connector = 'kafka', ...)`

### Use Case Diagram

```
                      ┌───────────────────────────────────────────────────┐
                      │               Kafka Cluster                       │
                      │  ┌─────────────────────────────────────────────┐  │
                      │  │  Topic: orders (4 partitions)              │  │
                      │  │  P0: [msg1, msg2, msg3, ...]               │  │
                      │  │  P1: [msg4, msg5, msg6, ...]               │  │
                      │  │  P2: [msg7, msg8, msg9, ...]               │  │
                      │  │  P3: [msg10, msg11, msg12, ...]            │  │
                      │  └──────────────────┬──────────────────────────┘  │
                      └─────────────────────┼─────────────────────────────┘
                                            │
                      ┌─────────────────────▼─────────────────────────────┐
                      │             KafkaSource (Ring 1)                   │
                      │                                                   │
                      │  ┌───────────────┐  ┌──────────────────────────┐  │
                      │  │ StreamConsumer│  │ RecordDeserializer       │  │
                      │  │ (rdkafka)     │──│ (JSON / Avro / Protobuf) │  │
                      │  └───────┬───────┘  └──────────┬───────────────┘  │
                      │          │                      │                  │
                      │  ┌───────▼──────────────────────▼──────────────┐  │
                      │  │     Per-Partition Offset Tracker            │  │
                      │  │  P0: offset=3  P1: offset=6                │  │
                      │  │  P2: offset=9  P3: offset=12               │  │
                      │  └───────┬────────────────────────────────────┘  │
                      │          │                                        │
                      │  ┌───────▼────────────────────────────────────┐  │
                      │  │     Per-Partition Watermark Tracker (F064) │  │
                      │  │  P0: wm=10:03  P1: wm=10:01 (idle)       │  │
                      │  │  P2: wm=10:05  P3: wm=10:04              │  │
                      │  └───────┬────────────────────────────────────┘  │
                      │          │                                        │
                      │  ┌───────▼────────────────────────────────────┐  │
                      │  │     Backpressure Controller                │  │
                      │  │  (pause/resume Kafka partitions)           │  │
                      │  └───────┬────────────────────────────────────┘  │
                      └──────────┼────────────────────────────────────────┘
                                 │
                     SPSC Channel│ RecordBatch
                                 │
                      ┌──────────▼────────────────────────────────────────┐
                      │              Source<ArrowRecord> (Ring 0)          │
                      │  push_arrow(), watermark()                        │
                      └───────────────────────────────────────────────────┘
                                 │
                      ┌──────────▼────────────────────────────────────────┐
                      │  Streaming Pipeline (Ring 0, <500ns per event)    │
                      │  Operators → State Store → Emit → Sink            │
                      └───────────────────────────────────────────────────┘
```

---

## 2. Design Principles

### 2.1 Core Principles (Aligned with LaminarDB Philosophy)

| Principle | Application |
|-----------|-------------|
| **Three-ring separation** | Kafka I/O in Ring 1 (`tokio` task), data flows into Ring 0 via SPSC channel, health/config in Ring 2 |
| **Zero-allocation hot path** | Kafka deserialization and batching happen in Ring 1; Ring 0 only receives pre-built `RecordBatch` via channel push |
| **No custom types unless benchmarks demand** | Implements `SourceConnector` trait from F034; reuses `Source<ArrowRecord>`, `SourceCheckpoint`, `PartitionedWatermarkTracker` |
| **Automatic complexity derivation** | Partition count, consumer group, and watermark strategy auto-derived from Kafka topic metadata and SQL `WITH` options |
| **SQL-first** | Fully configurable via `CREATE SOURCE TABLE ... WITH (connector = 'kafka', ...)` |
| **Exactly-once by default** | Offsets committed only on checkpoint; manual commit mode via `enable.auto.commit = false` |

### 2.2 Industry-Informed Patterns (2025-2026 Research)

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **Source API v2** | Flink FLIP-27 (2024) | `SourceConnector` trait with `open/poll_batch/commit_offsets/close` lifecycle |
| **Cooperative Rebalance** | Kafka KIP-429 (2023) | Use `CooperativeStickyAssignor` to minimize partition revocations during scaling |
| **Per-Partition Watermarks** | Flink Kafka Connector (2024) | Integrate with F064 `PartitionedWatermarkTracker` for partition-granular event time tracking |
| **Checkpoint-Coordinated Commits** | Flink (2024), Kafka Streams (2025) | Offsets committed only when checkpoint completes; no `enable.auto.commit` |
| **Schema Registry** | Confluent Schema Registry (2025) | Optional `schema.registry.url` for Avro/Protobuf schema discovery and evolution |
| **Backpressure-Driven Pause** | Kafka Consumer API `pause()`/`resume()` | When SPSC channel is >80% full, pause Kafka partitions to prevent Ring 0 overflow |
| **Debezium CDC Envelope** | Debezium 2.x (2025) | `DebeziumDeserializer` extracts `before`/`after`/`op` for CDC pipelines |
| **Lag-Based Health** | Burrow / Kafka Exporter (2025) | Track consumer lag per partition, expose via `ConnectorMetrics` |

---

## 3. Architecture

### 3.1 Core Struct

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{Message, Offset, TopicPartition};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use laminar_core::time::{PartitionId, PartitionedWatermarkTracker};

/// Kafka Source Connector for LaminarDB.
///
/// Consumes records from one or more Kafka topics and pushes them
/// into a LaminarDB streaming pipeline as Arrow `RecordBatch` data.
///
/// # Lifecycle
///
/// ```text
/// new(config) → open() → [poll_batch() loop] → close()
///                              ↑
///                        commit_offsets()
///                        (on checkpoint)
/// ```
///
/// # Ring Assignment
///
/// - **Ring 1** (Background I/O): Consumer polling, deserialization, offset tracking
/// - **Ring 0** (Hot Path): Only the SPSC channel push of `RecordBatch` data
/// - **Ring 2** (Control Plane): Health checks, configuration, rebalance handling
pub struct KafkaSource {
    /// rdkafka StreamConsumer for async message consumption.
    consumer: Option<StreamConsumer>,

    /// Connector configuration parsed from SQL WITH clause.
    config: KafkaSourceConfig,

    /// Pluggable deserializer (JSON, Avro, Protobuf, Debezium CDC).
    deserializer: Box<dyn RecordDeserializer>,

    /// Current committed offsets per topic-partition.
    /// Key: "topic-partition" (e.g., "orders-0"), Value: offset (i64).
    offsets: HashMap<TopicPartition, i64>,

    /// Per-partition watermark tracker (F064 integration).
    watermark_tracker: PartitionedWatermarkTracker,

    /// Current connector lifecycle state.
    state: ConnectorState,

    /// Operational metrics.
    metrics: KafkaSourceMetrics,

    /// Arrow schema for the source (resolved from SQL DDL or Schema Registry).
    schema: Option<SchemaRef>,

    /// Backpressure controller for partition pause/resume.
    backpressure: BackpressureController,

    /// Optional Schema Registry client.
    schema_registry: Option<SchemaRegistryClient>,

    /// Consumer group rebalance state.
    rebalance_state: RebalanceState,

    /// Source ID for partition watermark registration.
    source_id: usize,
}
```

### 3.2 Configuration

```rust
/// Kafka source configuration parsed from SQL `WITH` clause options.
///
/// All Kafka consumer properties are passed through to `rdkafka::ClientConfig`
/// via the `kafka_properties` map.
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    /// Kafka bootstrap servers (required).
    /// Maps to `bootstrap.servers` in librdkafka.
    pub bootstrap_servers: String,

    /// Kafka consumer group ID (required).
    /// Maps to `group.id` in librdkafka.
    pub group_id: String,

    /// Topics to subscribe to (at least one required).
    pub topics: Vec<String>,

    /// Auto offset reset policy when no committed offset exists.
    /// Default: `Earliest`.
    pub auto_offset_reset: OffsetReset,

    /// Maximum number of records to return per `poll_batch()` call.
    /// Default: 1000.
    pub max_poll_records: usize,

    /// Maximum time to wait for messages during a single poll.
    /// Default: 100ms.
    pub poll_timeout: Duration,

    /// Deserialization format for message values.
    /// Default: `Json`.
    pub format: Format,

    /// Optional key deserialization format.
    /// Default: `None` (keys are ignored).
    pub key_format: Option<Format>,

    /// Schema Registry URL for Avro/Protobuf schema resolution.
    /// Default: `None`.
    pub schema_registry_url: Option<String>,

    /// Maximum out-of-orderness for watermark generation.
    /// Events arriving later than this after the watermark are considered late.
    /// Default: 5 seconds.
    pub max_out_of_orderness: Duration,

    /// Idle partition timeout. Partitions inactive for longer than this
    /// are marked idle and excluded from watermark minimum computation.
    /// Default: 30 seconds.
    pub idle_timeout: Duration,

    /// Backpressure high watermark (fraction of channel capacity).
    /// When channel fill exceeds this, pause Kafka partitions.
    /// Default: 0.8 (80%).
    pub backpressure_high_watermark: f64,

    /// Backpressure low watermark (fraction of channel capacity).
    /// When channel fill drops below this, resume paused partitions.
    /// Default: 0.5 (50%).
    pub backpressure_low_watermark: f64,

    /// Additional Kafka consumer properties passed through to librdkafka.
    /// These override any defaults set by the connector.
    pub kafka_properties: HashMap<String, String>,

    /// Whether to include Kafka message metadata (partition, offset, timestamp)
    /// as additional columns in the output RecordBatch.
    /// Default: false.
    pub include_metadata: bool,

    /// Column name for event time extraction (for watermark generation).
    /// Corresponds to `WATERMARK FOR <column> AS ...` in SQL.
    pub event_time_column: Option<String>,

    /// Consumer group partition assignment strategy.
    /// Default: `CooperativeSticky`.
    pub partition_assignment_strategy: AssignmentStrategy,
}

/// Kafka offset reset policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    /// Start from the earliest available offset.
    Earliest,
    /// Start from the latest offset (only new messages).
    Latest,
    /// Fail if no committed offset exists.
    None,
}

/// Consumer group partition assignment strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentStrategy {
    /// Range assignment (default Kafka).
    Range,
    /// Round-robin assignment.
    RoundRobin,
    /// Cooperative sticky assignment (recommended).
    CooperativeSticky,
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            group_id: String::new(),
            topics: Vec::new(),
            auto_offset_reset: OffsetReset::Earliest,
            max_poll_records: 1000,
            poll_timeout: Duration::from_millis(100),
            format: Format::Json,
            key_format: None,
            schema_registry_url: None,
            max_out_of_orderness: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            backpressure_high_watermark: 0.8,
            backpressure_low_watermark: 0.5,
            kafka_properties: HashMap::new(),
            include_metadata: false,
            event_time_column: None,
            partition_assignment_strategy: AssignmentStrategy::CooperativeSticky,
        }
    }
}
```

### 3.3 Configuration Parsing from SQL WITH Clause

```rust
impl KafkaSourceConfig {
    /// Parses a KafkaSourceConfig from the SQL `WITH` clause options.
    ///
    /// # Required Options
    ///
    /// - `bootstrap.servers` or `'bootstrap.servers'` - Kafka broker addresses
    /// - `topic` - Kafka topic name (single topic)
    ///   OR `topics` - Comma-separated list of topics
    /// - `group.id` or `'group.id'` - Consumer group identifier
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::Config` if required options are missing
    /// or if values fail validation.
    pub fn from_connector_config(
        config: &ConnectorConfig,
    ) -> Result<Self, ConnectorError> {
        let mut result = Self::default();

        // Required: bootstrap.servers
        result.bootstrap_servers = config
            .require("bootstrap.servers")?
            .to_string();

        // Required: topic(s)
        if let Some(topic) = config.options.get("topic") {
            result.topics = vec![topic.clone()];
        } else if let Some(topics) = config.options.get("topics") {
            result.topics = topics
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
        } else {
            return Err(ConnectorError::Config(
                "Missing required option 'topic' or 'topics'".into(),
            ));
        }

        // Required: group.id
        result.group_id = config
            .require("group.id")?
            .to_string();

        // Optional: auto.offset.reset
        if let Some(reset) = config.options.get("auto.offset.reset") {
            result.auto_offset_reset = match reset.as_str() {
                "earliest" => OffsetReset::Earliest,
                "latest" => OffsetReset::Latest,
                "none" => OffsetReset::None,
                other => return Err(ConnectorError::Config(
                    format!("Invalid auto.offset.reset: '{other}'. \
                             Expected: earliest, latest, none"),
                )),
            };
        }

        // Optional: max.poll.records
        if let Some(max) = config.get::<usize>("max.poll.records") {
            result.max_poll_records = max;
        }

        // Optional: poll.timeout.ms
        if let Some(timeout_ms) = config.get::<u64>("poll.timeout.ms") {
            result.poll_timeout = Duration::from_millis(timeout_ms);
        }

        // Optional: format
        if let Some(format) = config.options.get("format") {
            result.format = Format::from_str(format)?;
        }

        // Optional: key.format
        if let Some(key_fmt) = config.options.get("key.format") {
            result.key_format = Some(Format::from_str(key_fmt)?);
        }

        // Optional: schema.registry.url
        result.schema_registry_url = config
            .options
            .get("schema.registry.url")
            .cloned();

        // Optional: watermark.max.out.of.orderness.ms
        if let Some(ms) = config.get::<u64>("watermark.max.out.of.orderness.ms") {
            result.max_out_of_orderness = Duration::from_millis(ms);
        }

        // Optional: watermark.idle.timeout.ms
        if let Some(ms) = config.get::<u64>("watermark.idle.timeout.ms") {
            result.idle_timeout = Duration::from_millis(ms);
        }

        // Optional: backpressure thresholds
        if let Some(hw) = config.get::<f64>("backpressure.high.watermark") {
            result.backpressure_high_watermark = hw;
        }
        if let Some(lw) = config.get::<f64>("backpressure.low.watermark") {
            result.backpressure_low_watermark = lw;
        }

        // Optional: include.metadata
        if let Some(include) = config.get::<bool>("include.metadata") {
            result.include_metadata = include;
        }

        // Optional: partition.assignment.strategy
        if let Some(strategy) = config.options.get("partition.assignment.strategy") {
            result.partition_assignment_strategy = match strategy.as_str() {
                "range" => AssignmentStrategy::Range,
                "roundrobin" => AssignmentStrategy::RoundRobin,
                "cooperative-sticky" => AssignmentStrategy::CooperativeSticky,
                other => return Err(ConnectorError::Config(
                    format!("Invalid partition.assignment.strategy: '{other}'"),
                )),
            };
        }

        // Pass through any remaining kafka.* properties
        for (key, value) in &config.options {
            if key.starts_with("kafka.") {
                let kafka_key = key.strip_prefix("kafka.").unwrap();
                result.kafka_properties.insert(
                    kafka_key.to_string(),
                    value.clone(),
                );
            }
        }

        result.validate()?;
        Ok(result)
    }

    /// Validates the configuration.
    fn validate(&self) -> Result<(), ConnectorError> {
        if self.bootstrap_servers.is_empty() {
            return Err(ConnectorError::Config(
                "bootstrap.servers must not be empty".into(),
            ));
        }
        if self.topics.is_empty() {
            return Err(ConnectorError::Config(
                "At least one topic must be specified".into(),
            ));
        }
        if self.group_id.is_empty() {
            return Err(ConnectorError::Config(
                "group.id must not be empty".into(),
            ));
        }
        if self.max_poll_records == 0 {
            return Err(ConnectorError::Config(
                "max.poll.records must be > 0".into(),
            ));
        }
        if self.backpressure_high_watermark <= self.backpressure_low_watermark {
            return Err(ConnectorError::Config(
                "backpressure.high.watermark must be > backpressure.low.watermark".into(),
            ));
        }
        Ok(())
    }

    /// Builds an rdkafka ClientConfig from this configuration.
    fn to_rdkafka_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("group.id", &self.group_id);
        config.set("enable.auto.commit", "false"); // Always manual commit
        config.set("auto.offset.reset", match self.auto_offset_reset {
            OffsetReset::Earliest => "earliest",
            OffsetReset::Latest => "latest",
            OffsetReset::None => "none",
        });
        config.set("partition.assignment.strategy", match self.partition_assignment_strategy {
            AssignmentStrategy::Range => "range",
            AssignmentStrategy::RoundRobin => "roundrobin",
            AssignmentStrategy::CooperativeSticky => "cooperative-sticky",
        });

        // Apply user-provided Kafka properties (override defaults)
        for (key, value) in &self.kafka_properties {
            config.set(key, value);
        }

        config
    }
}
```

### 3.4 SourceConnector Implementation

```rust
#[async_trait]
impl SourceConnector for KafkaSource {
    fn info(&self) -> ConnectorInfo {
        ConnectorInfo {
            connector_type: "kafka".to_string(),
            description: "Apache Kafka source connector".to_string(),
            config_keys: vec![
                ConfigKeySpec {
                    name: "bootstrap.servers".into(),
                    description: "Kafka broker addresses".into(),
                    required: true,
                    default: None,
                    example: Some("localhost:9092".into()),
                },
                ConfigKeySpec {
                    name: "topic".into(),
                    description: "Kafka topic to consume".into(),
                    required: true,
                    default: None,
                    example: Some("orders".into()),
                },
                ConfigKeySpec {
                    name: "group.id".into(),
                    description: "Consumer group ID".into(),
                    required: true,
                    default: None,
                    example: Some("laminardb-orders".into()),
                },
                ConfigKeySpec {
                    name: "format".into(),
                    description: "Message deserialization format".into(),
                    required: false,
                    default: Some("json".into()),
                    example: Some("avro".into()),
                },
                ConfigKeySpec {
                    name: "auto.offset.reset".into(),
                    description: "Offset reset policy".into(),
                    required: false,
                    default: Some("earliest".into()),
                    example: Some("latest".into()),
                },
                ConfigKeySpec {
                    name: "schema.registry.url".into(),
                    description: "Confluent Schema Registry URL".into(),
                    required: false,
                    default: None,
                    example: Some("http://registry:8081".into()),
                },
            ],
            supported_formats: vec![
                "json".into(),
                "avro".into(),
                "protobuf".into(),
                "debezium-json".into(),
                "raw".into(),
                "csv".into(),
            ],
        }
    }

    fn state(&self) -> ConnectorState {
        self.state
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.config = KafkaSourceConfig::from_connector_config(config)?;

        // Build rdkafka consumer
        let rdkafka_config = self.config.to_rdkafka_config();
        let consumer: StreamConsumer = rdkafka_config
            .create()
            .map_err(|e| ConnectorError::Connection(
                format!("Failed to create Kafka consumer: {e}"),
            ))?;

        // Subscribe to topics
        let topics: Vec<&str> = self.config.topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topics)
            .map_err(|e| ConnectorError::Connection(
                format!("Failed to subscribe to topics: {e}"),
            ))?;

        // Discover partition count for watermark tracker registration
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| ConnectorError::Connection(
                format!("Failed to fetch metadata: {e}"),
            ))?;

        for topic in metadata.topics() {
            let partition_count = topic.partitions().len();
            self.watermark_tracker
                .register_source(self.source_id, partition_count);

            tracing::info!(
                topic = topic.name(),
                partitions = partition_count,
                "Kafka source registered with watermark tracker"
            );
        }

        // Optionally discover schema from Schema Registry
        if let Some(ref registry_url) = self.config.schema_registry_url {
            self.schema_registry = Some(
                SchemaRegistryClient::new(registry_url)
                    .map_err(|e| ConnectorError::Connection(
                        format!("Failed to connect to Schema Registry: {e}"),
                    ))?,
            );
        }

        self.consumer = Some(consumer);
        self.state = ConnectorState::Running;
        self.metrics.started_at = Some(Instant::now());

        tracing::info!(
            group_id = %self.config.group_id,
            topics = ?self.config.topics,
            format = ?self.config.format,
            "Kafka source connector opened"
        );

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let consumer = self.consumer.as_ref().ok_or_else(|| {
            ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            }
        })?;

        // Check backpressure and pause/resume partitions
        self.backpressure.check_and_adjust(consumer)?;

        let effective_max = max_records.min(self.config.max_poll_records);
        let mut raw_messages: Vec<(Vec<u8>, i32, i64, Option<i64>)> = Vec::new();
        let poll_deadline = Instant::now() + self.config.poll_timeout;

        // Collect messages up to max_records or timeout
        while raw_messages.len() < effective_max && Instant::now() < poll_deadline {
            match tokio::time::timeout(
                self.config.poll_timeout,
                consumer.recv(),
            ).await {
                Ok(Ok(msg)) => {
                    let partition = msg.partition();
                    let offset = msg.offset();
                    let timestamp = msg.timestamp().to_millis();
                    let payload = msg.payload().unwrap_or_default().to_vec();

                    // Update offset tracking
                    let tp = TopicPartition::new(
                        msg.topic(),
                        partition,
                    );
                    self.offsets.insert(tp, offset);

                    raw_messages.push((payload, partition, offset, timestamp));

                    // Update metrics
                    self.metrics.records_polled
                        .fetch_add(1, Ordering::Relaxed);
                    self.metrics.bytes_polled
                        .fetch_add(payload.len() as u64, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %e, "Kafka poll error");

                    // Transient errors: return partial batch
                    if !raw_messages.is_empty() {
                        break;
                    }
                    return Err(ConnectorError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Kafka consumer error: {e}"),
                    )));
                }
                Err(_timeout) => {
                    // Poll timeout - return whatever we have
                    break;
                }
            }
        }

        if raw_messages.is_empty() {
            // Mark partitions as potentially idle
            self.check_idle_partitions();
            return Ok(None);
        }

        // Deserialize raw messages into Arrow RecordBatch
        let payloads: Vec<&[u8]> = raw_messages
            .iter()
            .map(|(payload, _, _, _)| payload.as_slice())
            .collect();

        let schema = self.schema.clone().ok_or_else(|| {
            ConnectorError::Config(
                "Schema not set. Call discover_schema() or set via SQL DDL.".into(),
            )
        })?;

        let records = self.deserializer
            .deserialize_batch(&payloads, &schema)
            .map_err(ConnectorError::Serde)?;

        // Extract event times for watermark generation
        let event_times = self.extract_event_times(&records);

        // Update per-partition watermarks (F064 integration)
        for (_, partition, _, kafka_ts) in &raw_messages {
            let partition_id = PartitionId::new(self.source_id, *partition as u32);

            // Use event time if available, otherwise Kafka timestamp
            let ts = if let Some(ref times) = event_times {
                // Use the maximum event time from this partition
                times.iter()
                    .filter(|&&t| t != i64::MIN)
                    .copied()
                    .max()
                    .unwrap_or(kafka_ts.unwrap_or(0))
            } else {
                kafka_ts.unwrap_or(0)
            };

            let watermark_ts = ts
                - self.config.max_out_of_orderness.as_millis() as i64;
            self.watermark_tracker
                .update_partition(partition_id, watermark_ts);
        }

        // Build checkpoint offsets
        let offsets = self.current_offsets();

        // Compute partition info
        let partition = raw_messages.first().map(|(_, p, _, _)| {
            PartitionInfo {
                partition_id: p.to_string(),
                total_partitions: self.watermark_tracker
                    .partition_count(self.source_id),
            }
        });

        self.metrics.batches_polled.fetch_add(1, Ordering::Relaxed);
        self.metrics.last_poll_time = Some(Instant::now());

        Ok(Some(SourceBatch {
            records,
            offsets,
            event_times,
            partition,
        }))
    }

    async fn commit_offsets(
        &mut self,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        let consumer = self.consumer.as_ref().ok_or_else(|| {
            ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            }
        })?;

        // Build a TopicPartitionList from checkpoint offsets
        let mut tpl = TopicPartitionList::new();
        for (key, value) in &checkpoint.offsets {
            // Parse "topic-partition" key format
            if let Some((topic, partition_str)) = key.rsplit_once('-') {
                if let Ok(partition) = partition_str.parse::<i32>() {
                    if let Ok(offset) = value.parse::<i64>() {
                        // Commit offset + 1 (Kafka convention: committed offset
                        // is the next offset to be read)
                        tpl.add_partition_offset(
                            topic,
                            partition,
                            Offset::Offset(offset + 1),
                        ).map_err(|e| ConnectorError::Transaction(
                            format!("Failed to add partition offset: {e}"),
                        ))?;
                    }
                }
            }
        }

        // Synchronous commit for exactly-once guarantees
        consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| ConnectorError::Transaction(
                format!("Offset commit failed: {e}"),
            ))?;

        self.metrics.commits.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            epoch = checkpoint.epoch,
            partitions = tpl.count(),
            "Kafka offsets committed"
        );

        Ok(())
    }

    fn current_offsets(&self) -> SourceCheckpoint {
        let mut offsets = HashMap::new();
        for (tp, offset) in &self.offsets {
            let key = format!("{}-{}", tp.topic(), tp.partition());
            offsets.insert(key, offset.to_string());
        }

        SourceCheckpoint {
            offsets,
            epoch: 0, // Set by checkpoint coordinator
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    async fn discover_schema(&self) -> Result<Option<SchemaRef>, ConnectorError> {
        // If schema was set via SQL DDL, return that
        if let Some(ref schema) = self.schema {
            return Ok(Some(schema.clone()));
        }

        // Try Schema Registry
        if let Some(ref registry) = self.schema_registry {
            let topic = self.config.topics.first().ok_or_else(|| {
                ConnectorError::Config("No topics configured".into())
            })?;

            let subject = format!("{topic}-value");
            match registry.get_latest_schema(&subject).await {
                Ok(schema) => Ok(Some(schema)),
                Err(e) => {
                    tracing::warn!(
                        subject = %subject,
                        error = %e,
                        "Schema Registry lookup failed"
                    );
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn health_check(&self) -> Result<HealthStatus, ConnectorError> {
        let mut details = HashMap::new();

        details.insert("group.id".into(), self.config.group_id.clone());
        details.insert(
            "topics".into(),
            self.config.topics.join(", "),
        );
        details.insert("state".into(), format!("{:?}", self.state));

        // Check consumer connectivity
        if let Some(ref consumer) = self.consumer {
            match consumer.fetch_metadata(None, Duration::from_secs(5)) {
                Ok(metadata) => {
                    let broker_count = metadata.brokers().len();
                    details.insert(
                        "brokers".into(),
                        broker_count.to_string(),
                    );

                    // Compute consumer lag
                    let total_lag = self.compute_consumer_lag(consumer);
                    details.insert("total_lag".into(), total_lag.to_string());

                    // Per-partition lag
                    for (tp, lag) in self.compute_partition_lag(consumer) {
                        details.insert(
                            format!("lag.{}.{}", tp.topic(), tp.partition()),
                            lag.to_string(),
                        );
                    }

                    Ok(HealthStatus {
                        healthy: true,
                        message: format!(
                            "Connected to {broker_count} broker(s), lag: {total_lag}"
                        ),
                        last_success: self.metrics.last_poll_time
                            .map(|t| t.elapsed()),
                        details,
                    })
                }
                Err(e) => Ok(HealthStatus {
                    healthy: false,
                    message: format!("Metadata fetch failed: {e}"),
                    last_success: self.metrics.last_poll_time
                        .map(|t| t.elapsed()),
                    details,
                }),
            }
        } else {
            Ok(HealthStatus {
                healthy: false,
                message: "Consumer not initialized".into(),
                last_success: None,
                details,
            })
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(consumer) = self.consumer.take() {
            // Commit final offsets before shutdown
            let checkpoint = self.current_offsets();
            if let Err(e) = self.commit_offsets(&checkpoint).await {
                tracing::warn!(error = %e, "Failed to commit offsets on close");
            }

            // Unsubscribe and drop consumer
            consumer.unsubscribe();
            drop(consumer);
        }

        self.state = ConnectorState::Stopped;

        tracing::info!(
            group_id = %self.config.group_id,
            "Kafka source connector closed"
        );

        Ok(())
    }

    fn metrics(&self) -> ConnectorMetrics {
        let uptime = self.metrics.started_at
            .map(|t| t.elapsed())
            .unwrap_or_default();

        let records_total = self.metrics.records_polled
            .load(Ordering::Relaxed);
        let records_per_sec = if uptime.as_secs() > 0 {
            records_total as f64 / uptime.as_secs_f64()
        } else {
            0.0
        };

        ConnectorMetrics {
            records_total,
            bytes_total: self.metrics.bytes_polled.load(Ordering::Relaxed),
            errors_total: self.metrics.errors.load(Ordering::Relaxed),
            records_per_sec,
            avg_batch_latency_us: self.metrics.avg_batch_latency_us(),
            p99_batch_latency_us: self.metrics.p99_batch_latency_us(),
            lag: Some(self.metrics.total_lag.load(Ordering::Relaxed)),
            custom: {
                let mut m = HashMap::new();
                m.insert(
                    "commits_total".into(),
                    self.metrics.commits.load(Ordering::Relaxed) as f64,
                );
                m.insert(
                    "batches_total".into(),
                    self.metrics.batches_polled
                        .load(Ordering::Relaxed) as f64,
                );
                m.insert(
                    "rebalances_total".into(),
                    self.metrics.rebalances.load(Ordering::Relaxed) as f64,
                );
                m.insert(
                    "paused_partitions".into(),
                    self.backpressure.paused_count() as f64,
                );
                m
            },
        }
    }
}
```

### 3.5 Metrics

```rust
/// Kafka source operational metrics.
#[derive(Debug)]
pub struct KafkaSourceMetrics {
    /// Total records polled from Kafka.
    pub records_polled: AtomicU64,
    /// Total bytes polled from Kafka.
    pub bytes_polled: AtomicU64,
    /// Total poll errors.
    pub errors: AtomicU64,
    /// Total successful offset commits.
    pub commits: AtomicU64,
    /// Total batches polled.
    pub batches_polled: AtomicU64,
    /// Total consumer group rebalances.
    pub rebalances: AtomicU64,
    /// Total consumer lag across all partitions.
    pub total_lag: AtomicU64,
    /// Connector start time.
    pub started_at: Option<Instant>,
    /// Last successful poll time.
    pub last_poll_time: Option<Instant>,
    /// Batch latency histogram (microseconds).
    batch_latencies: LatencyHistogram,
}

impl Default for KafkaSourceMetrics {
    fn default() -> Self {
        Self {
            records_polled: AtomicU64::new(0),
            bytes_polled: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            batches_polled: AtomicU64::new(0),
            rebalances: AtomicU64::new(0),
            total_lag: AtomicU64::new(0),
            started_at: None,
            last_poll_time: None,
            batch_latencies: LatencyHistogram::new(),
        }
    }
}

impl KafkaSourceMetrics {
    /// Returns the average batch latency in microseconds.
    pub fn avg_batch_latency_us(&self) -> u64 {
        self.batch_latencies.average()
    }

    /// Returns the p99 batch latency in microseconds.
    pub fn p99_batch_latency_us(&self) -> u64 {
        self.batch_latencies.percentile(99)
    }
}
```

### 3.6 Backpressure Controller

```rust
/// Controls Kafka partition pause/resume based on channel fill level.
///
/// When the SPSC channel to Ring 0 fills above `high_watermark`, the
/// controller pauses Kafka partitions to stop consuming. When the fill
/// level drops below `low_watermark`, consumption resumes.
///
/// This prevents Ring 0 from being overwhelmed by a burst of Kafka
/// messages while maintaining throughput under normal conditions.
///
/// ```text
/// Channel Fill Level:
///
///  100% ┌──────────────────────────────┐
///       │                              │
///   80% ├──── HIGH WATERMARK ──────────┤  ← pause() Kafka partitions
///       │                              │
///   50% ├──── LOW WATERMARK ───────────┤  ← resume() Kafka partitions
///       │                              │
///    0% └──────────────────────────────┘
/// ```
pub struct BackpressureController {
    /// High watermark fraction (0.0 - 1.0).
    high_watermark: f64,
    /// Low watermark fraction (0.0 - 1.0).
    low_watermark: f64,
    /// Set of currently paused partitions.
    paused_partitions: HashSet<(String, i32)>,
    /// Reference to channel capacity for fill-level computation.
    channel_capacity: usize,
    /// Reference to channel pending count (shared with Source).
    channel_pending: Arc<AtomicU64>,
    /// Whether partitions are currently paused.
    is_paused: bool,
}

impl BackpressureController {
    /// Creates a new backpressure controller.
    pub fn new(
        high_watermark: f64,
        low_watermark: f64,
        channel_capacity: usize,
        channel_pending: Arc<AtomicU64>,
    ) -> Self {
        Self {
            high_watermark,
            low_watermark,
            paused_partitions: HashSet::new(),
            channel_capacity,
            channel_pending,
            is_paused: false,
        }
    }

    /// Checks channel fill level and pauses/resumes Kafka partitions.
    pub fn check_and_adjust(
        &mut self,
        consumer: &StreamConsumer,
    ) -> Result<(), ConnectorError> {
        let pending = self.channel_pending.load(Ordering::Relaxed) as usize;
        let fill_ratio = pending as f64 / self.channel_capacity as f64;

        if !self.is_paused && fill_ratio >= self.high_watermark {
            // Pause all assigned partitions
            if let Ok(assignment) = consumer.assignment() {
                consumer.pause(&assignment).map_err(|e| {
                    ConnectorError::Other(Box::new(e))
                })?;
                self.is_paused = true;

                tracing::info!(
                    fill_ratio = fill_ratio,
                    partitions = assignment.count(),
                    "Backpressure: paused Kafka partitions"
                );
            }
        } else if self.is_paused && fill_ratio <= self.low_watermark {
            // Resume all paused partitions
            if let Ok(assignment) = consumer.assignment() {
                consumer.resume(&assignment).map_err(|e| {
                    ConnectorError::Other(Box::new(e))
                })?;
                self.is_paused = false;

                tracing::info!(
                    fill_ratio = fill_ratio,
                    "Backpressure: resumed Kafka partitions"
                );
            }
        }

        Ok(())
    }

    /// Returns the number of currently paused partitions.
    pub fn paused_count(&self) -> usize {
        if self.is_paused {
            self.paused_partitions.len()
        } else {
            0
        }
    }
}
```

### 3.7 Consumer Group Rebalance Handling

```rust
/// Tracks consumer group rebalance state.
///
/// When a rebalance occurs, Kafka may revoke some partitions and assign
/// new ones. The connector must:
///
/// 1. Commit offsets for revoked partitions before release
/// 2. Initialize offset tracking for newly assigned partitions
/// 3. Update the partition watermark tracker (F064)
/// 4. Log the rebalance for operational visibility
#[derive(Debug)]
pub struct RebalanceState {
    /// Currently assigned partitions.
    assigned: HashSet<(String, i32)>,
    /// Partitions pending revocation (during cooperative rebalance).
    pending_revocation: HashSet<(String, i32)>,
    /// Total rebalance count.
    rebalance_count: u64,
    /// Last rebalance timestamp.
    last_rebalance: Option<Instant>,
}

impl RebalanceState {
    /// Creates a new rebalance state.
    pub fn new() -> Self {
        Self {
            assigned: HashSet::new(),
            pending_revocation: HashSet::new(),
            rebalance_count: 0,
            last_rebalance: None,
        }
    }

    /// Handles partition assignment from a rebalance.
    pub fn on_assign(
        &mut self,
        partitions: &[(String, i32)],
        watermark_tracker: &mut PartitionedWatermarkTracker,
        source_id: usize,
    ) {
        for (topic, partition) in partitions {
            self.assigned.insert((topic.clone(), *partition));

            // Register new partition with watermark tracker
            // (tracker handles duplicates gracefully)
        }

        self.rebalance_count += 1;
        self.last_rebalance = Some(Instant::now());

        tracing::info!(
            assigned = partitions.len(),
            total = self.assigned.len(),
            rebalance_count = self.rebalance_count,
            "Kafka rebalance: partitions assigned"
        );
    }

    /// Handles partition revocation from a rebalance.
    pub fn on_revoke(
        &mut self,
        partitions: &[(String, i32)],
        offsets: &HashMap<TopicPartition, i64>,
    ) {
        for (topic, partition) in partitions {
            self.assigned.remove(&(topic.clone(), *partition));
        }

        tracing::info!(
            revoked = partitions.len(),
            remaining = self.assigned.len(),
            "Kafka rebalance: partitions revoked"
        );
    }
}
```

### 3.8 Schema Registry Client

```rust
/// Optional Confluent Schema Registry client for Avro/Protobuf
/// schema discovery and validation.
///
/// The Schema Registry provides:
/// - Automatic schema resolution for Avro and Protobuf topics
/// - Schema evolution compatibility checks
/// - Arrow schema derivation from Avro/Protobuf schemas
pub struct SchemaRegistryClient {
    /// HTTP client for Schema Registry API.
    client: reqwest::Client,
    /// Schema Registry base URL.
    base_url: String,
    /// Cached schemas by subject and version.
    cache: HashMap<String, SchemaRef>,
}

impl SchemaRegistryClient {
    /// Creates a new Schema Registry client.
    pub fn new(base_url: &str) -> Result<Self, ConnectorError> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            cache: HashMap::new(),
        })
    }

    /// Fetches the latest schema for a subject and converts to Arrow SchemaRef.
    pub async fn get_latest_schema(
        &self,
        subject: &str,
    ) -> Result<SchemaRef, ConnectorError> {
        // Check cache first
        if let Some(schema) = self.cache.get(subject) {
            return Ok(schema.clone());
        }

        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.base_url, subject
        );

        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(
                format!("Schema Registry request failed: {e}"),
            ))?;

        if !response.status().is_success() {
            return Err(ConnectorError::Connection(
                format!(
                    "Schema Registry returned {}: {}",
                    response.status(),
                    response.text().await.unwrap_or_default()
                ),
            ));
        }

        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ConnectorError::Serde(
                SerdeError::Json(format!("Failed to parse registry response: {e}")),
            ))?;

        let schema_str = body["schema"]
            .as_str()
            .ok_or_else(|| ConnectorError::Serde(
                SerdeError::Json("Missing 'schema' field in registry response".into()),
            ))?;

        // Convert Avro schema to Arrow schema
        let arrow_schema = avro_schema_to_arrow(schema_str)?;
        Ok(arrow_schema)
    }
}
```

### 3.9 Three-Ring Integration Table

| Ring | Kafka Source Responsibilities | Latency Budget |
|------|------------------------------|---------------|
| **Ring 0 (Hot Path)** | Receive `RecordBatch` from SPSC channel via `Source.push_arrow()`. No Kafka code runs here. Watermark updates via `Source.watermark()`. | <500ns per event |
| **Ring 1 (Background)** | `SourceRunner` task: `poll_batch()` from Kafka via rdkafka `StreamConsumer`, deserialize to `RecordBatch`, push into SPSC channel. Offset tracking. Checkpoint commit. Backpressure pause/resume. Watermark computation. | 1-100ms per poll cycle |
| **Ring 2 (Control Plane)** | Connector lifecycle (open/close/restart). Health checks. Schema discovery. Consumer group rebalance handling. Configuration changes. Lag monitoring. | No latency requirement |

---

## 4. Latency Considerations

### 4.1 Hot Path Budget

Kafka I/O never touches Ring 0. The only Ring 0 operation is the SPSC channel push:

| Component | Ring | Latency | Optimization |
|-----------|------|---------|-------------|
| SPSC channel push (`Source.push_arrow()`) | Ring 0 | ~5ns | Lock-free, cache-line padded |
| Watermark update (`Source.watermark()`) | Ring 0 | ~10ns | Atomic CAS, no allocation |
| Kafka `consumer.recv()` | Ring 1 | 1-100ms | Async, batched |
| JSON deserialization (per record) | Ring 1 | 0.5-5us | SIMD JSON parser, batch construction |
| Avro deserialization (per record) | Ring 1 | 0.2-2us | Zero-copy with schema cache |
| Arrow RecordBatch construction | Ring 1 | 5-50us/batch | Columnar builders, pre-allocated |
| Offset commit (on checkpoint) | Ring 1 | 5-50ms | Synchronous Kafka commit |
| Schema Registry lookup | Ring 2 | 10-200ms | Cached after first fetch |
| Health check (metadata fetch) | Ring 2 | 50-500ms | Periodic, not on data path |
| **Ring 0 total impact** | **Ring 0** | **~15ns** | **SPSC push + watermark only** |

### 4.2 What Stays OFF the Hot Path

All Kafka-specific operations remain in Ring 1 or Ring 2:

- Network I/O to Kafka brokers (poll, fetch, commit)
- Message deserialization (JSON, Avro, Protobuf parsing)
- Arrow RecordBatch columnar construction
- Offset tracking and checkpoint persistence
- Backpressure partition pause/resume
- Consumer group rebalance protocol
- Schema Registry HTTP requests
- Lag computation and metrics collection
- Health check metadata fetches

---

## 5. SQL Integration

### 5.1 CREATE SOURCE TABLE Syntax

```sql
-- Basic Kafka source with JSON format
CREATE SOURCE TABLE orders (
    order_id BIGINT,
    customer_id VARCHAR,
    amount DECIMAL(10, 2),
    ts TIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'orders',
    'bootstrap.servers' = 'localhost:9092',
    'group.id' = 'laminardb-orders',
    format = 'json',
    'auto.offset.reset' = 'earliest'
);
```

```sql
-- Multi-topic Kafka source with Avro + Schema Registry
CREATE SOURCE TABLE trades (
    symbol VARCHAR,
    price DOUBLE,
    volume BIGINT,
    exchange VARCHAR,
    trade_time TIMESTAMP,
    WATERMARK FOR trade_time AS trade_time - INTERVAL '3' SECOND
) WITH (
    connector = 'kafka',
    topics = 'trades-nyse, trades-nasdaq, trades-cboe',
    'bootstrap.servers' = 'broker1:9092,broker2:9092,broker3:9092',
    'group.id' = 'laminardb-trades',
    format = 'avro',
    'schema.registry.url' = 'http://registry:8081',
    'auto.offset.reset' = 'latest',
    'max.poll.records' = '5000',
    'watermark.max.out.of.orderness.ms' = '3000',
    'watermark.idle.timeout.ms' = '60000'
);
```

```sql
-- CDC source consuming Debezium envelope from Kafka
CREATE SOURCE TABLE users_cdc (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    updated_at TIMESTAMP,
    WATERMARK FOR updated_at AS updated_at - INTERVAL '10' SECOND
) WITH (
    connector = 'kafka',
    topic = 'dbserver1.public.users',
    'bootstrap.servers' = 'localhost:9092',
    'group.id' = 'laminardb-users-cdc',
    format = 'debezium-json',
    'auto.offset.reset' = 'earliest'
);
```

```sql
-- Kafka source with metadata columns and custom backpressure
CREATE SOURCE TABLE events (
    event_type VARCHAR,
    payload VARCHAR,
    event_time TIMESTAMP,
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    topic = 'events',
    'bootstrap.servers' = 'localhost:9092',
    'group.id' = 'laminardb-events',
    format = 'json',
    'include.metadata' = 'true',
    'backpressure.high.watermark' = '0.9',
    'backpressure.low.watermark' = '0.6',
    'poll.timeout.ms' = '50',
    'partition.assignment.strategy' = 'cooperative-sticky'
);
```

```sql
-- Protobuf format with Schema Registry
CREATE SOURCE TABLE sensor_readings (
    sensor_id VARCHAR,
    temperature DOUBLE,
    humidity DOUBLE,
    reading_time TIMESTAMP,
    WATERMARK FOR reading_time AS reading_time - INTERVAL '2' SECOND
) WITH (
    connector = 'kafka',
    topic = 'sensor-readings',
    'bootstrap.servers' = 'broker:9092',
    'group.id' = 'laminardb-sensors',
    format = 'protobuf',
    'schema.registry.url' = 'http://registry:8081',
    'kafka.security.protocol' = 'SASL_SSL',
    'kafka.sasl.mechanism' = 'PLAIN',
    'kafka.sasl.username' = 'user',
    'kafka.sasl.password' = 'secret'
);
```

### 5.2 Querying Kafka Source Metadata

```sql
-- Show all configured sources
SHOW SOURCES;

-- Describe Kafka source schema and configuration
DESCRIBE SOURCE orders;
-- Result:
-- | Column      | Type           | Nullable | Watermark                           |
-- |-------------|----------------|----------|-------------------------------------|
-- | order_id    | BIGINT         | NO       |                                     |
-- | customer_id | VARCHAR        | YES      |                                     |
-- | amount      | DECIMAL(10,2)  | YES      |                                     |
-- | ts          | TIMESTAMP      | NO       | ts - INTERVAL '5' SECOND            |
--
-- Connector: kafka
-- Topic: orders
-- Group ID: laminardb-orders
-- Format: json

-- Drop a Kafka source
DROP SOURCE orders;
```

---

## 6. Rust API Examples

### 6.1 Programmatic Kafka Source

```rust
use laminar_connectors::kafka::{KafkaSource, KafkaSourceConfig};
use laminar_connectors::{ConnectorRuntime, ConnectorConfig};

// Build Kafka source connector directly
let kafka_config = KafkaSourceConfig {
    bootstrap_servers: "localhost:9092".into(),
    group_id: "laminardb-orders".into(),
    topics: vec!["orders".into()],
    format: Format::Json,
    auto_offset_reset: OffsetReset::Earliest,
    max_poll_records: 1000,
    max_out_of_orderness: Duration::from_secs(5),
    ..Default::default()
};

let kafka_source = KafkaSource::new(kafka_config);

// Start via connector runtime (bridges to Ring 0)
let mut runtime = ConnectorRuntime::new();

let connector_config = ConnectorConfig {
    connector_type: "kafka".into(),
    format: "json".into(),
    options: HashMap::from([
        ("bootstrap.servers".into(), "localhost:9092".into()),
        ("topic".into(), "orders".into()),
        ("group.id".into(), "laminardb-orders".into()),
    ]),
    schema: Some(order_schema()),
    watermark: None,
    channel: Default::default(),
};

let source_handle = runtime
    .start_source(
        "orders",
        Box::new(kafka_source),
        Box::new(JsonDeserializer::default()),
        connector_config,
    )
    .await?;

// Subscribe to data flowing through Ring 0
let subscription = source_handle.sink.subscribe();
while let Some(batch) = subscription.poll() {
    println!("Received batch: {} rows", batch.num_rows());
}
```

### 6.2 Using LaminarDB Facade

```rust
use laminardb::prelude::*;

let db = LaminarDB::open()?;

// Create Kafka source via SQL
db.execute(r#"
    CREATE SOURCE TABLE orders (
        order_id BIGINT,
        amount DECIMAL(10, 2),
        ts TIMESTAMP,
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        connector = 'kafka',
        topic = 'orders',
        'bootstrap.servers' = 'localhost:9092',
        'group.id' = 'laminardb-orders',
        format = 'json'
    )
"#).await?;

// Query the streaming source
db.execute(r#"
    CREATE MATERIALIZED VIEW order_stats AS
    SELECT
        TUMBLE_START(ts, INTERVAL '1' MINUTE) as window_start,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM orders
    GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)
    EMIT ON WINDOW CLOSE
"#).await?;

// Subscribe to materialized view results
let sub = db.subscribe("order_stats").await?;
while let Some(batch) = sub.recv().await {
    println!("Window: {} orders, total: {}",
        batch.column(1), batch.column(2));
}
```

### 6.3 Health Monitoring

```rust
use laminar_connectors::kafka::KafkaSource;

// Check health of a running Kafka source
let health = runtime.health().await;
let kafka_health = health.get("orders").unwrap();

println!("Healthy: {}", kafka_health.healthy);
println!("Message: {}", kafka_health.message);
println!("Brokers: {}", kafka_health.details.get("brokers").unwrap());
println!("Total Lag: {}", kafka_health.details.get("total_lag").unwrap());

// Get detailed metrics
let metrics = runtime.metrics();
println!("Records ingested: {}",
    metrics.total_records_ingested.load(Ordering::Relaxed));
```

---

## 7. Configuration Reference

### 7.1 Required Options

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `connector` | String | Must be `'kafka'` | `'kafka'` |
| `topic` or `topics` | String | Topic name(s), comma-separated for multiple | `'orders'` or `'t1, t2'` |
| `bootstrap.servers` | String | Kafka broker addresses | `'b1:9092,b2:9092'` |
| `group.id` | String | Consumer group identifier | `'laminardb-orders'` |

### 7.2 Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `format` | String | `json` | Deserialization format: `json`, `avro`, `protobuf`, `debezium-json`, `csv`, `raw` |
| `key.format` | String | None | Key deserialization format (if keys are needed) |
| `auto.offset.reset` | String | `earliest` | `earliest`, `latest`, or `none` |
| `max.poll.records` | Integer | `1000` | Maximum records per poll batch |
| `poll.timeout.ms` | Integer | `100` | Poll timeout in milliseconds |
| `schema.registry.url` | String | None | Confluent Schema Registry URL |
| `watermark.max.out.of.orderness.ms` | Integer | `5000` | Maximum out-of-orderness for watermark computation |
| `watermark.idle.timeout.ms` | Integer | `30000` | Idle partition timeout for watermark advancement |
| `backpressure.high.watermark` | Float | `0.8` | Channel fill fraction to trigger pause |
| `backpressure.low.watermark` | Float | `0.5` | Channel fill fraction to trigger resume |
| `include.metadata` | Boolean | `false` | Add `_partition`, `_offset`, `_timestamp` columns |
| `partition.assignment.strategy` | String | `cooperative-sticky` | `range`, `roundrobin`, or `cooperative-sticky` |

### 7.3 Pass-Through Kafka Properties

Any option prefixed with `kafka.` is passed directly to librdkafka after stripping the prefix:

| Option | Description |
|--------|-------------|
| `kafka.security.protocol` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `kafka.sasl.mechanism` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI` |
| `kafka.sasl.username` | SASL username |
| `kafka.sasl.password` | SASL password |
| `kafka.ssl.ca.location` | CA certificate file path |
| `kafka.ssl.certificate.location` | Client certificate file path |
| `kafka.ssl.key.location` | Client key file path |
| `kafka.fetch.min.bytes` | Minimum bytes per fetch request |
| `kafka.fetch.max.bytes` | Maximum bytes per fetch request |
| `kafka.max.partition.fetch.bytes` | Maximum bytes per partition per fetch |
| `kafka.session.timeout.ms` | Consumer group session timeout |
| `kafka.heartbeat.interval.ms` | Consumer group heartbeat interval |
| `kafka.max.poll.interval.ms` | Maximum time between polls before rebalance |

---

## 8. Integration with Existing Features

### 8.1 Per-Partition Watermarks (F064)

The Kafka source registers each topic-partition with the `PartitionedWatermarkTracker`:

```rust
// On consumer group assignment
for partition in 0..partition_count {
    let pid = PartitionId::new(source_id, partition as u32);
    watermark_tracker.update_partition(pid, initial_watermark);
}

// On each message received
let pid = PartitionId::new(source_id, msg.partition() as u32);
let event_time = extract_event_time(&msg);
let watermark = event_time - max_out_of_orderness_ms;
watermark_tracker.update_partition(pid, watermark);

// Idle partition detection
for partition in all_partitions {
    if partition.last_activity.elapsed() > idle_timeout {
        watermark_tracker.mark_partition_idle(
            PartitionId::new(source_id, partition)
        );
    }
}
```

This ensures that a single slow or idle Kafka partition does not block watermark progress for the entire source.

### 8.2 Keyed Watermarks (F065)

When `WATERMARK FOR <column> AS ...` is specified in the SQL DDL, the Kafka source extracts event times from the deserialized records and can optionally feed them into the `KeyedWatermarkTracker` for per-key watermark accuracy:

```rust
// Extract event time from deserialized record
let event_times = batch
    .column_by_name(&event_time_column)
    .and_then(|col| col.as_any().downcast_ref::<TimestampArray>())
    .map(|arr| arr.values().to_vec());
```

### 8.3 Exactly-Once Semantics (F023)

The Kafka source achieves exactly-once via checkpoint-coordinated offset commits:

```text
┌─────────────────────────────────────────────────────────────────┐
│                 Exactly-Once Processing Flow                     │
│                                                                  │
│  1. KafkaSource.poll_batch() → returns records + offsets        │
│  2. Ring 0 processes records through operators                   │
│  3. Checkpoint coordinator triggers checkpoint(epoch=N)          │
│  4. KafkaSource.commit_offsets(checkpoint) → Kafka sync commit  │
│  5. SinkConnector.commit_epoch(N) → external system commit      │
│                                                                  │
│  On failure/restart:                                            │
│  1. Restore from checkpoint(epoch=N-1)                          │
│  2. KafkaSource resumes from committed offset (epoch N-1)       │
│  3. Re-process records from epoch N-1 to current                │
│  4. Idempotent sinks de-duplicate via epoch marker              │
│                                                                  │
│  Key: enable.auto.commit = false (always)                       │
│       Offsets committed ONLY on successful checkpoint           │
└─────────────────────────────────────────────────────────────────┘
```

### 8.4 Changelog/Retraction (F063)

When using `format = 'debezium-json'`, the Debezium CDC envelope is parsed and converted to LaminarDB's Z-set retraction format:

| Debezium `op` | Z-Set Weight | Meaning |
|----------------|-------------|---------|
| `c` (create) | `+1` | Insert new record |
| `u` (update) | `-1` + `+1` | Retract old, insert new |
| `d` (delete) | `-1` | Retract record |
| `r` (read/snapshot) | `+1` | Initial snapshot record |

### 8.5 Connector SDK (F034)

The Kafka source implements the `SourceConnector` trait from F034 and is registered in the `ConnectorRegistry`:

```rust
// Registration in ConnectorRegistry
registry.register_source("kafka", Box::new(|config| {
    let kafka_config = KafkaSourceConfig::from_connector_config(config)?;
    Ok(Box::new(KafkaSource::new(kafka_config)))
}));
```

---

## 9. Implementation Roadmap

### Phase 1: Core Consumer (3-4 days)
- [ ] `KafkaSource` struct with `StreamConsumer` lifecycle
- [ ] `KafkaSourceConfig` with validation and SQL `WITH` parsing
- [ ] `SourceConnector` trait implementation (`open`, `poll_batch`, `close`)
- [ ] Basic JSON deserialization into Arrow `RecordBatch`
- [ ] Per-partition offset tracking (`HashMap<TopicPartition, i64>`)
- [ ] Unit tests: config parsing, validation, offset tracking
- [ ] Integration test: consume from embedded Kafka (testcontainers)

### Phase 2: Watermarks and Checkpointing (2-3 days)
- [ ] Per-partition watermark integration (F064 `PartitionedWatermarkTracker`)
- [ ] Event time extraction from deserialized records
- [ ] Idle partition detection and marking
- [ ] `commit_offsets()` implementation with synchronous Kafka commit
- [ ] `current_offsets()` → `SourceCheckpoint` conversion
- [ ] Checkpoint round-trip test: commit, restore, verify offsets
- [ ] Exactly-once integration test with mock sink

### Phase 3: Multiple Formats (2-3 days)
- [ ] `AvroDeserializer` using `apache-avro` crate
- [ ] `ProtobufDeserializer` using `prost` crate
- [ ] `DebeziumDeserializer` with Z-set weight extraction (F063)
- [ ] Optional `SchemaRegistryClient` for Avro/Protobuf schema discovery
- [ ] Avro schema to Arrow schema conversion
- [ ] Format-specific unit tests with sample payloads
- [ ] CDC envelope parsing test with before/after/op fields

### Phase 4: Backpressure and Rebalance (2 days)
- [ ] `BackpressureController` with pause/resume logic
- [ ] Channel fill-level monitoring via shared `AtomicU64`
- [ ] `RebalanceState` tracking with cooperative protocol support
- [ ] Graceful shutdown with final offset commit
- [ ] Backpressure test: verify partitions pause/resume at thresholds
- [ ] Rebalance test: verify offset commit on revocation

### Phase 5: Health, Metrics, and SQL Integration (2 days)
- [ ] `health_check()` with broker connectivity and lag computation
- [ ] `KafkaSourceMetrics` with latency histogram
- [ ] Consumer lag computation per partition
- [ ] `ConnectorRegistry` registration for `"kafka"` type
- [ ] SQL integration test: `CREATE SOURCE ... WITH (connector = 'kafka')`
- [ ] `DESCRIBE SOURCE` output with Kafka-specific metadata
- [ ] End-to-end SQL test: create source, query, drop

### Phase 6: Hardening and Documentation (1-2 days)
- [ ] Error recovery with exponential backoff
- [ ] Connection retry on transient Kafka errors
- [ ] SSL/SASL authentication pass-through testing
- [ ] Performance benchmark: throughput, latency, lag
- [ ] API documentation for all public types
- [ ] Example code in `examples/kafka_source.rs`

---

## 10. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Ring 0 latency impact | < 15ns per event | Benchmark: with/without Kafka source attached |
| Source throughput | > 500K records/sec | Benchmark: JSON consumption from 4 partitions |
| Avro throughput | > 800K records/sec | Benchmark: Avro consumption (zero-copy) |
| Checkpoint commit | < 100ms | Benchmark: synchronous Kafka offset commit |
| Backpressure reaction | < 10ms | Benchmark: time from high watermark to partition pause |
| Consumer lag tracking | Accurate to 1s | Verify lag matches `kafka-consumer-groups.sh` output |
| Rebalance recovery | < 5s | Benchmark: time from rebalance trigger to resumed consumption |
| JSON deserialization | < 1us per record | Micro-benchmark with realistic payloads |
| Avro deserialization | < 0.5us per record | Micro-benchmark with schema cache |
| Test coverage | > 80% | `cargo tarpaulin` |
| Test count | 40+ tests | `cargo test --features kafka` |
| Health check accuracy | 100% | Verify against actual broker state |

---

## 11. Module Structure

```
crates/laminar-connectors/src/kafka/
├── mod.rs              # Module root, re-exports
├── source.rs           # KafkaSource struct, SourceConnector impl
├── config.rs           # KafkaSourceConfig, validation, SQL parsing
├── offsets.rs          # Offset tracking, TopicPartition management
├── watermarks.rs       # Per-partition watermark integration (F064)
├── backpressure.rs     # BackpressureController, pause/resume logic
├── rebalance.rs        # RebalanceState, cooperative protocol handling
├── metrics.rs          # KafkaSourceMetrics, LatencyHistogram
├── schema_registry.rs  # SchemaRegistryClient, Avro→Arrow conversion
└── tests/
    ├── config_tests.rs       # Configuration parsing and validation
    ├── consumer_tests.rs     # Consumer lifecycle and polling
    ├── offset_tests.rs       # Offset tracking and checkpoint
    ├── watermark_tests.rs    # Per-partition watermark integration
    ├── backpressure_tests.rs # Pause/resume behavior
    ├── format_tests.rs       # JSON, Avro, Protobuf, Debezium
    └── integration_tests.rs  # End-to-end with testcontainers
```

---

## 12. References

1. **Apache Kafka Consumer API** - Consumer group protocol and rebalancing
   - KIP-429: Cooperative incremental rebalancing (2023)
   - KIP-848: Consumer group protocol rewrite (2024)

2. **rdkafka** - Rust Kafka client
   - `StreamConsumer` for async message consumption
   - `TopicPartitionList` for offset management
   - Consumer group rebalance callbacks

3. **Flink Kafka Connector** - Per-partition watermarks and exactly-once
   - FLIP-27: Source API v2 (split-based model)
   - Per-partition watermark generation
   - Checkpoint-coordinated offset commits

4. **Confluent Schema Registry** - Schema discovery and evolution
   - REST API for schema lookup
   - Avro/Protobuf/JSON Schema support
   - Schema evolution compatibility modes

5. **Debezium** - CDC event envelope format
   - Standard CDC envelope: before/after/op/source/ts_ms
   - Kafka Connect integration patterns

6. **LaminarDB Internal Features**
   - F034: Connector SDK (`SourceConnector` trait)
   - F064: Per-Partition Watermarks (`PartitionedWatermarkTracker`)
   - F065: Keyed Watermarks (`KeyedWatermarkTracker`)
   - F063: Changelog/Retraction (Z-set weights)
   - F023: Exactly-Once Sinks (epoch-based checkpointing)
   - F-STREAM-004: Source API (`Source<T>.push_arrow()`)
   - F-STREAM-007: SQL DDL Translator (`SourceDefinition`)

7. **Research**
   - StreamNative: "Latency Numbers Every Data Streaming Engineer Should Know" (2025)
   - Flink 2.0: "Disaggregated State Management" (VLDB 2025)
   - Kafka Streams exactly-once semantics (KIP-129, KIP-447)

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Consumer Group** | A set of Kafka consumers that share topic-partition assignments for parallel consumption |
| **Topic-Partition** | A single ordered partition within a Kafka topic; the unit of parallelism and offset tracking |
| **Offset** | A monotonically increasing integer identifying a message's position within a partition |
| **Committed Offset** | The offset that has been durably saved; on restart, consumption resumes from here |
| **Consumer Lag** | The difference between the latest available offset and the committed offset per partition |
| **Cooperative Rebalance** | A Kafka rebalance protocol that minimizes partition revocations by only revoking partitions that must move |
| **Backpressure** | A flow control mechanism that slows down the producer (Kafka consumer) when the downstream (Ring 0 channel) is full |
| **Schema Registry** | A central service for managing and validating message schemas (Avro, Protobuf, JSON Schema) |
| **Debezium Envelope** | A standardized CDC message format containing `before`, `after`, `op`, `source`, and `ts_ms` fields |
| **Z-Set** | A mathematical model for incremental computation where records carry integer weights (+1 for insert, -1 for retract) |
| **Watermark** | A timestamp guarantee that no events with timestamps earlier than the watermark will arrive in the future |
| **Idle Partition** | A partition with no new messages for longer than the idle timeout; excluded from watermark minimum computation |

---

## Appendix B: Competitive Comparison

| Feature | LaminarDB F025 | Flink Kafka Connector | Kafka Streams | RisingWave Kafka Source | Materialize Kafka Source |
|---------|---------------|----------------------|---------------|------------------------|-------------------------|
| **Deployment** | Embedded (single binary) | Distributed cluster | Embedded (JVM) | Distributed cluster | Distributed cluster |
| **Ring 0 latency** | <15ns (SPSC only) | N/A (~10ms e2e) | N/A (~1ms e2e) | N/A (~10ms e2e) | N/A (~10ms e2e) |
| **Per-partition watermarks** | Yes (F064) | Yes (built-in) | Yes | Yes | Limited |
| **Exactly-once** | Checkpoint-coordinated | Barrier-based | Kafka transactions | Barrier-based | Barrier-based |
| **Format support** | JSON, Avro, Protobuf, Debezium, CSV | JSON, Avro, Protobuf, Debezium, CSV | Custom SerDe | JSON, Avro, Protobuf | JSON, Avro, Protobuf, CSV |
| **Schema Registry** | Optional (Confluent) | Optional (Confluent) | Optional | Optional (Confluent) | Optional (Confluent, Karapace) |
| **Backpressure** | Partition pause/resume | Credit-based | Bounded buffer | TCP backpressure | TCP backpressure |
| **Consumer group** | Cooperative sticky | Cooperative sticky | Kafka Streams protocol | Custom | Custom |
| **Rebalance handling** | Graceful (offset commit before revoke) | Graceful | Built-in | N/A (static assignment) | N/A (static assignment) |
| **Zero-copy Arrow** | Yes (RecordBatch) | No (Java objects) | No (Java objects) | Yes (Arrow) | No (Differential Dataflow) |
| **Memory model** | Zero allocation hot path | GC-managed (JVM) | GC-managed (JVM) | Rust (allocations on path) | Rust + Timely |
| **Language** | Rust | Java | Java | Rust | Rust |
| **SQL DDL** | `CREATE SOURCE TABLE ... WITH (connector = 'kafka')` | N/A (Java API) | N/A (Java DSL) | `CREATE SOURCE ... WITH (connector = 'kafka')` | `CREATE SOURCE ... FROM KAFKA CONNECTION` |
| **Lag monitoring** | Per-partition metrics | JMX metrics | JMX metrics | System tables | System tables |
| **Health checks** | Built-in `health_check()` | Flink REST API | N/A | pg_stat system | System tables |

---

## Appendix C: Wire Format Examples

### C.1 JSON Message

```json
{
  "order_id": 12345,
  "customer_id": "cust_789",
  "amount": 99.95,
  "ts": "2026-01-28T10:30:00Z"
}
```

Deserialized to Arrow RecordBatch:
```
+----------+-------------+--------+---------------------+
| order_id | customer_id | amount | ts                  |
+----------+-------------+--------+---------------------+
| 12345    | cust_789    | 99.95  | 2026-01-28T10:30:00 |
+----------+-------------+--------+---------------------+
```

### C.2 Debezium CDC Envelope

```json
{
  "before": null,
  "after": {
    "id": 1001,
    "name": "Alice",
    "email": "alice@example.com",
    "updated_at": 1706400000000
  },
  "op": "c",
  "ts_ms": 1706400001000,
  "source": {
    "connector": "postgresql",
    "db": "mydb",
    "schema": "public",
    "table": "users"
  }
}
```

Deserialized with Z-set weight:
```
+------+-------+-------------------+---------------------+--------+
| id   | name  | email             | updated_at          | _weight|
+------+-------+-------------------+---------------------+--------+
| 1001 | Alice | alice@example.com | 2024-01-28T00:00:00 | +1     |
+------+-------+-------------------+---------------------+--------+
```

### C.3 Avro Message (Schema Registry)

Binary Avro with magic byte + schema ID prefix:
```
[0x00][schema_id: 4 bytes][avro_payload: N bytes]
```

The Schema Registry client resolves `schema_id` to an Avro schema, which is converted to an Arrow schema for deserialization.

---

## Appendix D: Error Handling Matrix

| Error Condition | Behavior | Recovery |
|-----------------|----------|----------|
| Broker unreachable | `ConnectorError::Connection` | Exponential backoff retry (100ms-30s) |
| Authentication failure | `ConnectorError::Auth` | Fail immediately, log error |
| Topic does not exist | `ConnectorError::Config` | Fail on `open()`, user must fix topic name |
| Deserialization error (single message) | Log warning, skip message | Continue processing; increment `errors` metric |
| Deserialization error (batch) | `ConnectorError::Serde` | Return partial batch if possible |
| Offset commit failure | `ConnectorError::Transaction` | Retry on next checkpoint; at-least-once fallback |
| Consumer group rebalance | Normal operation | Commit offsets for revoked partitions, resume with new assignment |
| SPSC channel full | Backpressure pause | Pause Kafka partitions until channel drains below low watermark |
| Schema Registry unreachable | `ConnectorError::Connection` | Use cached schema; log warning |
| Poll timeout (no messages) | Return `Ok(None)` | Normal; check idle partitions |
| Connector shutdown | Commit final offsets, close consumer | Graceful; wait for in-flight batches |
