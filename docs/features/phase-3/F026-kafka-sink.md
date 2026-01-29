# F026: Kafka Sink Connector

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0 (Required for end-to-end streaming pipelines)
**Estimated Complexity:** Large (1-2 weeks)
**Prerequisites:** F034 (Connector SDK), F023 (Exactly-Once Sinks), F024 (Two-Phase Commit), F-STREAM-007 (SQL DDL)

---

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F026 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F034, F023, F024, F011B, F063 |
| **Owner** | TBD |

---

## Executive Summary

This specification defines the Kafka Sink Connector: a production-grade connector that writes Arrow RecordBatch data from LaminarDB streaming pipelines to Apache Kafka topics. The connector implements the `SinkConnector` trait from the Connector SDK (F034) and integrates with LaminarDB's exactly-once framework (F023) via Kafka's transactional producer API. It supports configurable partitioning strategies, multiple serialization formats (JSON, Avro, Protobuf), epoch-based commits aligned with LaminarDB checkpointing, schema registry integration, dead-letter queue routing for poison records, and graceful shutdown with in-flight record draining.

The Kafka Sink runs exclusively in Ring 1 (background I/O), communicating with Ring 0 through lock-free SPSC channels. No Kafka client code ever touches the hot path, preserving the sub-500ns latency guarantee.

---

## 1. Problem Statement

### Current Limitation

LaminarDB's Phase 3 streaming API (F-STREAM-001 to F-STREAM-007) and Connector SDK (F034) provide high-performance in-memory channels and standardized traits for external connectors. However, there is no implementation for writing processed results back to Kafka topics. The `laminar-connectors` crate has a feature-gated `kafka` module with no sink implementation:

```rust
// crates/laminar-connectors/src/lib.rs
#[cfg(feature = "kafka")]
pub mod kafka;  // No sink implementation yet
```

Without a Kafka sink, LaminarDB cannot:
- Complete end-to-end streaming pipelines (Kafka in, process, Kafka out)
- Participate in microservice event-driven architectures
- Feed downstream consumers with processed/enriched data
- Guarantee exactly-once delivery to Kafka output topics

### Required Capability

A Kafka Sink Connector that:
1. Implements `SinkConnector` from F034 for standard lifecycle management
2. Uses Kafka's transactional producer for exactly-once delivery
3. Aligns Kafka transaction commits with LaminarDB epoch boundaries
4. Supports configurable partitioning (key hash, round-robin, custom)
5. Supports multiple serialization formats via the F034 `RecordSerializer` trait
6. Integrates with Confluent Schema Registry for Avro/Protobuf schemas
7. Provides dead-letter queue support for records that fail serialization
8. Offers health monitoring, delivery confirmation, and graceful shutdown

### Use Case Diagram

```
+------------------------------------------------------------------------+
|                   Kafka Sink Connector Data Flow                        |
|                                                                        |
|  Ring 0 (Hot Path)        Ring 1 (Background I/O)     External Systems |
|  +-----------------+      +----------------------+    +-------------+  |
|  |                 |      |                      |    |             |  |
|  |  Streaming      | SPSC |   SinkRunner         |    |  Kafka      |  |
|  |  Pipeline       |----->|   +---------------+  |    |  Cluster    |  |
|  |                 |      |   | KafkaSink     |  |--->|             |  |
|  |  Operators      |      |   |               |  |    |  topic-A    |  |
|  |  (map, filter,  |      |   | serialize()   |  |    |  topic-B    |  |
|  |   window, join) |      |   | partition()   |  |    |  topic-DLQ  |  |
|  |                 |      |   | produce()     |  |    |             |  |
|  +-----------------+      |   +---------------+  |    +-------------+  |
|                           |                      |                     |
|  < 500ns budget           |   +---------------+  |    +-------------+  |
|                           |   | Txn Commit    |  |--->| Schema      |  |
|                           |   | (epoch-based) |  |    | Registry    |  |
|                           |   +---------------+  |    +-------------+  |
|                           +----------------------+                     |
|                                                                        |
|  Ring 2 (Control Plane)                                                |
|  +----------------------+                                              |
|  | CREATE SINK DDL      |                                              |
|  | Health monitoring    |                                              |
|  | Config management    |                                              |
|  | Metrics export       |                                              |
|  +----------------------+                                              |
+------------------------------------------------------------------------+
```

---

## 2. Design Principles

### 2.1 Core Principles (Aligned with LaminarDB Philosophy)

| Principle | Application |
|-----------|-------------|
| **Three-ring separation** | KafkaSink runs entirely in Ring 1. Ring 0 only interacts via SPSC Subscription channel. Ring 2 handles DDL, health, and metrics. |
| **Zero-allocation hot path** | No Kafka producer code runs in Ring 0. Serialization and network I/O are decoupled. |
| **Epoch-aligned transactions** | Kafka transactions map 1:1 to LaminarDB epochs. `begin_epoch()` starts a Kafka transaction; `commit_epoch()` commits it. |
| **No custom types unless benchmarks demand** | Reuses F034 `SinkConnector`, `RecordSerializer`, `ConnectorConfig`. Only adds Kafka-specific structs. |
| **SQL-first** | Fully configurable via `CREATE SINK TABLE ... WITH (connector = 'kafka', ...)` DDL syntax. |
| **Fail-safe defaults** | Default to `at-least-once` delivery. Exactly-once requires explicit `'delivery.guarantee' = 'exactly-once'`. |

### 2.2 Industry-Informed Patterns (2025-2026 Research)

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **Kafka Transactions** | Kafka 0.11+ (KIP-98, KIP-447) | `FutureProducer` with `begin_transaction()` / `commit_transaction()` |
| **Epoch-Transaction Alignment** | Flink KafkaSink (FLIP-143) | One Kafka transaction per LaminarDB epoch; commit on checkpoint barrier |
| **Transactional ID Fencing** | Kafka 2.5+ (KIP-360) | `transactional.id` includes sink name + epoch for zombie fencing |
| **Two-Phase Commit Sink** | Flink `TwoPhaseCommitSinkFunction` | Extends F023 `ExactlyOnceSink` + F024 2PC for multi-sink coordination |
| **Schema Registry** | Confluent Platform | Optional Avro/Protobuf schema registration and validation |
| **Dead Letter Queue** | Kafka Connect DLQ | Poison records routed to configurable DLQ topic instead of failing pipeline |
| **Idempotent Producer** | Kafka KIP-98 | `enable.idempotence = true` as baseline, even for at-least-once |
| **Sticky Partitioning** | Kafka KIP-794 | Batch records to same partition to reduce broker round-trips |

---

## 3. Architecture

### 3.1 Core Struct: `KafkaSink`

```rust
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Kafka Sink Connector that writes Arrow RecordBatch data to Kafka topics.
///
/// Implements the `SinkConnector` trait from F034 and integrates with
/// LaminarDB's exactly-once framework via Kafka's transactional producer.
///
/// # Lifecycle
///
/// ```text
/// new() --> open() --> [begin_epoch() --> write_batch()* --> commit_epoch()]* --> close()
///                                                   |
///                                            rollback_epoch()
///                                            (on checkpoint failure)
/// ```
///
/// # Thread Safety
///
/// `KafkaSink` is `Send` but not `Sync`. Each instance runs on a single
/// tokio task in Ring 1 via the `SinkRunner`.
pub struct KafkaSink {
    /// rdkafka transactional producer (FutureProducer for async sends).
    producer: Option<FutureProducer>,
    /// Sink-specific configuration parsed from SQL WITH clause.
    config: KafkaSinkConfig,
    /// Serializer for converting RecordBatch rows to byte payloads.
    /// Resolved from the `format` option (json, avro, protobuf).
    serializer: Box<dyn RecordSerializer>,
    /// Partitioner for determining target partition per record.
    partitioner: Box<dyn KafkaPartitioner>,
    /// Current connector lifecycle state.
    state: ConnectorState,
    /// Current LaminarDB epoch (maps to Kafka transaction).
    current_epoch: u64,
    /// Last successfully committed epoch.
    last_committed_epoch: u64,
    /// Whether a Kafka transaction is currently open.
    transaction_active: bool,
    /// In-flight delivery futures awaiting broker acknowledgment.
    in_flight: Vec<DeliveryFuture>,
    /// Dead letter queue producer (separate, non-transactional).
    dlq_producer: Option<FutureProducer>,
    /// Metrics collector for this sink instance.
    metrics: KafkaSinkMetrics,
    /// Optional schema registry client for Avro/Protobuf.
    schema_registry: Option<SchemaRegistryClient>,
}

/// Configuration for the Kafka Sink Connector.
///
/// Parsed from SQL `WITH (...)` clause options.
#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    /// Kafka broker addresses (comma-separated).
    /// SQL: `'bootstrap.servers' = 'broker1:9092,broker2:9092'`
    pub bootstrap_servers: String,
    /// Target Kafka topic name.
    /// SQL: `topic = 'order-results'`
    pub topic: String,
    /// Delivery guarantee level.
    /// SQL: `'delivery.guarantee' = 'exactly-once' | 'at-least-once'`
    pub delivery_guarantee: DeliveryGuarantee,
    /// Transactional ID prefix for exactly-once.
    /// SQL: `'transactional.id' = 'laminardb-sink-orders'`
    /// Defaults to `laminardb-{sink_name}`.
    pub transactional_id: Option<String>,
    /// Transaction timeout in milliseconds.
    /// SQL: `'transaction.timeout.ms' = '60000'`
    pub transaction_timeout_ms: u64,
    /// Key column name from the RecordBatch (for partitioning).
    /// SQL: `'key.column' = 'order_id'`
    pub key_column: Option<String>,
    /// Partitioning strategy.
    /// SQL: `'partitioner' = 'key-hash' | 'round-robin' | 'sticky'`
    pub partitioner: PartitionStrategy,
    /// Maximum time to wait before sending a batch (milliseconds).
    /// SQL: `'linger.ms' = '5'`
    pub linger_ms: u64,
    /// Maximum batch size in bytes.
    /// SQL: `'batch.size' = '16384'`
    pub batch_size: usize,
    /// Compression algorithm for produced messages.
    /// SQL: `'compression.type' = 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'`
    pub compression: CompressionType,
    /// Number of acknowledgments required.
    /// SQL: `'acks' = 'all' | '1' | '0'`
    pub acks: Acks,
    /// Maximum number of in-flight requests per connection.
    /// SQL: `'max.in.flight.requests' = '5'`
    pub max_in_flight: usize,
    /// Maximum time to wait for delivery confirmation (milliseconds).
    /// SQL: `'delivery.timeout.ms' = '120000'`
    pub delivery_timeout_ms: u64,
    /// Dead letter queue topic for failed records.
    /// SQL: `'dlq.topic' = 'order-results-dlq'`
    pub dlq_topic: Option<String>,
    /// Maximum number of records to buffer before flushing to Kafka.
    /// SQL: `'flush.batch.size' = '1000'`
    pub flush_batch_size: usize,
    /// Schema registry URL for Avro/Protobuf.
    /// SQL: `'schema.registry.url' = 'http://registry:8081'`
    pub schema_registry_url: Option<String>,
    /// Additional rdkafka client properties passed through.
    /// Any `WITH` key not recognized is forwarded to librdkafka.
    pub extra_properties: HashMap<String, String>,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            topic: String::new(),
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            transactional_id: None,
            transaction_timeout_ms: 60_000,
            key_column: None,
            partitioner: PartitionStrategy::KeyHash,
            linger_ms: 5,
            batch_size: 16_384,
            compression: CompressionType::None,
            acks: Acks::All,
            max_in_flight: 5,
            delivery_timeout_ms: 120_000,
            dlq_topic: None,
            flush_batch_size: 1_000,
            schema_registry_url: None,
            extra_properties: HashMap::new(),
        }
    }
}

/// Delivery guarantee level for the Kafka sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// At-least-once: idempotent producer, no transactions.
    /// Records may be duplicated on failure recovery.
    AtLeastOnce,
    /// Exactly-once: transactional producer with epoch-aligned commits.
    /// Requires `transactional.id` and Kafka broker 0.11+.
    ExactlyOnce,
}

/// Partitioning strategy for distributing records across Kafka partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    /// Hash the key column to determine partition (default).
    /// Requires `key.column` to be set.
    KeyHash,
    /// Round-robin across all partitions.
    RoundRobin,
    /// Sticky: batch records to the same partition until batch is full.
    /// Reduces broker round-trips (Kafka KIP-794).
    Sticky,
    /// Explicit partition column from RecordBatch.
    /// Requires `'partition.column' = 'col_name'`.
    Explicit,
}

/// Compression type for produced Kafka messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression.
    None,
    /// Gzip compression.
    Gzip,
    /// Snappy compression.
    Snappy,
    /// LZ4 compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
}

/// Acknowledgment level for Kafka producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    /// No acknowledgment (fire-and-forget).
    None,
    /// Leader acknowledgment only.
    Leader,
    /// All in-sync replica acknowledgment.
    All,
}
```

### 3.2 Partitioner Trait

```rust
/// Trait for determining the target Kafka partition for a record.
///
/// Implementations are stateful to support strategies like round-robin
/// and sticky partitioning.
pub trait KafkaPartitioner: Send {
    /// Returns the target partition for the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The serialized key bytes (None if no key column)
    /// * `num_partitions` - Total number of partitions for the topic
    ///
    /// # Returns
    ///
    /// The target partition number, or `None` for default (librdkafka) partitioning.
    fn partition(&mut self, key: Option<&[u8]>, num_partitions: i32) -> Option<i32>;

    /// Resets the partitioner state (e.g., on epoch boundary).
    fn reset(&mut self);
}

/// Key-hash partitioner (default). Murmur2 hash consistent with Kafka's
/// default partitioner.
pub struct KeyHashPartitioner;

impl KafkaPartitioner for KeyHashPartitioner {
    fn partition(&mut self, key: Option<&[u8]>, num_partitions: i32) -> Option<i32> {
        key.map(|k| {
            // Murmur2 hash consistent with Kafka's DefaultPartitioner
            let hash = murmur2(k) as i32 & 0x7fff_ffff;
            hash % num_partitions
        })
    }

    fn reset(&mut self) {}
}

/// Round-robin partitioner. Distributes records evenly across partitions.
pub struct RoundRobinPartitioner {
    counter: u64,
}

impl KafkaPartitioner for RoundRobinPartitioner {
    fn partition(&mut self, _key: Option<&[u8]>, num_partitions: i32) -> Option<i32> {
        let partition = (self.counter % num_partitions as u64) as i32;
        self.counter += 1;
        Some(partition)
    }

    fn reset(&mut self) {}
}

/// Sticky partitioner. Batches records to the same partition until
/// the batch threshold is reached, then rotates to the next partition.
pub struct StickyPartitioner {
    current_partition: i32,
    records_in_batch: usize,
    batch_threshold: usize,
}

impl KafkaPartitioner for StickyPartitioner {
    fn partition(&mut self, _key: Option<&[u8]>, num_partitions: i32) -> Option<i32> {
        if self.records_in_batch >= self.batch_threshold {
            self.current_partition = (self.current_partition + 1) % num_partitions;
            self.records_in_batch = 0;
        }
        self.records_in_batch += 1;
        Some(self.current_partition)
    }

    fn reset(&mut self) {
        self.records_in_batch = 0;
    }
}
```

### 3.3 SinkConnector Implementation

```rust
use async_trait::async_trait;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

#[async_trait]
impl SinkConnector for KafkaSink {
    fn info(&self) -> ConnectorInfo {
        ConnectorInfo {
            connector_type: "kafka".to_string(),
            description: "Apache Kafka sink connector with transactional support".to_string(),
            config_keys: kafka_sink_config_keys(),
            supported_formats: vec![
                "json".to_string(),
                "avro".to_string(),
                "protobuf".to_string(),
                "csv".to_string(),
                "raw".to_string(),
            ],
        }
    }

    fn state(&self) -> ConnectorState {
        self.state
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.config = KafkaSinkConfig::from_connector_config(config)?;

        // Build rdkafka ClientConfig
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("message.timeout.ms", &self.config.delivery_timeout_ms.to_string())
            .set("linger.ms", &self.config.linger_ms.to_string())
            .set("batch.size", &self.config.batch_size.to_string())
            .set("compression.type", self.config.compression.as_str())
            .set("acks", self.config.acks.as_str())
            .set("max.in.flight.requests.per.connection",
                 &self.config.max_in_flight.to_string());

        // Enable idempotence (required for exactly-once, recommended for at-least-once)
        client_config.set("enable.idempotence", "true");

        // Configure transactional producer for exactly-once
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            let txn_id = self.config.transactional_id
                .clone()
                .unwrap_or_else(|| format!("laminardb-kafka-sink-{}", self.config.topic));
            client_config
                .set("transactional.id", &txn_id)
                .set("transaction.timeout.ms",
                     &self.config.transaction_timeout_ms.to_string());
        }

        // Apply any extra pass-through properties
        for (key, value) in &self.config.extra_properties {
            client_config.set(key, value);
        }

        // Create the producer
        let producer: FutureProducer = client_config
            .set_log_level(RDKafkaLogLevel::Warning)
            .create()
            .map_err(|e| ConnectorError::Connection(
                format!("Failed to create Kafka producer: {e}")
            ))?;

        // Initialize transactions if exactly-once
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            producer.init_transactions(
                Duration::from_millis(self.config.transaction_timeout_ms)
            ).await.map_err(|e| ConnectorError::Transaction(
                format!("Failed to init transactions: {e}")
            ))?;
        }

        // Create DLQ producer if configured
        if let Some(ref dlq_topic) = self.config.dlq_topic {
            let dlq_producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.bootstrap_servers)
                .set("enable.idempotence", "true")
                .create()
                .map_err(|e| ConnectorError::Connection(
                    format!("Failed to create DLQ producer: {e}")
                ))?;
            self.dlq_producer = Some(dlq_producer);
        }

        // Initialize schema registry client if configured
        if let Some(ref url) = self.config.schema_registry_url {
            self.schema_registry = Some(SchemaRegistryClient::new(url)?);
        }

        self.producer = Some(producer);
        self.state = ConnectorState::Running;
        Ok(())
    }

    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<WriteResult, ConnectorError> {
        let producer = self.producer.as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                state: self.state,
                expected: ConnectorState::Running,
            })?;

        // Serialize the RecordBatch into per-row byte payloads
        let payloads = self.serializer.serialize(batch)
            .map_err(ConnectorError::Serde)?;

        // Extract keys if key column is configured
        let keys = self.extract_keys(batch)?;

        let mut records_written: usize = 0;
        let mut bytes_written: u64 = 0;

        for (i, payload) in payloads.iter().enumerate() {
            let key: Option<&[u8]> = keys.as_ref().map(|k| k[i].as_slice());

            // Determine partition
            let partition = self.partitioner.partition(key, self.topic_partitions());

            // Build the Kafka record
            let mut record = FutureRecord::to(&self.config.topic)
                .payload(payload);

            if let Some(k) = key {
                record = record.key(k);
            }
            if let Some(p) = partition {
                record = record.partition(p);
            }

            // Send to Kafka (async, non-blocking)
            match producer.send(record, Duration::from_secs(0)).await {
                Ok(_delivery) => {
                    records_written += 1;
                    bytes_written += payload.len() as u64;
                }
                Err((err, _msg)) => {
                    self.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                    // Route to DLQ if configured, otherwise propagate error
                    if let Some(ref dlq_producer) = self.dlq_producer {
                        self.route_to_dlq(dlq_producer, payload, key, &err).await?;
                        self.metrics.dlq_records.fetch_add(1, Ordering::Relaxed);
                    } else {
                        return Err(ConnectorError::Other(Box::new(err)));
                    }
                }
            }
        }

        self.metrics.records_written.fetch_add(
            records_written as u64, Ordering::Relaxed
        );
        self.metrics.bytes_written.fetch_add(bytes_written, Ordering::Relaxed);

        Ok(WriteResult { records_written, bytes_written })
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch = epoch;

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            let producer = self.producer.as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    state: self.state,
                    expected: ConnectorState::Running,
                })?;

            producer.begin_transaction()
                .map_err(|e| ConnectorError::Transaction(
                    format!("Failed to begin transaction for epoch {epoch}: {e}")
                ))?;

            self.transaction_active = true;
        }

        self.partitioner.reset();
        self.in_flight.clear();
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if epoch != self.current_epoch {
            return Err(ConnectorError::Transaction(format!(
                "Epoch mismatch: expected {}, got {epoch}", self.current_epoch
            )));
        }

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            let producer = self.producer.as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    state: self.state,
                    expected: ConnectorState::Running,
                })?;

            // Flush all pending messages before commit
            producer.flush(Duration::from_millis(self.config.delivery_timeout_ms))
                .map_err(|e| ConnectorError::Transaction(
                    format!("Failed to flush before commit: {e}")
                ))?;

            // Commit the Kafka transaction
            producer.commit_transaction(
                Duration::from_millis(self.config.transaction_timeout_ms)
            ).await.map_err(|e| ConnectorError::Transaction(
                format!("Failed to commit transaction for epoch {epoch}: {e}")
            ))?;

            self.transaction_active = false;
        } else {
            // At-least-once: just flush pending messages
            let producer = self.producer.as_ref().unwrap();
            producer.flush(Duration::from_millis(self.config.delivery_timeout_ms))
                .map_err(|e| ConnectorError::Transaction(
                    format!("Failed to flush for epoch {epoch}: {e}")
                ))?;
        }

        self.last_committed_epoch = epoch;
        self.metrics.epochs_committed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && self.transaction_active
        {
            let producer = self.producer.as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    state: self.state,
                    expected: ConnectorState::Running,
                })?;

            producer.abort_transaction(
                Duration::from_millis(self.config.transaction_timeout_ms)
            ).await.map_err(|e| ConnectorError::Transaction(
                format!("Failed to abort transaction for epoch {epoch}: {e}")
            ))?;

            self.transaction_active = false;
        }

        self.in_flight.clear();
        self.metrics.epochs_rolled_back.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn last_committed_epoch(&self) -> u64 {
        self.last_committed_epoch
    }

    async fn health_check(&self) -> Result<HealthStatus, ConnectorError> {
        // Check producer liveness by attempting metadata fetch
        let healthy = if let Some(ref producer) = self.producer {
            producer.client()
                .fetch_metadata(
                    Some(&self.config.topic),
                    Duration::from_secs(5),
                )
                .is_ok()
        } else {
            false
        };

        Ok(HealthStatus {
            healthy,
            message: if healthy {
                "Kafka producer connected".to_string()
            } else {
                "Kafka producer not connected".to_string()
            },
            last_success: None,
            details: {
                let mut d = HashMap::new();
                d.insert("topic".into(), self.config.topic.clone());
                d.insert("bootstrap.servers".into(),
                         self.config.bootstrap_servers.clone());
                d.insert("delivery.guarantee".into(),
                         format!("{:?}", self.config.delivery_guarantee));
                d.insert("current_epoch".into(), self.current_epoch.to_string());
                d.insert("last_committed_epoch".into(),
                         self.last_committed_epoch.to_string());
                d
            },
        })
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Abort any active transaction
        if self.transaction_active {
            self.rollback_epoch(self.current_epoch).await?;
        }

        // Flush remaining messages with timeout
        if let Some(ref producer) = self.producer {
            producer.flush(Duration::from_secs(30))
                .map_err(|e| ConnectorError::Io(
                    std::io::Error::new(std::io::ErrorKind::Other,
                        format!("Failed to flush on close: {e}"))
                ))?;
        }

        self.producer = None;
        self.dlq_producer = None;
        self.state = ConnectorState::Stopped;
        Ok(())
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.metrics.records_written.load(Ordering::Relaxed),
            bytes_total: self.metrics.bytes_written.load(Ordering::Relaxed),
            errors_total: self.metrics.errors_total.load(Ordering::Relaxed),
            records_per_sec: self.metrics.calculate_throughput(),
            avg_batch_latency_us: self.metrics.avg_write_latency_us(),
            p99_batch_latency_us: self.metrics.p99_write_latency_us(),
            lag: None,
            custom: {
                let mut m = HashMap::new();
                m.insert("epochs_committed".into(),
                    self.metrics.epochs_committed.load(Ordering::Relaxed) as f64);
                m.insert("epochs_rolled_back".into(),
                    self.metrics.epochs_rolled_back.load(Ordering::Relaxed) as f64);
                m.insert("dlq_records".into(),
                    self.metrics.dlq_records.load(Ordering::Relaxed) as f64);
                m.insert("transaction_active".into(),
                    if self.transaction_active { 1.0 } else { 0.0 });
                m
            },
        }
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            transactional: self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce,
            idempotent: true,  // Always enable idempotent producer
            partitioned: true,
            schema_evolution: self.schema_registry.is_some(),
            two_phase_commit: self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce,
        }
    }
}
```

### 3.4 Metrics

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Metrics for the Kafka Sink Connector.
///
/// All counters use relaxed atomic ordering (snapshot-only reads,
/// no cross-counter consistency required).
pub struct KafkaSinkMetrics {
    /// Total records successfully written to Kafka.
    pub records_written: AtomicU64,
    /// Total bytes written to Kafka (payload only).
    pub bytes_written: AtomicU64,
    /// Total errors encountered.
    pub errors_total: AtomicU64,
    /// Total epochs committed.
    pub epochs_committed: AtomicU64,
    /// Total epochs rolled back.
    pub epochs_rolled_back: AtomicU64,
    /// Total records routed to dead letter queue.
    pub dlq_records: AtomicU64,
    /// Total serialization errors.
    pub serialization_errors: AtomicU64,
    /// Timestamp of last successful write (epoch millis).
    pub last_write_timestamp: AtomicU64,
    /// Start time for throughput calculation.
    start_time: Instant,
    /// Write latency histogram (Ring 1 only, not allocation-sensitive).
    write_latencies_us: parking_lot::Mutex<Vec<u64>>,
}

impl KafkaSinkMetrics {
    /// Creates a new metrics instance.
    pub fn new() -> Self {
        Self {
            records_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            epochs_committed: AtomicU64::new(0),
            epochs_rolled_back: AtomicU64::new(0),
            dlq_records: AtomicU64::new(0),
            serialization_errors: AtomicU64::new(0),
            last_write_timestamp: AtomicU64::new(0),
            start_time: Instant::now(),
            write_latencies_us: parking_lot::Mutex::new(Vec::with_capacity(10_000)),
        }
    }

    /// Calculates current throughput (records/sec).
    pub fn calculate_throughput(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.records_written.load(Ordering::Relaxed) as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Returns average write latency in microseconds.
    pub fn avg_write_latency_us(&self) -> u64 {
        let latencies = self.write_latencies_us.lock();
        if latencies.is_empty() {
            return 0;
        }
        latencies.iter().sum::<u64>() / latencies.len() as u64
    }

    /// Returns p99 write latency in microseconds.
    pub fn p99_write_latency_us(&self) -> u64 {
        let mut latencies = self.write_latencies_us.lock().clone();
        if latencies.is_empty() {
            return 0;
        }
        latencies.sort_unstable();
        latencies[(latencies.len() * 99) / 100]
    }

    /// Records a write latency sample.
    pub fn record_write_latency(&self, latency_us: u64) {
        let mut latencies = self.write_latencies_us.lock();
        if latencies.len() >= 10_000 {
            // Ring buffer behavior: drop oldest
            latencies.remove(0);
        }
        latencies.push(latency_us);
    }
}
```

### 3.5 Dead Letter Queue Support

```rust
impl KafkaSink {
    /// Routes a failed record to the dead letter queue topic.
    ///
    /// DLQ records include error metadata in Kafka headers:
    /// - `__dlq.error`: Error message
    /// - `__dlq.topic`: Original target topic
    /// - `__dlq.timestamp`: Time of failure (epoch millis)
    /// - `__dlq.epoch`: LaminarDB epoch when failure occurred
    async fn route_to_dlq(
        &self,
        dlq_producer: &FutureProducer,
        payload: &[u8],
        key: Option<&[u8]>,
        error: &rdkafka::error::KafkaError,
    ) -> Result<(), ConnectorError> {
        let dlq_topic = self.config.dlq_topic.as_ref()
            .ok_or_else(|| ConnectorError::Config(
                "DLQ topic not configured".to_string()
            ))?;

        let mut headers = rdkafka::message::OwnedHeaders::new();
        headers = headers
            .insert(rdkafka::message::Header {
                key: "__dlq.error",
                value: Some(error.to_string().as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "__dlq.topic",
                value: Some(self.config.topic.as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "__dlq.timestamp",
                value: Some(
                    &chrono::Utc::now().timestamp_millis().to_string().into_bytes()
                ),
            })
            .insert(rdkafka::message::Header {
                key: "__dlq.epoch",
                value: Some(&self.current_epoch.to_string().into_bytes()),
            });

        let mut record = FutureRecord::to(dlq_topic)
            .payload(payload)
            .headers(headers);

        if let Some(k) = key {
            record = record.key(k);
        }

        dlq_producer.send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| ConnectorError::Other(Box::new(e)))?;

        Ok(())
    }

    /// Extracts key bytes from the configured key column.
    ///
    /// Returns `None` if no key column is configured.
    /// Returns `Vec<Vec<u8>>` with one key per row.
    fn extract_keys(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<Vec<Vec<u8>>>, ConnectorError> {
        let key_col = match &self.config.key_column {
            Some(col) => col,
            None => return Ok(None),
        };

        let col_idx = batch.schema().index_of(key_col)
            .map_err(|_| ConnectorError::Config(
                format!("Key column '{key_col}' not found in schema")
            ))?;

        let array = batch.column(col_idx);
        let mut keys = Vec::with_capacity(batch.num_rows());

        // Serialize each key value to bytes using Arrow's display format
        for i in 0..batch.num_rows() {
            let key_str = arrow::util::display::array_value_to_string(array, i)
                .map_err(|e| ConnectorError::Serde(SerdeError::TypeConversion {
                    from: format!("{:?}", array.data_type()),
                    to: "bytes".to_string(),
                }))?;
            keys.push(key_str.into_bytes());
        }

        Ok(Some(keys))
    }

    /// Returns the number of partitions for the configured topic.
    ///
    /// Fetched once on open() and cached.
    fn topic_partitions(&self) -> i32 {
        // Cached from metadata fetch during open()
        self.metrics.topic_partitions.load(Ordering::Relaxed) as i32
    }
}
```

### 3.6 Schema Registry Integration

```rust
/// Client for Confluent Schema Registry.
///
/// Used for Avro and Protobuf serialization with schema evolution.
pub struct SchemaRegistryClient {
    /// Schema registry base URL.
    url: String,
    /// HTTP client for registry API calls.
    client: reqwest::Client,
    /// Cached schema IDs by subject.
    cache: HashMap<String, u32>,
}

impl SchemaRegistryClient {
    /// Creates a new schema registry client.
    pub fn new(url: &str) -> Result<Self, ConnectorError> {
        Ok(Self {
            url: url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            cache: HashMap::new(),
        })
    }

    /// Registers a schema for the given subject and returns the schema ID.
    ///
    /// Caches the result for subsequent calls with the same subject.
    pub async fn register_schema(
        &mut self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
    ) -> Result<u32, ConnectorError> {
        if let Some(&id) = self.cache.get(subject) {
            return Ok(id);
        }

        let url = format!("{}/subjects/{}/versions", self.url, subject);
        let body = serde_json::json!({
            "schema": schema,
            "schemaType": schema_type.as_str(),
        });

        let response = self.client.post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(
                format!("Schema registry error: {e}")
            ))?;

        let result: serde_json::Value = response.json().await
            .map_err(|e| ConnectorError::Serde(SerdeError::Json(e.to_string())))?;

        let id = result["id"].as_u64()
            .ok_or_else(|| ConnectorError::Serde(
                SerdeError::Json("Missing schema ID in response".into())
            ))? as u32;

        self.cache.insert(subject.to_string(), id);
        Ok(id)
    }

    /// Gets a schema by ID.
    pub async fn get_schema(
        &self,
        id: u32,
    ) -> Result<String, ConnectorError> {
        let url = format!("{}/schemas/ids/{}", self.url, id);
        let response = self.client.get(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(
                format!("Schema registry error: {e}")
            ))?;

        let result: serde_json::Value = response.json().await
            .map_err(|e| ConnectorError::Serde(SerdeError::Json(e.to_string())))?;

        result["schema"].as_str()
            .map(String::from)
            .ok_or_else(|| ConnectorError::Serde(
                SerdeError::Json("Missing schema in response".into())
            ))
    }
}

/// Schema type for the schema registry.
#[derive(Debug, Clone, Copy)]
pub enum SchemaType {
    /// Apache Avro.
    Avro,
    /// Protocol Buffers.
    Protobuf,
    /// JSON Schema.
    JsonSchema,
}

impl SchemaType {
    fn as_str(&self) -> &str {
        match self {
            Self::Avro => "AVRO",
            Self::Protobuf => "PROTOBUF",
            Self::JsonSchema => "JSON",
        }
    }
}
```

### 3.7 Three-Ring Integration

| Ring | Kafka Sink Responsibilities |
|------|----------------------------|
| **Ring 0 (Hot Path)** | Streaming pipeline operators emit results into `Sink<T>` SPSC channel. No Kafka code runs here. Budget: < 500ns per event. |
| **Ring 1 (Background I/O)** | `SinkRunner` polls the `Subscription`, batches records, invokes `KafkaSink.write_batch()`, `begin_epoch()`, `commit_epoch()`. Serialization, network I/O, transaction management all happen here. |
| **Ring 2 (Control Plane)** | SQL DDL processing (`CREATE SINK TABLE`), connector lifecycle (start/stop/restart), health monitoring, metrics export, configuration changes. |

```
Ring 0                 Ring 1                         Ring 2
+---------+  SPSC     +---------------------+        +-----------------+
| Operator|---------->| SinkRunner          |        | ConnectorRuntime|
| Pipeline|  channel  |   |                 |        |   |             |
|         |  (~5ns)   |   v                 |        |   v             |
|         |           | KafkaSink           |        | CREATE SINK DDL |
|         |           |   |                 |        | SHOW SINKS      |
|         |           |   v                 |        | DESCRIBE SINK   |
+---------+           | Serialize (1-50us)  |        | Health check    |
                      |   |                 |        | Metrics export  |
                      |   v                 |        +-----------------+
                      | Partition           |
                      |   |                 |
                      |   v                 |
                      | FutureProducer.send |------> Kafka Cluster
                      |   |                 |
                      |   v                 |
                      | commit_transaction  |
                      +---------------------+
```

---

## 4. Latency Considerations

### 4.1 Hot Path Budget

The Kafka Sink is designed to have zero impact on Ring 0 latency. All heavy operations are confined to Ring 1:

| Component | Ring | Latency | Notes |
|-----------|------|---------|-------|
| Subscription poll (Ring 0 emit) | Ring 0 | ~5ns | Lock-free SPSC push, existing implementation |
| **Total Ring 0 impact** | **Ring 0** | **~5ns** | **Channel operation only** |
| RecordBatch serialization | Ring 1 | 1-50us | JSON: ~5us/record, Avro: ~2us/record |
| Key extraction + hashing | Ring 1 | ~100ns | Murmur2 hash of key bytes |
| Partitioner decision | Ring 1 | ~50ns | Arithmetic only |
| `FutureProducer.send()` | Ring 1 | 10-100us | Enqueue to librdkafka buffer (not network) |
| Network round-trip | Ring 1 | 1-10ms | Kafka broker acknowledgment |
| Transaction commit | Ring 1 | 5-50ms | Kafka transaction protocol |
| DLQ routing (on error) | Ring 1 | 1-10ms | Separate producer, best-effort |

### 4.2 Batching Strategy

Batching is critical for Kafka throughput. Two levels of batching are employed:

1. **LaminarDB-level**: The `SinkRunner` accumulates records from the `Subscription` until `flush_batch_size` is reached, then calls `write_batch()`.

2. **rdkafka-level**: librdkafka's internal batching (`linger.ms`, `batch.size`) coalesces individual `send()` calls into broker requests.

```
Subscription --> SinkRunner batch (flush_batch_size)
                       |
                       v
                  KafkaSink.write_batch()
                       |
                       v (per record)
                  FutureProducer.send()
                       |
                       v (librdkafka internal)
                  linger.ms / batch.size batching
                       |
                       v
                  Broker request (ProduceRequest)
```

---

## 5. SQL Integration

### 5.1 CREATE SINK TABLE Syntax

```sql
-- Basic Kafka sink with JSON serialization
CREATE SINK TABLE order_results
FROM processed_orders
WITH (
    connector = 'kafka',
    topic = 'order-results',
    'bootstrap.servers' = 'localhost:9092',
    format = 'json'
);

-- Exactly-once Kafka sink with key partitioning
CREATE SINK TABLE order_results
FROM processed_orders
WITH (
    connector = 'kafka',
    topic = 'order-results',
    'bootstrap.servers' = 'broker1:9092,broker2:9092',
    format = 'json',
    'delivery.guarantee' = 'exactly-once',
    'transaction.timeout.ms' = '60000',
    'key.column' = 'order_id',
    'partitioner' = 'key-hash'
);

-- Avro sink with schema registry
CREATE SINK TABLE enriched_events
FROM enrichment_pipeline
WITH (
    connector = 'kafka',
    topic = 'enriched-events',
    'bootstrap.servers' = 'broker1:9092',
    format = 'avro',
    'schema.registry.url' = 'http://schema-registry:8081',
    'delivery.guarantee' = 'exactly-once',
    'compression.type' = 'zstd'
);

-- High-throughput sink with batching and DLQ
CREATE SINK TABLE metrics_output
FROM aggregated_metrics
WITH (
    connector = 'kafka',
    topic = 'metrics-aggregated',
    'bootstrap.servers' = 'broker1:9092,broker2:9092,broker3:9092',
    format = 'json',
    'delivery.guarantee' = 'at-least-once',
    'linger.ms' = '50',
    'batch.size' = '65536',
    'compression.type' = 'lz4',
    'flush.batch.size' = '5000',
    'dlq.topic' = 'metrics-aggregated-dlq'
);

-- Round-robin partitioning (no key)
CREATE SINK TABLE broadcast_alerts
FROM alert_pipeline
WITH (
    connector = 'kafka',
    topic = 'system-alerts',
    'bootstrap.servers' = 'localhost:9092',
    format = 'json',
    'partitioner' = 'round-robin',
    'acks' = 'all'
);
```

### 5.2 DDL Resolution Flow

```
CREATE SINK TABLE ... WITH (connector = 'kafka', ...)
       |
       v
StreamingParser::parse_create_sink()
       |
       v
SinkDefinition { connector: "kafka", options: {...} }
       |
       v
ConnectorRegistry::create_sink("kafka", config)
       |
       v
KafkaSinkFactory::create(config) --> KafkaSink
       |
       v
ConnectorRuntime::start_sink(KafkaSink, serializer, config)
       |
       v
SinkRunner { connector: KafkaSink, subscription: Subscription<ArrowRecord> }
```

---

## 6. Rust API Examples

### 6.1 Programmatic Usage (laminar-db crate)

```rust
use laminardb::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = LaminarDB::open()?;

    // Create a source
    db.execute("
        CREATE SOURCE TABLE orders (
            order_id BIGINT,
            customer_id VARCHAR,
            amount DOUBLE,
            ts TIMESTAMP,
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            connector = 'kafka',
            topic = 'orders',
            'bootstrap.servers' = 'localhost:9092',
            format = 'json'
        )
    ").await?;

    // Create a processing query
    db.execute("
        CREATE MATERIALIZED VIEW order_totals AS
        SELECT
            customer_id,
            SUM(amount) AS total_amount,
            COUNT(*) AS order_count
        FROM orders
        GROUP BY
            TUMBLE(ts, INTERVAL '1' MINUTE),
            customer_id
        EMIT ON WINDOW CLOSE
    ").await?;

    // Create a Kafka sink for the results
    db.execute("
        CREATE SINK TABLE customer_totals_sink
        FROM order_totals
        WITH (
            connector = 'kafka',
            topic = 'customer-totals',
            'bootstrap.servers' = 'localhost:9092',
            format = 'json',
            'delivery.guarantee' = 'exactly-once',
            'key.column' = 'customer_id',
            'compression.type' = 'snappy'
        )
    ").await?;

    // Pipeline is now running:
    // Kafka(orders) --> TUMBLE window --> SUM/COUNT --> Kafka(customer-totals)

    Ok(())
}
```

### 6.2 Direct Connector API Usage

```rust
use laminar_connectors::kafka::sink::{KafkaSink, KafkaSinkConfig, DeliveryGuarantee};
use laminar_connectors::sdk::{SinkConnector, ConnectorConfig};
use laminar_connectors::serde::json::JsonSerializer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create Kafka sink directly
    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "output-topic".to_string(),
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
            transaction_timeout_ms: 60_000,
            key_column: Some("id".to_string()),
            ..Default::default()
        },
        Box::new(JsonSerializer::default()),
    );

    // Open the connector
    let config = ConnectorConfig::from_options(&HashMap::from([
        ("connector".into(), "kafka".into()),
        ("topic".into(), "output-topic".into()),
        ("bootstrap.servers".into(), "localhost:9092".into()),
    ]))?;
    sink.open(&config).await?;

    // Begin epoch (starts Kafka transaction)
    sink.begin_epoch(1).await?;

    // Write batches
    let batch = create_test_batch(); // Arrow RecordBatch
    sink.write_batch(&batch).await?;

    // Commit epoch (commits Kafka transaction)
    sink.commit_epoch(1).await?;

    // Check health
    let health = sink.health_check().await?;
    println!("Healthy: {}, Message: {}", health.healthy, health.message);

    // Close
    sink.close().await?;
    Ok(())
}
```

### 6.3 Integration with ConnectorRuntime

```rust
use laminar_connectors::runtime::ConnectorRuntime;
use laminar_connectors::kafka::sink::KafkaSinkFactory;
use laminar_connectors::serde::json::JsonSerializer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut runtime = ConnectorRuntime::new();

    // Register Kafka sink factory
    runtime.registry_mut().register_sink("kafka", Box::new(|config| {
        KafkaSinkFactory::create(config)
    }));

    // Start sink (bridges Subscription to KafkaSink via SinkRunner)
    let connector = KafkaSinkFactory::create(&config)?;
    let serializer = Box::new(JsonSerializer::default());

    let sink_handle = runtime.start_sink(
        "order-results-sink",
        connector,
        serializer,
        config,
    ).await?;

    // The SinkRunner is now consuming from the Subscription
    // and writing to Kafka in Ring 1

    // Check health
    let health = runtime.health().await;
    println!("Sink health: {:?}", health.get("order-results-sink"));

    // Graceful shutdown
    runtime.stop_sink("order-results-sink").await?;
    Ok(())
}
```

---

## 7. Configuration Reference

### 7.1 Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'kafka'` |
| `topic` | String | Target Kafka topic name |
| `'bootstrap.servers'` | String | Comma-separated Kafka broker addresses |

### 7.2 Serialization Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `format` | String | `'json'` | Serialization format: `json`, `avro`, `protobuf`, `csv`, `raw` |
| `'schema.registry.url'` | String | None | Confluent Schema Registry URL (required for `avro`/`protobuf`) |
| `'json.include.nulls'` | Boolean | `true` | Include null fields in JSON output |
| `'json.timestamp.format'` | String | `'iso8601'` | Timestamp format: `iso8601`, `epoch_millis`, `epoch_micros` |

### 7.3 Delivery Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `'delivery.guarantee'` | String | `'at-least-once'` | `at-least-once` or `exactly-once` |
| `'transactional.id'` | String | Auto-generated | Kafka transactional ID (exactly-once only) |
| `'transaction.timeout.ms'` | Integer | `60000` | Transaction timeout in milliseconds |
| `'acks'` | String | `'all'` | Acknowledgment level: `0`, `1`, `all` |
| `'delivery.timeout.ms'` | Integer | `120000` | Maximum delivery timeout in milliseconds |

### 7.4 Partitioning Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `'key.column'` | String | None | Column name to use as Kafka message key |
| `'partitioner'` | String | `'key-hash'` | Strategy: `key-hash`, `round-robin`, `sticky`, `explicit` |
| `'partition.column'` | String | None | Column with explicit partition numbers (for `explicit` strategy) |

### 7.5 Performance Tuning Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `'linger.ms'` | Integer | `5` | Time to wait before sending batch |
| `'batch.size'` | Integer | `16384` | Maximum batch size in bytes |
| `'compression.type'` | String | `'none'` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `'max.in.flight.requests'` | Integer | `5` | Max in-flight requests per connection |
| `'flush.batch.size'` | Integer | `1000` | Records to buffer before calling `write_batch()` |

### 7.6 Error Handling Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `'dlq.topic'` | String | None | Dead letter queue topic for failed records |
| `'retries'` | Integer | `2147483647` | Number of retries for transient errors |
| `'retry.backoff.ms'` | Integer | `100` | Backoff between retries |

### 7.7 Pass-Through Options

Any option not listed above is passed directly to the underlying `librdkafka` client configuration. This allows fine-grained control over:
- Security (SASL, SSL/TLS)
- Network tuning
- Broker-specific settings

Example:
```sql
WITH (
    connector = 'kafka',
    topic = 'secure-topic',
    'bootstrap.servers' = 'broker:9093',
    format = 'json',
    -- Security pass-through options
    'security.protocol' = 'SASL_SSL',
    'sasl.mechanism' = 'PLAIN',
    'sasl.username' = 'myuser',
    'sasl.password' = 'mypassword',
    'ssl.ca.location' = '/etc/kafka/ca.pem'
);
```

---

## 8. Error Handling

### 8.1 Error Categories

| Error Type | Behavior | Recovery |
|------------|----------|----------|
| **Serialization failure** | Route to DLQ if configured; otherwise fail batch | Skip record (DLQ) or retry batch |
| **Broker unavailable** | Retry with exponential backoff | rdkafka handles retries internally |
| **Transaction timeout** | Abort transaction, rollback epoch | Coordinator retries epoch |
| **Authorization failure** | Fail immediately, transition to `Failed` state | Requires config fix |
| **Schema registry error** | Fail batch, log error | Retry on next batch |
| **Producer queue full** | Backpressure: block until space available | Bounded by `queue.buffering.max.messages` |

### 8.2 Exactly-Once Recovery Protocol

```
On checkpoint failure or crash recovery:

1. ConnectorRuntime restores KafkaSink from checkpoint
2. KafkaSink.open() re-creates FutureProducer with same transactional.id
3. init_transactions() fences zombie producers with same transactional.id
4. rollback_epoch(last_uncommitted_epoch) aborts any in-doubt transaction
5. begin_epoch(last_committed_epoch + 1) starts fresh transaction
6. Normal processing resumes -- no duplicates, no data loss

Kafka's transaction protocol guarantees:
- Committed records are never lost
- Uncommitted records are never visible to consumers (read_committed)
- Zombie producers (old instances) are fenced by init_transactions()
```

### 8.3 Graceful Shutdown

```
On shutdown signal:

1. SinkRunner stops polling Subscription
2. KafkaSink.close() is called
3. If transaction is active:
   a. Flush all pending messages (30s timeout)
   b. Commit current transaction if all deliveries succeed
   c. Abort transaction if any delivery failed
4. Producer.flush() drains internal librdkafka queue
5. Producer is dropped (connection closed)
6. SinkRunner task exits

On forceful shutdown (timeout exceeded):
1. Abort active transaction
2. Drop producer immediately
3. Records in librdkafka buffer are lost (will be replayed from checkpoint)
```

---

## 9. Exactly-Once Semantics Deep Dive

### 9.1 Epoch-Transaction Mapping

LaminarDB epochs map 1:1 to Kafka transactions:

```
LaminarDB Epochs          Kafka Transactions
+------------------+      +------------------+
| Epoch 1          |      | Txn 1            |
|  write_batch()   | ---> |  produce()       |
|  write_batch()   | ---> |  produce()       |
|  commit_epoch(1) | ---> |  commit_txn()    |
+------------------+      +------------------+
| Epoch 2          |      | Txn 2            |
|  write_batch()   | ---> |  produce()       |
|  commit_epoch(2) | ---> |  commit_txn()    |
+------------------+      +------------------+
| Epoch 3          |      | Txn 3            |
|  write_batch()   | ---> |  produce()       |
|  [FAILURE]       |      |                  |
|  rollback(3)     | ---> |  abort_txn()     |
+------------------+      +------------------+
| Epoch 3 (retry)  |      | Txn 3 (retry)    |
|  write_batch()   | ---> |  produce()       |
|  commit_epoch(3) | ---> |  commit_txn()    |
+------------------+      +------------------+
```

### 9.2 Transactional ID Strategy

The transactional ID must be unique per sink instance and stable across restarts:

```
transactional.id = "laminardb-{sink_name}-{core_id}"

Examples:
- "laminardb-order-results-0"  (core 0)
- "laminardb-order-results-1"  (core 1)
```

For thread-per-core deployments, each core's sink instance has its own transactional ID, preventing conflicts.

### 9.3 Consumer Offsets in Transaction (Source + Sink Coordination)

When operating in a full Kafka-to-Kafka pipeline, the sink can atomically commit consumer offsets within the transaction:

```rust
/// Commits consumer offsets as part of the sink transaction.
///
/// This ensures exactly-once semantics for the full pipeline:
/// consumed offsets and produced records are committed atomically.
async fn commit_epoch_with_offsets(
    &mut self,
    epoch: u64,
    consumer_offsets: &SourceCheckpoint,
) -> Result<(), ConnectorError> {
    if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
        return self.commit_epoch(epoch).await;
    }

    let producer = self.producer.as_ref().unwrap();

    // Flush all pending messages
    producer.flush(Duration::from_millis(self.config.delivery_timeout_ms))?;

    // Convert source offsets to Kafka TopicPartitionList
    let tpl = offsets_to_tpl(consumer_offsets)?;

    // Send offsets to transaction (atomic with produced records)
    producer.send_offsets_to_transaction(
        &tpl,
        &consumer_group_metadata,
        Duration::from_millis(self.config.transaction_timeout_ms),
    ).await?;

    // Commit the transaction (records + offsets atomically)
    producer.commit_transaction(
        Duration::from_millis(self.config.transaction_timeout_ms),
    ).await?;

    self.transaction_active = false;
    self.last_committed_epoch = epoch;
    Ok(())
}
```

---

## 10. Implementation Roadmap

### Phase 1: Core KafkaSink (3-4 days)
- [ ] Define `KafkaSinkConfig` with all configuration options
- [ ] Implement `KafkaSink` struct with `FutureProducer`
- [ ] Implement `SinkConnector` trait: `open()`, `write_batch()`, `close()`
- [ ] Implement `begin_epoch()` / `commit_epoch()` / `rollback_epoch()` with Kafka transactions
- [ ] Add `KafkaSinkConfig::from_connector_config()` for SQL option parsing
- [ ] Unit tests for config parsing and validation
- [ ] Integration test with embedded Kafka (testcontainers)

### Phase 2: Partitioning and Key Extraction (2-3 days)
- [ ] Implement `KafkaPartitioner` trait
- [ ] Implement `KeyHashPartitioner` (Murmur2, Kafka-compatible)
- [ ] Implement `RoundRobinPartitioner`
- [ ] Implement `StickyPartitioner`
- [ ] Implement key extraction from `RecordBatch` column
- [ ] Unit tests for each partitioner strategy
- [ ] Integration test: verify partition assignment

### Phase 3: Serialization Integration (2-3 days)
- [ ] Wire `RecordSerializer` trait to `write_batch()`
- [ ] Test JSON serialization end-to-end
- [ ] Add Avro serializer with schema registry integration
- [ ] Add Protobuf serializer
- [ ] Schema registry client implementation
- [ ] Tests for each serialization format with edge cases

### Phase 4: Error Handling and DLQ (1-2 days)
- [ ] Implement dead letter queue routing
- [ ] Add DLQ headers (error, topic, timestamp, epoch)
- [ ] Implement error categorization and retry logic
- [ ] Test DLQ routing for serialization failures
- [ ] Test DLQ routing for produce failures

### Phase 5: Metrics and Health (1-2 days)
- [ ] Implement `KafkaSinkMetrics` with atomic counters
- [ ] Implement `health_check()` via metadata fetch
- [ ] Add write latency histogram
- [ ] Expose throughput calculation
- [ ] Wire metrics to `ConnectorMetrics` return
- [ ] Test metrics accuracy

### Phase 6: SQL DDL and Runtime Integration (1-2 days)
- [ ] Register `KafkaSinkFactory` in `ConnectorRegistry`
- [ ] Wire `CREATE SINK TABLE ... WITH (connector = 'kafka')` end-to-end
- [ ] Test DDL resolution from SQL to running KafkaSink
- [ ] Integration with `SinkRunner` for background execution
- [ ] Graceful shutdown test (drain in-flight records)

### Phase 7: Exactly-Once End-to-End (2-3 days)
- [ ] Test exactly-once: write + commit + consume with `read_committed`
- [ ] Test exactly-once: abort on failure + replay
- [ ] Test zombie fencing after restart with same `transactional.id`
- [ ] Test `send_offsets_to_transaction` for source-sink coordination
- [ ] Benchmark: measure overhead of exactly-once vs at-least-once
- [ ] Stress test: high-throughput with transaction commits

---

## 11. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Ring 0 latency impact | < 10ns per event | Benchmark with/without Kafka sink |
| Sink throughput (at-least-once) | > 300K records/sec | Benchmark with embedded Kafka |
| Sink throughput (exactly-once) | > 100K records/sec | Benchmark with transactional producer |
| Transaction commit overhead | < 50ms per epoch | Measure `commit_epoch()` latency |
| Serialization throughput (JSON) | > 500K records/sec | Micro-benchmark on RecordBatch |
| Serialization throughput (Avro) | > 800K records/sec | Micro-benchmark with schema |
| DLQ routing latency | < 10ms per record | Measure `route_to_dlq()` latency |
| Recovery time (exactly-once) | < 5s from crash to first produce | End-to-end recovery test |
| Test coverage | > 80% | cargo tarpaulin |
| Test count | 25+ tests | cargo test |

---

## 12. Module Structure

```
crates/laminar-connectors/src/kafka/
    mod.rs              # Re-exports, feature gate
    sink.rs             # KafkaSink struct, SinkConnector impl
    config.rs           # KafkaSinkConfig, DeliveryGuarantee, enums
    partitioner.rs      # KafkaPartitioner trait, KeyHash/RoundRobin/Sticky
    metrics.rs          # KafkaSinkMetrics
    dlq.rs              # Dead letter queue routing
    schema_registry.rs  # SchemaRegistryClient
    factory.rs          # KafkaSinkFactory for ConnectorRegistry
    tests/
        sink_tests.rs           # Unit tests for KafkaSink
        config_tests.rs         # Config parsing tests
        partitioner_tests.rs    # Partitioner strategy tests
        integration_tests.rs    # End-to-end with testcontainers
        exactly_once_tests.rs   # Transaction semantics tests
```

---

## 13. Test Plan

### 13.1 Unit Tests

```rust
#[test]
fn test_kafka_sink_config_from_options() {
    let options = HashMap::from([
        ("connector".into(), "kafka".into()),
        ("topic".into(), "test-topic".into()),
        ("bootstrap.servers".into(), "localhost:9092".into()),
        ("delivery.guarantee".into(), "exactly-once".into()),
        ("key.column".into(), "order_id".into()),
        ("linger.ms".into(), "10".into()),
        ("compression.type".into(), "zstd".into()),
    ]);
    let config = KafkaSinkConfig::from_options(&options).unwrap();
    assert_eq!(config.topic, "test-topic");
    assert_eq!(config.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
    assert_eq!(config.key_column, Some("order_id".to_string()));
    assert_eq!(config.linger_ms, 10);
    assert_eq!(config.compression, CompressionType::Zstd);
}

#[test]
fn test_kafka_sink_config_defaults() {
    let options = HashMap::from([
        ("connector".into(), "kafka".into()),
        ("topic".into(), "test".into()),
        ("bootstrap.servers".into(), "localhost:9092".into()),
    ]);
    let config = KafkaSinkConfig::from_options(&options).unwrap();
    assert_eq!(config.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
    assert_eq!(config.compression, CompressionType::None);
    assert_eq!(config.acks, Acks::All);
    assert_eq!(config.linger_ms, 5);
    assert_eq!(config.batch_size, 16384);
}

#[test]
fn test_kafka_sink_config_missing_required() {
    let options = HashMap::from([
        ("connector".into(), "kafka".into()),
        // Missing topic and bootstrap.servers
    ]);
    assert!(KafkaSinkConfig::from_options(&options).is_err());
}

#[test]
fn test_key_hash_partitioner() {
    let mut partitioner = KeyHashPartitioner;
    let key = b"order-123";
    let p1 = partitioner.partition(Some(key), 8);
    let p2 = partitioner.partition(Some(key), 8);
    // Same key should always get same partition
    assert_eq!(p1, p2);
    // Partition should be in range
    assert!(p1.unwrap() >= 0 && p1.unwrap() < 8);
}

#[test]
fn test_round_robin_partitioner() {
    let mut partitioner = RoundRobinPartitioner { counter: 0 };
    let partitions: Vec<i32> = (0..6)
        .map(|_| partitioner.partition(None, 3).unwrap())
        .collect();
    assert_eq!(partitions, vec![0, 1, 2, 0, 1, 2]);
}

#[test]
fn test_sticky_partitioner() {
    let mut partitioner = StickyPartitioner {
        current_partition: 0,
        records_in_batch: 0,
        batch_threshold: 3,
    };
    let partitions: Vec<i32> = (0..9)
        .map(|_| partitioner.partition(None, 4).unwrap())
        .collect();
    // Should stick to partition 0 for 3 records, then 1, then 2
    assert_eq!(partitions, vec![0, 0, 0, 1, 1, 1, 2, 2, 2]);
}

#[test]
fn test_metrics_throughput() {
    let metrics = KafkaSinkMetrics::new();
    metrics.records_written.store(10_000, Ordering::Relaxed);
    // Throughput depends on elapsed time
    assert!(metrics.calculate_throughput() > 0.0);
}

#[test]
fn test_capabilities_exactly_once() {
    let config = KafkaSinkConfig {
        delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
        ..Default::default()
    };
    let sink = KafkaSink::new(config, Box::new(JsonSerializer::default()));
    let caps = sink.capabilities();
    assert!(caps.transactional);
    assert!(caps.idempotent);
    assert!(caps.two_phase_commit);
}

#[test]
fn test_capabilities_at_least_once() {
    let config = KafkaSinkConfig {
        delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
        ..Default::default()
    };
    let sink = KafkaSink::new(config, Box::new(JsonSerializer::default()));
    let caps = sink.capabilities();
    assert!(!caps.transactional);
    assert!(caps.idempotent);
    assert!(!caps.two_phase_commit);
}
```

### 13.2 Integration Tests (with testcontainers)

```rust
#[tokio::test]
async fn test_kafka_sink_end_to_end_at_least_once() {
    let kafka = KafkaContainer::start().await;
    let mut sink = create_test_sink(&kafka, DeliveryGuarantee::AtLeastOnce).await;

    sink.begin_epoch(1).await.unwrap();
    let batch = create_test_batch(100);
    let result = sink.write_batch(&batch).await.unwrap();
    assert_eq!(result.records_written, 100);
    sink.commit_epoch(1).await.unwrap();

    // Verify records arrived in Kafka
    let consumed = consume_all(&kafka, "test-topic", 100).await;
    assert_eq!(consumed.len(), 100);
}

#[tokio::test]
async fn test_kafka_sink_exactly_once_commit() {
    let kafka = KafkaContainer::start().await;
    let mut sink = create_test_sink(&kafka, DeliveryGuarantee::ExactlyOnce).await;

    sink.begin_epoch(1).await.unwrap();
    sink.write_batch(&create_test_batch(50)).await.unwrap();
    sink.commit_epoch(1).await.unwrap();

    // With read_committed, records should be visible
    let consumed = consume_read_committed(&kafka, "test-topic", 50).await;
    assert_eq!(consumed.len(), 50);
}

#[tokio::test]
async fn test_kafka_sink_exactly_once_abort() {
    let kafka = KafkaContainer::start().await;
    let mut sink = create_test_sink(&kafka, DeliveryGuarantee::ExactlyOnce).await;

    sink.begin_epoch(1).await.unwrap();
    sink.write_batch(&create_test_batch(50)).await.unwrap();
    sink.rollback_epoch(1).await.unwrap();

    // With read_committed, aborted records should NOT be visible
    let consumed = consume_read_committed(&kafka, "test-topic", 0).await;
    assert_eq!(consumed.len(), 0);
}

#[tokio::test]
async fn test_kafka_sink_dlq_routing() {
    let kafka = KafkaContainer::start().await;
    // Create a sink with a serializer that fails on certain records
    let mut sink = create_test_sink_with_dlq(&kafka, "test-dlq").await;

    sink.begin_epoch(1).await.unwrap();
    // This batch has a poison record that fails serialization
    sink.write_batch(&create_poison_batch()).await.unwrap();
    sink.commit_epoch(1).await.unwrap();

    // Verify poison record arrived in DLQ
    let dlq_records = consume_all(&kafka, "test-dlq", 1).await;
    assert_eq!(dlq_records.len(), 1);
    // Verify DLQ headers
    let headers = dlq_records[0].headers().unwrap();
    assert!(headers.get("__dlq.error").is_some());
}

#[tokio::test]
async fn test_kafka_sink_graceful_shutdown() {
    let kafka = KafkaContainer::start().await;
    let mut sink = create_test_sink(&kafka, DeliveryGuarantee::ExactlyOnce).await;

    sink.begin_epoch(1).await.unwrap();
    sink.write_batch(&create_test_batch(100)).await.unwrap();

    // Close should abort the uncommitted transaction
    sink.close().await.unwrap();
    assert_eq!(sink.state(), ConnectorState::Stopped);

    // Uncommitted records should not be visible
    let consumed = consume_read_committed(&kafka, "test-topic", 0).await;
    assert_eq!(consumed.len(), 0);
}
```

---

## 14. References

1. **Apache Kafka Transactional Producer (KIP-98)**
   - Exactly-once semantics via `init_transactions()`, `begin_transaction()`, `commit_transaction()`
   - https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging

2. **Kafka Idempotent Producer (KIP-98)**
   - `enable.idempotence = true` for at-least-once deduplication at the broker
   - Sequence numbers per partition prevent duplicate writes

3. **Kafka Consumer Offset in Transaction (KIP-98)**
   - `send_offsets_to_transaction()` for atomic source-offset + sink-data commits
   - Enables end-to-end exactly-once in Kafka-to-Kafka pipelines

4. **Kafka Sticky Partitioning (KIP-794)**
   - Batch records to same partition to reduce latency
   - Improves throughput for keyless messages

5. **rdkafka Crate**
   - Rust bindings for librdkafka
   - `FutureProducer` for async send with delivery notifications
   - https://docs.rs/rdkafka/latest/rdkafka/

6. **Apache Flink KafkaSink (FLIP-143)**
   - Epoch-transaction alignment pattern
   - TwoPhaseCommitSinkFunction design
   - https://cwiki.apache.org/confluence/display/FLINK/FLIP-143

7. **Confluent Schema Registry**
   - REST API for schema registration and retrieval
   - Avro/Protobuf/JSON Schema support
   - https://docs.confluent.io/platform/current/schema-registry/

8. **F034: Connector SDK** - `SinkConnector` trait, `ConnectorRuntime`, `SinkRunner`
   - [Link](F034-connector-sdk.md)

9. **F023: Exactly-Once Sinks** - `ExactlyOnceSink` trait, `TransactionalSink<S>`
   - [Link](../phase-2/F023-exactly-once-sinks.md)

10. **F024: Two-Phase Commit** - `TwoPhaseCoordinator`, multi-sink coordination
    - [Link](../phase-2/F024-two-phase-commit.md)

11. **F025: Kafka Source Connector** - Companion source connector
    - [Link](F025-kafka-source.md)

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **KafkaSink** | Connector that writes Arrow RecordBatch data to Kafka topics via `FutureProducer` |
| **Epoch** | LaminarDB checkpoint boundary; maps 1:1 to a Kafka transaction for exactly-once |
| **Transactional ID** | Unique identifier for a Kafka transactional producer; used for zombie fencing |
| **Zombie Fencing** | Mechanism where `init_transactions()` aborts any pending transaction from a previous producer with the same transactional ID |
| **DLQ (Dead Letter Queue)** | Separate Kafka topic where records that fail serialization or delivery are routed |
| **FutureProducer** | rdkafka async producer that returns futures for delivery confirmation |
| **Murmur2** | Hash algorithm used by Kafka's default partitioner; ensures key-based partition consistency |
| **Sticky Partitioning** | Strategy that batches records to the same partition to reduce broker round-trips |
| **Schema Registry** | Confluent service for managing Avro/Protobuf schemas with versioning and compatibility |
| **read_committed** | Kafka consumer isolation level that only reads records from committed transactions |
| **SinkRunner** | F034 background task that bridges a `Subscription<T>` to a `SinkConnector` in Ring 1 |
| **ConnectorConfig** | F034 configuration object parsed from SQL `WITH (...)` clause options |
| **RecordSerializer** | F034 trait for converting Arrow RecordBatch to byte payloads |

---

## Appendix B: Competitive Comparison

| Feature | LaminarDB F026 | Kafka Connect Sink | Flink KafkaSink | RisingWave Kafka Sink |
|---------|---------------|-------------------|-----------------|----------------------|
| **Deployment** | Embedded (in-process) | Cluster (Connect workers) | Cluster (TaskManagers) | Cluster (compute nodes) |
| **Ring 0 impact** | ~5ns (SPSC channel only) | N/A | N/A | N/A |
| **Exactly-once** | Kafka transactions + epoch alignment | Kafka transactions (Connect framework) | Kafka transactions (barrier-aligned) | Kafka transactions |
| **Serialization** | Pluggable (JSON, Avro, Protobuf) | Converters (Connect framework) | SerializationSchema | JSON, Avro, Protobuf |
| **Partitioning** | Key-hash, round-robin, sticky, explicit | FlinkKafkaPartitioner | Partitioner interface | Key-based |
| **DLQ support** | Built-in with headers | Connect DLQ | Custom error handler | No built-in DLQ |
| **Schema Registry** | Confluent compatible | Confluent native | Confluent compatible | Confluent compatible |
| **Latency** | < 50ms commit (Ring 1) | ~100ms+ (Connect overhead) | ~100ms+ (barrier overhead) | ~100ms+ (barrier overhead) |
| **Configuration** | SQL `CREATE SINK TABLE` | JSON/REST API | Java API / SQL | SQL `CREATE SINK` |
| **Consumer offset commit** | `send_offsets_to_transaction` | Offset management API | `send_offsets_to_transaction` | Internal offset tracking |
| **Health monitoring** | Metadata fetch + metrics | REST API | JMX metrics | Internal metrics |
| **Language** | Rust (zero-copy Arrow) | Java | Java | Rust |

---

## Appendix C: Configuration Examples

### C.1 Minimal Configuration

```sql
CREATE SINK TABLE output FROM my_query
WITH (
    connector = 'kafka',
    topic = 'output-topic',
    'bootstrap.servers' = 'localhost:9092'
);
```

### C.2 Production Exactly-Once

```sql
CREATE SINK TABLE output FROM my_query
WITH (
    connector = 'kafka',
    topic = 'output-topic',
    'bootstrap.servers' = 'broker1:9092,broker2:9092,broker3:9092',
    format = 'avro',
    'schema.registry.url' = 'http://schema-registry:8081',
    'delivery.guarantee' = 'exactly-once',
    'transaction.timeout.ms' = '60000',
    'key.column' = 'customer_id',
    'partitioner' = 'key-hash',
    'compression.type' = 'zstd',
    'acks' = 'all',
    'dlq.topic' = 'output-topic-dlq'
);
```

### C.3 High-Throughput At-Least-Once

```sql
CREATE SINK TABLE output FROM my_query
WITH (
    connector = 'kafka',
    topic = 'output-topic',
    'bootstrap.servers' = 'broker1:9092,broker2:9092,broker3:9092',
    format = 'json',
    'delivery.guarantee' = 'at-least-once',
    'linger.ms' = '100',
    'batch.size' = '131072',
    'compression.type' = 'lz4',
    'partitioner' = 'sticky',
    'flush.batch.size' = '10000',
    'max.in.flight.requests' = '10'
);
```

### C.4 Secure (SASL/SSL)

```sql
CREATE SINK TABLE output FROM my_query
WITH (
    connector = 'kafka',
    topic = 'secure-output',
    'bootstrap.servers' = 'broker:9093',
    format = 'json',
    'delivery.guarantee' = 'exactly-once',
    'security.protocol' = 'SASL_SSL',
    'sasl.mechanism' = 'SCRAM-SHA-256',
    'sasl.username' = 'laminardb',
    'sasl.password' = 'secret',
    'ssl.ca.location' = '/etc/ssl/certs/kafka-ca.pem',
    'ssl.certificate.location' = '/etc/ssl/certs/client.pem',
    'ssl.key.location' = '/etc/ssl/private/client-key.pem'
);
```

---

## Completion Checklist

- [ ] Core `KafkaSink` struct implementing `SinkConnector`
- [ ] Configuration parsing from SQL `WITH` clause
- [ ] Exactly-once via Kafka transactions (begin/commit/abort)
- [ ] At-least-once via idempotent producer
- [ ] Key extraction and partitioning strategies
- [ ] JSON serialization end-to-end
- [ ] Avro serialization with schema registry
- [ ] Dead letter queue routing
- [ ] Health check via metadata fetch
- [ ] Metrics (records, bytes, errors, epochs, latency)
- [ ] `ConnectorRegistry` factory registration
- [ ] SQL DDL resolution (`CREATE SINK TABLE ... WITH (connector = 'kafka')`)
- [ ] `SinkRunner` integration (Ring 1 background task)
- [ ] Graceful shutdown (drain in-flight, abort/commit transaction)
- [ ] Recovery: zombie fencing via `init_transactions()`
- [ ] Unit tests (config, partitioner, metrics, capabilities)
- [ ] Integration tests (end-to-end, exactly-once, DLQ, shutdown)
- [ ] 25+ tests total
