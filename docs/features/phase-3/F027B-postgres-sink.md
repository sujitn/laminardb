# F027B: PostgreSQL Sink Connector

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0 (Critical — complements F027 CDC Source for bidirectional PostgreSQL pipelines)
**Estimated Complexity:** L (1-2 weeks)
**Prerequisites:** F034 (Connector SDK), F023 (Exactly-Once Sinks), F063 (Changelog/Retraction)

---

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F027B |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F034, F023, F063 |
| **Blocks** | F031 (Delta Lake Sink — shares batch-write patterns) |
| **Owner** | TBD |
| **Crate** | `laminar-connectors` (feature: `postgres-sink`) |
| **Created** | 2026-01-31 |

---

## Executive Summary

This specification defines the PostgreSQL Sink Connector for LaminarDB. The connector writes streaming query results (Arrow `RecordBatch`) to PostgreSQL tables using two write strategies: **COPY BINARY** for maximum-throughput append-only loads, and **INSERT ... ON CONFLICT DO UPDATE** with batched UNNEST arrays for upsert workloads. Exactly-once semantics are achieved via co-transactional offset storage — data writes and epoch markers are committed in the same PostgreSQL transaction, leveraging PostgreSQL's native ACID guarantees without distributed two-phase commit. The connector uses `deadpool-postgres` for connection pooling and `pgpq` for zero-copy Arrow-to-PostgreSQL binary encoding. All I/O runs in Ring 1 (background), with no impact on Ring 0 hot path latency.

---

## 1. Problem Statement

### 1.1 Current Limitation

LaminarDB can consume data from PostgreSQL via the F027 CDC Source, process it through streaming operators, and sink it to Kafka (F026). However, there is no connector to write results **back to PostgreSQL** — one of the most common destinations for streaming pipeline outputs.

Without a PostgreSQL sink, users cannot:
- Write aggregated streaming results to PostgreSQL for BI/reporting tools
- Materialize streaming views as PostgreSQL tables for application consumption
- Build CDC-to-PostgreSQL pipelines (e.g., replicate between PostgreSQL instances)
- Power real-time dashboards backed by PostgreSQL

### 1.2 Required Capability

A PostgreSQL Sink Connector that:
1. Writes Arrow `RecordBatch` to PostgreSQL tables via two strategies:
   - **Append-only**: COPY BINARY protocol for maximum throughput (>500K rows/sec)
   - **Upsert**: `INSERT ... ON CONFLICT DO UPDATE` with UNNEST for deduplication
2. Implements `SinkConnector` trait with epoch-based exactly-once semantics
3. Co-transactionally stores epoch offsets alongside data writes
4. Handles Z-set changelog records (F063): applies INSERT (+1) and DELETE (-1) correctly
5. Maps Arrow types to PostgreSQL types for schema-compatible writes
6. Uses connection pooling for efficient resource management
7. Creates target tables automatically if configured (schema inference)
8. Provides health checks monitoring connection status and write latency

### 1.3 Use Case Diagram

```
+------------------------------------------------------------------+
|                 PostgreSQL Sink Pipeline                           |
|                                                                   |
|  Source               LaminarDB                  PostgreSQL       |
|  +----------+     +------------------+        +----------+        |
|  | Kafka /  | --> | Streaming        | -----> | Target   |        |
|  | CDC /    |     | Operators        | COPY   | Table    |        |
|  | In-Memory|     | (Ring 0)         | BINARY | (append) |        |
|  +----------+     +------------------+  or    +----------+        |
|                          |              INSERT                    |
|                    +------------------+ ON     +----------+        |
|                    | PostgresSink     | CONFL  | Target   |        |
|                    | (Ring 1)         | ICT    | Table    |        |
|                    +------------------+        | (upsert) |        |
|                          |                    +----------+        |
|                    +------------------+                            |
|                    | _laminardb_sink  |                            |
|                    | _offsets         |  (epoch tracking table)    |
|                    +------------------+                            |
+------------------------------------------------------------------+
```

**Example: Real-Time Aggregation to PostgreSQL**

```sql
-- Source: CDC from orders table
CREATE SOURCE TABLE orders_cdc (...) WITH (
    connector = 'postgres-cdc', ...
);

-- Sink: write hourly aggregates to PostgreSQL reporting table
CREATE SINK TABLE hourly_revenue WITH (
    connector = 'postgres-sink',
    hostname = 'reporting-db.example.com',
    port = '5432',
    username = 'laminardb_writer',
    password = 'secret',
    database = 'reporting',
    'table.name' = 'hourly_revenue',
    'write.mode' = 'upsert',
    'primary.key' = 'window_start,region'
) AS
SELECT
    TUMBLE_START(order_time, INTERVAL '1' HOUR) as window_start,
    region,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue
FROM orders_cdc
WHERE _op IN ('I', 'U')
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR), region
EMIT ON WINDOW CLOSE;
```

---

## 2. Design Principles

### 2.1 Core Principles

| Principle | Application |
|-----------|-------------|
| **Three-ring separation** | All PostgreSQL writes in Ring 1; connection pool management in Ring 2; no sink code in Ring 0 |
| **Zero hot-path impact** | Data enters Ring 1 via SPSC channel (~5ns); all I/O is async in Ring 1 |
| **Exactly-once via co-transaction** | Data + epoch offset committed in same PostgreSQL transaction |
| **COPY-first throughput** | Default to COPY BINARY for append workloads (>500K rows/sec); fall back to INSERT for upsert |
| **Changelog-aware** | Handle F063 Z-set records: apply INSERT (+1) as writes, DELETE (-1) as removes |
| **SQL-first configuration** | `CREATE SINK TABLE ... WITH (connector = 'postgres-sink', ...)` |

### 2.2 Industry-Informed Patterns (2026 Research)

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **COPY BINARY protocol** | PostgreSQL 16-17 optimizations (300%+ improvement in PG 16) | `tokio-postgres` `BinaryCopyInWriter` or `pgpq` crate for Arrow encoding |
| **Batched UNNEST upsert** | RisingWave, TigerData research (2x over naive multi-row INSERT) | `INSERT ... SELECT UNNEST($1::type[], ...) ON CONFLICT DO UPDATE` |
| **Co-transactional offsets** | JustOne/Confluent Kafka Connect PostgreSQL Sink | Data + epoch in same PG transaction — no distributed 2PC needed |
| **Connection pooling** | Production Rust services | `deadpool-postgres` (no deadlocks, no background tasks, any executor) |
| **Arrow→PG binary** | `pgpq` crate (encodes RecordBatch to PG binary COPY in <1s for 1M rows) | Direct Arrow-to-binary encoding for COPY path |
| **ON CONFLICT over MERGE** | pganalyze, PostgreSQL docs | `ON CONFLICT` is concurrency-safe; `MERGE` (PG 15+) can produce duplicate key errors under concurrency |

---

## 3. Architecture

### 3.1 Core Structures

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use tokio_postgres::types::Type as PgType;

use crate::connector::{
    ConnectorConfig, ConnectorError, ConnectorMetrics, ConnectorState,
    HealthStatus, SinkConnector, SinkConnectorCapabilities, WriteResult,
};

/// PostgreSQL sink connector configuration.
#[derive(Debug, Clone)]
pub struct PostgresSinkConfig {
    /// PostgreSQL hostname.
    pub hostname: String,
    /// PostgreSQL port (default: 5432).
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Username for authentication.
    pub username: String,
    /// Password for authentication.
    pub password: String,
    /// Target schema name (default: "public").
    pub schema_name: String,
    /// Target table name.
    pub table_name: String,
    /// Write mode: append or upsert.
    pub write_mode: WriteMode,
    /// Primary key columns (required for upsert mode).
    pub primary_key_columns: Vec<String>,
    /// Maximum records to buffer before flushing.
    pub batch_size: usize,
    /// Maximum time to buffer before flushing.
    pub flush_interval: Duration,
    /// Connection pool size.
    pub pool_size: usize,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// SSL mode for connections.
    pub ssl_mode: SslMode,
    /// Whether to create the target table if it doesn't exist.
    pub auto_create_table: bool,
    /// Whether to handle changelog/retraction records (F063 Z-sets).
    pub changelog_mode: bool,
    /// Exactly-once delivery guarantee.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Sink ID for offset tracking (auto-generated if not set).
    pub sink_id: String,
}

impl Default for PostgresSinkConfig {
    fn default() -> Self {
        Self {
            hostname: "localhost".to_string(),
            port: 5432,
            database: String::new(),
            username: String::new(),
            password: String::new(),
            schema_name: "public".to_string(),
            table_name: String::new(),
            write_mode: WriteMode::Append,
            primary_key_columns: Vec::new(),
            batch_size: 4096,
            flush_interval: Duration::from_secs(1),
            pool_size: 4,
            connect_timeout: Duration::from_secs(10),
            ssl_mode: SslMode::Prefer,
            auto_create_table: false,
            changelog_mode: false,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            sink_id: String::new(),
        }
    }
}

impl PostgresSinkConfig {
    /// Creates config from generic ConnectorConfig.
    pub fn from_connector_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let hostname = config.require("hostname")?.to_string();
        let port = config.get_parsed::<u16>("port").unwrap_or(5432);
        let database = config.require("database")?.to_string();
        let username = config.require("username")?.to_string();
        let password = config.get("password").cloned().unwrap_or_default();
        let schema_name = config.get("schema.name")
            .cloned().unwrap_or_else(|| "public".to_string());
        let table_name = config.require("table.name")?.to_string();

        let write_mode = config.get("write.mode")
            .and_then(|s| WriteMode::from_str(s))
            .unwrap_or(WriteMode::Append);

        let primary_key_columns: Vec<String> = config.get("primary.key")
            .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
            .unwrap_or_default();

        let delivery_guarantee = config.get("delivery.guarantee")
            .and_then(|s| DeliveryGuarantee::from_str(s))
            .unwrap_or(DeliveryGuarantee::AtLeastOnce);

        // Validate: upsert mode requires primary key
        if write_mode == WriteMode::Upsert && primary_key_columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "Upsert mode requires 'primary.key' to be set".into(),
            ));
        }

        Ok(Self {
            hostname,
            port,
            database,
            username,
            password,
            schema_name,
            table_name,
            write_mode,
            primary_key_columns,
            delivery_guarantee,
            ..Default::default()
        })
    }

    /// Returns the fully qualified table name.
    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

/// Write mode for the PostgreSQL sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Append-only: uses COPY BINARY for maximum throughput.
    /// No deduplication — every record is inserted.
    Append,
    /// Upsert: INSERT ... ON CONFLICT DO UPDATE.
    /// Requires primary key columns. Deduplicates on key.
    Upsert,
}

impl WriteMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "append" => Some(Self::Append),
            "upsert" => Some(Self::Upsert),
            _ => None,
        }
    }
}

/// Delivery guarantee for the sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// At-least-once: records may be duplicated on failure.
    AtLeastOnce,
    /// Exactly-once: co-transactional offset storage in PostgreSQL.
    ExactlyOnce,
}

impl DeliveryGuarantee {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "at_least_once" => Some(Self::AtLeastOnce),
            "exactly_once" => Some(Self::ExactlyOnce),
            _ => None,
        }
    }
}

/// SSL mode for PostgreSQL connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}
```

### 3.2 PostgresSink Implementation

```rust
/// PostgreSQL sink connector.
///
/// Writes Arrow `RecordBatch` to PostgreSQL tables using two strategies:
/// - **Append mode**: COPY BINARY (via `pgpq` or `BinaryCopyInWriter`)
/// - **Upsert mode**: `INSERT ... ON CONFLICT DO UPDATE` with UNNEST arrays
///
/// Exactly-once semantics use co-transactional offset storage:
/// data and epoch markers are committed in the same PostgreSQL transaction.
///
/// # Ring Architecture
///
/// - **Ring 0**: No sink code. Data arrives via SPSC channel.
/// - **Ring 1**: Batch buffering, COPY/INSERT writes, transaction management.
/// - **Ring 2**: Connection pool management, health checks, table creation.
pub struct PostgresSink {
    /// Connection pool (deadpool-postgres).
    pool: Option<Pool>,
    /// Sink configuration.
    config: PostgresSinkConfig,
    /// Arrow schema for input batches.
    schema: SchemaRef,
    /// PostgreSQL column types (mapped from Arrow schema).
    pg_types: Vec<PgType>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Current epoch (for exactly-once).
    current_epoch: u64,
    /// Last committed epoch.
    last_committed_epoch: u64,
    /// Whether a transaction is currently active.
    transaction_active: bool,
    /// Buffered records awaiting flush.
    buffer: Vec<RecordBatch>,
    /// Total rows in buffer.
    buffered_rows: usize,
    /// Last flush time.
    last_flush: Instant,
    /// Sink metrics.
    metrics: PostgresSinkMetrics,
    /// Prepared upsert statement (cached for upsert mode).
    upsert_sql: Option<String>,
}

/// Metrics for the PostgreSQL sink.
#[derive(Debug, Clone, Default)]
pub struct PostgresSinkMetrics {
    /// Total records written.
    pub records_written: u64,
    /// Total bytes written (estimated from RecordBatch).
    pub bytes_written: u64,
    /// Total batches flushed.
    pub batches_flushed: u64,
    /// Total COPY operations (append mode).
    pub copy_operations: u64,
    /// Total upsert operations.
    pub upsert_operations: u64,
    /// Total epochs committed (exactly-once).
    pub epochs_committed: u64,
    /// Total epochs rolled back.
    pub epochs_rolled_back: u64,
    /// Write errors (non-fatal, retried).
    pub write_errors: u64,
    /// Average flush latency (microseconds).
    pub avg_flush_latency_us: u64,
    /// p99 flush latency (microseconds).
    pub p99_flush_latency_us: u64,
    /// Connection pool active count.
    pub pool_active_connections: u32,
    /// Last successful write time.
    pub last_write_time: Option<Instant>,
    /// Changelog deletes applied (Z-set -1 weight).
    pub changelog_deletes: u64,
}
```

### 3.3 SinkConnector Trait Implementation

```rust
#[async_trait]
impl SinkConnector for PostgresSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.config = PostgresSinkConfig::from_connector_config(config)?;
        self.pg_types = self.arrow_schema_to_pg_types(&self.schema);

        // Step 1: Create connection pool (Ring 2)
        let pool = self.create_pool()?;
        self.pool = Some(pool);

        // Step 2: Verify connectivity
        let client = self.get_client().await?;

        // Step 3: Auto-create target table if configured
        if self.config.auto_create_table {
            self.ensure_table(&client).await?;
        }

        // Step 4: Ensure offset tracking table exists (exactly-once)
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            self.ensure_offset_table(&client).await?;
            self.recover_epoch(&client).await?;
        }

        // Step 5: Cache upsert SQL (if upsert mode)
        if self.config.write_mode == WriteMode::Upsert {
            self.upsert_sql = Some(self.build_upsert_sql());
        }

        self.state = ConnectorState::Running;
        self.last_flush = Instant::now();

        tracing::info!(
            table = %self.config.qualified_table_name(),
            mode = ?self.config.write_mode,
            guarantee = ?self.config.delivery_guarantee,
            "PostgreSQL sink opened"
        );

        Ok(())
    }

    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<WriteResult, ConnectorError> {
        if batch.num_rows() == 0 {
            return Ok(WriteResult { records_written: 0, bytes_written: 0 });
        }

        // Buffer the batch
        self.buffer.push(batch.clone());
        self.buffered_rows += batch.num_rows();

        // Check if we should flush
        let should_flush = self.buffered_rows >= self.config.batch_size
            || self.last_flush.elapsed() >= self.config.flush_interval;

        if should_flush {
            return self.flush_buffer().await;
        }

        Ok(WriteResult {
            records_written: 0,
            bytes_written: 0,
        })
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            return Ok(());
        }

        // Flush any remaining data from previous epoch
        if !self.buffer.is_empty() {
            self.flush_buffer().await?;
        }

        self.current_epoch = epoch;
        self.transaction_active = false; // Transaction starts on first write

        tracing::debug!(epoch = epoch, "PostgreSQL sink epoch started");
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            return Ok(());
        }

        // Flush any remaining buffered data
        if !self.buffer.is_empty() {
            self.flush_buffer().await?;
        }

        // Commit the epoch: write offset record in same transaction as data
        let client = self.get_client().await?;
        client.execute(
            "INSERT INTO _laminardb_sink_offsets (sink_id, epoch, updated_at) \
             VALUES ($1, $2, NOW()) \
             ON CONFLICT (sink_id) DO UPDATE SET epoch = $2, updated_at = NOW()",
            &[&self.config.sink_id, &(epoch as i64)],
        ).await.map_err(|e| ConnectorError::TransactionError(
            format!("Failed to commit epoch {epoch}: {e}")
        ))?;

        // If a transaction is active, commit it
        if self.transaction_active {
            // Transaction was opened in flush_buffer
            // Commit happens here
            self.transaction_active = false;
        }

        self.last_committed_epoch = epoch;
        self.metrics.epochs_committed += 1;

        tracing::debug!(epoch = epoch, "PostgreSQL sink epoch committed");
        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            return Ok(());
        }

        // Discard buffered data
        self.buffer.clear();
        self.buffered_rows = 0;

        // Rollback active transaction
        if self.transaction_active {
            self.transaction_active = false;
        }

        self.metrics.epochs_rolled_back += 1;
        tracing::warn!(epoch = epoch, "PostgreSQL sink epoch rolled back");
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        if self.state != ConnectorState::Running {
            return HealthStatus::Unhealthy(
                format!("Sink is in {:?} state", self.state)
            );
        }

        if let Some(pool) = &self.pool {
            let status = pool.status();
            if status.available == 0 && status.waiting > 0 {
                return HealthStatus::Degraded(
                    format!("Connection pool exhausted: {} waiting", status.waiting)
                );
            }
        }

        HealthStatus::Healthy
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.metrics.records_written,
            bytes_total: self.metrics.bytes_written,
            errors_total: self.metrics.write_errors,
            records_per_sec: 0.0, // Computed by runtime
            avg_batch_latency_us: self.metrics.avg_flush_latency_us,
            p99_batch_latency_us: self.metrics.p99_flush_latency_us,
            lag: None,
            custom: HashMap::from([
                ("copy_operations".into(), self.metrics.copy_operations as f64),
                ("upsert_operations".into(), self.metrics.upsert_operations as f64),
                ("epochs_committed".into(), self.metrics.epochs_committed as f64),
                ("changelog_deletes".into(), self.metrics.changelog_deletes as f64),
                ("pool_active".into(), self.metrics.pool_active_connections as f64),
            ]),
        }
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = SinkConnectorCapabilities::default()
            .with_idempotent()
            .with_changelog();

        if self.config.write_mode == WriteMode::Upsert {
            caps = caps.with_upsert();
        }
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            caps = caps.with_exactly_once();
        }
        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if !self.buffer.is_empty() {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Flush remaining data
        if !self.buffer.is_empty() {
            let _ = self.flush_buffer().await;
        }

        // Rollback any active transaction
        if self.transaction_active {
            self.transaction_active = false;
        }

        self.state = ConnectorState::Closed;
        self.pool = None;

        tracing::info!(
            table = %self.config.qualified_table_name(),
            records = self.metrics.records_written,
            epochs = self.metrics.epochs_committed,
            "PostgreSQL sink closed"
        );

        Ok(())
    }
}
```

### 3.4 Write Strategies

```rust
impl PostgresSink {
    /// Flushes the internal buffer to PostgreSQL.
    ///
    /// Dispatches to the appropriate write strategy based on config.
    async fn flush_buffer(&mut self) -> Result<WriteResult, ConnectorError> {
        let batches = std::mem::take(&mut self.buffer);
        let total_rows = self.buffered_rows;
        self.buffered_rows = 0;

        let start = Instant::now();

        let result = match self.config.write_mode {
            WriteMode::Append => self.write_copy_binary(&batches).await?,
            WriteMode::Upsert => self.write_upsert(&batches).await?,
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.metrics.avg_flush_latency_us = elapsed_us; // Simplified; use EMA in production
        self.metrics.batches_flushed += 1;
        self.metrics.records_written += result.records_written as u64;
        self.metrics.bytes_written += result.bytes_written;
        self.metrics.last_write_time = Some(Instant::now());
        self.last_flush = Instant::now();

        Ok(result)
    }

    /// Append-only write via COPY BINARY protocol.
    ///
    /// Uses `pgpq` to encode Arrow RecordBatch directly to PostgreSQL's
    /// native binary COPY format. This is the fastest path (>500K rows/sec).
    ///
    /// PG 16 improved COPY performance by >300%. PG 17 adds parallel COPY.
    async fn write_copy_binary(
        &self,
        batches: &[RecordBatch],
    ) -> Result<WriteResult, ConnectorError> {
        let client = self.get_client().await?;
        let mut total_rows = 0usize;
        let mut total_bytes = 0u64;

        for batch in batches {
            if self.config.changelog_mode {
                // Split batch: apply inserts via COPY, deletes via DELETE
                let (inserts, deletes) = self.split_changelog_batch(batch)?;

                if inserts.num_rows() > 0 {
                    let bytes = self.copy_batch(&client, &inserts).await?;
                    total_rows += inserts.num_rows();
                    total_bytes += bytes;
                }

                if deletes.num_rows() > 0 {
                    self.apply_deletes(&client, &deletes).await?;
                    self.metrics.changelog_deletes += deletes.num_rows() as u64;
                }
            } else {
                let bytes = self.copy_batch(&client, batch).await?;
                total_rows += batch.num_rows();
                total_bytes += bytes;
            }
        }

        self.metrics.copy_operations += 1;

        Ok(WriteResult {
            records_written: total_rows,
            bytes_written: total_bytes,
        })
    }

    /// Performs the actual COPY BINARY write for a single batch.
    async fn copy_batch(
        &self,
        client: &tokio_postgres::Client,
        batch: &RecordBatch,
    ) -> Result<u64, ConnectorError> {
        let columns: Vec<String> = batch.schema().fields()
            .iter()
            .filter(|f| !f.name().starts_with('_')) // Skip metadata columns
            .map(|f| f.name().clone())
            .collect();

        let col_list = columns.join(", ");
        let copy_stmt = format!(
            "COPY {} ({}) FROM STDIN BINARY",
            self.config.qualified_table_name(),
            col_list,
        );

        let sink = client.copy_in(&copy_stmt).await
            .map_err(|e| ConnectorError::WriteError(
                format!("COPY IN failed: {e}")
            ))?;

        // Use pgpq to encode Arrow RecordBatch to PG binary format
        // pgpq handles the binary COPY header, tuple encoding, and trailer
        let encoded = pgpq::encode_batch(batch, &self.pg_types)
            .map_err(|e| ConnectorError::WriteError(
                format!("Arrow to PG binary encoding failed: {e}")
            ))?;

        let bytes_written = encoded.len() as u64;

        sink.send(encoded).await
            .map_err(|e| ConnectorError::WriteError(
                format!("COPY send failed: {e}")
            ))?;

        sink.finish().await
            .map_err(|e| ConnectorError::WriteError(
                format!("COPY finish failed: {e}")
            ))?;

        Ok(bytes_written)
    }

    /// Upsert write via INSERT ... ON CONFLICT DO UPDATE with UNNEST.
    ///
    /// Batches values into PostgreSQL arrays and uses UNNEST to expand
    /// them into rows. This gives ~2x performance over naive multi-row
    /// INSERT while supporting ON CONFLICT for deduplication.
    async fn write_upsert(
        &self,
        batches: &[RecordBatch],
    ) -> Result<WriteResult, ConnectorError> {
        let client = self.get_client().await?;
        let upsert_sql = self.upsert_sql.as_ref()
            .ok_or(ConnectorError::ConfigurationError(
                "Upsert SQL not prepared".into()
            ))?;

        let mut total_rows = 0usize;

        for batch in batches {
            if self.config.changelog_mode {
                let (inserts, deletes) = self.split_changelog_batch(batch)?;

                if inserts.num_rows() > 0 {
                    self.execute_upsert(&client, upsert_sql, &inserts).await?;
                    total_rows += inserts.num_rows();
                }

                if deletes.num_rows() > 0 {
                    self.apply_deletes(&client, &deletes).await?;
                    self.metrics.changelog_deletes += deletes.num_rows() as u64;
                }
            } else {
                self.execute_upsert(&client, upsert_sql, batch).await?;
                total_rows += batch.num_rows();
            }
        }

        self.metrics.upsert_operations += 1;

        Ok(WriteResult {
            records_written: total_rows,
            bytes_written: 0, // Not easily measurable for upsert
        })
    }

    /// Builds the UNNEST-based upsert SQL statement.
    ///
    /// Example output:
    /// ```sql
    /// INSERT INTO public.target (id, value, updated_at)
    /// SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::timestamptz[])
    /// ON CONFLICT (id) DO UPDATE SET
    ///     value = EXCLUDED.value,
    ///     updated_at = EXCLUDED.updated_at
    /// ```
    fn build_upsert_sql(&self) -> String {
        let columns: Vec<&str> = self.schema.fields()
            .iter()
            .filter(|f| !f.name().starts_with('_'))
            .map(|f| f.name().as_str())
            .collect();

        let pg_type_names: Vec<String> = columns.iter()
            .enumerate()
            .map(|(i, _)| {
                let pg_type = arrow_type_to_pg_sql(&self.schema.field(i).data_type());
                format!("${}::{}[]", i + 1, pg_type)
            })
            .collect();

        let non_key_columns: Vec<&&str> = columns.iter()
            .filter(|c| !self.config.primary_key_columns.contains(&c.to_string()))
            .collect();

        let update_clause: Vec<String> = non_key_columns.iter()
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect();

        let pk_list = self.config.primary_key_columns.join(", ");

        format!(
            "INSERT INTO {} ({}) \
             SELECT * FROM UNNEST({}) \
             ON CONFLICT ({}) DO UPDATE SET {}",
            self.config.qualified_table_name(),
            columns.join(", "),
            pg_type_names.join(", "),
            pk_list,
            update_clause.join(", "),
        )
    }
}
```

### 3.5 Architecture Diagram

```
+------------------------------------------------------------------------+
|                     PostgreSQL Sink Architecture                          |
|                                                                          |
|  RING 0 (Hot Path) - NO SINK CODE HERE                                  |
|  +--------------------+  +--------------------+  +--------------+        |
|  | Source<T>          |->| Streaming Operators|->| Sink<T>      |        |
|  | (CDC, Kafka, etc.) |  | (window, agg, join)|  | (SPSC push)  |        |
|  +--------------------+  +--------------------+  +------+-------+        |
|                                                         |                |
|  ........................................................|...........    |
|  .                  SPSC Channel (~5ns)                  |          .    |
|  ........................................................|...........    |
|                                                         v                |
|  RING 1 (Background I/O)                                                 |
|  +--------------------+  +--------------------+  +--------------+        |
|  | Batch Buffer       |  | Write Strategy     |  | Transaction  |        |
|  | - collect batches   |->| - COPY BINARY      |->| Manager      |        |
|  | - flush on size/time|  | - UNNEST upsert    |  | - begin/     |        |
|  |                    |  | - changelog split   |  |   commit/    |        |
|  +--------------------+  +--------------------+  |   rollback   |        |
|                                                  +--------------+        |
|         |                                               |                |
|  RING 2 (Control Plane)                                 |                |
|  +--------------------+  +--------------------+  +--------------+        |
|  | Connection Pool    |  | Table Manager      |  | Offset Store |        |
|  | (deadpool-postgres)|  | - auto-create      |  | _laminardb_  |        |
|  | - 4 connections    |  | - schema validate  |  | sink_offsets |        |
|  +--------------------+  +--------------------+  +--------------+        |
|                                                                          |
+--------------------------------------------------------------------------+
|                                                                          |
|  PostgreSQL Server                                                       |
|  +------------------+  +--------------------+                            |
|  | Target Table     |  | _laminardb_sink_   |                            |
|  | (user data)      |  |  offsets           |                            |
|  |                  |  | (epoch tracking)   |                            |
|  +------------------+  +--------------------+                            |
+--------------------------------------------------------------------------+
```

### 3.6 Three-Ring Integration

| Ring | Component | Responsibilities | Latency Budget |
|------|-----------|-----------------|----------------|
| **Ring 0** | `Sink<T>` | Push RecordBatch into SPSC channel | < 500ns (channel push only) |
| **Ring 1** | `PostgresSink` | Batch buffering, COPY/INSERT, transaction management | 1-100ms (I/O bound) |
| **Ring 2** | Pool + Offset Store | Connection pooling, table creation, epoch recovery | No latency requirement |

---

## 4. Latency Considerations

### 4.1 End-to-End Write Latency

| Stage | Latency | Ring | Notes |
|-------|---------|------|-------|
| SPSC channel push | ~5ns | Ring 0/1 boundary | Lock-free |
| Batch buffering | 0-1s | Ring 1 | Configurable flush interval |
| COPY BINARY encode (pgpq) | 10-100us | Ring 1 | Per batch, CPU bound |
| Network + PG COPY write | 1-50ms | Ring 1 | Depends on batch size, network |
| UNNEST upsert | 5-100ms | Ring 1 | Depends on batch size, index |
| Transaction commit | 1-10ms | Ring 1 | fsync cost |
| **Total (append)** | **2-60ms** | - | **Dominated by network + fsync** |
| **Total (upsert)** | **5-150ms** | - | **Dominated by index updates** |

### 4.2 Hot Path Budget Impact

The sink connector adds exactly **~5ns** to Ring 0 latency (SPSC channel push). All write logic runs in Ring 1.

---

## 5. SQL Integration

### 5.1 CREATE SINK Syntax

```sql
-- Append-only sink (COPY BINARY, maximum throughput)
CREATE SINK TABLE event_archive WITH (
    connector = 'postgres-sink',
    hostname = 'warehouse.example.com',
    port = '5432',
    username = 'writer',
    password = 'secret',
    database = 'warehouse',
    'table.name' = 'events',
    'write.mode' = 'append',
    'batch.size' = '8192',
    'auto.create.table' = 'true'
) AS
SELECT event_id, event_type, payload, event_time
FROM events_stream;
```

```sql
-- Upsert sink (ON CONFLICT, deduplication)
CREATE SINK TABLE user_profiles WITH (
    connector = 'postgres-sink',
    hostname = 'app-db.example.com',
    username = 'writer',
    password = 'secret',
    database = 'app',
    'table.name' = 'user_profiles',
    'write.mode' = 'upsert',
    'primary.key' = 'user_id',
    'delivery.guarantee' = 'exactly_once'
) AS
SELECT user_id, name, email, last_seen
FROM users_cdc
WHERE _op <> 'D';
```

```sql
-- Changelog-aware sink (handles Z-set retractions)
CREATE SINK TABLE order_counts WITH (
    connector = 'postgres-sink',
    hostname = 'reporting-db.example.com',
    username = 'writer',
    password = 'secret',
    database = 'reporting',
    'table.name' = 'order_counts',
    'write.mode' = 'upsert',
    'primary.key' = 'region',
    'changelog.mode' = 'true'
) AS
SELECT region, COUNT(*) as cnt
FROM orders_cdc
GROUP BY region
EMIT CHANGES;
```

### 5.2 Rust API Examples

```rust
use laminar_connectors::postgres::sink::{PostgresSink, PostgresSinkConfig, WriteMode, DeliveryGuarantee};
use arrow_schema::{Schema, Field, DataType};

let schema = Arc::new(Schema::new(vec![
    Field::new("user_id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true),
    Field::new("email", DataType::Utf8, true),
]));

let config = PostgresSinkConfig {
    hostname: "localhost".to_string(),
    port: 5432,
    database: "app".to_string(),
    username: "writer".to_string(),
    password: "secret".to_string(),
    table_name: "user_profiles".to_string(),
    write_mode: WriteMode::Upsert,
    primary_key_columns: vec!["user_id".to_string()],
    delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
    ..Default::default()
};

let mut sink = PostgresSink::new(schema, config);
```

---

## 6. Type Mapping: Arrow to PostgreSQL

| Arrow DataType | PostgreSQL Type | UNNEST Cast | Notes |
|---------------|----------------|-------------|-------|
| `Boolean` | `BOOLEAN` | `bool[]` | |
| `Int16` | `SMALLINT` | `int2[]` | |
| `Int32` | `INT` | `int4[]` | |
| `Int64` | `BIGINT` | `int8[]` | |
| `UInt32` | `BIGINT` | `int8[]` | Widened (no unsigned in PG) |
| `Float32` | `REAL` | `float4[]` | |
| `Float64` | `DOUBLE PRECISION` | `float8[]` | |
| `Decimal128(p,s)` | `NUMERIC(p,s)` | `numeric[]` | |
| `Utf8` | `TEXT` | `text[]` | |
| `LargeUtf8` | `TEXT` | `text[]` | |
| `Binary` | `BYTEA` | `bytea[]` | |
| `Date32` | `DATE` | `date[]` | |
| `Time64(us)` | `TIME` | `time[]` | |
| `Timestamp(us, None)` | `TIMESTAMP` | `timestamp[]` | |
| `Timestamp(us, Some)` | `TIMESTAMPTZ` | `timestamptz[]` | |
| `FixedSizeBinary(16)` | `UUID` | `uuid[]` | |
| `List(T)` | `T[]` | Nested array | |

```rust
/// Maps Arrow DataType to PostgreSQL SQL type name (for UNNEST casts).
pub fn arrow_type_to_pg_sql(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "bool",
        DataType::Int16 => "int2",
        DataType::Int32 => "int4",
        DataType::Int64 | DataType::UInt32 => "int8",
        DataType::Float32 => "float4",
        DataType::Float64 => "float8",
        DataType::Decimal128(_, _) => "numeric",
        DataType::Utf8 | DataType::LargeUtf8 => "text",
        DataType::Binary | DataType::LargeBinary => "bytea",
        DataType::Date32 => "date",
        DataType::Time64(_) => "time",
        DataType::Timestamp(_, None) => "timestamp",
        DataType::Timestamp(_, Some(_)) => "timestamptz",
        DataType::FixedSizeBinary(16) => "uuid",
        _ => "text", // Fallback
    }
}
```

---

## 7. Exactly-Once Semantics

### 7.1 Co-Transactional Offset Storage

The key insight: by storing epoch markers in the **same PostgreSQL transaction** as data writes, we get exactly-once without distributed two-phase commit.

```
BEGIN TRANSACTION;
  -- 1. Write data (COPY or INSERT)
  COPY target_table FROM STDIN BINARY;
  -- ... or INSERT ... ON CONFLICT ...

  -- 2. Update epoch marker
  INSERT INTO _laminardb_sink_offsets (sink_id, epoch, updated_at)
  VALUES ('my_sink', 42, NOW())
  ON CONFLICT (sink_id) DO UPDATE SET epoch = 42, updated_at = NOW();
COMMIT;
```

If the transaction fails or the process crashes, both the data and the epoch marker are rolled back. On recovery, the sink reads the last committed epoch and resumes from there.

### 7.2 Offset Tracking Table

```sql
CREATE TABLE IF NOT EXISTS _laminardb_sink_offsets (
    sink_id TEXT PRIMARY KEY,
    epoch BIGINT NOT NULL,
    source_offsets JSONB,          -- Optional: source checkpoint data
    watermark BIGINT,              -- Optional: last watermark
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 7.3 Recovery Flow

```rust
impl PostgresSink {
    /// Recovers the last committed epoch from PostgreSQL.
    async fn recover_epoch(
        &mut self,
        client: &tokio_postgres::Client,
    ) -> Result<(), ConnectorError> {
        let row = client.query_opt(
            "SELECT epoch FROM _laminardb_sink_offsets WHERE sink_id = $1",
            &[&self.config.sink_id],
        ).await.map_err(|e| ConnectorError::CheckpointError(e.to_string()))?;

        if let Some(row) = row {
            let epoch: i64 = row.get(0);
            self.last_committed_epoch = epoch as u64;
            self.current_epoch = epoch as u64;

            tracing::info!(
                sink_id = %self.config.sink_id,
                epoch = epoch,
                "Recovered last committed epoch"
            );
        }

        Ok(())
    }
}
```

### 7.4 Idempotent Fallback

Even with exactly-once, writes are made idempotent where possible:
- **Upsert mode**: `ON CONFLICT DO UPDATE` naturally handles duplicates
- **Append mode**: Combine with source deduplication or add a unique constraint on a natural key

---

## 8. Changelog/Retraction Handling (F063)

When `changelog.mode = true`, the sink handles Z-set records:

```rust
impl PostgresSink {
    /// Splits a changelog batch into inserts and deletes.
    ///
    /// Uses the `_op` metadata column:
    /// - 'I' (insert) and 'U' (update-after) → insert/upsert batch
    /// - 'D' (delete) and '-U' (update-before) → delete batch
    fn split_changelog_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, RecordBatch), ConnectorError> {
        // Find _op column
        let op_idx = batch.schema().index_of("_op")
            .map_err(|_| ConnectorError::ConfigurationError(
                "Changelog mode requires '_op' column".into()
            ))?;

        let op_array = batch.column(op_idx)
            .as_any().downcast_ref::<arrow_array::StringArray>()
            .ok_or(ConnectorError::ConfigurationError(
                "_op column must be String type".into()
            ))?;

        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        for i in 0..op_array.len() {
            match op_array.value(i) {
                "I" | "U" | "r" => insert_indices.push(i as u32),
                "D" => delete_indices.push(i as u32),
                _ => {} // Skip unknown ops
            }
        }

        // Filter batch by indices (exclude _op column from output)
        let insert_batch = self.filter_batch(batch, &insert_indices)?;
        let delete_batch = self.filter_batch(batch, &delete_indices)?;

        Ok((insert_batch, delete_batch))
    }

    /// Applies DELETE operations using primary key columns.
    async fn apply_deletes(
        &self,
        client: &tokio_postgres::Client,
        deletes: &RecordBatch,
    ) -> Result<(), ConnectorError> {
        if deletes.num_rows() == 0 {
            return Ok(());
        }

        // Build DELETE ... WHERE pk IN (UNNEST($1::type[], ...))
        let pk_conditions: Vec<String> = self.config.primary_key_columns.iter()
            .enumerate()
            .map(|(i, col)| {
                let pg_type = arrow_type_to_pg_sql(
                    &deletes.schema().field_with_name(col).unwrap().data_type()
                );
                format!("{col} = ANY(${}::{}[])", i + 1, pg_type)
            })
            .collect();

        let delete_sql = format!(
            "DELETE FROM {} WHERE {}",
            self.config.qualified_table_name(),
            pk_conditions.join(" AND "),
        );

        // Extract PK column arrays as parameters
        // ... (convert Arrow arrays to PG array parameters)

        client.execute(&delete_sql, &[/* pk arrays */]).await
            .map_err(|e| ConnectorError::WriteError(
                format!("DELETE failed: {e}")
            ))?;

        Ok(())
    }
}
```

---

## 9. Configuration Reference

### 9.1 Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'postgres-sink'` |
| `hostname` | String | PostgreSQL server hostname |
| `database` | String | Target database name |
| `username` | String | User with INSERT/UPDATE/DELETE privileges |
| `table.name` | String | Target table name |

### 9.2 Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | Integer | `5432` | PostgreSQL port |
| `password` | String | `""` | Authentication password |
| `schema.name` | String | `"public"` | Target schema |
| `write.mode` | String | `"append"` | `append` (COPY) or `upsert` (ON CONFLICT) |
| `primary.key` | String | `""` | Comma-separated PK columns (required for upsert) |
| `delivery.guarantee` | String | `"at_least_once"` | `at_least_once` or `exactly_once` |
| `batch.size` | Integer | `4096` | Records to buffer before flush |
| `flush.interval.ms` | Integer | `1000` | Max time before flush (ms) |
| `pool.size` | Integer | `4` | Connection pool size |
| `connect.timeout.ms` | Integer | `10000` | Connection timeout |
| `ssl.mode` | String | `"prefer"` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `auto.create.table` | Boolean | `false` | Create target table from Arrow schema if missing |
| `changelog.mode` | Boolean | `false` | Handle F063 Z-set records (split INSERT/DELETE by `_op`) |
| `sink.id` | String | auto | Unique ID for offset tracking (auto-generated from table name if not set) |

---

## 10. Error Handling

| Error Category | Ring | Recovery Strategy | Example |
|---------------|------|-------------------|---------|
| **Connection Lost** | Ring 1 | Pool auto-reconnects; retry flush | Network partition |
| **Table Not Found** | Ring 2 | Auto-create if configured; else fail | Dropped table |
| **Constraint Violation** | Ring 1 | Log + skip record (upsert absorbs conflicts) | Duplicate key on append |
| **Type Mismatch** | Ring 1 | Fail with schema error | Arrow Int64 vs PG TEXT |
| **Transaction Timeout** | Ring 1 | Rollback + retry from epoch | Long-running write |
| **Pool Exhausted** | Ring 2 | Backpressure; wait for available connection | High concurrency |
| **Authentication Failure** | Ring 2 | Fail permanently | Password change |
| **Disk Full** | Ring 1 | Fail with PG error; alert via health check | PostgreSQL WAL/data full |

---

## 11. Implementation Roadmap

### Phase 1: Core PostgresSink and COPY BINARY (3-4 days)
- [ ] `PostgresSinkConfig` with `from_connector_config()`
- [ ] `PostgresSink` struct with `SinkConnector` impl
- [ ] `write_copy_binary()` using `pgpq` for Arrow encoding
- [ ] Connection pool via `deadpool-postgres`
- [ ] Arrow-to-PostgreSQL type mapping (`arrow_type_to_pg_sql()`)
- [ ] Batch buffering with size/time flush triggers
- [ ] Unit tests: config parsing, type mapping
- [ ] Integration tests: COPY writes to test PostgreSQL

### Phase 2: Upsert Mode (2-3 days)
- [ ] `build_upsert_sql()` UNNEST-based statement generation
- [ ] `write_upsert()` with Arrow array extraction
- [ ] `execute_upsert()` batched execution
- [ ] Primary key validation in config
- [ ] Unit tests: SQL generation for various schemas
- [ ] Integration tests: upsert with concurrent writes

### Phase 3: Changelog/Retraction Support (2 days)
- [ ] `split_changelog_batch()` by `_op` column
- [ ] `apply_deletes()` via UNNEST DELETE
- [ ] Integration with F063 `CdcOperation` types
- [ ] Unit tests: changelog splitting
- [ ] Integration tests: insert/update/delete lifecycle

### Phase 4: Exactly-Once Semantics (2-3 days)
- [ ] `_laminardb_sink_offsets` table creation
- [ ] `begin_epoch()` / `commit_epoch()` / `rollback_epoch()`
- [ ] Co-transactional data + epoch writes
- [ ] `recover_epoch()` on startup
- [ ] Integration tests: crash recovery with epoch verification

### Phase 5: Table Management and Health (1-2 days)
- [ ] `ensure_table()` auto-creation from Arrow schema
- [ ] `health_check()` with pool status and write latency
- [ ] `PostgresSinkMetrics` population
- [ ] Schema validation (Arrow vs existing PG table)
- [ ] Unit tests: DDL generation, health status

### Phase 6: SQL Integration and Registry (1 day)
- [ ] Register `postgres-sink` in `ConnectorRegistry`
- [ ] Wire `CREATE SINK TABLE ... WITH (connector = 'postgres-sink')` end-to-end
- [ ] Documentation and API examples

---

## 12. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Ring 0 latency impact | < 10ns per event | Benchmark with/without sink |
| Append throughput (COPY) | > 500K rows/sec | Benchmark with pgbench-scale table |
| Upsert throughput | > 100K rows/sec | Benchmark with ON CONFLICT |
| Transaction commit overhead | < 50ms per epoch | Measure `commit_epoch()` latency |
| Arrow→PG binary encode | < 1ms per 4K-row batch | Micro-benchmark `pgpq` encoding |
| Recovery time | < 5s (read epoch + resume) | End-to-end recovery test |
| Changelog delete throughput | > 200K deletes/sec | Benchmark with UNNEST DELETE |
| Connection pool efficiency | > 95% utilization | Monitor pool stats under load |
| Test coverage | > 80% | `cargo tarpaulin` |
| Test count | 30+ tests | `cargo test` |

---

## 13. Module Structure

```
crates/laminar-connectors/src/
  postgres/
    mod.rs                # Module root, re-exports, feature gate
    sink.rs               # PostgresSink struct, SinkConnector impl
    sink_config.rs        # PostgresSinkConfig, WriteMode, DeliveryGuarantee
    copy_writer.rs        # COPY BINARY encoding (pgpq integration)
    upsert_writer.rs      # UNNEST-based upsert SQL generation + execution
    types.rs              # Arrow↔PostgreSQL type mapping (shared with F027 source)
    changelog.rs          # Changelog batch splitting, DELETE application
    offset_store.rs       # _laminardb_sink_offsets management, epoch recovery
    table_manager.rs      # Auto-create table, schema validation
    metrics.rs            # PostgresSinkMetrics
    factory.rs            # PostgresSinkFactory for ConnectorRegistry
    tests/
      sink_tests.rs       # Core SinkConnector trait tests
      config_tests.rs     # Config parsing and validation
      copy_tests.rs       # COPY BINARY write tests
      upsert_tests.rs     # Upsert SQL generation and execution
      types_tests.rs      # Arrow↔PG type mapping
      changelog_tests.rs  # Z-set changelog handling
      offset_tests.rs     # Epoch tracking and recovery
      integration.rs      # End-to-end tests (requires PostgreSQL)
```

---

## 14. Dependencies (Cargo.toml additions)

```toml
[dependencies]
# Under [features] section:
postgres-sink = ["dep:tokio-postgres", "dep:deadpool-postgres", "dep:pgpq", "dep:postgres-types"]

[dependencies]
tokio-postgres = { version = "0.7", optional = true }
deadpool-postgres = { version = "0.12", optional = true }
pgpq = { version = "0.9", optional = true }
postgres-types = { version = "0.2", optional = true }
```

---

## 15. References

1. **PostgreSQL COPY Protocol**
   - [COPY documentation](https://www.postgresql.org/docs/current/sql-copy.html)
   - [tokio-postgres BinaryCopyInWriter](https://docs.rs/tokio-postgres/latest/tokio_postgres/binary_copy/struct.BinaryCopyInWriter.html)

2. **pgpq — Arrow to PostgreSQL Binary Encoding**
   - [pgpq GitHub](https://github.com/adriangb/pgpq)
   - [Loading data into Postgres](https://adriangb.com/2023/04/02/loading-data-into-postgres/)

3. **deadpool-postgres — Connection Pooling**
   - [deadpool-postgres on crates.io](https://crates.io/crates/deadpool-postgres)

4. **Upsert Performance Research**
   - [PostgreSQL UPSERT: INSERT ON CONFLICT vs MERGE (pganalyze)](https://pganalyze.com/blog/5mins-postgres-15-merge-vs-insert-on-conflict)
   - [Boosting INSERT performance with UNNEST (TigerData)](https://www.tigerdata.com/blog/boosting-postgres-insert-performance)

5. **Exactly-Once Sink Patterns**
   - [JustOne Kafka Connect PostgreSQL Sink (Confluent)](https://www.confluent.io/blog/kafka-connect-sink-for-postgresql-from-justone-database/)
   - [SQLstream Exactly-Once Semantics](https://docs.sqlstream.com/integrating-sqlstream/exactly-once/)

6. **LaminarDB Internal References**
   - [F034: Connector SDK](F034-connector-sdk.md) — `SinkConnector` trait
   - [F023: Exactly-Once Sinks](../phase-2/F023-exactly-once-sinks.md) — `ExactlyOnceSinkAdapter`
   - [F063: Changelog/Retraction](../phase-2/F063-changelog-retraction.md) — Z-set types
   - [F027: PostgreSQL CDC Source](F027-postgres-cdc.md) — Shared type mapping

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **COPY BINARY** | PostgreSQL's binary bulk-load protocol — fastest write path |
| **UNNEST** | PostgreSQL function that expands arrays into rows — enables batched parameterized queries |
| **ON CONFLICT** | PostgreSQL upsert clause — atomically inserts or updates based on unique constraint |
| **Co-transactional** | Data and metadata committed in the same database transaction |
| **Epoch** | Checkpoint boundary in LaminarDB's exactly-once framework |
| **Changelog** | Stream of INSERT/UPDATE/DELETE operations with Z-set weights |
| **pgpq** | Rust crate for encoding Arrow RecordBatch to PostgreSQL binary COPY format |
| **deadpool-postgres** | Async connection pool for tokio-postgres with no background tasks |

---

## Appendix B: Competitive Comparison

| Feature | LaminarDB F027B | Flink JDBC Sink | Kafka Connect PG Sink | RisingWave PG Sink |
|---------|----------------|----------------|----------------------|-------------------|
| **Write Mode** | COPY BINARY + upsert | Prepared statement | INSERT / upsert | INSERT / upsert |
| **Throughput** | >500K rows/sec (COPY) | ~50K rows/sec | ~100K rows/sec | ~200K rows/sec |
| **Exactly-Once** | Co-transactional | Checkpoint + XA | Offset commit | Barrier checkpoint |
| **Changelog** | Native Z-set | Retraction mode | N/A | Internal retraction |
| **Connection Pool** | deadpool-postgres | HikariCP (JVM) | JVM pool | Internal pool |
| **Arrow Native** | Yes (pgpq) | No (row-based) | No (row-based) | Internal columnar |
| **Dependencies** | tokio-postgres only | JVM + JDBC | JVM + Kafka Connect | Full cluster |

---

## Completion Checklist

- [ ] All implementation phases complete
- [ ] 30+ tests passing
- [ ] Clippy clean (`-D warnings`)
- [ ] `postgres-sink` feature gate in Cargo.toml
- [ ] Registered in `ConnectorRegistry`
- [ ] `CREATE SINK TABLE ... WITH (connector = 'postgres-sink')` works end-to-end
- [ ] Append mode (COPY BINARY) benchmarked at >500K rows/sec
- [ ] Upsert mode benchmarked at >100K rows/sec
- [ ] Exactly-once verified with crash recovery test
- [ ] Changelog mode verified with INSERT/DELETE lifecycle
- [ ] Documentation complete
