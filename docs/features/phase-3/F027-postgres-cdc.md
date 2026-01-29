# F027: PostgreSQL CDC Source Connector

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0 (Critical for real-world data pipelines)
**Estimated Complexity:** XL (2-3 weeks)
**Prerequisites:** F034 (Connector SDK), F063 (Changelog/Retraction), F064 (Per-Partition Watermarks), F-STREAM-007 (SQL DDL)

---

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F027 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | XL (2-3 weeks) |
| **Dependencies** | F034, F063, F064, F-STREAM-007 |
| **Blocks** | F028 (MySQL CDC), F061 (Historical Backfill) |
| **Owner** | TBD |
| **Crate** | `laminar-connectors` (feature: `postgres-cdc`) |
| **Created** | 2026-01-29 |

---

## Executive Summary

This specification defines the PostgreSQL CDC (Change Data Capture) Source Connector for LaminarDB. The connector consumes change events from PostgreSQL's built-in logical replication protocol using the `pgoutput` output plugin, translating WAL (Write-Ahead Log) entries into LaminarDB's Z-set changelog format (F063). It supports full CDC operations (INSERT, UPDATE with before/after images, DELETE), replication slot management, initial snapshot bootstrapping, schema discovery, and exactly-once consumption via LSN (Log Sequence Number) checkpointing. The connector runs entirely in Ring 1 (background I/O), communicating with Ring 0 through SPSC channels, preserving sub-500ns hot path latency guarantees.

---

## 1. Problem Statement

### 1.1 Current Limitation

LaminarDB's Phase 3 Connector SDK (F034) provides the `SourceConnector` trait and runtime infrastructure for bridging external systems to the streaming API. However, there is currently no implementation that captures real-time changes from relational databases. The `laminar-connectors` crate contains:

- `cdc.rs`: Empty placeholder module (single doc comment)
- `Cargo.toml`: `tokio-postgres` dependency declared as optional behind `postgres-cdc` feature flag
- No logical replication, WAL consumption, or CDC event parsing logic

Without a PostgreSQL CDC connector, LaminarDB cannot:
- Capture real-time inserts, updates, and deletes from PostgreSQL tables
- Build materialized views over relational data
- Power event-driven architectures that react to database changes
- Replace ETL batch pipelines with streaming CDC pipelines

### 1.2 Required Capability

A PostgreSQL CDC Source Connector that:
1. Connects to PostgreSQL via logical replication protocol
2. Creates and manages replication slots (create, drop, advance LSN)
3. Consumes WAL changes via the `pgoutput` output plugin
4. Emits INSERT, UPDATE (with before/after images), and DELETE as Z-set changelog records
5. Performs initial snapshot (backfill) for bootstrapping new sources
6. Tracks LSN position for exactly-once semantics via `SourceCheckpoint`
7. Handles schema changes gracefully (column add/drop/rename)
8. Integrates with per-partition watermarks (F064) using PostgreSQL commit timestamps
9. Provides heartbeat/keepalive for idle source detection
10. Maps PostgreSQL types to Arrow `DataType` for zero-copy columnar processing

### 1.3 Use Case Diagram

```
+------------------------------------------------------------------+
|                    PostgreSQL CDC Pipeline                         |
|                                                                   |
|  PostgreSQL                LaminarDB                 Downstream   |
|  +----------+          +----------------+          +----------+   |
|  |  Table:  |  WAL     | PostgresCdc    |  SPSC    | Streaming|   |
|  |  users   |--------->| Source         |--------->| Operators|   |
|  |          | pgoutput | (Ring 1)       | Channel  | (Ring 0) |   |
|  +----------+          +----------------+          +----------+   |
|       |                      |                          |         |
|       |                      | LSN Checkpoint           | Emit    |
|       |                      v                          v         |
|  +----------+          +----------------+          +----------+   |
|  |  Table:  |  WAL     | Replication    |          | Sink     |   |
|  |  orders  |--------->| Slot Manager   |          | (Kafka,  |   |
|  |          | pgoutput | (Ring 2)       |          |  Delta)  |   |
|  +----------+          +----------------+          +----------+   |
|                                                                   |
|  Ring 2: Slot lifecycle, health checks, schema discovery          |
|  Ring 1: WAL consumption, deserialization, watermark emission     |
|  Ring 0: Zero-copy record processing via SPSC channels            |
+------------------------------------------------------------------+
```

**Example: Real-Time User Activity Dashboard**

```sql
-- Capture all changes to the users table
CREATE SOURCE TABLE users_cdc (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    status VARCHAR,
    updated_at TIMESTAMP,
    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
) WITH (
    connector = 'postgres-cdc',
    hostname = 'db.example.com',
    port = '5432',
    username = 'replication_user',
    password = 'secret',
    database = 'myapp',
    'schema.name' = 'public',
    'table.name' = 'users',
    'slot.name' = 'laminardb_users',
    'publication.name' = 'laminardb_pub',
    format = 'debezium-json'
);

-- Real-time aggregate: active users per status
CREATE MATERIALIZED VIEW active_user_counts AS
SELECT status, COUNT(*) as cnt
FROM users_cdc
WHERE _op IN ('I', 'U')
GROUP BY status
EMIT CHANGES;
```

---

## 2. Design Principles

### 2.1 Core Principles (Aligned with LaminarDB Philosophy)

| Principle | Application |
|-----------|-------------|
| **Three-ring separation** | WAL consumption and deserialization in Ring 1; slot lifecycle and health checks in Ring 2; no CDC code touches Ring 0 |
| **Zero-allocation hot path** | CDC data enters Ring 0 only as Arrow `RecordBatch` via pre-existing SPSC channels (~5ns push) |
| **Exactly-once via LSN** | `SourceCheckpoint` stores PostgreSQL LSN; on recovery, replay resumes from committed LSN |
| **Z-set changelog native** | CDC operations map directly to F063 `CdcOperation`: INSERT=+1, DELETE=-1, UPDATE=(-1,+1) pair |
| **SQL-first configuration** | `CREATE SOURCE TABLE ... WITH (connector = 'postgres-cdc', ...)` configures everything |
| **No custom types unless benchmarks demand** | Reuse `SourceConnector`, `SourceBatch`, `SourceCheckpoint`, `DebeziumDeserializer` from F034 |

### 2.2 Industry-Informed Patterns (2025-2026 Research)

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **Logical Replication Protocol** | PostgreSQL 10+ built-in | Native `pgoutput` plugin via `tokio-postgres` replication API |
| **Replication Slot Management** | Debezium PostgreSQL Connector | Create/drop/advance slots; monitor slot lag to prevent WAL bloat |
| **Initial Snapshot + Streaming** | Debezium snapshot modes | Consistent snapshot via `SERIALIZABLE` txn, then switch to WAL stream |
| **Before/After Images** | PostgreSQL `REPLICA IDENTITY FULL` | Full before-image for UPDATE/DELETE when `REPLICA IDENTITY` is `FULL` |
| **LSN-Based Checkpointing** | Flink CDC Connector | LSN stored in `SourceCheckpoint.offsets["lsn"]`; deterministic recovery |
| **Heartbeat Mechanism** | Debezium heartbeat.interval.ms | Periodic status update to PostgreSQL to prevent replication timeout |
| **Publication Filtering** | PostgreSQL 10+ PUBLICATION | Restrict WAL decoding to specific tables via publication membership |
| **Z-Set CDC Mapping** | DBSP/Feldera (VLDB 2025) | INSERT=weight(+1), DELETE=weight(-1), UPDATE=pair(-1,+1) |

---

## 3. Architecture

### 3.1 Core Structures

```rust
use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio_postgres::Client;

use crate::connector::{
    ConnectorConfig, ConnectorError, ConnectorInfo, ConnectorMetrics,
    ConnectorState, HealthStatus, SourceBatch, SourceCheckpoint,
    SourceConnector,
};

/// PostgreSQL Log Sequence Number.
///
/// LSN represents a position in the PostgreSQL WAL (Write-Ahead Log).
/// Format: `XXXXXXXX/YYYYYYYY` (two 32-bit hex values forming a 64-bit offset).
///
/// LSNs are monotonically increasing and serve as the checkpoint offset
/// for exactly-once consumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgLsn(pub u64);

impl PgLsn {
    /// Creates a new LSN from a 64-bit value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Parses an LSN from the PostgreSQL text format `XXXXXXXX/YYYYYYYY`.
    pub fn from_pg_str(s: &str) -> Result<Self, ConnectorError> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(ConnectorError::Config(
                format!("Invalid LSN format: '{s}', expected 'XXXXXXXX/YYYYYYYY'"),
            ));
        }
        let high = u32::from_str_radix(parts[0], 16)
            .map_err(|e| ConnectorError::Config(format!("Invalid LSN high: {e}")))?;
        let low = u32::from_str_radix(parts[1], 16)
            .map_err(|e| ConnectorError::Config(format!("Invalid LSN low: {e}")))?;
        Ok(Self(((high as u64) << 32) | (low as u64)))
    }

    /// Formats the LSN as PostgreSQL text format.
    pub fn to_pg_string(&self) -> String {
        format!("{:X}/{:X}", (self.0 >> 32) as u32, self.0 as u32)
    }

    /// Returns the raw 64-bit LSN value.
    pub const fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for PgLsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", (self.0 >> 32) as u32, self.0 as u32)
    }
}

/// PostgreSQL CDC source connector configuration.
#[derive(Debug, Clone)]
pub struct PostgresCdcConfig {
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
    /// Schema name (default: "public").
    pub schema_name: String,
    /// Table name to capture changes from.
    pub table_name: String,
    /// Replication slot name.
    pub slot_name: String,
    /// Publication name (created if not exists).
    pub publication_name: String,
    /// Maximum number of records per poll batch.
    pub max_batch_size: usize,
    /// Poll interval when no WAL changes are available.
    pub poll_interval: Duration,
    /// Heartbeat interval for keepalive messages.
    pub heartbeat_interval: Duration,
    /// Whether to perform initial snapshot on first connect.
    pub snapshot_mode: SnapshotMode,
    /// SSL mode for connections.
    pub ssl_mode: SslMode,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Whether to include before-images for UPDATE/DELETE.
    /// Requires REPLICA IDENTITY FULL on the source table.
    pub include_before_image: bool,
    /// Additional tables to include in the publication (optional).
    pub additional_tables: Vec<String>,
    /// Maximum WAL sender lag before health check warns (bytes).
    pub max_wal_lag_bytes: u64,
    /// Slot advance interval (how often to confirm LSN to PostgreSQL).
    pub slot_advance_interval: Duration,
}

impl Default for PostgresCdcConfig {
    fn default() -> Self {
        Self {
            hostname: "localhost".to_string(),
            port: 5432,
            database: String::new(),
            username: String::new(),
            password: String::new(),
            schema_name: "public".to_string(),
            table_name: String::new(),
            slot_name: String::new(),
            publication_name: "laminardb_publication".to_string(),
            max_batch_size: 1024,
            poll_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(10),
            snapshot_mode: SnapshotMode::Initial,
            ssl_mode: SslMode::Prefer,
            connect_timeout: Duration::from_secs(10),
            include_before_image: true,
            additional_tables: Vec::new(),
            max_wal_lag_bytes: 100 * 1024 * 1024, // 100 MB
            slot_advance_interval: Duration::from_secs(5),
        }
    }
}

impl PostgresCdcConfig {
    /// Creates a config from the Connector SDK's generic `ConnectorConfig`.
    pub fn from_connector_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let hostname = config.require("hostname")?.to_string();
        let port = config.get::<u16>("port").unwrap_or(5432);
        let database = config.require("database")?.to_string();
        let username = config.require("username")?.to_string();
        let password = config
            .options
            .get("password")
            .cloned()
            .unwrap_or_default();
        let schema_name = config
            .options
            .get("schema.name")
            .cloned()
            .unwrap_or_else(|| "public".to_string());
        let table_name = config.require("table.name")?.to_string();
        let slot_name = config.require("slot.name")?.to_string();
        let publication_name = config
            .options
            .get("publication.name")
            .cloned()
            .unwrap_or_else(|| "laminardb_publication".to_string());
        let snapshot_mode = config
            .options
            .get("snapshot.mode")
            .and_then(|s| SnapshotMode::from_str(s))
            .unwrap_or(SnapshotMode::Initial);

        Ok(Self {
            hostname,
            port,
            database,
            username,
            password,
            schema_name,
            table_name,
            slot_name,
            publication_name,
            snapshot_mode,
            ..Default::default()
        })
    }

    /// Returns the PostgreSQL connection string.
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.hostname, self.port, self.database, self.username, self.password,
        )
    }

    /// Returns the fully qualified table name.
    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

/// Snapshot mode for initial data loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Perform initial snapshot on first connect, then switch to streaming.
    Initial,
    /// Never perform snapshot; start from current WAL position.
    Never,
    /// Always perform snapshot on every restart.
    Always,
    /// Export snapshot using a consistent SERIALIZABLE transaction.
    InitialOnly,
}

impl SnapshotMode {
    /// Parses from string representation.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "initial" => Some(Self::Initial),
            "never" => Some(Self::Never),
            "always" => Some(Self::Always),
            "initial_only" | "initial-only" => Some(Self::InitialOnly),
            _ => None,
        }
    }
}

/// SSL mode for PostgreSQL connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Disable SSL.
    Disable,
    /// Prefer SSL but allow plaintext.
    Prefer,
    /// Require SSL.
    Require,
    /// Require SSL and verify server certificate.
    VerifyCa,
    /// Require SSL, verify server certificate, and verify hostname.
    VerifyFull,
}

/// Metrics specific to the PostgreSQL CDC connector.
#[derive(Debug, Clone, Default)]
pub struct CdcMetrics {
    /// Total WAL bytes consumed.
    pub wal_bytes_consumed: u64,
    /// Total CDC events processed (inserts + updates + deletes).
    pub events_total: u64,
    /// Insert events processed.
    pub inserts: u64,
    /// Update events processed.
    pub updates: u64,
    /// Delete events processed.
    pub deletes: u64,
    /// Snapshot rows processed (during initial snapshot).
    pub snapshot_rows: u64,
    /// Current LSN position.
    pub current_lsn: u64,
    /// Confirmed (flushed) LSN position.
    pub confirmed_lsn: u64,
    /// Replication lag in bytes (current WAL tip - confirmed LSN).
    pub replication_lag_bytes: u64,
    /// Heartbeats sent.
    pub heartbeats_sent: u64,
    /// Schema changes detected.
    pub schema_changes: u64,
    /// Deserialization errors (non-fatal).
    pub deser_errors: u64,
    /// Last event timestamp (for watermark tracking).
    pub last_event_timestamp: i64,
    /// Time of last successful poll.
    pub last_poll_time: Option<Instant>,
    /// Connection reconnect count.
    pub reconnects: u64,
}

/// Internal state of the CDC connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcPhase {
    /// Not yet connected.
    Uninitialized,
    /// Performing initial snapshot.
    Snapshotting,
    /// Streaming WAL changes.
    Streaming,
    /// Handling schema change.
    SchemaChange,
    /// Connector is stopped.
    Stopped,
}
```

### 3.2 PostgresCdcSource Implementation

```rust
/// PostgreSQL CDC source connector.
///
/// Captures real-time changes from PostgreSQL tables using logical
/// replication (pgoutput plugin). Runs in Ring 1 as a tokio task,
/// pushing Arrow RecordBatch data into Ring 0 via SPSC channels.
///
/// # Lifecycle
///
/// ```text
/// new() --> open() --> [snapshot()] --> [poll_batch() loop] --> close()
///              |             |                   |
///              |             |             commit_offsets()
///              |             |             (on checkpoint)
///              |             |
///         create slot    read table
///         create pub     emit rows
///         discover       switch to
///         schema         streaming
/// ```
///
/// # Exactly-Once Semantics
///
/// The connector achieves exactly-once consumption by:
/// 1. Storing the confirmed LSN in `SourceCheckpoint`
/// 2. On recovery, resuming WAL consumption from the checkpointed LSN
/// 3. PostgreSQL guarantees that logical replication replays from
///    the slot's confirmed_flush_lsn
///
/// # Ring Architecture
///
/// - **Ring 0**: No CDC code runs here. Data arrives via SPSC channel.
/// - **Ring 1**: WAL consumption, event parsing, Arrow conversion, watermark emission.
/// - **Ring 2**: Slot lifecycle, publication management, health checks, schema discovery.
pub struct PostgresCdcSource {
    /// Primary client for queries (schema discovery, snapshot).
    client: Option<Client>,
    /// Replication client for WAL streaming.
    replication_client: Option<ReplicationClient>,
    /// Replication slot name.
    slot_name: String,
    /// Publication name.
    publication: String,
    /// Connector configuration.
    config: PostgresCdcConfig,
    /// Current LSN position (last received).
    lsn: PgLsn,
    /// Confirmed (flushed) LSN position (last checkpointed).
    confirmed_lsn: PgLsn,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Internal CDC phase.
    phase: CdcPhase,
    /// CDC-specific metrics.
    metrics: CdcMetrics,
    /// Discovered table schema (Arrow format).
    schema: Option<SchemaRef>,
    /// PostgreSQL column metadata (for type mapping).
    pg_columns: Vec<PgColumn>,
    /// Row decoder for converting pgoutput tuples to Arrow arrays.
    decoder: Option<PgOutputDecoder>,
    /// Timestamp of last heartbeat sent to PostgreSQL.
    last_heartbeat: Instant,
    /// Whether initial snapshot has been completed.
    snapshot_completed: bool,
}

/// Wrapper around the PostgreSQL replication connection.
///
/// Uses `tokio-postgres` with the `replication` connection parameter
/// to establish a streaming replication session.
pub struct ReplicationClient {
    /// The underlying tokio-postgres client in replication mode.
    client: Client,
    /// Replication stream for receiving WAL data.
    stream: Option<ReplicationStream>,
}

/// Decoded WAL message from pgoutput.
#[derive(Debug)]
pub enum WalMessage {
    /// Relation (table) definition - sent before first row of a relation.
    Relation(RelationMessage),
    /// INSERT operation.
    Insert(InsertMessage),
    /// UPDATE operation with optional before-image.
    Update(UpdateMessage),
    /// DELETE operation with before-image (if REPLICA IDENTITY is set).
    Delete(DeleteMessage),
    /// Transaction begin.
    Begin(BeginMessage),
    /// Transaction commit with commit LSN and timestamp.
    Commit(CommitMessage),
    /// Truncate operation.
    Truncate(TruncateMessage),
    /// Keepalive from the server (requires response if reply_requested).
    Keepalive(KeepaliveMessage),
    /// Type definition (for custom types).
    Type(TypeMessage),
    /// Origin (replication origin name).
    Origin(OriginMessage),
}

/// Relation (table) metadata from pgoutput.
#[derive(Debug, Clone)]
pub struct RelationMessage {
    /// Relation OID.
    pub relation_id: u32,
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Replica identity setting (default, nothing, full, index).
    pub replica_identity: ReplicaIdentity,
    /// Column definitions.
    pub columns: Vec<PgOutputColumn>,
}

/// A column in a pgoutput relation message.
#[derive(Debug, Clone)]
pub struct PgOutputColumn {
    /// Column name.
    pub name: String,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// Type modifier (e.g., varchar length).
    pub type_modifier: i32,
    /// Whether this column is part of the replica identity key.
    pub is_key: bool,
}

/// INSERT WAL message.
#[derive(Debug)]
pub struct InsertMessage {
    /// Relation OID.
    pub relation_id: u32,
    /// New tuple values.
    pub new_tuple: Vec<TupleValue>,
}

/// UPDATE WAL message.
#[derive(Debug)]
pub struct UpdateMessage {
    /// Relation OID.
    pub relation_id: u32,
    /// Old tuple values (present only with REPLICA IDENTITY FULL or key changes).
    pub old_tuple: Option<Vec<TupleValue>>,
    /// New tuple values.
    pub new_tuple: Vec<TupleValue>,
}

/// DELETE WAL message.
#[derive(Debug)]
pub struct DeleteMessage {
    /// Relation OID.
    pub relation_id: u32,
    /// Old tuple values (key columns only, or all with REPLICA IDENTITY FULL).
    pub old_tuple: Vec<TupleValue>,
}

/// BEGIN transaction message.
#[derive(Debug)]
pub struct BeginMessage {
    /// Transaction commit LSN (final LSN, set retroactively).
    pub final_lsn: PgLsn,
    /// Commit timestamp (microseconds since PostgreSQL epoch).
    pub commit_time: i64,
    /// Transaction ID (XID).
    pub xid: u32,
}

/// COMMIT transaction message.
#[derive(Debug)]
pub struct CommitMessage {
    /// Flags (currently unused by PostgreSQL).
    pub flags: u8,
    /// LSN of the commit record.
    pub commit_lsn: PgLsn,
    /// End LSN (position after the commit record).
    pub end_lsn: PgLsn,
    /// Commit timestamp (microseconds since PostgreSQL epoch).
    pub commit_time: i64,
}

/// Keepalive message from PostgreSQL.
#[derive(Debug)]
pub struct KeepaliveMessage {
    /// Current WAL end position on the server.
    pub wal_end: PgLsn,
    /// Server timestamp (microseconds since PostgreSQL epoch).
    pub timestamp: i64,
    /// Whether the server is requesting an immediate reply.
    pub reply_requested: bool,
}

/// A decoded tuple value from pgoutput.
#[derive(Debug, Clone)]
pub enum TupleValue {
    /// NULL value.
    Null,
    /// Text representation of the value.
    Text(String),
    /// Binary representation (for binary format).
    Binary(Vec<u8>),
    /// Unchanged TOAST value (not sent in the WAL message).
    Unchanged,
}

/// Replica identity setting on a PostgreSQL table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// Default: primary key columns only.
    Default,
    /// Nothing: no old values sent.
    Nothing,
    /// Full: all columns sent as old values.
    Full,
    /// Index: columns of a specific index.
    Index,
}

/// PostgreSQL column metadata for type mapping.
#[derive(Debug, Clone)]
pub struct PgColumn {
    /// Column name.
    pub name: String,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Whether the column is part of the primary key.
    pub is_primary_key: bool,
    /// Mapped Arrow DataType.
    pub arrow_type: arrow_schema::DataType,
}
```

### 3.3 SourceConnector Trait Implementation

```rust
#[async_trait]
impl SourceConnector for PostgresCdcSource {
    fn info(&self) -> ConnectorInfo {
        ConnectorInfo {
            connector_type: "postgres-cdc".to_string(),
            description: "PostgreSQL CDC source via logical replication".to_string(),
            config_keys: vec![
                ConfigKeySpec {
                    name: "hostname".into(),
                    description: "PostgreSQL server hostname".into(),
                    required: true,
                    default: None,
                    example: Some("localhost".into()),
                },
                ConfigKeySpec {
                    name: "port".into(),
                    description: "PostgreSQL server port".into(),
                    required: false,
                    default: Some("5432".into()),
                    example: Some("5432".into()),
                },
                ConfigKeySpec {
                    name: "database".into(),
                    description: "Database name".into(),
                    required: true,
                    default: None,
                    example: Some("mydb".into()),
                },
                ConfigKeySpec {
                    name: "username".into(),
                    description: "Username with REPLICATION privilege".into(),
                    required: true,
                    default: None,
                    example: Some("replication_user".into()),
                },
                ConfigKeySpec {
                    name: "password".into(),
                    description: "Password for authentication".into(),
                    required: false,
                    default: None,
                    example: None,
                },
                ConfigKeySpec {
                    name: "schema.name".into(),
                    description: "PostgreSQL schema name".into(),
                    required: false,
                    default: Some("public".into()),
                    example: Some("public".into()),
                },
                ConfigKeySpec {
                    name: "table.name".into(),
                    description: "Table to capture changes from".into(),
                    required: true,
                    default: None,
                    example: Some("users".into()),
                },
                ConfigKeySpec {
                    name: "slot.name".into(),
                    description: "Logical replication slot name".into(),
                    required: true,
                    default: None,
                    example: Some("laminardb_users_slot".into()),
                },
                ConfigKeySpec {
                    name: "publication.name".into(),
                    description: "PostgreSQL publication name (created if absent)".into(),
                    required: false,
                    default: Some("laminardb_publication".into()),
                    example: Some("my_publication".into()),
                },
                ConfigKeySpec {
                    name: "snapshot.mode".into(),
                    description: "Snapshot mode: initial, never, always, initial_only".into(),
                    required: false,
                    default: Some("initial".into()),
                    example: Some("initial".into()),
                },
            ],
            supported_formats: vec!["debezium-json".into(), "json".into()],
        }
    }

    fn state(&self) -> ConnectorState {
        self.state
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.config = PostgresCdcConfig::from_connector_config(config)?;
        self.state = ConnectorState::Running;

        // Step 1: Establish query connection (Ring 2 - setup)
        let client = self.connect_query_client().await?;
        self.client = Some(client);

        // Step 2: Discover schema from pg_catalog
        self.pg_columns = self.discover_pg_columns().await?;
        self.schema = Some(self.build_arrow_schema());

        // Step 3: Ensure publication exists
        self.ensure_publication().await?;

        // Step 4: Create or reuse replication slot
        let slot_lsn = self.ensure_replication_slot().await?;

        // Step 5: Perform initial snapshot if required
        if self.should_snapshot() {
            self.phase = CdcPhase::Snapshotting;
            self.perform_snapshot().await?;
            self.snapshot_completed = true;
        }

        // Step 6: Establish replication connection (Ring 1 - streaming)
        let repl_client = self.connect_replication_client().await?;
        self.replication_client = Some(repl_client);

        // Step 7: Start WAL streaming from slot LSN (or snapshot LSN)
        let start_lsn = if self.lsn > PgLsn::new(0) {
            self.lsn
        } else {
            slot_lsn
        };
        self.start_replication(start_lsn).await?;

        self.phase = CdcPhase::Streaming;
        self.last_heartbeat = Instant::now();

        tracing::info!(
            slot = %self.config.slot_name,
            table = %self.config.qualified_table_name(),
            lsn = %start_lsn,
            "PostgreSQL CDC source started"
        );

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let effective_max = max_records.min(self.config.max_batch_size);
        let mut events: Vec<CdcEvent> = Vec::with_capacity(effective_max);
        let mut last_lsn = self.lsn;

        // Send heartbeat if interval elapsed
        if self.last_heartbeat.elapsed() >= self.config.heartbeat_interval {
            self.send_heartbeat().await?;
            self.last_heartbeat = Instant::now();
        }

        // Read WAL messages until batch is full or no more data
        let repl = self.replication_client.as_mut()
            .ok_or(ConnectorError::InvalidState {
                state: ConnectorState::Failed,
                expected: ConnectorState::Running,
            })?;

        while events.len() < effective_max {
            match repl.try_next_message().await? {
                Some(msg) => {
                    match self.decode_wal_message(msg)? {
                        WalMessage::Insert(insert) => {
                            events.push(CdcEvent {
                                operation: CdcOperation::Insert,
                                after: Some(self.decode_tuple(&insert.new_tuple)?),
                                before: None,
                                lsn: last_lsn,
                                timestamp: self.metrics.last_event_timestamp,
                            });
                            self.metrics.inserts += 1;
                            self.metrics.events_total += 1;
                        }
                        WalMessage::Update(update) => {
                            let before = if let Some(old) = &update.old_tuple {
                                Some(self.decode_tuple(old)?)
                            } else {
                                None
                            };
                            events.push(CdcEvent {
                                operation: CdcOperation::UpdateAfter,
                                after: Some(self.decode_tuple(&update.new_tuple)?),
                                before,
                                lsn: last_lsn,
                                timestamp: self.metrics.last_event_timestamp,
                            });
                            self.metrics.updates += 1;
                            self.metrics.events_total += 1;
                        }
                        WalMessage::Delete(delete) => {
                            events.push(CdcEvent {
                                operation: CdcOperation::Delete,
                                after: None,
                                before: Some(self.decode_tuple(&delete.old_tuple)?),
                                lsn: last_lsn,
                                timestamp: self.metrics.last_event_timestamp,
                            });
                            self.metrics.deletes += 1;
                            self.metrics.events_total += 1;
                        }
                        WalMessage::Commit(commit) => {
                            last_lsn = commit.end_lsn;
                            self.metrics.last_event_timestamp = commit.commit_time;
                        }
                        WalMessage::Begin(begin) => {
                            self.metrics.last_event_timestamp = begin.commit_time;
                        }
                        WalMessage::Relation(rel) => {
                            self.handle_relation_message(rel)?;
                        }
                        WalMessage::Keepalive(ka) => {
                            if ka.reply_requested {
                                self.send_heartbeat().await?;
                                self.last_heartbeat = Instant::now();
                            }
                        }
                        _ => {
                            // Truncate, Type, Origin - log and skip
                            tracing::debug!("Skipping non-data WAL message");
                        }
                    }
                }
                None => break, // No more messages available
            }
        }

        if events.is_empty() {
            self.metrics.last_poll_time = Some(Instant::now());
            return Ok(None);
        }

        // Update LSN
        self.lsn = last_lsn;
        self.metrics.current_lsn = last_lsn.value();

        // Convert events to Arrow RecordBatch
        let record_batch = self.events_to_record_batch(&events)?;
        let event_times: Vec<i64> = events.iter().map(|e| e.timestamp).collect();

        let checkpoint = SourceCheckpoint {
            offsets: HashMap::from([
                ("lsn".to_string(), last_lsn.to_pg_string()),
            ]),
            epoch: 0, // Set by runtime
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.metrics.last_poll_time = Some(Instant::now());

        Ok(Some(SourceBatch {
            records: record_batch,
            offsets: checkpoint,
            event_times: Some(event_times),
            partition: None, // Single partition (WAL is sequential)
        }))
    }

    async fn commit_offsets(
        &mut self,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        if let Some(lsn_str) = checkpoint.offsets.get("lsn") {
            let lsn = PgLsn::from_pg_str(lsn_str)?;

            // Advance the replication slot to the confirmed LSN.
            // This tells PostgreSQL it can reclaim WAL segments up to this point.
            self.advance_slot(lsn).await?;

            self.confirmed_lsn = lsn;
            self.metrics.confirmed_lsn = lsn.value();

            tracing::debug!(lsn = %lsn, "PostgreSQL CDC offsets committed");
        }
        Ok(())
    }

    fn current_offsets(&self) -> SourceCheckpoint {
        SourceCheckpoint {
            offsets: HashMap::from([
                ("lsn".to_string(), self.lsn.to_pg_string()),
            ]),
            epoch: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    async fn discover_schema(&self) -> Result<Option<SchemaRef>, ConnectorError> {
        Ok(self.schema.clone())
    }

    async fn health_check(&self) -> Result<HealthStatus, ConnectorError> {
        let healthy = self.state == ConnectorState::Running
            && self.phase == CdcPhase::Streaming;

        let lag = self.metrics.replication_lag_bytes;
        let lag_warning = lag > self.config.max_wal_lag_bytes;

        let mut details = HashMap::new();
        details.insert("phase".to_string(), format!("{:?}", self.phase));
        details.insert("current_lsn".to_string(), self.lsn.to_pg_string());
        details.insert("confirmed_lsn".to_string(), self.confirmed_lsn.to_pg_string());
        details.insert("replication_lag_bytes".to_string(), lag.to_string());
        details.insert("events_total".to_string(), self.metrics.events_total.to_string());
        details.insert("slot_name".to_string(), self.config.slot_name.clone());

        Ok(HealthStatus {
            healthy: healthy && !lag_warning,
            message: if !healthy {
                format!("Connector is {:?} in phase {:?}", self.state, self.phase)
            } else if lag_warning {
                format!("Replication lag is {lag} bytes (threshold: {})", self.config.max_wal_lag_bytes)
            } else {
                "Healthy".to_string()
            },
            last_success: self.metrics.last_poll_time.map(|t| t.elapsed()),
            details,
        })
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Stopped;
        self.phase = CdcPhase::Stopped;

        // Send final standby status update with confirmed LSN
        if let Some(repl) = &mut self.replication_client {
            let _ = self.send_heartbeat().await;
        }

        // Close replication connection
        self.replication_client = None;

        // Note: We do NOT drop the replication slot here.
        // Slot cleanup is a Ring 2 administrative operation.
        // Users must explicitly DROP the slot via SQL or admin API.

        tracing::info!(
            slot = %self.config.slot_name,
            final_lsn = %self.lsn,
            events = self.metrics.events_total,
            "PostgreSQL CDC source closed"
        );

        Ok(())
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.metrics.events_total,
            bytes_total: self.metrics.wal_bytes_consumed,
            errors_total: self.metrics.deser_errors,
            records_per_sec: 0.0, // Computed by runtime
            avg_batch_latency_us: 0,
            p99_batch_latency_us: 0,
            lag: Some(self.metrics.replication_lag_bytes),
            custom: HashMap::from([
                ("inserts".into(), self.metrics.inserts as f64),
                ("updates".into(), self.metrics.updates as f64),
                ("deletes".into(), self.metrics.deletes as f64),
                ("snapshot_rows".into(), self.metrics.snapshot_rows as f64),
                ("heartbeats_sent".into(), self.metrics.heartbeats_sent as f64),
                ("schema_changes".into(), self.metrics.schema_changes as f64),
                ("reconnects".into(), self.metrics.reconnects as f64),
            ]),
        }
    }
}
```

### 3.4 CDC Event and Conversion

```rust
/// Internal CDC event before Arrow conversion.
#[derive(Debug)]
struct CdcEvent {
    /// CDC operation type (maps to F063 CdcOperation).
    operation: CdcOperation,
    /// Row values after the change (INSERT, UPDATE).
    after: Option<Vec<TupleValue>>,
    /// Row values before the change (UPDATE, DELETE).
    before: Option<Vec<TupleValue>>,
    /// LSN at which this event occurred.
    lsn: PgLsn,
    /// Event timestamp (PostgreSQL commit time).
    timestamp: i64,
}

impl PostgresCdcSource {
    /// Converts a batch of CDC events to an Arrow RecordBatch.
    ///
    /// The RecordBatch includes the user-defined columns plus metadata columns:
    /// - `_op`: CDC operation type (I/U/D)
    /// - `_ts`: Event timestamp
    /// - `_lsn`: PostgreSQL LSN (for debugging)
    fn events_to_record_batch(
        &self,
        events: &[CdcEvent],
    ) -> Result<RecordBatch, ConnectorError> {
        let schema = self.schema.as_ref()
            .ok_or(ConnectorError::Config("Schema not discovered".into()))?;

        // Build Arrow arrays from CDC events
        // For each column in the schema, create an array builder
        // and populate it from the event tuples.
        //
        // UPDATE events: use the "after" image for column values.
        // DELETE events: use the "before" image for column values.
        // INSERT events: use the "after" image for column values.
        //
        // The _op column carries the operation type for downstream
        // Z-set changelog processing (F063 integration).

        let batch = self.build_record_batch_from_events(events, schema)?;
        Ok(batch)
    }
}
```

### 3.5 Architecture Diagram

```
+------------------------------------------------------------------------+
|                     PostgreSQL CDC Source Architecture                    |
|                                                                          |
|  PostgreSQL Server                                                       |
|  +------------------+                                                    |
|  | WAL (Write-Ahead |    Logical Replication Protocol                    |
|  |     Log)         |    (pgoutput plugin)                               |
|  |                  |                                                    |
|  | Table: users     |--+                                                 |
|  | Table: orders    |  |                                                 |
|  +------------------+  |                                                 |
|         |              |                                                 |
|  +------v-----------+  |                                                 |
|  | Publication      |  |                                                 |
|  | (table filter)   |  |                                                 |
|  +------------------+  |                                                 |
|         |              |                                                 |
|  +------v-----------+  |                                                 |
|  | Replication Slot |  |                                                 |
|  | (LSN tracking)   |  |                                                 |
|  +------------------+  |                                                 |
|         |              |                                                 |
+---------|--------------+                                                 |
|         | TCP/SSL Connection                                             |
|         v                                                                |
|  +====================================================================+ |
|  |                     LaminarDB                                       | |
|  |                                                                     | |
|  |  RING 2 (Control Plane)                                             | |
|  |  +--------------------+  +--------------------+                     | |
|  |  | Slot Manager       |  | Schema Discovery   |                     | |
|  |  | - create_slot()    |  | - pg_catalog query |                     | |
|  |  | - drop_slot()      |  | - type mapping     |                     | |
|  |  | - advance_slot()   |  | - change detection |                     | |
|  |  +--------------------+  +--------------------+                     | |
|  |  +--------------------+                                             | |
|  |  | Health Monitor     |                                             | |
|  |  | - lag tracking     |                                             | |
|  |  | - reconnection     |                                             | |
|  |  +--------------------+                                             | |
|  |                                                                     | |
|  |  RING 1 (Background I/O)                                           | |
|  |  +--------------------+  +--------------------+  +--------------+  | |
|  |  | ReplicationClient  |  | PgOutputDecoder    |  | Arrow        |  | |
|  |  | - WAL streaming    |->| - parse messages   |->| Converter    |  | |
|  |  | - heartbeat        |  | - decode tuples    |  | - RecordBatch|  | |
|  |  | - keepalive        |  | - Z-set mapping    |  | - type cast  |  | |
|  |  +--------------------+  +--------------------+  +------+-------+  | |
|  |                                                         |          | |
|  |  ........................................................|.........| |
|  |  .                  SPSC Channel (~5ns)                 |        . | |
|  |  ........................................................|.........| |
|  |                                                         v          | |
|  |  RING 0 (Hot Path) - NO CDC CODE HERE                              | |
|  |  +--------------------+  +--------------------+  +--------------+  | |
|  |  | Source<ArrowRecord>|  | Streaming Operators|  | Sink<T>      |  | |
|  |  | (receives batch)   |->| (filter, window,  |->| (emit to     |  | |
|  |  |                    |  |  join, aggregate)  |  |  downstream) |  | |
|  |  +--------------------+  +--------------------+  +--------------+  | |
|  |                                                                     | |
|  +====================================================================+ |
+--------------------------------------------------------------------------+
```

### 3.6 Three-Ring Integration

| Ring | Component | Responsibilities | Latency Budget |
|------|-----------|-----------------|----------------|
| **Ring 0 (Hot Path)** | `Source<ArrowRecord>` | Receive RecordBatch from SPSC channel, process through operators | < 500ns (channel pop only) |
| **Ring 1 (Background)** | `PostgresCdcSource` | WAL streaming, pgoutput decoding, Arrow conversion, heartbeat, watermark emission | 1-100ms (network I/O bound) |
| **Ring 2 (Control)** | Slot Manager, Health | Create/drop replication slots, schema discovery, health checks, lag monitoring, reconnection | No latency requirement |

---

## 4. Latency Considerations

### 4.1 End-to-End Latency Breakdown

| Stage | Latency | Ring | Notes |
|-------|---------|------|-------|
| PostgreSQL WAL write | ~1ms | N/A | Server-side |
| WAL decode + network | 1-10ms | Ring 1 | Depends on network |
| pgoutput parse | 1-10us | Ring 1 | Per event, CPU bound |
| Arrow RecordBatch build | 10-100us | Ring 1 | Per batch, allocation OK |
| SPSC channel push | ~5ns | Ring 0/1 boundary | Lock-free |
| Operator processing | < 500ns | Ring 0 | Zero-allocation |
| **Total end-to-end** | **2-15ms** | - | **Dominated by network** |

### 4.2 What Stays OFF the Hot Path

- TCP connection to PostgreSQL (Ring 1)
- WAL byte stream reading (Ring 1)
- pgoutput message parsing and tuple decoding (Ring 1)
- Arrow array building and type conversion (Ring 1)
- Heartbeat/keepalive messages (Ring 1)
- Replication slot management (Ring 2)
- Schema discovery queries (Ring 2)
- Health checks and lag calculation (Ring 2)
- Reconnection logic (Ring 2)

### 4.3 Hot Path Budget Impact

The CDC connector adds exactly **~5ns** to Ring 0 latency, which is the cost of a single SPSC channel pop operation. All CDC-specific logic runs in Ring 1 or Ring 2.

---

## 5. SQL Integration

### 5.1 CREATE SOURCE Syntax

```sql
-- Full syntax with all options
CREATE SOURCE TABLE users_cdc (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _op VARCHAR,          -- CDC operation: I (insert), U (update), D (delete)
    _ts TIMESTAMP,        -- Event timestamp from PostgreSQL commit time
    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
) WITH (
    connector = 'postgres-cdc',
    hostname = 'db.example.com',
    port = '5432',
    username = 'replication_user',
    password = 'secret',
    database = 'myapp',
    'schema.name' = 'public',
    'table.name' = 'users',
    'slot.name' = 'laminardb_users_slot',
    'publication.name' = 'laminardb_pub',
    format = 'debezium-json',
    'snapshot.mode' = 'initial'
);
```

### 5.2 Minimal Configuration

```sql
-- Minimal: uses defaults for port, schema, publication, snapshot
CREATE SOURCE TABLE orders_cdc (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10, 2),
    status VARCHAR,
    order_time TIMESTAMP,
    WATERMARK FOR order_time AS order_time - INTERVAL '10' SECOND
) WITH (
    connector = 'postgres-cdc',
    hostname = 'localhost',
    username = 'laminardb',
    password = 'secret',
    database = 'shop',
    'table.name' = 'orders',
    'slot.name' = 'laminardb_orders'
);
```

### 5.3 Schema Discovery

```sql
-- Discover schema from PostgreSQL catalog
DESCRIBE CONNECTOR 'postgres-cdc' WITH (
    hostname = 'db.example.com',
    username = 'replication_user',
    password = 'secret',
    database = 'myapp',
    'schema.name' = 'public',
    'table.name' = 'users'
);

-- Returns:
-- +------------+----------+----------+-------------+
-- | column     | pg_type  | arrow_type | nullable   |
-- +------------+----------+----------+-------------+
-- | id         | int8     | Int64    | false       |
-- | name       | varchar  | Utf8     | true        |
-- | email      | varchar  | Utf8     | true        |
-- | status     | varchar  | Utf8     | true        |
-- | updated_at | timestamptz | Timestamp | true     |
-- +------------+----------+----------+-------------+
```

### 5.4 Downstream Query Examples

```sql
-- Materialized view with CDC: count active users by status
CREATE MATERIALIZED VIEW user_status_counts AS
SELECT status, COUNT(*) as cnt
FROM users_cdc
WHERE _op <> 'D'
GROUP BY status
EMIT CHANGES;

-- Real-time join: enrich orders with user data
SELECT o.order_id, o.amount, u.name, u.email
FROM orders_cdc o
JOIN users_cdc u ON o.customer_id = u.id
WHERE o._op = 'I';

-- Tumbling window aggregate over CDC events
SELECT
    TUMBLE_START(order_time, INTERVAL '1' MINUTE) as window_start,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders_cdc
WHERE _op = 'I'
GROUP BY TUMBLE(order_time, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;
```

---

## 6. Rust API Examples

### 6.1 Programmatic Usage

```rust
use laminar_connectors::cdc::postgres::{PostgresCdcSource, PostgresCdcConfig, SnapshotMode};
use laminar_connectors::connector::{ConnectorConfig, ConnectorRuntime};

// Create the CDC source
let config = PostgresCdcConfig {
    hostname: "localhost".to_string(),
    port: 5432,
    database: "myapp".to_string(),
    username: "replication_user".to_string(),
    password: "secret".to_string(),
    schema_name: "public".to_string(),
    table_name: "users".to_string(),
    slot_name: "laminardb_users_slot".to_string(),
    publication_name: "laminardb_pub".to_string(),
    snapshot_mode: SnapshotMode::Initial,
    ..Default::default()
};

let mut source = PostgresCdcSource::new(config);

// Use with ConnectorRuntime
let mut runtime = ConnectorRuntime::new();
let handle = runtime.start_source(
    "users_cdc",
    Box::new(source),
    Box::new(DebeziumDeserializer::default()),
    connector_config,
).await?;

// Subscribe to CDC events
let subscription = handle.sink.subscribe();
while let Some(record) = subscription.recv().await {
    let batch = record.to_record_batch();
    println!("Received {} CDC events", batch.num_rows());
}
```

### 6.2 Using LaminarDB Facade

```rust
use laminardb::prelude::*;

let db = LaminarDB::open()?;

// Create CDC source via SQL
db.execute(r#"
    CREATE SOURCE TABLE users_cdc (
        id BIGINT,
        name VARCHAR,
        email VARCHAR,
        _op VARCHAR,
        _ts TIMESTAMP,
        WATERMARK FOR _ts AS _ts - INTERVAL '5' SECOND
    ) WITH (
        connector = 'postgres-cdc',
        hostname = 'localhost',
        username = 'laminardb',
        password = 'secret',
        database = 'myapp',
        'table.name' = 'users',
        'slot.name' = 'laminardb_users_slot'
    )
"#).await?;

// Query CDC data
let results = db.execute(
    "SELECT name, email FROM users_cdc WHERE _op = 'I'"
).await?;
```

### 6.3 Slot Management API

```rust
use laminar_connectors::cdc::postgres::SlotManager;

let manager = SlotManager::connect("host=localhost dbname=myapp user=admin").await?;

// Create replication slot
let lsn = manager.create_slot("laminardb_users_slot", "pgoutput").await?;
println!("Slot created at LSN: {lsn}");

// Check slot status
let status = manager.slot_status("laminardb_users_slot").await?;
println!("Slot lag: {} bytes", status.wal_lag_bytes);

// Drop slot (administrative cleanup)
manager.drop_slot("laminardb_users_slot").await?;
```

---

## 7. Type Mapping: PostgreSQL to Arrow

### 7.1 Type Mapping Table

| PostgreSQL Type | OID | Arrow DataType | Notes |
|----------------|-----|----------------|-------|
| `boolean` | 16 | `Boolean` | |
| `smallint` / `int2` | 21 | `Int16` | |
| `integer` / `int4` | 23 | `Int32` | |
| `bigint` / `int8` | 20 | `Int64` | |
| `real` / `float4` | 700 | `Float32` | |
| `double precision` / `float8` | 701 | `Float64` | |
| `numeric` / `decimal` | 1700 | `Decimal128(38, scale)` | Scale from type modifier |
| `varchar` / `character varying` | 1043 | `Utf8` | |
| `text` | 25 | `Utf8` | |
| `char` / `character` | 1042 | `Utf8` | Fixed width, right-padded |
| `bytea` | 17 | `Binary` | |
| `date` | 1082 | `Date32` | Days since epoch |
| `time` | 1083 | `Time64(Microsecond)` | |
| `timestamp` | 1114 | `Timestamp(Microsecond, None)` | Without timezone |
| `timestamptz` | 1184 | `Timestamp(Microsecond, Some("UTC"))` | With timezone |
| `interval` | 1186 | `Duration(Microsecond)` | |
| `uuid` | 2950 | `FixedSizeBinary(16)` | |
| `json` | 114 | `Utf8` | Stored as text |
| `jsonb` | 3802 | `Utf8` | Stored as text |
| `int4[]` | 1007 | `List(Int32)` | Array types |
| `text[]` | 1009 | `List(Utf8)` | Array types |
| `inet` | 869 | `Utf8` | IP address as text |
| `macaddr` | 829 | `Utf8` | MAC address as text |
| `point` | 600 | `Struct({x: Float64, y: Float64})` | Geometric point |
| *Unknown* | - | `Utf8` | Fallback: text representation |

### 7.2 Type Mapping Implementation

```rust
/// Maps PostgreSQL type OIDs to Arrow DataType.
pub fn pg_type_to_arrow(type_oid: u32, type_modifier: i32) -> arrow_schema::DataType {
    use arrow_schema::DataType;

    match type_oid {
        16 => DataType::Boolean,
        21 => DataType::Int16,
        23 => DataType::Int32,
        20 => DataType::Int64,
        700 => DataType::Float32,
        701 => DataType::Float64,
        1700 => {
            // numeric/decimal: extract precision and scale from type modifier
            let scale = if type_modifier > 0 {
                ((type_modifier - 4) & 0xFFFF) as i8
            } else {
                6 // default scale
            };
            DataType::Decimal128(38, scale)
        }
        25 | 1042 | 1043 => DataType::Utf8,
        17 => DataType::Binary,
        1082 => DataType::Date32,
        1083 => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        1114 => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        1184 => DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            Some("UTC".into()),
        ),
        2950 => DataType::FixedSizeBinary(16),
        114 | 3802 => DataType::Utf8, // json/jsonb as text
        _ => DataType::Utf8, // Fallback: text representation
    }
}
```

---

## 8. Key Subsystems

### 8.1 Initial Snapshot

```rust
impl PostgresCdcSource {
    /// Performs initial snapshot of the table for bootstrapping.
    ///
    /// Uses a SERIALIZABLE transaction to get a consistent snapshot,
    /// then reads the table in batches. The snapshot LSN is recorded
    /// so that WAL streaming can resume from the correct position.
    ///
    /// # Algorithm
    ///
    /// 1. BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY
    /// 2. SELECT pg_current_wal_lsn() -- record snapshot LSN
    /// 3. SELECT * FROM schema.table ORDER BY pk LIMIT batch_size OFFSET n
    ///    (repeated until all rows read)
    /// 4. COMMIT
    /// 5. Switch to WAL streaming from snapshot LSN
    async fn perform_snapshot(&mut self) -> Result<(), ConnectorError> {
        let client = self.client.as_ref()
            .ok_or(ConnectorError::Connection("No query client".into()))?;

        // Start serializable transaction for consistent snapshot
        client.execute(
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
            &[],
        ).await.map_err(|e| ConnectorError::Connection(e.to_string()))?;

        // Record snapshot LSN
        let row = client.query_one("SELECT pg_current_wal_lsn()::text", &[]).await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let lsn_str: &str = row.get(0);
        let snapshot_lsn = PgLsn::from_pg_str(lsn_str)?;

        tracing::info!(
            table = %self.config.qualified_table_name(),
            lsn = %snapshot_lsn,
            "Starting initial snapshot"
        );

        // Read table in batches
        let query = format!(
            "SELECT * FROM {} ORDER BY ctid",
            self.config.qualified_table_name()
        );

        let rows = client.query(&query, &[]).await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        // Convert rows to CDC events (all as INSERT operations)
        let total_rows = rows.len();
        self.metrics.snapshot_rows = total_rows as u64;

        // Commit snapshot transaction
        client.execute("COMMIT", &[]).await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        // Set LSN to snapshot position for WAL streaming start
        self.lsn = snapshot_lsn;
        self.metrics.current_lsn = snapshot_lsn.value();

        tracing::info!(
            table = %self.config.qualified_table_name(),
            rows = total_rows,
            lsn = %snapshot_lsn,
            "Initial snapshot complete"
        );

        Ok(())
    }

    fn should_snapshot(&self) -> bool {
        match self.config.snapshot_mode {
            SnapshotMode::Initial => !self.snapshot_completed,
            SnapshotMode::Always => true,
            SnapshotMode::Never => false,
            SnapshotMode::InitialOnly => !self.snapshot_completed,
        }
    }
}
```

### 8.2 Replication Slot Management

```rust
/// Replication slot management operations (Ring 2).
///
/// Handles creation, deletion, and monitoring of PostgreSQL logical
/// replication slots. Slots track the WAL position for the consumer
/// and prevent PostgreSQL from recycling WAL segments that have not
/// yet been consumed.
///
/// WARNING: Unused slots cause WAL bloat. Always drop slots when
/// the connector is permanently removed.
pub struct SlotManager {
    client: Client,
}

impl SlotManager {
    /// Connects to PostgreSQL for slot management operations.
    pub async fn connect(conn_string: &str) -> Result<Self, ConnectorError> {
        let (client, connection) = tokio_postgres::connect(conn_string, tokio_postgres::NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        // Spawn connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "PostgreSQL connection error");
            }
        });

        Ok(Self { client })
    }

    /// Creates a logical replication slot.
    ///
    /// Returns the consistent point (LSN) at which the slot becomes active.
    pub async fn create_slot(
        &self,
        slot_name: &str,
        plugin: &str,
    ) -> Result<PgLsn, ConnectorError> {
        let row = self.client.query_one(
            "SELECT lsn FROM pg_create_logical_replication_slot($1, $2)",
            &[&slot_name, &plugin],
        ).await.map_err(|e| ConnectorError::Connection(
            format!("Failed to create slot '{slot_name}': {e}")
        ))?;

        let lsn_str: &str = row.get(0);
        PgLsn::from_pg_str(lsn_str)
    }

    /// Drops a logical replication slot.
    pub async fn drop_slot(&self, slot_name: &str) -> Result<(), ConnectorError> {
        self.client.execute(
            "SELECT pg_drop_replication_slot($1)",
            &[&slot_name],
        ).await.map_err(|e| ConnectorError::Connection(
            format!("Failed to drop slot '{slot_name}': {e}")
        ))?;
        Ok(())
    }

    /// Advances the replication slot's confirmed flush position.
    ///
    /// This tells PostgreSQL that WAL up to this LSN has been consumed
    /// and can be recycled.
    pub async fn advance_slot(
        &self,
        slot_name: &str,
        lsn: PgLsn,
    ) -> Result<(), ConnectorError> {
        self.client.execute(
            "SELECT pg_replication_slot_advance($1, $2::pg_lsn)",
            &[&slot_name, &lsn.to_pg_string()],
        ).await.map_err(|e| ConnectorError::Connection(
            format!("Failed to advance slot '{slot_name}' to {lsn}: {e}")
        ))?;
        Ok(())
    }

    /// Returns the status of a replication slot.
    pub async fn slot_status(
        &self,
        slot_name: &str,
    ) -> Result<SlotStatus, ConnectorError> {
        let row = self.client.query_one(
            "SELECT active, restart_lsn::text, confirmed_flush_lsn::text, \
             pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        ).await.map_err(|e| ConnectorError::Connection(
            format!("Slot '{slot_name}' not found: {e}")
        ))?;

        Ok(SlotStatus {
            active: row.get(0),
            restart_lsn: PgLsn::from_pg_str(row.get(1))?,
            confirmed_flush_lsn: PgLsn::from_pg_str(row.get(2))?,
            wal_lag_bytes: row.get::<_, i64>(3) as u64,
        })
    }

    /// Lists all replication slots.
    pub async fn list_slots(&self) -> Result<Vec<SlotInfo>, ConnectorError> {
        let rows = self.client.query(
            "SELECT slot_name, plugin, active, restart_lsn::text \
             FROM pg_replication_slots WHERE slot_type = 'logical'",
            &[],
        ).await.map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(rows.iter().map(|row| SlotInfo {
            name: row.get(0),
            plugin: row.get(1),
            active: row.get(2),
            restart_lsn: row.get::<_, Option<&str>>(3)
                .and_then(|s| PgLsn::from_pg_str(s).ok()),
        }).collect())
    }
}

/// Status of a replication slot.
#[derive(Debug, Clone)]
pub struct SlotStatus {
    /// Whether the slot is currently active (has a consumer).
    pub active: bool,
    /// The oldest WAL position needed by this slot.
    pub restart_lsn: PgLsn,
    /// The LSN up to which the consumer has confirmed receipt.
    pub confirmed_flush_lsn: PgLsn,
    /// WAL lag in bytes (current WAL position - confirmed flush LSN).
    pub wal_lag_bytes: u64,
}

/// Information about a replication slot.
#[derive(Debug, Clone)]
pub struct SlotInfo {
    /// Slot name.
    pub name: String,
    /// Output plugin name (e.g., "pgoutput").
    pub plugin: String,
    /// Whether the slot is active.
    pub active: bool,
    /// Restart LSN (may be None if never used).
    pub restart_lsn: Option<PgLsn>,
}
```

### 8.3 Schema Change Detection

```rust
impl PostgresCdcSource {
    /// Handles a Relation message from pgoutput.
    ///
    /// Relation messages are sent before the first row of a table
    /// (or after a schema change). We compare the new schema against
    /// the known schema and take appropriate action.
    fn handle_relation_message(
        &mut self,
        relation: RelationMessage,
    ) -> Result<(), ConnectorError> {
        let qualified_name = format!("{}.{}", relation.schema, relation.table);

        if qualified_name != self.config.qualified_table_name() {
            // Not our table - skip
            return Ok(());
        }

        // Compare with known columns
        let new_columns: Vec<PgColumn> = relation.columns.iter().map(|col| {
            PgColumn {
                name: col.name.clone(),
                type_oid: col.type_oid,
                nullable: true, // pgoutput does not send nullability
                is_primary_key: col.is_key,
                arrow_type: pg_type_to_arrow(col.type_oid, col.type_modifier),
            }
        }).collect();

        if new_columns != self.pg_columns {
            self.metrics.schema_changes += 1;

            // Detect type of change
            let changes = detect_schema_changes(&self.pg_columns, &new_columns);

            tracing::warn!(
                table = %qualified_name,
                changes = ?changes,
                "Schema change detected in PostgreSQL table"
            );

            // Update internal schema
            self.pg_columns = new_columns;
            self.schema = Some(self.build_arrow_schema());

            // Update decoder
            self.decoder = Some(PgOutputDecoder::new(&self.pg_columns));
        }

        Ok(())
    }
}

/// Types of schema changes detected.
#[derive(Debug)]
pub enum SchemaChange {
    /// New column added.
    ColumnAdded { name: String, type_oid: u32 },
    /// Column removed.
    ColumnRemoved { name: String },
    /// Column type changed.
    ColumnTypeChanged { name: String, old_oid: u32, new_oid: u32 },
    /// Column renamed.
    ColumnRenamed { old_name: String, new_name: String },
}

/// Detects schema changes between old and new column lists.
fn detect_schema_changes(old: &[PgColumn], new: &[PgColumn]) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    let old_names: HashMap<&str, &PgColumn> = old.iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let new_names: HashMap<&str, &PgColumn> = new.iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    // Detect removed columns
    for (name, _col) in &old_names {
        if !new_names.contains_key(name) {
            changes.push(SchemaChange::ColumnRemoved {
                name: name.to_string(),
            });
        }
    }

    // Detect added columns and type changes
    for (name, new_col) in &new_names {
        if let Some(old_col) = old_names.get(name) {
            if old_col.type_oid != new_col.type_oid {
                changes.push(SchemaChange::ColumnTypeChanged {
                    name: name.to_string(),
                    old_oid: old_col.type_oid,
                    new_oid: new_col.type_oid,
                });
            }
        } else {
            changes.push(SchemaChange::ColumnAdded {
                name: name.to_string(),
                type_oid: new_col.type_oid,
            });
        }
    }

    changes
}
```

### 8.4 Heartbeat and Keepalive

```rust
impl PostgresCdcSource {
    /// Sends a standby status update (heartbeat) to PostgreSQL.
    ///
    /// This serves two purposes:
    /// 1. Prevents PostgreSQL from timing out the replication connection
    /// 2. Reports the confirmed flush LSN so PostgreSQL can reclaim WAL
    ///
    /// The standby status update includes:
    /// - write_lsn: Last WAL position written to local storage
    /// - flush_lsn: Last WAL position flushed/confirmed
    /// - apply_lsn: Last WAL position applied to state
    /// - timestamp: Current time (microseconds since 2000-01-01)
    /// - reply_requested: false (we initiate, not responding)
    async fn send_heartbeat(&mut self) -> Result<(), ConnectorError> {
        let repl = self.replication_client.as_mut()
            .ok_or(ConnectorError::Connection("No replication client".into()))?;

        repl.send_standby_status(
            self.lsn,           // write position
            self.confirmed_lsn, // flush position
            self.confirmed_lsn, // apply position
        ).await.map_err(|e| ConnectorError::Connection(
            format!("Failed to send heartbeat: {e}")
        ))?;

        self.metrics.heartbeats_sent += 1;
        Ok(())
    }
}
```

### 8.5 Z-Set Changelog Integration (F063)

```rust
use laminar_core::operator::changelog::{CdcOperation, ChangelogRecord};

impl PostgresCdcSource {
    /// Maps PostgreSQL CDC events to F063 Z-set ChangelogRecord format.
    ///
    /// PostgreSQL CDC operations map to Z-set weights as follows:
    /// - INSERT  -> ChangelogRecord::insert()  -> weight = +1
    /// - DELETE  -> ChangelogRecord::delete()   -> weight = -1
    /// - UPDATE  -> ChangelogRecord::retraction() -> pair of (-1, +1)
    ///
    /// For UPDATE events with REPLICA IDENTITY FULL:
    ///   1. Emit UpdateBefore (weight=-1) with old row values
    ///   2. Emit UpdateAfter  (weight=+1) with new row values
    ///
    /// This ensures downstream operators (aggregates, joins, MVs) can
    /// correctly retract old values and apply new values incrementally.
    fn to_changelog_records(
        &self,
        events: &[CdcEvent],
    ) -> Vec<ChangelogRecord<RecordBatch>> {
        let mut records = Vec::with_capacity(events.len() * 2); // *2 for update pairs

        for event in events {
            match event.operation {
                CdcOperation::Insert => {
                    if let Some(ref after) = event.after {
                        records.push(ChangelogRecord::insert(
                            self.tuple_to_batch(after),
                            event.timestamp,
                        ));
                    }
                }
                CdcOperation::Delete => {
                    if let Some(ref before) = event.before {
                        records.push(ChangelogRecord::delete(
                            self.tuple_to_batch(before),
                            event.timestamp,
                        ));
                    }
                }
                CdcOperation::UpdateAfter => {
                    // Emit retraction pair if before-image available
                    if let (Some(ref before), Some(ref after)) =
                        (&event.before, &event.after)
                    {
                        let (retract, insert) = ChangelogRecord::retraction(
                            self.tuple_to_batch(before),
                            self.tuple_to_batch(after),
                            event.timestamp,
                        );
                        records.push(retract);
                        records.push(insert);
                    } else if let Some(ref after) = event.after {
                        // No before-image: emit as insert (best effort)
                        records.push(ChangelogRecord::insert(
                            self.tuple_to_batch(after),
                            event.timestamp,
                        ));
                    }
                }
                _ => {}
            }
        }

        records
    }
}
```

---

## 9. Configuration Reference

### 9.1 Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'postgres-cdc'` |
| `hostname` | String | PostgreSQL server hostname or IP address |
| `database` | String | Target database name |
| `username` | String | User with `REPLICATION` privilege |
| `table.name` | String | Table to capture changes from |
| `slot.name` | String | Logical replication slot name (unique per consumer) |

### 9.2 Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | Integer | `5432` | PostgreSQL server port |
| `password` | String | `""` | Authentication password |
| `schema.name` | String | `"public"` | PostgreSQL schema containing the table |
| `publication.name` | String | `"laminardb_publication"` | PostgreSQL publication name |
| `format` | String | `"debezium-json"` | Deserialization format |
| `snapshot.mode` | String | `"initial"` | Snapshot mode: `initial`, `never`, `always`, `initial_only` |
| `ssl.mode` | String | `"prefer"` | SSL mode: `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `heartbeat.interval.ms` | Integer | `10000` | Heartbeat interval in milliseconds |
| `max.batch.size` | Integer | `1024` | Maximum records per poll batch |
| `poll.interval.ms` | Integer | `100` | Poll interval when no data available |
| `max.wal.lag.bytes` | Integer | `104857600` | WAL lag threshold for health warning (100MB) |
| `slot.advance.interval.ms` | Integer | `5000` | How often to confirm LSN to PostgreSQL |
| `connect.timeout.ms` | Integer | `10000` | Connection timeout |
| `include.before.image` | Boolean | `true` | Include old row values for UPDATE/DELETE (requires REPLICA IDENTITY FULL) |

### 9.3 PostgreSQL Server Prerequisites

The PostgreSQL server must be configured for logical replication:

```ini
# postgresql.conf
wal_level = logical
max_replication_slots = 4      # At least 1 per CDC source
max_wal_senders = 4            # At least 1 per CDC source
wal_sender_timeout = 60s       # Must be > heartbeat interval
```

The connecting user must have the `REPLICATION` role:

```sql
-- Create replication user
CREATE ROLE replication_user WITH REPLICATION LOGIN PASSWORD 'secret';

-- Grant SELECT on target tables (for schema discovery and snapshots)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;

-- Set REPLICA IDENTITY FULL for before-images on UPDATE/DELETE
ALTER TABLE users REPLICA IDENTITY FULL;

-- Create publication (optional - connector creates if absent)
CREATE PUBLICATION laminardb_pub FOR TABLE users, orders;
```

---

## 10. Error Handling and Recovery

### 10.1 Error Categories

| Error Category | Ring | Recovery Strategy | Example |
|---------------|------|-------------------|---------|
| **Connection Lost** | Ring 1 | Exponential backoff reconnect (100ms -> 30s) | Network partition, server restart |
| **Slot Does Not Exist** | Ring 2 | Re-create slot + full snapshot | Manual slot deletion |
| **Publication Missing** | Ring 2 | Re-create publication | Manual publication drop |
| **Schema Mismatch** | Ring 1 | Log warning, adapt schema dynamically | ALTER TABLE ADD COLUMN |
| **Deserialization Error** | Ring 1 | Skip record, increment error counter | Corrupt WAL entry |
| **WAL Lag Exceeded** | Ring 2 | Health check warning, alert | Slow consumer |
| **Authentication Failure** | Ring 2 | Fail permanently, require reconfiguration | Password change |
| **Replication Timeout** | Ring 1 | Reconnect, resume from confirmed LSN | Idle too long without heartbeat |

### 10.2 Reconnection Logic

```rust
impl PostgresCdcSource {
    /// Reconnects to PostgreSQL with exponential backoff.
    ///
    /// On reconnection:
    /// 1. Re-establish query connection
    /// 2. Verify replication slot still exists
    /// 3. Re-establish replication connection
    /// 4. Resume WAL streaming from confirmed_lsn
    async fn reconnect(&mut self) -> Result<(), ConnectorError> {
        let mut backoff = Duration::from_millis(100);
        let max_backoff = Duration::from_secs(30);
        let mut attempts = 0;

        loop {
            attempts += 1;
            tracing::warn!(
                attempt = attempts,
                backoff_ms = backoff.as_millis(),
                "Attempting PostgreSQL CDC reconnection"
            );

            match self.try_reconnect().await {
                Ok(()) => {
                    self.metrics.reconnects += 1;
                    tracing::info!(
                        attempt = attempts,
                        lsn = %self.confirmed_lsn,
                        "PostgreSQL CDC reconnected successfully"
                    );
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        attempt = attempts,
                        error = %e,
                        "Reconnection failed"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
    }

    async fn try_reconnect(&mut self) -> Result<(), ConnectorError> {
        // Close existing connections
        self.replication_client = None;
        self.client = None;

        // Re-establish connections
        let client = self.connect_query_client().await?;
        self.client = Some(client);

        let repl_client = self.connect_replication_client().await?;
        self.replication_client = Some(repl_client);

        // Resume from confirmed LSN
        self.start_replication(self.confirmed_lsn).await?;
        self.phase = CdcPhase::Streaming;

        Ok(())
    }
}
```

---

## 11. Per-Partition Watermark Integration (F064)

Although PostgreSQL logical replication is a single WAL stream (not partitioned like Kafka), the CDC connector integrates with per-partition watermarks in two scenarios:

### 11.1 Single-Partition Mode (Default)

For a single table source, the connector registers as a single partition:

```rust
// During open()
watermark_tracker.register_source(source_id, 1); // 1 partition

// During poll_batch()
let partition = PartitionId::new(source_id, 0);
watermark_tracker.update_partition(partition, commit_timestamp);
```

### 11.2 Multi-Table Mode (Publication with Multiple Tables)

When a publication covers multiple tables, each table is treated as a logical partition:

```rust
// During open(), register N partitions for N tables in publication
watermark_tracker.register_source(source_id, table_count);

// During poll_batch(), route events by relation_id
let partition = PartitionId::new(source_id, table_index);
watermark_tracker.update_partition(partition, commit_timestamp);
```

### 11.3 Idle Detection

PostgreSQL commit timestamps drive watermark advancement. When no transactions are committed, the heartbeat mechanism provides a fallback:

```rust
// If no events for > idle_timeout, mark partition idle
// The heartbeat's server timestamp can advance the watermark
if let WalMessage::Keepalive(ka) = msg {
    // Use server timestamp as watermark hint for idle detection
    watermark_tracker.update_partition(partition, ka.timestamp);
}
```

---

## 12. Implementation Roadmap

### Phase 1: Core Connection and Slot Management (3-4 days)
- [ ] `PgLsn` type with parse/format/compare
- [ ] `PostgresCdcConfig` with `from_connector_config()`
- [ ] `SlotManager`: create, drop, advance, status
- [ ] `PostgresCdcSource::open()` establishing query and replication connections
- [ ] Publication creation via `ensure_publication()`
- [ ] Unit tests for LSN parsing, config validation
- [ ] Unit tests for slot management (mocked)

### Phase 2: WAL Streaming and pgoutput Decoding (4-5 days)
- [ ] `ReplicationClient` with WAL streaming
- [ ] pgoutput message parser: Relation, Insert, Update, Delete, Begin, Commit
- [ ] `WalMessage` enum with full decode support
- [ ] `TupleValue` decoding from text/binary format
- [ ] Keepalive handling and heartbeat response
- [ ] `poll_batch()` implementation
- [ ] Unit tests for message parsing
- [ ] Unit tests for tuple decoding

### Phase 3: Arrow Conversion and Type Mapping (3-4 days)
- [ ] `pg_type_to_arrow()` type mapping function
- [ ] `PgOutputDecoder` for converting tuples to Arrow arrays
- [ ] `events_to_record_batch()` building RecordBatch from CDC events
- [ ] Schema discovery from `pg_catalog`
- [ ] CDC metadata columns (`_op`, `_ts`, `_lsn`)
- [ ] Unit tests for type mapping (all PostgreSQL types)
- [ ] Unit tests for RecordBatch construction

### Phase 4: Initial Snapshot (2-3 days)
- [ ] `perform_snapshot()` with SERIALIZABLE transaction
- [ ] Batched table reading with progress logging
- [ ] Snapshot LSN recording for seamless transition to streaming
- [ ] Snapshot mode configuration (initial, never, always)
- [ ] Unit tests for snapshot logic
- [ ] Integration test: snapshot + streaming transition

### Phase 5: Schema Change Detection and Z-Set Integration (2-3 days)
- [ ] `handle_relation_message()` for schema change detection
- [ ] `detect_schema_changes()` diff algorithm
- [ ] Dynamic schema adaptation (add/remove columns)
- [ ] `to_changelog_records()` F063 Z-set mapping
- [ ] Retraction pair generation for UPDATE events
- [ ] Unit tests for schema change scenarios
- [ ] Unit tests for Z-set mapping correctness

### Phase 6: Checkpointing, Health, and Hardening (2-3 days)
- [ ] `commit_offsets()` with slot advance
- [ ] `health_check()` with lag monitoring
- [ ] Reconnection with exponential backoff
- [ ] Per-partition watermark integration (F064)
- [ ] Error handling for all failure modes
- [ ] CdcMetrics population
- [ ] Integration tests: checkpoint + recovery
- [ ] Integration tests: reconnection after failure

### Phase 7: SQL Integration and Documentation (1-2 days)
- [ ] Register `postgres-cdc` in `ConnectorRegistry`
- [ ] Wire `CREATE SOURCE ... WITH (connector = 'postgres-cdc')` end-to-end
- [ ] `DESCRIBE CONNECTOR` support
- [ ] API documentation with examples
- [ ] Configuration reference documentation

---

## 13. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| End-to-end CDC latency | < 20ms (PostgreSQL commit to Ring 0) | Timestamped integration test |
| Ring 0 latency impact | < 10ns (SPSC channel only) | Benchmark with/without connector |
| Throughput | > 50K CDC events/sec | Benchmark with pgbench workload |
| Snapshot speed | > 100K rows/sec | Benchmark with large table |
| WAL parsing | < 5us per event | Micro-benchmark |
| Arrow conversion | < 10us per batch (1024 rows) | Micro-benchmark |
| Checkpoint (LSN commit) | < 1ms | Benchmark slot advance |
| Recovery time | < 5s (reconnect + resume) | Integration test |
| Test coverage | > 80% | `cargo tarpaulin` |
| Test count | 40+ tests | `cargo test` |
| Memory usage | < 50MB per connector | Profile with valgrind/heaptrack |

---

## 14. Module Structure

```
crates/laminar-connectors/src/
  cdc/
    mod.rs               # CDC module root, re-exports
    postgres/
      mod.rs             # PostgresCdcSource, PgLsn, config types
      config.rs          # PostgresCdcConfig, SnapshotMode, SslMode
      slot.rs            # SlotManager, SlotStatus, SlotInfo
      decoder.rs         # PgOutputDecoder, WalMessage parsing
      types.rs           # pg_type_to_arrow(), PgColumn
      snapshot.rs        # Initial snapshot logic
      schema.rs          # Schema discovery and change detection
      replication.rs     # ReplicationClient, WAL streaming
      metrics.rs         # CdcMetrics
      changelog.rs       # Z-set integration (F063 mapping)
      tests/
        mod.rs           # Test module root
        lsn_tests.rs     # PgLsn parsing and formatting
        config_tests.rs  # Config validation
        decoder_tests.rs # WAL message decoding
        types_tests.rs   # Type mapping
        slot_tests.rs    # Slot management (mocked)
        snapshot_tests.rs# Snapshot logic
        schema_tests.rs  # Schema change detection
        changelog_tests.rs # Z-set mapping
        integration.rs   # End-to-end tests (requires PostgreSQL)
```

---

## 15. References

1. **PostgreSQL Logical Replication Protocol**
   - [Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html)
   - [Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding.html)
   - [pgoutput Plugin](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)

2. **Debezium PostgreSQL Connector**
   - [Debezium PostgreSQL Documentation](https://debezium.io/documentation/reference/connectors/postgresql.html)
   - CDC envelope format and event structure

3. **DBSP / Feldera Z-Sets (VLDB 2025)**
   - Z-set mathematical foundation for incremental computation
   - Weight semantics: INSERT=+1, DELETE=-1

4. **LaminarDB Internal References**
   - [F034: Connector SDK](F034-connector-sdk.md) - `SourceConnector` trait, `ConnectorRuntime`
   - [F063: Changelog/Retraction](../phase-2/F063-changelog-retraction.md) - Z-set types, `CdcOperation`, `ChangelogRecord`
   - [F064: Per-Partition Watermarks](../phase-2/F064-per-partition-watermarks.md) - `PartitionedWatermarkTracker`
   - [F-STREAM-007: SQL DDL](streaming/F-STREAM-007-sql-ddl.md) - `CREATE SOURCE` parsing
   - [F022: Incremental Checkpointing](../phase-2/F022-incremental-checkpointing.md) - Checkpoint framework

5. **Research Papers**
   - [Emit Patterns Research 2026](../../research/emit-patterns-research-2026.md) - CDC envelope format decisions
   - [Watermark Generator Research 2026](../../research/watermark-generator-research-2026.md) - Idle detection patterns

6. **Crate Dependencies**
   - [`tokio-postgres`](https://docs.rs/tokio-postgres/) - Async PostgreSQL client (v0.7)
   - [`arrow-array`](https://docs.rs/arrow-array/) - Arrow columnar arrays
   - [`arrow-schema`](https://docs.rs/arrow-schema/) - Arrow schema definitions

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **CDC** | Change Data Capture - capturing row-level changes from a database |
| **WAL** | Write-Ahead Log - PostgreSQL's transaction log used for durability and replication |
| **LSN** | Log Sequence Number - monotonically increasing position in the PostgreSQL WAL |
| **pgoutput** | PostgreSQL's built-in logical replication output plugin (since PG 10) |
| **Replication Slot** | Server-side resource that tracks consumer position in the WAL stream |
| **Publication** | A set of tables whose changes are exposed via logical replication |
| **REPLICA IDENTITY** | Table-level setting controlling what old row values are included in WAL |
| **Standby Status Update** | Message sent from consumer to server confirming WAL consumption progress |
| **Z-Set** | Mathematical structure where elements have integer weights (+1/-1) for incremental computation |
| **Before-Image** | Old row values before an UPDATE or DELETE (requires REPLICA IDENTITY FULL) |
| **After-Image** | New row values after an INSERT or UPDATE |
| **Snapshot** | Full table scan for bootstrapping, performed before switching to WAL streaming |
| **DebeziumDeserializer** | F034 deserializer for Debezium CDC envelope JSON format |
| **SourceCheckpoint** | F034 generic checkpoint containing connector-specific offsets |
| **PartitionId** | F064 identifier for a partition within a source |

---

## Appendix B: Competitive Comparison

| Feature | LaminarDB F027 | Debezium PostgreSQL | Flink CDC PostgreSQL | RisingWave PostgreSQL CDC |
|---------|---------------|---------------------|---------------------|--------------------------|
| **Deployment** | Embedded (in-process) | Kafka Connect cluster | Flink cluster | Distributed cluster |
| **Protocol** | pgoutput (native) | pgoutput / decoderbufs | pgoutput | pgoutput |
| **Latency** | < 20ms end-to-end | 50-500ms | 50-500ms | 100ms+ |
| **Exactly-Once** | LSN checkpoint | At-least-once (Kafka offset) | Barrier checkpoint | Barrier checkpoint |
| **Initial Snapshot** | SERIALIZABLE txn | Lock-free snapshot | Snapshot supported | Snapshot supported |
| **Schema Evolution** | Dynamic adaptation | Schema registry | Schema change restart | Dynamic DDL |
| **Before-Images** | Native (REPLICA IDENTITY) | Native | Native | Native |
| **Z-Set Integration** | Native (F063) | N/A (raw CDC) | N/A (retraction mode) | Internal retraction |
| **Arrow Format** | Native RecordBatch | Java objects | Java objects | Internal columnar |
| **Ring Architecture** | Ring 1 (no hot path impact) | N/A | N/A | N/A |
| **Dependencies** | tokio-postgres only | Kafka + Connect + JVM | Flink + JVM | Full RisingWave cluster |
| **WAL Lag Monitoring** | Built-in health check | JMX metrics | N/A | System tables |
| **Multi-Table** | Publication-based | Per-table or regex | Per-table | Per-table |

---

## Appendix C: PostgreSQL Version Compatibility

| PostgreSQL Version | Support Level | Notes |
|-------------------|---------------|-------|
| 10.x | Full | Minimum version (logical replication introduced) |
| 11.x | Full | |
| 12.x | Full | |
| 13.x | Full | Improved logical replication performance |
| 14.x | Full | Binary mode for pgoutput (optional optimization) |
| 15.x | Full | Row filtering in publications, column lists |
| 16.x | Full | Logical replication from standby |
| 17.x | Full | Slot failover support |

**Minimum Requirement:** PostgreSQL 10+ with `wal_level = logical`

**Recommended:** PostgreSQL 14+ for binary mode optimization and publication column lists.
