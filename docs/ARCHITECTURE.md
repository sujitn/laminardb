# Architecture

## Overview

LaminarDB is an embedded streaming database designed for sub-microsecond latency. It combines the deployment simplicity of SQLite with the streaming capabilities of Apache Flink.

## Design Principles

1. **Embedded First**: Single binary, no external dependencies
2. **Sub-Microsecond Latency**: Zero allocations on hot path
3. **SQL Native**: Full SQL support via DataFusion
4. **Thread-Per-Core**: Linear scaling with CPU cores
5. **Exactly-Once**: End-to-end exactly-once semantics

## Ring Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RING 0: HOT PATH                         │
│  Constraints: Zero allocations, no locks, < 1μs latency         │
│                                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │ Reactor │─▶│Operators│─▶│  State  │─▶│  Emit   │            │
│  │  Loop   │  │ (map,   │  │  Store  │  │ (output)│            │
│  │         │  │ filter, │  │ (lookup)│  │         │            │
│  │         │  │ window) │  │         │  │         │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
│       │                                                         │
│       │ SPSC Queues (lock-free)                                │
│       ▼                                                         │
├─────────────────────────────────────────────────────────────────┤
│                     RING 1: BACKGROUND                          │
│  Constraints: Can allocate, async I/O, bounded latency impact   │
│                                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │Checkpoint│  │   WAL   │  │Compaction│  │  Timer  │            │
│  │ Manager │  │  Writer │  │  Thread │  │  Wheel  │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
│       │                                                         │
│       │ Channels (bounded)                                      │
│       ▼                                                         │
├─────────────────────────────────────────────────────────────────┤
│                     RING 2: CONTROL PLANE                       │
│  Constraints: No latency requirements, full flexibility         │
│                                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │  Admin  │  │ Metrics │  │  Auth   │  │  Config │            │
│  │   API   │  │ Export  │  │ Engine  │  │ Manager │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

### Ring 0: Hot Path

The event processing path where latency matters most.

**Components:**
- **Reactor**: Single-threaded event loop, CPU-pinned
- **Operators**: Stateless transforms (map, filter) and stateful (window, join)
- **State Store**: Lock-free hash map for operator state
- **Emit**: Output to Ring 1 for sink processing

**Constraints:**
- No heap allocations (use arena allocators)
- No locks (SPSC queues for communication)
- No system calls (io_uring for async I/O)
- Predictable branching (likely/unlikely hints)

### Ring 1: Background

Handles durability and I/O without blocking the hot path.

**Components:**
- **Checkpoint Manager**: Periodic state snapshots
- **WAL Writer**: Write-ahead log with group commit
- **Compaction**: Background state store maintenance
- **Timer Wheel**: Efficient timer management

**Communication with Ring 0:**
- SPSC queues for commands and results
- Never blocks Ring 0 operations

### Ring 2: Control Plane

Administrative and observability functions.

**Components:**
- **Admin API**: REST/gRPC management interface
- **Metrics Export**: Prometheus/OpenTelemetry
- **Auth Engine**: RBAC/ABAC evaluation
- **Config Manager**: Dynamic configuration

## Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                         Data Flow                                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Source ──▶ Deserialize ──▶ [Operators] ──▶ Serialize ──▶ Sink  │
│    │                            │                           │    │
│    │                            ▼                           │    │
│    │                      ┌──────────┐                      │    │
│    │                      │  State   │                      │    │
│    │                      │  Store   │                      │    │
│    │                      └──────────┘                      │    │
│    │                            │                           │    │
│    │                            ▼                           │    │
│    │                      ┌──────────┐                      │    │
│    │                      │   WAL    │                      │    │
│    │                      └──────────┘                      │    │
│    │                                                        │    │
│    └────────────── Offset Tracking ─────────────────────────┘    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Crate Structure

```
crates/
├── laminar-core/       # Ring 0: Reactor, operators, state
│   ├── reactor/        # Event loop implementation
│   ├── operator/       # Streaming operators
│   ├── state/          # State store implementations
│   └── time/           # Event time, watermarks
│
├── laminar-sql/        # SQL layer
│   ├── parser/         # SQL parsing (streaming extensions)
│   ├── planner/        # Query planning
│   └── datafusion/     # DataFusion integration
│
├── laminar-storage/    # Ring 1: Durability
│   ├── wal/            # Write-ahead log
│   ├── checkpoint/     # Checkpointing
│   └── lakehouse/      # Delta Lake/Iceberg integration
│
├── laminar-connectors/ # External system connectors
│   ├── kafka/          # Kafka source/sink
│   ├── cdc/            # CDC connectors
│   └── lookup/         # Lookup tables
│
├── laminar-auth/       # Ring 2: Security
│   ├── authn/          # Authentication (JWT, mTLS)
│   ├── authz/          # Authorization (RBAC, ABAC)
│   └── rls/            # Row-level security
│
├── laminar-admin/      # Ring 2: Administration
│   ├── api/            # REST API
│   ├── dashboard/      # Web UI
│   └── cli/            # Command-line tools
│
├── laminar-observe/    # Ring 2: Observability
│   ├── metrics/        # Prometheus metrics
│   ├── tracing/        # Distributed tracing
│   └── health/         # Health checks
│
└── laminar-server/     # Standalone server
    ├── config/         # Configuration loading
    └── main.rs         # Entry point
```

## Key Abstractions

### Operator

```rust
pub trait Operator: Send {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> Vec<Output>;
    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> Vec<Output>;
    fn checkpoint(&self) -> OperatorState;
    fn restore(&mut self, state: OperatorState);
}
```

### State Store

```rust
pub trait StateStore: Send {
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn prefix_scan(&self, prefix: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])>;
}
```

### Source/Sink

```rust
#[async_trait]
pub trait Source: Send {
    async fn poll(&mut self) -> Option<SourceBatch>;
    async fn commit(&mut self, offsets: &Offsets);
}

#[async_trait]
pub trait Sink: Send {
    async fn write(&mut self, batch: RecordBatch);
    async fn commit(&mut self);
}
```

## Performance Characteristics

| Operation | Target | Technique |
|-----------|--------|-----------|
| State lookup | < 500ns | FxHashMap, cache-aligned |
| Event processing | < 1μs | Zero allocation, inlined |
| Throughput/core | 500K/s | Batch processing, SIMD |
| Checkpoint | < 10s recovery | Incremental, async I/O |
| Window trigger | < 10μs | Hierarchical timer wheel |
