# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-07

### What Was Accomplished
- **F-CONN-002B: Connector-Backed Table Population** - COMPLETE (19 new tests, 441 connector / 181 laminar-db tests)
  - `reference.rs` (NEW): `ReferenceTableSource` trait, `RefreshMode` enum, `MockReferenceTableSource` (6 tests)
  - `registry.rs`: Added `TableSourceFactory`, `register_table_source()`, `create_table_source()`, `list_table_sources()` (3 tests)
  - `connector_manager.rs`: Added `refresh` field to `TableRegistration`, `build_table_config()`, `parse_refresh_mode()` (5 tests)
  - `pipeline_checkpoint.rs`: Added `table_offsets` field with `#[serde(default)]` backward compat (2 tests)
  - `db.rs`: `table_store` → `Arc<Mutex<>>`, snapshot phase before spawn, incremental CDC polling in loop, checkpoint wiring, `sync_table_to_df()` free fn, refresh mode DDL parsing (5 tests)
  - Tables are now snapshot-populated on startup and incrementally refreshed during processing

Previous session (2026-02-07):
- **F-SQL-006: Window Frame (ROWS BETWEEN)** - COMPLETE (22 new tests, 494 laminar-sql / 170 laminar-db tests)
- **F-SQL-005: Multi-Way JOIN Support** - COMPLETE (21 new tests)
- **F-SQL-004: HAVING Clause Execution** - COMPLETE (22 new tests)
- **F-CONN-003: Avro Serialization Hardening** - COMPLETE (~40 new tests)

### Where We Left Off

**Phase 3: 51/67 features COMPLETE (76%)**

All Phase 1 (12), Phase 1.5 (1), and Phase 2 (34) features are complete.
See [INDEX.md](./features/INDEX.md) for the full feature-by-feature breakdown.

**Test counts**: ~2,560 base, ~2,910 with all feature flags (`kafka`, `postgres-cdc`, `postgres-sink`, `delta-lake`, `mysql-cdc`, `ffi`)

### Immediate Next Steps
1. F-CONN-002C: PARTIAL Cache Mode & Xor Filter
2. F-CONN-002D: RocksDB-Backed Persistent Table Store
3. F-OBS-001: Pipeline Observability API
4. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
5. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)

### Open Issues
- **iceberg-rust crate**: Deferred until compatible with workspace DataFusion. Business logic complete in F032.
- No other blockers.

---

## Quick Reference

### Key Modules
```
laminar-core/src/
  dag/            # DAG pipeline: topology, multicast, routing, executor, checkpointing
  streaming/      # In-memory streaming: ring buffer, SPSC/MPSC channels, source, sink
  subscription/   # Reactive push-based: events, notifications, registry, dispatcher, backpressure, filtering
  time/           # Watermarks: partitioned, keyed, alignment groups
  operator/       # Windows, joins (stream/asof/temporal), changelog, lag_lead
  mv/             # Cascading materialized views
  tpc/            # Thread-per-core: SPSC, key router, core handle, backpressure, runtime
  sink/           # Exactly-once: transactional sink, epoch adapter
  alloc/          # Zero-allocation enforcement
  numa/           # NUMA-aware memory
  io_uring/       # io_uring + three-ring I/O
  xdp/            # XDP/eBPF network optimization
  budget/         # Task budget enforcement

laminar-sql/src/
  parser/         # Streaming SQL: windows, emit, late data, joins, aggregation, analytics, ranking
  planner/        # StreamingPlanner, QueryPlan
  translator/     # Operator config builders: window, join, analytic, order, having, DDL
  datafusion/     # DataFusion integration: UDFs, aggregate bridge, execute_streaming_sql

laminar-connectors/src/
  kafka/          # Source, sink, Avro serde, schema registry, partitioner, backpressure
  postgres/       # Sink (COPY BINARY + upsert + exactly-once)
  cdc/postgres/   # CDC source (pgoutput decoder, Z-set changelog)
  cdc/mysql/      # CDC source (binlog decoder, GTID, Z-set changelog)
  lakehouse/      # Delta Lake + Iceberg sinks (buffering, epoch, changelog)
  storage/        # Cloud storage: provider detection, credential resolver, config validation, secret masking
  bridge/         # DAG ↔ connector bridge (source/sink bridges, runtime orchestration)
  sdk/            # Connector SDK: retry, rate limiting, circuit breaker, test harness, schema discovery
  serde/          # Format implementations: JSON, CSV, raw, Debezium, Avro

laminar-storage/src/
  incremental/    # Incremental checkpointing (RocksDB backend)
  per_core_wal/   # Per-core WAL segments

laminar-db/src/
  api/            # FFI-ready API: Connection, Writer, QueryStream, ArrowSubscription
  ffi/            # C FFI: opaque handles, Arrow C Data Interface, async callbacks
```

### Useful Commands
```bash
cargo test --all --lib                    # Run all tests (base features)
cargo test --all --lib --features kafka   # Include Kafka/Avro tests
cargo bench --bench dag_bench             # DAG pipeline benchmarks
cargo clippy --all -- -D warnings         # Lint
```
