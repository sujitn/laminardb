# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-11

### What Was Accomplished
- **F082: Streaming Query Lifecycle** — Unified runtime object wiring compiled/fallback pipelines to bridges with lifecycle management
  - 2 new files: `metrics.rs` (NOT jit-gated), `query.rs` (jit-gated)
  - `QueryId`, `QueryState`, `QueryConfig`, `QueryError`, `SubmitResult`, `QueryMetadata`, `QueryMetrics` — always-available types
  - `StreamingQueryBuilder`: add_pipeline triplets (ExecutablePipeline + PipelineBridge + BridgeConsumer), build() validates equal lengths
  - `StreamingQuery`: lifecycle (start/pause/resume/stop), submit_row (compiled JIT + fallback passthrough), advance_watermark, checkpoint, send_eof
  - `poll_ring1()` / `check_latency_flush()` drain all consumers for Ring 1 actions
  - `swap()` hot-swap: validates same pipeline count, swaps internals, old query goes Stopped
  - `metrics()` aggregates PipelineStats + BridgeStatsSnapshot across all pipelines/consumers
  - 1 file modified: `mod.rs` (wiring + re-exports)
  - 35 tests (8 metrics + 27 query), clippy clean with `-D warnings`
  - Phase 2.5 Plan Compiler: **5/5 features COMPLETE (100%)** ✅

Previous session (2026-02-11):
- **F081: Ring 0/Ring 1 Pipeline Bridge** — Lock-free SPSC bridge from compiled Ring 0 to Ring 1 stateful operators
  - 2 new files: `policy.rs`, `pipeline_bridge.rs`
  - `BatchPolicy` (max_rows, max_latency, flush_on_watermark) + `BackpressureStrategy` (DropNewest, PauseSource, SpillToDisk)
  - `PipelineBridge` (producer): sends Event/Watermark/CheckpointBarrier/Eof via `SmallVec<[u8; 128]>`
  - `BridgeConsumer` (consumer): drains SPSC queue, accumulates rows via `RowBatchBridge`, emits `Ring1Action`s
  - Watermark-aware flushing prevents partial-batch emissions (Issue #55)
  - `BridgeStats` with `AtomicU64` counters shared between producer/consumer
  - `create_pipeline_bridge()` factory for paired construction
  - 1 file modified: `mod.rs` (wiring + re-exports, NOT gated behind `jit` feature)
  - 36 tests (6 policy + 30 bridge), clippy clean with `-D warnings`
  - Phase 2.5 Plan Compiler: 4/5 features complete (80%)

Previous session (2026-02-11):
- **F080: Plan Compiler Core** — Compile entire DataFusion LogicalPlan pipeline segments into single native Cranelift JIT functions
  - 5 new files: `pipeline.rs`, `extractor.rs`, `pipeline_compiler.rs`, `cache.rs`, `fallback.rs`
  - `PipelineExtractor` walks LogicalPlan top-down, splitting at stateful operators (Aggregate/Sort/Join) into compilable pipeline chains
  - `PipelineCompiler` generates Cranelift codegen fusing Filter→Project→KeyExtract stages into a single `fn(input_row, output_row) -> u8` native function
  - `CompilerCache` with FxHashMap + FIFO eviction avoids redundant compilation
  - `ExecutablePipeline` Compiled/Fallback for graceful degradation to DataFusion interpreted execution
  - 3 files modified: `expr.rs` (7 items pub(crate)), `error.rs` (+ExtractError), `mod.rs` (wiring)
  - 75 new tests (123 total compiler tests), clippy clean with `-D warnings`
  - Phase 2.5 Plan Compiler: 3/5 features complete (60%)

Previous session (2026-02-10):
- **F079: Compiled Expression Evaluator** — Cranelift JIT for DataFusion Expr trees
- **F017D+E: Session Emit Strategies + Timer Persistence** — Completes Issue #55

Previous session (2026-02-08):
- **pgwire-replication integration for PostgreSQL CDC WAL streaming** (PR #74, closes #58)
- **Unified Checkpoint System (F-CKP-001 through F-CKP-009)** — ALL 9 FEATURES COMPLETE

### Where We Left Off

**Phase 2: 38/38 features COMPLETE (100%)** ✅
**Phase 2.5: 5/5 features COMPLETE (100%)** ✅ — F078 ✅, F079 ✅, F080 ✅, F081 ✅, F082 ✅
**Phase 3: 67/76 features COMPLETE (88%)**

**Test counts**: ~1,635 laminar-core (with jit), ~3,100+ with all feature flags

### Immediate Next Steps

**Phase 3 remaining work**:
1. F027 follow-ups: TLS cert path support for pgwire-replication, initial snapshot, auto-reconnect
2. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
3. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)
4. Remaining Phase 3 gaps (F029, F030, F033, F058, F061)

### Open Issues
- **pgwire-replication TLS**: `SslMode::Require`/`VerifyCa`/`VerifyFull` currently fall back to disabled — need cert path configuration plumbing.
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
  operator/       # Windows, joins (stream/asof/temporal), changelog, lag_lead, table_cache (LRU + xor)
  state/          # State stores: InMemoryStore, ChangelogAwareStore (Ring 0 wrapper), ChangelogSink trait
  mv/             # Cascading materialized views
  tpc/            # Thread-per-core: SPSC, key router, core handle, backpressure, runtime
  sink/           # Exactly-once: transactional sink, epoch adapter
  alloc/          # Zero-allocation enforcement
  numa/           # NUMA-aware memory
  io_uring/       # io_uring + three-ring I/O
  xdp/            # XDP/eBPF network optimization
  budget/         # Task budget enforcement
  compiler/       # Plan compiler: EventRow, JIT expr compiler (Cranelift), constant folding,
                  #   pipeline extraction, pipeline compilation, cache, fallback,
                  #   metrics (always-available types), query (StreamingQuery lifecycle)

laminar-sql/src/
  parser/         # Streaming SQL: windows, emit, late data, joins, aggregation, analytics, ranking
  planner/        # StreamingPlanner, QueryPlan
  translator/     # Operator config builders: window, join, analytic, order, having, DDL
  datafusion/     # DataFusion integration: UDFs, aggregate bridge, execute_streaming_sql

laminar-connectors/src/
  kafka/          # Source, sink, Avro serde, schema registry, partitioner, backpressure
  postgres/       # Sink (COPY BINARY + upsert + exactly-once)
  cdc/postgres/   # CDC source (pgoutput decoder, Z-set changelog, replication I/O)
  cdc/mysql/      # CDC source (binlog decoder, GTID, Z-set changelog)
  lakehouse/      # Delta Lake + Iceberg sinks (buffering, epoch, changelog)
  storage/        # Cloud storage: provider detection, credential resolver, config validation, secret masking
  bridge/         # DAG ↔ connector bridge (source/sink bridges, runtime orchestration)
  sdk/            # Connector SDK: retry, rate limiting, circuit breaker, test harness, schema discovery
  serde/          # Format implementations: JSON, CSV, raw, Debezium, Avro

laminar-storage/src/
  incremental/    # Incremental checkpointing (RocksDB backend)
  per_core_wal/   # Per-core WAL segments
  checkpoint_manifest.rs  # Unified CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint
  checkpoint_store.rs     # CheckpointStore trait, FileSystemCheckpointStore (atomic writes)
  changelog_drainer.rs    # Ring 1 SPSC changelog consumer

laminar-db/src/
  checkpoint_coordinator.rs  # Unified checkpoint orchestrator (F-CKP-003)
  recovery_manager.rs       # Unified recovery: load manifest, restore all state (F-CKP-007)
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
