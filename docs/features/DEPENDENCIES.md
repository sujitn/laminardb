# Feature Dependency Graph

> Extracted from INDEX.md for readability. See [INDEX.md](INDEX.md) for feature status.

## Phase 1

```
F001 (Reactor) ──┬──▶ F002 (State Store)
                 ├──▶ F003 (State Interface)
                 ├──▶ F004 (Tumbling Windows)
                 └──▶ F009 (Event Time)
                          │
F005 (DataFusion) ✅ ────▶ F006 (SQL Parser) ✅
                          │
F007 (WAL) ──────────────▶ F008 (Checkpointing)
                          │
F009 (Event Time) ───────▶ F010 (Watermarks) ──▶ F012 (Late Data)
                                              ──▶ F011 (EMIT)
```

## Phase 1.5 (SQL Parser)

```
F006 ──▶ Phase1 (CREATE SOURCE/SINK) ✅
              │
              ├──▶ Phase2 (Windows) ✅ ──▶ Phase3 (EMIT) ✅
              │                                │
              └──▶ Phase4 (Joins) ✅ ──────────┤
                                               ▼
                                        Phase5 (Planner) ✅
                                               │
Configures: F004, F016, F019, F020 ◀──────────┘

Output: parser/, planner/, translator/ modules (129 tests)
```

## Phase 2

### Core Dependencies

```
F001 ──▶ F013 (Thread-per-Core) ──▶ F014 (SPSC) ──▶ F015 (CPU Pinning)
F004 ──▶ F016 (Sliding) ──▶ F017 (Session) ──▶ F018 (Hopping)
F003 ──▶ F019 (Stream Joins) ──▶ F020 (Lookup) ──▶ F021 (Temporal)
                    │
                    ├──▶ F056 (ASOF Joins)
                    └──▶ F057 (Join Optimizations)
```

### Checkpoint Architecture

```
Ring 0: Changelog ──▶ Ring 1: Per-Core WAL ──▶ RocksDB ──▶ Checkpoint

F002 (mmap) + F063 (ChangelogBuffer) ──▶ F062 (Per-Core WAL) ──▶ F022
                                                                    │
F007 + F013 ──▶ F062 ──────────────────────────────────────────────┘
                                                                    │
F008 ──▶ F022 (Incremental) ──▶ F023 (Exactly-Once) ──▶ F024 (2PC)
```

### Emit & Changelog

```
F011 (EMIT Clause) ──▶ F011B (Extension) ──┐
                                           ├──▶ F023 (Exactly-Once Sinks)
F063 (Changelog/Retraction) ──────────────┘
                           │
                           └──▶ F060 (Cascading MVs)
```

### Watermark Evolution

```
F010 (Watermarks)
      │
      ├──▶ F064 (Per-Partition) ──┬──▶ F025 (Kafka Source)
      │                           │
      │                           └──▶ F065 (Keyed Watermarks)
      │
      └──▶ F066 (Alignment Groups) ──▶ F019 (Stream Joins)
```

### Thread-Per-Core Advanced

```
F013 (Thread-Per-Core) ✅
      │
      ├──▶ F067 (io_uring) ──▶ F069 (Three-Ring I/O)
      │
      ├──▶ F068 (NUMA Awareness)
      │
      ├──▶ F070 (Task Budget)
      │
      ├──▶ F071 (Zero-Alloc) ──▶ F073 (Zero-Alloc Polling)
      │
      └──▶ F072 (XDP) [P2]
```

### Financial Analytics

```
F004 (Tumbling) ──▶ F059 (FIRST/LAST) ──▶ F060 (Cascading MVs)
                                              │
                                              ▼
                                    F061 (Historical Backfill) [Phase 3]
```

## Phase 3

### Connector Dependencies

```
F006B ──▶ F025-F034 (Connectors need CREATE SOURCE/SINK)
F013 + F019 ──▶ F058 (Async State Access)
F060 + F031/F032 ──▶ F061 (Historical Backfill)
F063 ──▶ F027/F028 (CDC Connectors need changelog format)
F034 + F023 + F063 ──▶ F027B (PostgreSQL Sink)
```

### Cloud Storage & Delta Lake

```
F-CLOUD-001 (Credential Resolver)
      │
      ├──▶ F-CLOUD-002 (Config Validation) ──┬──▶ F031A (Delta I/O)
      │                                       ├──▶ F032 (Iceberg Sink)
      └──▶ F-CLOUD-003 (Secret Masking)       └──▶ F033 (Parquet Source)
```

### Delta Lake I/O Evolution

```
F031 (Delta Lake Sink) ✅
      │
      ├──▶ F031A (I/O Integration) ──┬──▶ F031B (Recovery & Exactly-Once)
      │                               ├──▶ F031C (Compaction)
      │                               └──▶ F031D (Schema Evolution)
      │
      └──▶ F061 (Historical Backfill)
```
