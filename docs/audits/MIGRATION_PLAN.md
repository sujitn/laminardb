# LaminarDB API Implementation Plan

> Date: 2026-01-28

## Overview

This plan adds a developer-friendly API layer on top of the existing LaminarDB engine. All existing code remains unchanged; the new API wraps and connects existing components.

---

## Phase A: Foundation (derive macros + facade skeleton)

### A-1: Create `laminar-derive` crate

**New crate:** `crates/laminar-derive/`

**Deliverables:**
- `#[derive(Record)]` proc macro
  - Generates `Record::schema()` from struct field types
  - Generates `Record::to_record_batch()` with correct Arrow array builders
  - Generates `Record::event_time()` from `#[event_time]` attribute
  - Supports: `String`, `i32`, `i64`, `f32`, `f64`, `bool`, `Option<T>`, `Vec<u8>`
  - `#[column("name")]` for column name override
  - `#[nullable]` for nullable columns
- `#[derive(FromRecordBatch)]` proc macro
  - Generates `FromRecordBatch::from_batch(batch, row_index) -> Self`
  - Column matching by field name (or `#[column("name")]`)
  - Proper Arrow downcast for each type
  - Error on schema mismatch (column not found, wrong type)

**Dependencies:** `syn`, `quote`, `proc-macro2`, `arrow` (for DataType constants)

**Tests:** 20+ tests covering:
- All supported field types
- `#[event_time]` attribute
- `#[column("name")]` override
- `Option<T>` nullable fields
- Round-trip: Record → RecordBatch → FromRecordBatch
- Error cases: missing column, wrong type

### A-2: Create `laminar-db` crate

**New crate:** `crates/laminar-db/`

**Deliverables:**
- `LaminarDB` struct (facade)
- `LaminarConfig` with `Default`
- `SourceCatalog` (in-memory registry of sources/sinks by name)
- `db.execute(sql)` dispatching to:
  - CREATE SOURCE → parse, register in catalog, create Source<ArrowRecord>/Sink
  - CREATE SINK → parse, register in catalog
  - DROP SOURCE/SINK → remove from catalog
  - SHOW SOURCES/SINKS → query catalog
  - DESCRIBE → look up schema from catalog
  - SELECT → execute via `execute_streaming_sql`, return QueryHandle
- `db.source::<T>(name)` → `SourceHandle<T>`
- `db.source_untyped(name)` → `UntypedSourceHandle`
- `QueryHandle` with `subscribe::<T>()` and `subscribe_raw()`
- `TypedSubscription<T>` wrapping `Subscription` with deserialization

**Dependencies:** `laminar-core`, `laminar-sql`, `laminar-derive`

**Tests:** 15+ tests:
- Create source via SQL, push typed data, verify schema match
- Execute windowed query, subscribe to typed results
- DROP SOURCE removes from catalog
- SHOW SOURCES returns correct list
- DESCRIBE returns schema
- Schema mismatch on `source::<WrongType>()` returns error

### A-3: Create `laminardb` convenience crate

**New crate:** `crates/laminardb/` (or root-level `laminardb/`)

**Deliverables:**
- Re-exports from `laminar-db` and `laminar-derive`
- `prelude` module with common imports
- Root-level `Cargo.toml` with `laminardb` package name

---

## Phase B: SQL Completions

### B-1: INSERT INTO

**Crate:** `laminar-sql` (parser) + `laminar-db` (execution)

**Deliverables:**
- Parse `INSERT INTO source_name VALUES (...)` via sqlparser
- Add `InsertInto` variant to `StreamingStatement`
- In `db.execute()`: convert values to RecordBatch, push to source
- Support single row and multi-row INSERT
- Return `ExecuteResult::RowsAffected(n)`

**Tests:** 8+ tests

### B-2: DROP SOURCE / DROP SINK

**Crate:** `laminar-sql` (parser) + `laminar-db` (execution)

**Deliverables:**
- Parse `DROP SOURCE [IF EXISTS] name`
- Parse `DROP SINK [IF EXISTS] name`
- Add `DropSource`/`DropSink` variants to `StreamingStatement`
- Remove from `SourceCatalog`
- Close associated channels

**Tests:** 6+ tests

### B-3: SHOW / DESCRIBE

**Crate:** `laminar-sql` (parser) + `laminar-db` (execution)

**Deliverables:**
- Parse `SHOW SOURCES`, `SHOW SINKS`, `SHOW QUERIES`
- Parse `DESCRIBE source_name`
- Return results as `RecordBatch` (column metadata)
- DESCRIBE includes: column name, data type, nullable, watermark info

**Tests:** 8+ tests

### B-4: CREATE MATERIALIZED VIEW

**Crate:** `laminar-sql` (parser) + `laminar-db` (execution)

**Deliverables:**
- Parse `CREATE MATERIALIZED VIEW name AS SELECT ...`
- Wire to `MvRegistry::register()`
- Support cascading (MV reading from MV)
- `DROP MATERIALIZED VIEW [IF EXISTS] name [CASCADE]`
- Query MVs via regular SELECT

**Tests:** 10+ tests (including cascading OHLC 1s → 1m → 1h)

---

## Phase C: Examples

### C-1: Create `examples/` directory

**Location:** `examples/` at repo root

**Deliverables:**
- `examples/quickstart.rs` - Minimal "hello world" (10 lines)
- `examples/ohlc_bars.rs` - Financial OHLC with TUMBLE window
- `examples/alerting.rs` - Sensor alerting with HAVING filter
- `examples/stream_join.rs` - Order + Payment join
- `examples/sql_only.rs` - Pure SQL workflow, no Rust types

Each example should be self-contained and runnable with `cargo run --example <name>`.

---

## Phase D: Advanced Features

### D-1: Pipeline Lifecycle

- `QueryHandle::cancel()` - Stop a running query
- `QueryHandle::is_active()` - Check if still running
- `db.close()` - Graceful shutdown with drain
- Error propagation from query to subscriber

### D-2: EXPLAIN

- Parse `EXPLAIN SELECT ...`
- Format `QueryPlan` as readable text
- Include window config, join config, emit strategy

### D-3: ASOF JOIN Direction

- Parse direction from `<=` (backward), `>=` (forward)
- Parse TOLERANCE clause
- Generate `AsofJoinConfig` in planner
- Wire to `AsofJoinOperator`

---

## Validation Criteria

| Criterion | Target |
|-----------|--------|
| Lines to "hello world" | ≤ 15 |
| Lines to OHLC pipeline | ≤ 40 |
| Schema boilerplate per struct | 2 lines (`#[derive(Record)]`) |
| All existing tests still pass | `cargo test --all` |
| New tests added | 60+ |
| Examples runnable | `cargo run --example quickstart` |
| No breaking changes to existing API | Zero |
