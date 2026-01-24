# Session Context

> This file tracks session continuity. Update at the end of each session.

## Last Session

**Date**: 2026-01-24
**Duration**: ~1 hour

### What Was Accomplished
- ✅ Implemented F011 - EMIT Clause:
  - Added `EmitStrategy` enum to laminar-core with 3 strategies:
    - `OnWatermark` (default) - emit when watermark passes window end
    - `Periodic(Duration)` - emit intermediate results at fixed intervals
    - `OnUpdate` - emit after every state change
  - Updated `TumblingWindowOperator` to support configurable emit strategies
  - Implemented periodic timer system with special key encoding
  - Added `EmitClause::OnUpdate` to SQL parser
  - Enhanced EMIT clause parsing for all syntax variants
  - Added 12 new tests (9 for EmitStrategy, 3 for SQL parsing)
- ✅ All tests passing (204 total: 131 in laminar-core, 47 in laminar-sql)
- ✅ Clippy clean for laminar-core

### Where We Left Off
Successfully completed F011 - EMIT Clause. Window operators now support three emit strategies, allowing users to control the trade-off between result freshness and efficiency.

### Immediate Next Steps
1. **F012 - Late Data Handling** (P2) - Side output for late events
2. Phase 1 completion (11/12 features done, 92% complete)

### Open Issues
- None currently - F001 through F011 are complete

### Code Pointers
- **EmitStrategy enum**: `crates/laminar-core/src/operator/window.rs:66-86`
- **TumblingWindowOperator.set_emit_strategy**: `crates/laminar-core/src/operator/window.rs:803-821`
- **Periodic timer handling**: `crates/laminar-core/src/operator/window.rs:1000-1030`
- **EmitClause SQL AST**: `crates/laminar-sql/src/parser/statements.rs:84-110`
- **EMIT clause parsing**: `crates/laminar-sql/src/parser/parser_simple.rs:127-220`

---

## Session Notes

**EMIT Strategy Architecture:**
- `EmitStrategy` enum controls when window results are output
- Three strategies with different latency/efficiency trade-offs:
  - `OnWatermark` - most efficient, highest latency
  - `Periodic` - balanced, configurable interval
  - `OnUpdate` - lowest latency, highest overhead

**Using EmitStrategy:**
```rust
use laminar_core::operator::window::{
    TumblingWindowAssigner, TumblingWindowOperator, CountAggregator, EmitStrategy,
};
use std::time::Duration;

let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));
let mut operator = TumblingWindowOperator::new(
    assigner,
    CountAggregator::new(),
    Duration::from_secs(5),
);

// Emit intermediate results every 10 seconds (for dashboards)
operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(10)));

// Or emit on every update (lowest latency, use with caution)
operator.set_emit_strategy(EmitStrategy::OnUpdate);
```

**SQL Syntax:**
```sql
-- Default: emit on watermark
SELECT COUNT(*) FROM events
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
EMIT ON WATERMARK;

-- Periodic: emit every 10 seconds
SELECT SUM(amount) FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)
EMIT EVERY INTERVAL '10' SECOND;

-- On update: emit after every change
SELECT AVG(temperature) FROM sensors
GROUP BY TUMBLE(reading_time, INTERVAL '5' MINUTE)
EMIT ON UPDATE;
```

**Periodic Timer Encoding:**
- Periodic timers use high bit of first key byte as marker
- This distinguishes them from final watermark timers
- Both timer types use 16-byte keys (WindowId)

---

## Quick Reference

### Current Focus
- **Phase**: 1 - Core Engine (92% complete)
- **Completed**: F001 (Reactor), F002 (Memory-Mapped State Store), F003 (State Store Interface), F004 (Tumbling Windows), F005 (DataFusion Integration), F006 (Basic SQL Parser), F007 (Write-Ahead Log), F008 (Basic Checkpointing), F009 (Event Time Processing), F010 (Watermarks), F011 (EMIT Clause)
- **Remaining**: F012 (Late Data Handling)

### Key Files
```
crates/laminar-core/src/operator/
├── mod.rs              # Operator trait, Event, Output, Timer
└── window.rs           # TumblingWindowOperator, EmitStrategy, Aggregators

crates/laminar-sql/src/parser/
├── mod.rs              # Parser exports
├── statements.rs       # StreamingStatement, EmitClause, WindowFunction
└── parser_simple.rs    # StreamingParser with EMIT clause parsing
```

### Useful Commands
```bash
# Build and test laminar-core
cargo build -p laminar-core
cargo test -p laminar-core --lib

# Run clippy
cargo clippy -p laminar-core -- -D warnings

# Build all
cargo build --release
cargo test --all
```

### Recent Decisions
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-24 | 3 emit strategies | Trade-off between latency and efficiency |
| 2026-01-24 | High-bit timer key encoding | Distinguish periodic from final timers in 16 bytes |
| 2026-01-24 | OnUpdate emits in process() | Immediate feedback without waiting for timer |
| 2026-01-24 | 5 watermark strategies | Different sources need different strategies |
| 2026-01-24 | WatermarkTracker for multi-source | Joins need aligned watermarks |
| 2026-01-22 | Migrate bincode → rkyv | Zero-copy deserialization (~1.2ns access) |

---

## History

### Previous Sessions

<details>
<summary>Session - 2026-01-24 (Watermarks - F010)</summary>

**Accomplished**:
- ✅ Implemented F010 - Watermarks with 5 generation strategies
- ✅ Implemented WatermarkTracker for multi-source alignment
- ✅ Added idle source detection and MeteredGenerator wrapper
- ✅ All tests passing (122 in laminar-core)

**Notes**:
- Different sources need different watermark strategies
- Joins require aligned watermarks from multiple sources
- Idle sources can block watermark progress

</details>

<details>
<summary>Session - 2026-01-23 (Checkpointing and hot path fixes)</summary>

**Accomplished**:
- ✅ Fixed Ring 0 hot path violations (SmallVec, atomic counter, buffer swapping)
- ✅ Implemented reactor features (CPU affinity, sinks, graceful shutdown)
- ✅ Implemented F008 - Basic Checkpointing
- ✅ All tests passing

**Notes**:
- Checkpoints reduce recovery time vs full WAL replay
- Automatic checkpoint cleanup prevents disk space issues

</details>

<details>
<summary>Session - 2026-01-22 (bincode → rkyv migration)</summary>

**Accomplished**:
- ✅ Migrated serialization from bincode to rkyv
- ✅ Updated StateSnapshot, StateStoreExt, WindowId, and all accumulators
- ✅ All 89 tests passing

**Notes**:
- bincode was discontinued in December 2025
- rkyv provides zero-copy deserialization (~1.2ns access vs microseconds)

</details>

<details>
<summary>Session - 2026-01-22 (F005 Implementation)</summary>

**Accomplished**:
- ✅ Implemented F005 - DataFusion Integration
- ✅ StreamSource trait, StreamBridge, StreamingScanExec, StreamingTableProvider
- ✅ 35 tests passing

**Notes**:
- Push-to-pull bridge using tokio mpsc channels
- Aggregations on unbounded streams correctly rejected

</details>

<details>
<summary>Session - 2026-01-22 (F004 Implementation)</summary>

**Accomplished**:
- ✅ Implemented F004 - Tumbling Windows with full functionality
- ✅ Built-in aggregators: Count, Sum, Min, Max, Avg
- ✅ Performance targets met/exceeded

**Notes**:
- Window assignment: ~4.4ns (target < 10ns)
- Window emit: ~773ns (target < 1μs)

</details>
