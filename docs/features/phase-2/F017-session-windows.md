# F017: Session Windows

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F017 |
| **Status** | 🔧 Hardening |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F016 |
| **Owner** | TBD |

## Summary

Implement session windows that group events by activity periods separated by gaps. Sessions are dynamic windows that grow with activity and close after inactivity.

**Status Update (2026-02-09)**: Initial implementation complete but has critical issues with micro-batch boundaries (Issue #55). See F017B-F017E for hardening work.

## Goals

- Gap-based session detection
- Per-key session tracking
- Session merging on late data
- Configurable gap timeout

## Known Issues

**Issue #55: Partial Results Across Micro-Batches**

The current implementation emits partial/incorrect results when events for the same session span multiple micro-batches. Root cause: architectural mismatch between continuous streaming assumptions and DataFusion micro-batch execution.

**Specific problems**:
1. In-memory `active_sessions` HashMap lost between batches
2. Only supports single session per key (cannot handle concurrent sessions)
3. Session merging prepared but not implemented
4. Premature emission at micro-batch boundaries (lines 586-593)
5. Timer persistence across batches uncertain

**Hardening features**: F017B (state refactoring), F017C (merging logic), F017D (emit strategies), F017E (watermark closure)

## Implementation

**Current Module**: `crates/laminar-core/src/operator/session_window.rs`

### Key Components

#### `SessionState`
```rust
pub struct SessionState {
    /// Session start timestamp (inclusive)
    pub start: i64,
    /// Session end timestamp (exclusive, = last event time + gap)
    pub end: i64,
    /// Key bytes for this session
    pub key: Vec<u8>,
}
```

**Limitation**: Only one session per key stored in state (`ses:<key_hash>`). Needs refactoring to session index.

#### `SessionWindowOperator<A: Aggregator>`
```rust
pub struct SessionWindowOperator<A: Aggregator> {
    gap_ms: i64,                              // Gap timeout in milliseconds
    aggregator: A,                            // Aggregation function
    allowed_lateness_ms: i64,                 // Grace period for late data
    active_sessions: FxHashMap<u64, SessionState>,  // ⚠️ IN-MEMORY CACHE (volatile)
    pending_timers: FxHashMap<u64, i64>,      // ⚠️ IN-MEMORY CACHE (volatile)
    emit_strategy: EmitStrategy,              // Emission strategy
    late_data_config: LateDataConfig,         // Late data handling
    key_column: Option<usize>,                // Key column for partitioning
    // ...
}
```

### Features

- **Gap-based detection**: Sessions close when no events arrive within the gap period
- **Per-key tracking**: Each unique key maintains independent session state (single session only)
- **Timer-based closure**: Timers fire to close sessions after gap + allowed lateness
- **Emit strategies**: All F011B strategies supported (OnWatermark, OnUpdate, Changelog, OnWindowClose, Final)
- **Late data handling**: Configurable drop, side output, or silent drop (Final)
- **Checkpoint/restore**: Full state serialization via rkyv

### Usage

```rust
use laminar_core::operator::session_window::SessionWindowOperator;
use laminar_core::operator::window::CountAggregator;
use std::time::Duration;

// Create a session window with 30-second gap
let mut operator = SessionWindowOperator::new(
    Duration::from_secs(30),    // gap timeout
    CountAggregator::new(),
    Duration::from_secs(60),    // allowed lateness
);

// Optional: key by a specific column for per-key sessions
operator.set_key_column(0);

// Optional: set emit strategy
operator.set_emit_strategy(EmitStrategy::OnUpdate);
```

### Session Lifecycle (Current - Simplified)

1. **Start**: First event for a key creates a new session
2. **Extend**: Events within gap period extend the session end time
3. **Close**: Timer fires when gap expires, emitting aggregation results
4. **Cleanup**: Session state is deleted after emission

**Missing**: Session merging when late data bridges gaps

### State Keys

- `ses:<key_hash>` - Session metadata (start, end, key) - **⚠️ Only one session per key**
- `sac:<key_hash>` - Accumulator state - **⚠️ Only one accumulator per key**

**Needs**: Session index structure to support multiple sessions per key

## SQL Syntax

```sql
SELECT
    SESSION_START(ts, INTERVAL '30' MINUTE) as session_start,
    SESSION_END(ts, INTERVAL '30' MINUTE) as session_end,
    user_id,
    COUNT(*) as events
FROM clickstream
GROUP BY SESSION(ts, INTERVAL '30' MINUTE), user_id;
```

> **Note**: SQL syntax parsing not yet implemented (requires F006B). The operator can be used programmatically.

## Tests

23 unit tests covering:
- Session state creation, contains, extend, merge (stub)
- Operator creation and configuration
- Single event processing
- Multiple events in same session
- Gap-based new session creation
- Timer-triggered emission
- Per-key tracking
- Late event handling (drop, side output, Final)
- Emit strategies (OnUpdate, Changelog)
- Checkpoint/restore
- Stale timer handling
- Sum aggregation

**Missing tests**: Multi-session per key, session merging, micro-batch boundary scenarios

## Completion Checklist

- [x] Gap detection working
- [x] Per-key tracking correct (single session only)
- [x] Timer-based closure
- [x] All emit strategies supported
- [x] Late data handling
- [x] Checkpoint/restore (pending_timers only)
- [x] 23 unit tests passing
- [ ] **Multi-session per key support** → F017B
- [ ] **Session merging on late data** → F017C
- [ ] **Correct emit across micro-batch boundaries** → F017D
- [ ] **Watermark-driven closure for OnWindowClose** → F017E
- [ ] SQL syntax working (requires F006B)

## References

- Issue #55: SESSION Window Emits Partial Results Across Micro-Batches
- Research: `docs/research/session-window-fix-2026.md` (comprehensive analysis)
- Hardening features: F017B, F017C, F017D, F017E
