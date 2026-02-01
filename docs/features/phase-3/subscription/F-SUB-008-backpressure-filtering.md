# F-SUB-008: Backpressure & Filtering

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-008 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SUB-004 (Dispatcher), F-SUB-005 (Push API) |
| **Created** | 2026-02-01 |

## Summary

Implements configurable backpressure strategies, subscription-level filtering with Ring 0 predicate push-down, and notification batching for throughput optimization. These are the optimization features that make the reactive subscription system production-ready under load.

**Backpressure**: Configurable strategies (DropOldest, DropNewest, Block, Sample) that determine behavior when a subscriber's channel buffer is full. Default is `DropOldest` for real-time/financial use cases where stale data is worse than missing data.

**Filtering**: Predicate expressions that filter events before they reach subscribers. Lightweight predicates (column equality, range checks) are pushed to Ring 0 to avoid unnecessary notifications. Complex predicates are evaluated in Ring 1.

**Batching**: Accumulates multiple notifications and delivers them as `ChangeEventBatch` to reduce per-event overhead for high-throughput scenarios.

**Research Reference**: [Reactive Subscriptions Research - Sections on Backpressure and Batching](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: Configurable `BackpressureStrategy` per subscription
- **FR-2**: `DropOldest`: when buffer full, oldest events are discarded
- **FR-3**: `DropNewest`: when buffer full, new events are discarded
- **FR-4**: `Block`: dispatcher blocks until buffer has space (Ring 1 only, never Ring 0)
- **FR-5**: `Sample(n)`: deliver every Nth event
- **FR-6**: Subscription filter predicate: `filter: "price > 100.0 AND symbol = 'AAPL'"`
- **FR-7**: Ring 0 predicate push-down for simple equality/range filters
- **FR-8**: Notification batching with configurable max_batch_size and max_batch_delay
- **FR-9**: Lagging subscriber detection with configurable threshold
- **FR-10**: Metrics: events filtered, events dropped, batch sizes, lag

### Non-Functional Requirements

- **NFR-1**: Ring 0 predicate evaluation < 50ns (simple column checks only)
- **NFR-2**: Batching reduces per-event dispatch overhead by 5-10x
- **NFR-3**: Backpressure never blocks Ring 0 (only Ring 1 Block strategy)
- **NFR-4**: Filter compilation at subscription creation time (not per-event)

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RING 0                                        │
│                                                                 │
│  [Operator Output] ──► Ring 0 Predicate Check ──► NotificationSlot
│                        (optional, simple only)                   │
│                        "price > 100.0" → skip if false           │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                    RING 1                                        │
│                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │ Notification │     │   Filter     │     │   Batcher    │    │
│  │   Drain      │────►│ (complex     │────►│ (accumulate  │    │
│  │              │     │  predicates) │     │  up to N or  │    │
│  │              │     │              │     │  timeout)    │    │
│  └──────────────┘     └──────────────┘     └──────┬───────┘    │
│                                                    │            │
│                    ┌───────────────────────────────┘            │
│                    ▼                                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │             Backpressure Controller                        │  │
│  │                                                           │  │
│  │  DropOldest: send (overwrites old if lagged)              │  │
│  │  DropNewest: skip send if full                            │  │
│  │  Block: await send (blocks dispatcher, not Ring 0)        │  │
│  │  Sample(n): counter % n == 0                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                    │                                            │
├────────────────────┼────────────────────────────────────────────┤
│                    │  RING 2                                    │
│                    ▼                                            │
│               Subscriber channels                               │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::time::{Duration, Instant};

/// Backpressure strategy for subscriptions.
///
/// Determines behavior when a subscriber's channel buffer is full.
///
/// # Recommendation
///
/// Use `DropOldest` for real-time/financial applications where
/// stale data is worse than missing data. Use `Block` for
/// exactly-once pipelines where completeness matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureStrategy {
    /// Drop oldest events when buffer full (real-time priority).
    ///
    /// Uses tokio broadcast's natural lagging behavior.
    /// Subscribers receive `Lagged(n)` error and skip ahead.
    DropOldest,

    /// Drop newest events when buffer full (completeness priority).
    ///
    /// New events are silently discarded until the subscriber catches up.
    DropNewest,

    /// Block the dispatcher until buffer has space.
    ///
    /// WARNING: This blocks the Ring 1 dispatcher, NOT Ring 0.
    /// Only use when a single slow subscriber is acceptable.
    Block,

    /// Sample: deliver every Nth event.
    ///
    /// Reduces load on slow subscribers at the cost of completeness.
    Sample(usize),
}

/// Compiled subscription filter.
///
/// Simple predicates are pushed to Ring 0 for zero-overhead filtering.
/// Complex predicates are evaluated in Ring 1.
#[derive(Debug, Clone)]
pub struct SubscriptionFilter {
    /// Original filter expression.
    pub expression: String,
    /// Ring 0 predicates (simple, zero-allocation checks).
    pub ring0_predicates: Vec<Ring0Predicate>,
    /// Ring 1 predicates (complex, may allocate).
    pub ring1_predicates: Vec<Ring1Predicate>,
    /// Interned string table for Ring 0 string comparisons.
    pub intern_table: StringInternTable,
}

/// Ring 0 predicate (must be zero-allocation, < 50ns).
///
/// Supports column-level checks that can be evaluated with no heap allocation.
/// Complex expressions (AND/OR, multi-column, UDFs) are deferred to Ring 1.
#[derive(Debug, Clone)]
pub enum Ring0Predicate {
    /// Column equals a constant value: `column = value`
    Eq {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column not equal: `column != value` / `column <> value`
    NotEq {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column greater than: `column > value`
    Gt {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column greater than or equal: `column >= value`
    GtEq {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column less than: `column < value`
    Lt {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column less than or equal: `column <= value`
    LtEq {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column is between two values (inclusive): `column BETWEEN low AND high`
    Between {
        column_index: usize,
        low: ScalarValue,
        high: ScalarValue,
    },
    /// Column is not null: `column IS NOT NULL`
    IsNotNull {
        column_index: usize,
    },
    /// Column is null: `column IS NULL`
    IsNull {
        column_index: usize,
    },
    /// Column value is in a set (small sets only, ≤8 values): `column IN (v1, v2, ...)`
    In {
        column_index: usize,
        /// Fixed-capacity inline set. Max 8 values to stay on stack.
        values: smallvec::SmallVec<[ScalarValue; 8]>,
    },
}

/// Scalar value for Ring 0 predicate comparison.
///
/// Fixed-size, stack-allocated, no heap. The `StringIndex` variant
/// uses a pre-interned index into the `StringInternTable`, avoiding
/// heap allocation during evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    /// Interned string index (not the string itself).
    ///
    /// At filter compile time, string literals are interned into a
    /// `StringInternTable`. At evaluation time, the column's string
    /// value is looked up in the same table. Comparison is O(1)
    /// integer equality on the intern index.
    StringIndex(u32),
}

/// Intern table for string values used in Ring 0 predicates.
///
/// Built at filter compilation time (Ring 2), immutable during evaluation.
/// Shared across all predicates for a subscription.
pub struct StringInternTable {
    /// String → index mapping (used at compile time).
    forward: HashMap<String, u32>,
    /// Index → string mapping (used for diagnostics).
    reverse: Vec<String>,
}

impl StringInternTable {
    pub fn new() -> Self {
        Self { forward: HashMap::new(), reverse: Vec::new() }
    }

    /// Interns a string, returning its index.
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&idx) = self.forward.get(s) {
            return idx;
        }
        let idx = self.reverse.len() as u32;
        self.forward.insert(s.to_string(), idx);
        self.reverse.push(s.to_string());
        idx
    }

    /// Looks up a string by index.
    pub fn resolve(&self, idx: u32) -> Option<&str> {
        self.reverse.get(idx as usize).map(|s| s.as_str())
    }
}

/// Ring 1 predicate (can allocate, evaluated in Ring 1 dispatcher).
#[derive(Debug, Clone)]
pub enum Ring1Predicate {
    /// DataFusion expression evaluated against RecordBatch.
    Expression(String),
    /// Pre-compiled filter function.
    Compiled(Arc<dyn Fn(&RecordBatch) -> Result<RecordBatch, ArrowError> + Send + Sync>),
}

/// Batching configuration for subscriptions.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum events per batch.
    pub max_batch_size: usize,
    /// Maximum time to wait for batch to fill.
    pub max_batch_delay: Duration,
    /// Whether batching is enabled.
    pub enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            max_batch_delay: Duration::from_micros(100),
            enabled: false,
        }
    }
}

/// Notification batcher that accumulates events before delivery.
pub struct NotificationBatcher {
    /// Buffer of accumulated events per source.
    buffers: HashMap<u32, Vec<ChangeEvent>>,
    /// Last flush time per source.
    last_flush: HashMap<u32, Instant>,
    /// Configuration.
    config: BatchConfig,
}

impl NotificationBatcher {
    /// Creates a new batcher.
    pub fn new(config: BatchConfig) -> Self {
        Self {
            buffers: HashMap::new(),
            last_flush: HashMap::new(),
            config,
        }
    }

    /// Adds an event to the batcher.
    ///
    /// Returns `Some(ChangeEventBatch)` if the batch is ready to deliver.
    pub fn add(
        &mut self,
        source_id: u32,
        source_name: &str,
        event: ChangeEvent,
    ) -> Option<ChangeEventBatch> {
        if !self.config.enabled {
            return Some(ChangeEventBatch::new(
                source_name.to_string(),
                vec![event],
            ));
        }

        let buffer = self.buffers.entry(source_id).or_default();
        buffer.push(event);

        let now = Instant::now();
        let last = self.last_flush.entry(source_id).or_insert(now);

        // Flush if batch is full or timeout expired
        if buffer.len() >= self.config.max_batch_size
            || now.duration_since(*last) >= self.config.max_batch_delay
        {
            *last = now;
            let events = std::mem::take(buffer);
            Some(ChangeEventBatch::new(source_name.to_string(), events))
        } else {
            None
        }
    }

    /// Forces flush of all pending batches.
    pub fn flush_all(&mut self) -> Vec<(u32, ChangeEventBatch)> {
        let mut results = Vec::new();

        for (source_id, buffer) in self.buffers.iter_mut() {
            if !buffer.is_empty() {
                let events = std::mem::take(buffer);
                results.push((
                    *source_id,
                    ChangeEventBatch::new(String::new(), events),
                ));
            }
        }

        results
    }

    /// Checks for timed-out batches and flushes them.
    pub fn flush_expired(&mut self) -> Vec<(u32, ChangeEventBatch)> {
        let now = Instant::now();
        let mut results = Vec::new();

        for (source_id, buffer) in self.buffers.iter_mut() {
            if buffer.is_empty() {
                continue;
            }

            let last = self.last_flush.get(source_id).copied().unwrap_or(now);
            if now.duration_since(last) >= self.config.max_batch_delay {
                let events = std::mem::take(buffer);
                self.last_flush.insert(*source_id, now);
                results.push((
                    *source_id,
                    ChangeEventBatch::new(String::new(), events),
                ));
            }
        }

        results
    }
}

/// Backpressure controller per subscription.
pub struct BackpressureController {
    /// Strategy.
    strategy: BackpressureStrategy,
    /// Events dropped count.
    dropped: u64,
    /// Sample counter (for Sample strategy).
    sample_counter: u64,
    /// Lag threshold for warning.
    lag_warning_threshold: u64,
}

impl BackpressureController {
    /// Creates a new controller.
    pub fn new(strategy: BackpressureStrategy) -> Self {
        Self {
            strategy,
            dropped: 0,
            sample_counter: 0,
            lag_warning_threshold: 1000,
        }
    }

    /// Determines whether to deliver an event to this subscriber.
    ///
    /// Returns `true` if the event should be sent, `false` if dropped.
    pub fn should_deliver(&mut self) -> bool {
        match self.strategy {
            BackpressureStrategy::DropOldest => true, // Handled by broadcast lagging
            BackpressureStrategy::DropNewest => true, // Checked after send attempt
            BackpressureStrategy::Block => true,
            BackpressureStrategy::Sample(n) => {
                self.sample_counter += 1;
                if self.sample_counter % n as u64 == 0 {
                    true
                } else {
                    self.dropped += 1;
                    false
                }
            }
        }
    }

    /// Records a dropped event.
    pub fn record_drop(&mut self) {
        self.dropped += 1;
    }

    /// Returns the number of events dropped.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

/// Compiles a filter expression into Ring 0 and Ring 1 predicates.
///
/// Uses the F006B SQL parser to parse the expression, then classifies
/// each conjunct (AND-separated term) as Ring 0 or Ring 1:
///
/// **Ring 0 eligible** (zero-alloc, < 50ns):
/// - Single column comparison: `price > 100.0`, `symbol = 'AAPL'`
/// - IS NULL / IS NOT NULL: `name IS NOT NULL`
/// - IN with ≤8 literal values: `status IN ('A', 'B', 'C')`
/// - BETWEEN with literal bounds: `price BETWEEN 10.0 AND 20.0`
///
/// **Ring 1 fallback** (may allocate):
/// - Multi-column expressions: `price > min_price`
/// - Function calls: `UPPER(name) = 'FOO'`
/// - OR/NOT expressions: `price > 100 OR qty > 50`
/// - LIKE/regex patterns: `name LIKE '%foo%'`
/// - Subqueries
///
/// # Implementation Strategy
///
/// 1. Parse `expression` using `sqlparser::Parser::parse_expr()`
/// 2. Flatten top-level AND into conjuncts
/// 3. For each conjunct, attempt Ring 0 classification:
///    - Check if it's a `BinaryOp { left: Column, op, right: Literal }`
///    - Check if it's `IsNull`/`IsNotNull` on a single column
///    - Check if it's `InList` with all literal values and count ≤ 8
///    - Check if it's `Between { low: Literal, high: Literal }`
/// 4. Resolve column names → column indices via schema
/// 5. Intern string literals into `StringInternTable`
/// 6. Anything that doesn't match Ring 0 patterns → Ring 1 (DataFusion expr)
///
/// # Example
///
/// Input: `"price > 100.0 AND symbol = 'AAPL' AND UPPER(name) LIKE '%FOO%'"`
///
/// Result:
/// - Ring 0: `[Gt { col: 1, value: Float64(100.0) }, Eq { col: 0, value: StringIndex(0) }]`
/// - Ring 1: `[Expression("UPPER(name) LIKE '%FOO%'")]`
/// - String table: `{ "AAPL" → 0 }`
pub fn compile_filter(
    expression: &str,
    schema: &SchemaRef,
) -> Result<SubscriptionFilter, FilterCompileError> {
    use sqlparser::parser::Parser;
    use sqlparser::dialect::GenericDialect;

    let dialect = GenericDialect {};
    let expr = Parser::new(&dialect)
        .try_with_sql(expression)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| FilterCompileError::InvalidExpression(e.to_string()))?;

    let mut ring0 = Vec::new();
    let mut ring1 = Vec::new();
    let mut intern_table = StringInternTable::new();

    // Flatten AND conjuncts
    let conjuncts = flatten_and(expr);

    for conjunct in conjuncts {
        match classify_predicate(&conjunct, schema, &mut intern_table) {
            Ok(PredicateClass::Ring0(pred)) => ring0.push(pred),
            Ok(PredicateClass::Ring1(pred)) => ring1.push(pred),
            Err(e) => return Err(e),
        }
    }

    Ok(SubscriptionFilter {
        expression: expression.to_string(),
        ring0_predicates: ring0,
        ring1_predicates: ring1,
        intern_table,
    })
}

/// Filter compilation errors.
#[derive(Debug, thiserror::Error)]
pub enum FilterCompileError {
    #[error("invalid filter expression: {0}")]
    InvalidExpression(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("type mismatch: column {column} is {actual}, expected {expected}")]
    TypeMismatch {
        column: String,
        actual: String,
        expected: String,
    },
}
```

### Ring 0 Predicate Evaluation

```rust
impl Ring0Predicate {
    /// Evaluates the predicate against a RecordBatch row.
    ///
    /// Returns true if the row matches the predicate.
    /// This runs on the Ring 0 hot path - must be < 50ns.
    #[inline(always)]
    pub fn evaluate(&self, batch: &RecordBatch, row: usize) -> bool {
        match self {
            Ring0Predicate::Eq { column_index, value } => {
                Self::compare_eq(batch, *column_index, row, value)
            }
            Ring0Predicate::Gt { column_index, value } => {
                Self::compare_gt(batch, *column_index, row, value)
            }
            Ring0Predicate::Lt { column_index, value } => {
                Self::compare_lt(batch, *column_index, row, value)
            }
            Ring0Predicate::Between { column_index, low, high } => {
                Self::compare_gt(batch, *column_index, row, low)
                    && Self::compare_lt(batch, *column_index, row, high)
            }
        }
    }

    #[inline(always)]
    fn compare_eq(batch: &RecordBatch, col: usize, row: usize, value: &ScalarValue) -> bool {
        match value {
            ScalarValue::Int64(v) => {
                batch.column(col)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.value(row) == *v)
                    .unwrap_or(false)
            }
            ScalarValue::Float64(v) => {
                batch.column(col)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|a| (a.value(row) - *v).abs() < f64::EPSILON)
                    .unwrap_or(false)
            }
            ScalarValue::Bool(v) => {
                batch.column(col)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(|a| a.value(row) == *v)
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    // compare_gt and compare_lt follow the same pattern...
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| SubscriptionConfig | `subscription/registry.rs` | Add backpressure and filter fields |
| SubscriptionDispatcher | `subscription/dispatcher.rs` | Integrate batcher and filter |
| NotificationHub | `subscription/notification.rs` | Ring 0 predicate check before notify |

### New Files

- `crates/laminar-core/src/subscription/backpressure.rs` - BackpressureController, BackpressureStrategy
- `crates/laminar-core/src/subscription/filter.rs` - SubscriptionFilter, Ring0Predicate, compile_filter
- `crates/laminar-core/src/subscription/batcher.rs` - NotificationBatcher, BatchConfig

## Test Plan

### Unit Tests

**Backpressure:**
- [ ] `test_backpressure_drop_oldest` - DropOldest behavior
- [ ] `test_backpressure_drop_newest` - DropNewest skips on full
- [ ] `test_backpressure_sample` - Sample(n) delivers every Nth
- [ ] `test_backpressure_controller_dropped_count` - Counter accuracy
- [ ] `test_backpressure_demand_request` - Demand request(n) delivers exactly n
- [ ] `test_backpressure_demand_zero` - No events when demand is 0
- [ ] `test_backpressure_demand_concurrent` - CAS loop under contention
- [ ] `test_lagging_subscriber_detection` - Lag threshold warning

**Batching:**
- [ ] `test_batcher_immediate_when_disabled` - No batching when disabled
- [ ] `test_batcher_size_trigger` - Flush on max_batch_size
- [ ] `test_batcher_timeout_trigger` - Flush on max_batch_delay
- [ ] `test_batcher_flush_all` - Force flush all sources
- [ ] `test_batcher_flush_expired` - Timed flush

**Ring 0 Predicates:**
- [ ] `test_ring0_predicate_eq_int64` - Integer equality
- [ ] `test_ring0_predicate_eq_float64` - Float equality
- [ ] `test_ring0_predicate_not_eq` - Not-equal comparison
- [ ] `test_ring0_predicate_gt` - Greater than
- [ ] `test_ring0_predicate_gt_eq` - Greater than or equal
- [ ] `test_ring0_predicate_lt` - Less than
- [ ] `test_ring0_predicate_lt_eq` - Less than or equal
- [ ] `test_ring0_predicate_between` - Range check (inclusive)
- [ ] `test_ring0_predicate_is_null` - IS NULL
- [ ] `test_ring0_predicate_is_not_null` - IS NOT NULL
- [ ] `test_ring0_predicate_in_set` - IN with small value set
- [ ] `test_ring0_predicate_in_set_max_8` - IN rejects >8 values to Ring 1

**String Interning:**
- [ ] `test_string_intern_basic` - Intern and resolve
- [ ] `test_string_intern_dedup` - Same string returns same index
- [ ] `test_string_intern_multiple` - Multiple strings get unique indices
- [ ] `test_ring0_predicate_eq_string` - String equality via intern index

**Filter Compilation:**
- [ ] `test_filter_compile_simple_eq` - `"price = 100"` → Ring 0
- [ ] `test_filter_compile_and_split` - `"price > 100 AND UPPER(name) = 'X'"` → Ring 0 + Ring 1
- [ ] `test_filter_compile_column_not_found` - Error on bad column
- [ ] `test_filter_compile_type_mismatch` - Error on wrong type
- [ ] `test_filter_compile_complex_to_ring1` - OR/NOT → Ring 1 fallback
- [ ] `test_ring1_predicate_expression` - DataFusion expr evaluation

**Subscription Checkpointing:**
- [ ] `test_subscription_checkpoint_snapshot` - Checkpoint captures all active subs
- [ ] `test_subscription_restore` - Restore re-creates entries
- [ ] `test_subscription_checkpoint_roundtrip` - Checkpoint → serialize → deserialize → restore

### Integration Tests

- [ ] `test_backpressure_with_slow_subscriber` - End-to-end slow consumer
- [ ] `test_filter_push_down` - Ring 0 filter reduces notifications
- [ ] `test_batched_delivery` - Batched events received correctly

### Benchmarks

- [ ] `bench_ring0_predicate_eval` - Target: < 50ns
- [ ] `bench_batcher_add_event` - Target: < 20ns
- [ ] `bench_batcher_flush_64` - Target: < 200ns for 64 events
- [ ] `bench_backpressure_sample_overhead` - Target: < 5ns
- [ ] `bench_throughput_with_batching` - Target: > 10M events/sec

## Completion Checklist

- [ ] BackpressureStrategy enum with 5 strategies (+ Demand)
- [ ] BackpressureController per-subscription
- [ ] DemandBackpressure with atomic request(n) / try_consume()
- [ ] NotificationBatcher with size/time triggers
- [ ] Ring0Predicate with 10 variants (Eq, NotEq, Gt, GtEq, Lt, LtEq, Between, IsNull, IsNotNull, In)
- [ ] ScalarValue with 4 types (Int64, Float64, Bool, StringIndex)
- [ ] StringInternTable for Ring 0 string comparisons
- [ ] Ring1Predicate with DataFusion expression support
- [ ] compile_filter() using F006B SQL parser with Ring 0/Ring 1 classification
- [ ] Integration with dispatcher pipeline
- [ ] Lagging subscriber detection and metrics
- [ ] NUMA affinity in SubscriptionConfig
- [ ] Eventfd notifier (Linux only, fallback on other platforms)
- [ ] SubscriptionCheckpoint / restore for recovery
- [ ] Unit tests passing (35+ tests)
- [ ] Benchmarks meeting targets
- [ ] Memory overhead documented
- [ ] Code reviewed

## Advanced Features (from Research)

### Demand-Based Backpressure (Reactive Streams)

The Reactive Streams specification (research Section 2 + 7) defines demand-based
flow control where consumers explicitly request events via `request(n)`. This is
a fifth backpressure strategy beyond the four buffer-based strategies above.

```rust
/// Demand-based backpressure (Reactive Streams `request(n)` model).
///
/// The subscriber must call `request(n)` to receive the next N events.
/// The dispatcher tracks pending demand per subscription and only sends
/// events when `pending > 0`. This provides precise flow control at the
/// cost of additional atomic operations per event.
///
/// # When to use
///
/// - Downstream sink has strict rate limits (e.g., external API)
/// - Consumer wants to process exactly N events then stop
/// - Integration with Reactive Streams-compatible frameworks
///
/// # When NOT to use
///
/// - Real-time/financial (use DropOldest instead — demand adds latency)
/// - High-throughput scenarios (atomic ops on demand counter add overhead)
pub struct DemandBackpressure {
    /// Pending demand: subscriber has requested this many more events.
    pending: Arc<AtomicU64>,
}

impl DemandBackpressure {
    /// Creates demand-based backpressure with initial demand of 0.
    pub fn new() -> (Self, DemandHandle) {
        let pending = Arc::new(AtomicU64::new(0));
        let handle = DemandHandle { pending: Arc::clone(&pending) };
        (Self { pending }, handle)
    }

    /// Checks if there's demand, and decrements if so.
    #[inline(always)]
    pub fn try_consume(&self) -> bool {
        // CAS loop: decrement if > 0
        loop {
            let current = self.pending.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            if self.pending.compare_exchange_weak(
                current, current - 1,
                Ordering::AcqRel, Ordering::Relaxed
            ).is_ok() {
                return true;
            }
        }
    }
}

/// Extend BackpressureStrategy
pub enum BackpressureStrategy {
    DropOldest,
    DropNewest,
    Block,
    Sample(usize),
    /// Demand-based: subscriber must call request(n).
    Demand,
}
```

### NUMA-Aware Subscription Routing

The research (Phase 4) identifies NUMA-aware routing as an optimization for
thread-per-core architectures. Subscribers should be dispatched on the same
NUMA node as their source to minimize cross-socket memory traffic.

```rust
/// NUMA affinity for subscription dispatch.
///
/// When the pipeline runs in thread-per-core mode (F013), each source
/// operator is pinned to a specific core/NUMA node. The dispatcher
/// should route notifications to subscribers on the same NUMA node
/// to avoid cross-socket traffic (2-3x latency penalty).
#[derive(Debug, Clone, Copy)]
pub struct SubscriptionAffinity {
    /// Preferred NUMA node for this subscription.
    pub numa_node: Option<u32>,
    /// Preferred core for this subscription's dispatch task.
    pub preferred_core: Option<u32>,
}

impl SubscriptionConfig {
    /// Sets NUMA affinity for this subscription.
    ///
    /// The dispatcher will attempt to process this subscription's
    /// notifications on the specified NUMA node. If the preferred
    /// node is busy, falls back to any available core.
    pub fn with_numa_affinity(mut self, node: u32) -> Self {
        self.affinity = Some(SubscriptionAffinity {
            numa_node: Some(node),
            preferred_core: None,
        });
        self
    }
}
```

Integration with F068 (NUMA-Aware Memory):
- Subscription channel buffers are allocated NUMA-local
- NotificationRing per-core instances stay on their NUMA node
- Dispatch task pinned to source's NUMA node when possible

### io_uring eventfd Integration

The research (Section 5) identifies io_uring eventfd as a kernel-level push
notification mechanism for Linux deployments. Instead of polling or spinning
for notifications, the dispatcher can wait on an eventfd that is signaled
by io_uring completions.

```rust
/// Eventfd-based wakeup for the subscription dispatcher.
///
/// When Ring 0 writes a notification to the NotificationRing, it also
/// signals an eventfd. The Ring 1 dispatcher can wait on this eventfd
/// instead of spin-polling, reducing CPU usage during idle periods.
///
/// This integrates with LaminarDB's io_uring architecture (F067/F069):
/// the eventfd is registered with the io_uring instance, so signaling
/// is zero-syscall from Ring 0's perspective.
pub struct EventfdNotifier {
    /// eventfd file descriptor.
    fd: RawFd,
}

impl EventfdNotifier {
    /// Signals the eventfd (Ring 0 — single write, no syscall via io_uring).
    #[inline(always)]
    pub fn notify(&self) {
        // When registered with io_uring, this is a single SQE submission
        // that completes asynchronously. Cost: ~20ns.
    }

    /// Waits for notification (Ring 1 — async await on eventfd readability).
    pub async fn wait(&self) {
        // tokio::io::AsyncFd or io_uring CQE wait
    }
}
```

**Platform note**: eventfd is Linux-only. On other platforms, the dispatcher
falls back to the spin → yield → sleep adaptive wait strategy from F-SUB-004.

### Subscription Checkpointing for Recovery

The research (Phase 3) identifies subscription checkpointing as necessary for
durability. If the pipeline restarts, subscriptions should be recoverable.

```rust
/// Checkpoint state for a subscription.
///
/// Persisted as part of the pipeline's checkpoint (F022). On recovery,
/// subscriptions are re-registered with the last known position.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionCheckpoint {
    /// Subscription ID.
    pub id: SubscriptionId,
    /// Query that created this subscription.
    pub query: String,
    /// Configuration at checkpoint time.
    pub config: SubscriptionConfig,
    /// Last delivered sequence number.
    pub last_sequence: u64,
    /// Last delivered epoch.
    pub last_epoch: u64,
}

impl SubscriptionRegistry {
    /// Creates a checkpoint of all active subscriptions.
    pub fn checkpoint(&self) -> Vec<SubscriptionCheckpoint> {
        // Read-lock the registry and snapshot all Active entries
        // ...
    }

    /// Restores subscriptions from a checkpoint.
    ///
    /// Re-creates broadcast channels and registry entries. Subscribers
    /// must re-connect via `subscribe_push()` — the registry recognizes
    /// the query and resumes from the checkpointed position.
    pub fn restore(&self, checkpoints: &[SubscriptionCheckpoint]) {
        // ...
    }
}
```

**Recovery flow**:
1. Pipeline restarts from checkpoint (F022)
2. `SubscriptionRegistry::restore()` re-creates entries with checkpointed positions
3. WAL replay delivers events from `last_epoch` to current
4. Subscribers re-connect via `subscribe_push()` and receive from the replay point
5. Once replay completes, live events flow normally

**Limitation**: This provides at-least-once delivery during recovery. Events
between the last checkpoint and the crash may be re-delivered. For exactly-once,
see the `ExactlyOncePushSubscription` in F-SUB-005.

### Memory Overhead Estimates

Per-subscription memory overhead:

| Component | Size | Notes |
|-----------|------|-------|
| `SubscriptionEntry` in registry | ~256 bytes | ID, config, state, metrics, sender |
| `broadcast::Sender` | ~64 bytes | Channel metadata |
| `broadcast` buffer | `buffer_size * sizeof(ChangeEvent)` | Default 1024 * ~96 = ~96 KB |
| Ring 0 predicates (if filtered) | ~64-256 bytes | Depends on predicate count |
| String intern table (if filtered) | Variable | ~32 bytes per interned string |
| **Total per subscription** | **~97 KB** | Dominated by channel buffer |
| **Total for 10K subscriptions** | **~970 MB** | Reduce buffer_size for high sub counts |

Recommendations:
- Default `buffer_size`: 1024 for general use
- High subscriber counts (>1000): Reduce to 64-128
- Watch subscriptions (F-SUB-005): Use `watch` channel (only stores latest, ~96 bytes)

## References

- [F-SUB-004: Subscription Dispatcher](F-SUB-004-subscription-dispatcher.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [Reactive Streams Specification - Backpressure](https://www.reactive-streams.org/)
- [tokio broadcast lagging behavior](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)
- [F067: io_uring Advanced Optimization](../../phase-2/F067-io-uring-optimization.md)
- [F068: NUMA-Aware Memory Allocation](../../phase-2/F068-numa-aware-memory.md)
- [F022: Incremental Checkpointing](../../phase-2/F022-incremental-checkpointing.md)
