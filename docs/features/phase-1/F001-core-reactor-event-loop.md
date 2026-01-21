# F001: Core Reactor Event Loop

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F001 |
| **Status** | 📝 Draft |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | None |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-20 |

## Summary

The Core Reactor is the heart of LaminarDB - a single-threaded event loop that processes streaming events with minimal latency. It implements the fundamental scheduling and dispatch mechanism that all operators build upon. This is the foundation for the thread-per-core architecture in Phase 2.

## Goals

- Process events with < 1μs overhead per event
- Support 500K+ events/sec on a single core
- Provide clean abstractions for operator registration
- Enable timer-based triggering for windows

## Non-Goals

- Multi-threading (Phase 2: F013)
- State persistence (F007, F008)
- Network I/O (handled by connectors)

## Technical Design

### Architecture

The Reactor lives in Ring 0 (hot path) within `crates/laminar-core/src/reactor/`.

```
┌─────────────────────────────────────────────────────┐
│                    Reactor                          │
├─────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │  Event   │  │ Operator │  │  Timer   │         │
│  │  Queue   │─▶│  Chain   │─▶│  Wheel   │         │
│  └──────────┘  └──────────┘  └──────────┘         │
│       │              │              │              │
│       │              ▼              │              │
│       │        ┌──────────┐        │              │
│       │        │  Output  │        │              │
│       │        │  Buffer  │        │              │
│       │        └──────────┘        │              │
│       │              │              │              │
│       └──────────────┴──────────────┘              │
└─────────────────────────────────────────────────────┘
```

### API/Interface

```rust
/// Core event loop for stream processing
pub struct Reactor {
    operators: Vec<Box<dyn Operator>>,
    timer_wheel: TimerWheel,
    event_queue: VecDeque<Event>,
    output_buffer: Vec<Output>,
}

impl Reactor {
    /// Create a new reactor
    pub fn new() -> Self;
    
    /// Register an operator in the processing chain
    pub fn add_operator(&mut self, operator: Box<dyn Operator>);
    
    /// Submit an event for processing
    pub fn submit(&mut self, event: Event);
    
    /// Run one iteration of the event loop
    /// Returns outputs ready for downstream
    pub fn poll(&mut self) -> Vec<Output>;
    
    /// Register a timer to fire at the given timestamp
    pub fn register_timer(&mut self, timestamp: u64, callback: TimerCallback);
}

/// Trait for streaming operators
pub trait Operator: Send {
    /// Process a single event
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> Vec<Output>;
    
    /// Handle a timer firing
    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> Vec<Output>;
    
    /// Create a checkpoint of operator state
    fn checkpoint(&self) -> OperatorState;
    
    /// Restore from a checkpoint
    fn restore(&mut self, state: OperatorState);
}

/// Context provided to operators during processing
pub struct OperatorContext<'a> {
    /// Current event time
    pub event_time: u64,
    /// Current processing time
    pub processing_time: u64,
    /// Timer registration
    pub timers: &'a mut TimerWheel,
    /// State store access
    pub state: &'a mut dyn StateStore,
}
```

### Data Structures

```rust
/// An event to be processed
#[derive(Debug, Clone)]
pub struct Event {
    /// Event timestamp (event time)
    pub timestamp: u64,
    /// Partition key for routing
    pub key: Vec<u8>,
    /// Event payload
    pub payload: Vec<u8>,
}

/// Output from operator processing
#[derive(Debug)]
pub struct Output {
    /// Timestamp of the output
    pub timestamp: u64,
    /// Output data
    pub data: RecordBatch,
}

/// Timer callback identifier
pub struct TimerCallback {
    pub operator_id: usize,
    pub timer_id: u64,
}
```

### Algorithm/Flow

1. **Event Submission**: Events enter via `submit()` into the event queue
2. **Poll Loop**: `poll()` processes events in order:
   - Check timer wheel for expired timers
   - Fire timer callbacks to operators
   - Dequeue next event
   - Pass through operator chain
   - Collect outputs
3. **Output Collection**: Outputs accumulated in buffer, returned to caller

```rust
pub fn poll(&mut self) -> Vec<Output> {
    // 1. Fire expired timers
    let now = self.current_time();
    while let Some(timer) = self.timer_wheel.pop_expired(now) {
        let outputs = self.operators[timer.operator_id]
            .on_timer(timer, &mut self.context);
        self.output_buffer.extend(outputs);
    }
    
    // 2. Process events
    while let Some(event) = self.event_queue.pop_front() {
        for operator in &mut self.operators {
            let outputs = operator.process(&event, &mut self.context);
            self.output_buffer.extend(outputs);
        }
    }
    
    // 3. Return outputs
    std::mem::take(&mut self.output_buffer)
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `OperatorPanic` | Operator panicked during processing | Log, skip event, continue |
| `QueueFull` | Event queue at capacity | Apply backpressure to source |
| `TimerOverflow` | Too many timers registered | Compact timer wheel |

## Test Plan

### Unit Tests

- [ ] `test_reactor_new_creates_empty_reactor`
- [ ] `test_reactor_add_operator_registers_operator`
- [ ] `test_reactor_submit_queues_event`
- [ ] `test_reactor_poll_processes_events_in_order`
- [ ] `test_reactor_poll_fires_expired_timers`
- [ ] `test_reactor_poll_returns_outputs`
- [ ] `test_operator_receives_correct_context`

### Integration Tests

- [ ] End-to-end: Submit 1000 events, verify all processed
- [ ] Timer accuracy: Register timer, verify fires within 1ms
- [ ] Operator chain: Multiple operators process in sequence

### Property Tests

- [ ] Events processed in timestamp order
- [ ] No events lost under normal operation
- [ ] Timer callbacks fire in order

### Benchmarks

- [ ] `bench_reactor_submit` - Target: < 100ns
- [ ] `bench_reactor_poll_single_event` - Target: < 500ns
- [ ] `bench_reactor_throughput` - Target: 500K events/sec

## Rollout Plan

1. **Phase 1**: Core reactor struct and event queue (2 days)
2. **Phase 2**: Operator trait and chain processing (2 days)
3. **Phase 3**: Timer wheel integration (2 days)
4. **Phase 4**: Benchmarks and optimization (2 days)
5. **Phase 5**: Documentation and review (1 day)

## Open Questions

- [ ] Should we use a priority queue for events or process in arrival order?
- [ ] What's the optimal timer wheel granularity?
- [ ] How to handle operator state during checkpointing?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets (500K events/sec)
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

- Consider using `bumpalo` arena allocator for event processing
- Timer wheel implementation can be based on Kafka's hierarchical design
- Profile with `perf` to identify hot spots

## References

- [Kafka Timer Wheel](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/util/timer/TimingWheel.java)
- [Tokio's timer implementation](https://github.com/tokio-rs/tokio/tree/master/tokio/src/time)
- [Thread-per-core architecture](https://www.datadoghq.com/blog/engineering/introducing-glommio/)
