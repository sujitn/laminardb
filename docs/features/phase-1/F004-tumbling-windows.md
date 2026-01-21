# F004: Tumbling Windows

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F004 |
| **Status** | 📝 Draft |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F001, F003 |
| **Owner** | TBD |

## Summary

Implement tumbling (non-overlapping, fixed-size) window aggregations. Tumbling windows are the simplest window type and serve as the foundation for more complex windowing strategies.

## Goals

- Support configurable window sizes (1s, 1m, 1h, etc.)
- Event-time based windowing with watermarks
- Efficient state management per window
- Clean window cleanup after emission

## Technical Design

### Window Assignment

```rust
pub struct TumblingWindow {
    size_ms: u64,
}

impl TumblingWindow {
    pub fn assign(&self, timestamp: u64) -> WindowId {
        let window_start = (timestamp / self.size_ms) * self.size_ms;
        let window_end = window_start + self.size_ms;
        WindowId { start: window_start, end: window_end }
    }
}
```

### Operator Implementation

```rust
pub struct TumblingWindowOperator<A: Aggregator> {
    window: TumblingWindow,
    aggregator: A,
    state: Box<dyn StateStore>,
}

impl<A: Aggregator> Operator for TumblingWindowOperator<A> {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> Vec<o> {
        let window_id = self.window.assign(event.timestamp);
        let mut state = self.get_window_state(&window_id);
        self.aggregator.add(&mut state, &event.payload);
        self.put_window_state(&window_id, state);
        vec![]  // Output on watermark trigger
    }
    
    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> Vec<o> {
        let window_id = timer.window_id;
        let state = self.get_window_state(&window_id);
        let result = self.aggregator.result(&state);
        self.delete_window_state(&window_id);
        vec![Output { timestamp: window_id.end, data: result }]
    }
}
```

## SQL Syntax

```sql
SELECT
    TUMBLE_START(ts, INTERVAL '1' HOUR) as window_start,
    TUMBLE_END(ts, INTERVAL '1' HOUR) as window_end,
    COUNT(*) as event_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
```

## Benchmarks

- [ ] `bench_window_assign` - Target: < 10ns
- [ ] `bench_window_aggregate` - Target: < 100ns per event
- [ ] `bench_window_emit` - Target: < 1μs

## Completion Checklist

- [ ] Window assignment correct
- [ ] State management working
- [ ] Watermark triggering
- [ ] SQL syntax supported
- [ ] Benchmarks passing
