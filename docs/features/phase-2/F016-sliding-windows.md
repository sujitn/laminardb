# F016: Sliding Windows

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F016 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F004 |
| **Owner** | TBD |

## Summary

Implement sliding (hopping) windows that overlap. Each event can belong to multiple windows, enabling moving averages and other overlapping aggregations.

## Goals

- Configurable size and slide
- Efficient multi-window assignment
- Shared state across overlapping windows
- Memory-efficient implementation

## Technical Design

```rust
pub struct SlidingWindow {
    size: Duration,
    slide: Duration,
}

impl SlidingWindow {
    pub fn assign(&self, timestamp: u64) -> Vec<WindowId> {
        // Event belongs to multiple windows
        let mut windows = Vec::new();
        let first_window_start = timestamp - (timestamp % self.slide.as_millis() as u64);
        let mut window_start = first_window_start;
        
        while window_start + self.size.as_millis() as u64 > timestamp {
            windows.push(WindowId { 
                start: window_start, 
                end: window_start + self.size.as_millis() as u64 
            });
            window_start -= self.slide.as_millis() as u64;
        }
        windows
    }
}
```

## SQL Syntax

```sql
SELECT 
    HOP_START(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as window_start,
    AVG(value) as avg_value
FROM stream
GROUP BY HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR);
```

## Completion Checklist

- [ ] Multi-window assignment correct
- [ ] Memory-efficient state
- [ ] SQL syntax working
- [ ] Benchmarks passing
