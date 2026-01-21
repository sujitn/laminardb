# F010: Watermarks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F010 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F009 |
| **Owner** | TBD |

## Summary

Implement watermarks to track event-time progress and trigger window emissions. Watermarks indicate that no events with timestamps earlier than the watermark are expected.

## Goals

- Generate watermarks from event streams
- Propagate watermarks through operators
- Trigger window emissions on watermark advance
- Support bounded out-of-orderness

## Technical Design

```rust
pub struct WatermarkGenerator {
    max_out_of_orderness: Duration,
    current_watermark: i64,
}

impl WatermarkGenerator {
    pub fn on_event(&mut self, timestamp: i64) -> Option<Watermark>;
    pub fn on_periodic(&mut self) -> Option<Watermark>;
}

pub struct Watermark {
    pub timestamp: i64,
}
```

## Completion Checklist

- [ ] Watermark generation working
- [ ] Propagation through operators
- [ ] Window triggering tested
- [ ] Out-of-order handling verified
