# F009: Event Time Processing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F009 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 1 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F001 |
| **Owner** | TBD |

## Summary

Implement event-time semantics where events are processed according to their embedded timestamps rather than arrival time. This is essential for correct windowing and out-of-order event handling.

## Goals

- Extract timestamps from event payloads
- Support configurable timestamp fields
- Track event-time progress
- Enable event-time windowing

## Technical Design

```rust
pub struct EventTimeExtractor {
    field: String,
    format: TimestampFormat,
}

impl EventTimeExtractor {
    pub fn extract(&self, event: &Event) -> Result<u64>;
}

pub enum TimestampFormat {
    UnixMillis,
    UnixSeconds,
    Iso8601,
    Custom(String),
}
```

## Completion Checklist

- [ ] Timestamp extraction working
- [ ] Multiple formats supported
- [ ] Unit tests passing
