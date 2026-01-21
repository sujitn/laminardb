# F011: EMIT Clause

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F011 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 1 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F010 |
| **Owner** | TBD |

## Summary

Implement the EMIT clause to control when window results are output. Options include emitting on watermark, periodically, or on every update.

## Goals

- EMIT ON WATERMARK (default, most efficient)
- EMIT EVERY interval (periodic updates)
- EMIT ON UPDATE (every change)

## Technical Design

```rust
pub enum EmitStrategy {
    OnWatermark,
    Periodic(Duration),
    OnUpdate,
}

impl WindowOperator {
    pub fn set_emit_strategy(&mut self, strategy: EmitStrategy);
}
```

## SQL Syntax

```sql
SELECT ... FROM stream
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT ON WATERMARK;

SELECT ... FROM stream  
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT EVERY INTERVAL '10' SECOND;
```

## Completion Checklist

- [ ] All emit strategies implemented
- [ ] SQL parsing working
- [ ] Integration tests passing
