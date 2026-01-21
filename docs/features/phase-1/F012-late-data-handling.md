# F012: Late Data Handling

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F012 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F010 |
| **Owner** | TBD |

## Summary

Handle events that arrive after their window has closed (late data). Options include dropping, updating, or routing to a side output.

## Goals

- Configurable allowed lateness
- Update closed windows within lateness period
- Side output for late data
- Metrics for late event tracking

## Technical Design

```rust
pub struct LateDataConfig {
    pub allowed_lateness: Duration,
    pub side_output: Option<String>,
}

impl WindowOperator {
    pub fn handle_late(&mut self, event: &Event) -> LateDataResult {
        if self.within_lateness(event) {
            LateDataResult::Update(self.update_window(event))
        } else if let Some(side) = &self.config.side_output {
            LateDataResult::SideOutput(side.clone())
        } else {
            LateDataResult::Drop
        }
    }
}
```

## SQL Syntax

```sql
SELECT ... FROM stream
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
ALLOW LATENESS INTERVAL '1' HOUR;

SELECT ... FROM stream
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
LATE DATA TO late_events;
```

## Completion Checklist

- [ ] Allowed lateness working
- [ ] Side output implemented
- [ ] Metrics tracking
- [ ] SQL parsing complete
