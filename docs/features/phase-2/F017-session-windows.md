# F017: Session Windows

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F017 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F016 |
| **Owner** | TBD |

## Summary

Implement session windows that group events by activity periods separated by gaps. Sessions are dynamic windows that grow with activity and close after inactivity.

## Goals

- Gap-based session detection
- Per-key session tracking
- Session merging on late data
- Configurable gap timeout

## Technical Design

```rust
pub struct SessionWindow {
    gap: Duration,
}

pub struct SessionState {
    start: u64,
    end: u64,
    data: AggregateState,
}

impl SessionWindowOperator {
    pub fn process(&mut self, event: &Event) -> Vec<o> {
        let key = &event.key;
        
        if let Some(session) = self.sessions.get_mut(key) {
            if event.timestamp - session.end <= self.gap.as_millis() as u64 {
                // Extend session
                session.end = event.timestamp;
                session.data.add(event);
            } else {
                // Close old session, start new
                let output = self.emit_session(session);
                *session = SessionState::new(event);
                return vec![output];
            }
        } else {
            self.sessions.insert(key.clone(), SessionState::new(event));
        }
        vec![]
    }
}
```

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

## Completion Checklist

- [ ] Gap detection working
- [ ] Session merging implemented
- [ ] Per-key tracking correct
- [ ] SQL syntax working
