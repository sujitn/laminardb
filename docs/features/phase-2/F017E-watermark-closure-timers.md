# F017E: Watermark-Driven Closure & Timer Persistence

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F017E |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F017D |
| **Owner** | TBD |

## Summary

Ensure session closure timers persist across micro-batch boundaries and work correctly with DataFusion's execution model. Verify watermark-driven closure works for `OnWindowClose` and `Final` strategies.

## Goals

- Timers survive operator drop/recreate between micro-batches
- Watermark advancement triggers session closure
- Sessions closed at correct time: `watermark > session.end + allowed_lateness`
- Timer service integration with `StreamExecutor`

## Motivation

**Uncertainty**: Current code registers timers (line 614 in `session_window.rs`), but unclear if `TimerService` persists state across `StreamExecutor.execute_cycle()` calls.

**Risk**: If timers are lost between batches, sessions **never close** via timers → only watermark-based closure works → `OnWindowClose` strategy breaks.

## Implementation

### 1. Verify TimerService Persistence

**Investigation needed**:
```rust
// In stream_executor.rs, does TimerService persist between cycles?
pub struct StreamExecutor {
    ctx: SessionContext,
    queries: Vec<StreamQuery>,
    // ... no TimerService field visible
}
```

**Hypothesis**: `TimerService` is scoped to `OperatorContext` in each `process()` call. If recreated per batch, timers are lost.

**Solution**: Ensure `TimerService` is part of persistent state or re-registered on restore.

### 2. Re-Register Timers on Restore

```rust
impl<A: Aggregator> SessionWindowOperator<A> {
    fn restore(&mut self, state: OperatorState, ctx: &mut OperatorContext) -> Result<(), OperatorError> {
        // Existing: restore pending_timers map
        let archived = rkyv::access::<Archived<Vec<(SessionId, i64)>>>(&state.data)?;
        let timers: Vec<(SessionId, i64)> = rkyv::deserialize(archived)?;
        self.pending_timers = timers.into_iter().collect();

        // NEW: Re-register all timers with TimerService
        for (&session_id, &trigger_time) in &self.pending_timers {
            let timer_key = Self::timer_key(session_id);
            ctx.timers.register_timer(trigger_time, Some(timer_key), Some(ctx.operator_index));
        }

        Ok(())
    }
}
```

**Alternative**: If `TimerService` is persistent, no changes needed - verify via test.

### 3. Watermark Advancement Hook

Integrate `on_watermark_advance()` (from F017D) into operator execution flow:

**Option A: Explicit Call in `process()`**
```rust
fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
    let mut output = OutputVec::new();

    // Existing: update watermark
    let emitted_watermark = ctx.watermark_generator.on_event(event.timestamp);

    // NEW: Trigger closure for sessions passed by watermark
    if let Some(wm) = emitted_watermark {
        let closure_outputs = self.on_watermark_advance(wm.timestamp(), ctx);
        output.extend(closure_outputs);
    }

    // ... rest of processing
}
```

**Option B: Separate Watermark Operator**
- Create `WatermarkOperator` that calls `on_watermark_advance()` on downstream operators
- More complex, deferred for future

**Choose Option A** for simplicity.

### 4. Timer Handling Verification

Ensure `on_timer()` (lines 644-687) correctly handles timers for sessions:

```rust
fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> OutputVec {
    let mut output = OutputVec::new();

    // Parse session ID from timer key
    let Some(session_id) = Self::session_id_from_timer(&timer.key) else {
        return output;
    };

    // Check if timer is still valid
    let Some(&expected_time) = self.pending_timers.get(&session_id) else {
        return output; // Session already closed/deleted
    };

    if expected_time != timer.timestamp {
        return output; // Stale timer
    }

    // Load session metadata
    // Need to find which key this session belongs to - track reverse map?
    // OR: Embed key_hash in timer key

    // ... emit logic (already correct)
}
```

**Issue**: `session_id_from_timer()` extracts session ID, but to load session from index, need `key_hash`.

**Solution**: Store reverse mapping `session_id -> key_hash`, OR embed key_hash in timer key.

```rust
// Option: Extend timer key format
fn timer_key(key_hash: u64, session_id: SessionId) -> super::TimerKey {
    let mut key = super::TimerKey::new();
    key.push(SESSION_TIMER_PREFIX);
    key.extend_from_slice(&key_hash.to_be_bytes());
    key.extend_from_slice(&session_id.to_bytes());
    key
}

fn parse_timer_key(key: &[u8]) -> Option<(u64, SessionId)> {
    if key.len() != 17 || key[0] != SESSION_TIMER_PREFIX {
        return None;
    }
    let key_hash = u64::from_be_bytes(key[1..9].try_into().ok()?);
    let session_id = SessionId::from_bytes(&key[9..17])?;
    Some((key_hash, session_id))
}
```

## Tests

### Unit Test: Timer Persistence

```rust
#[test]
fn test_timer_survives_restore() {
    let aggregator = CountAggregator::new();
    let mut operator = SessionWindowOperator::with_id(
        Duration::from_millis(1000),
        aggregator.clone(),
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    let mut state = InMemoryStore::new();
    let mut timers = TimerService::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Create session
    let event = create_test_event(500, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Checkpoint
    let checkpoint = operator.checkpoint();
    assert!(!checkpoint.data.is_empty());

    // Simulate operator drop/recreate
    let mut operator2 = SessionWindowOperator::with_id(
        Duration::from_millis(1000),
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Restore
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator2.restore(checkpoint, &mut ctx).unwrap();
    }

    // Verify pending_timers restored
    assert_eq!(operator2.pending_timers.len(), 1);

    // Verify timers re-registered in TimerService
    // (Requires TimerService to expose registered timers for testing)
    // let registered = timers.list_timers();
    // assert_eq!(registered.len(), 1);
}
```

### Integration Test: Watermark Closure

```rust
#[tokio::test]
async fn test_watermark_closes_session() {
    let db = LaminarDB::new_in_memory();

    db.execute_sql("CREATE SOURCE events (ts BIGINT, value INT)").await?;
    db.execute_sql(r#"
        CREATE STREAM sessions AS
        SELECT
            SESSION_START(ts, INTERVAL '1' SECOND) as start,
            SESSION_END(ts, INTERVAL '1' SECOND) as end,
            SUM(value) as total
        FROM events
        GROUP BY SESSION(ts, INTERVAL '1' SECOND)
        EMIT ON WINDOW CLOSE
    "#).await?;

    // Batch 1: Send events
    db.write_to_source("events", batch![
        (0, 10),
        (500, 20),
    ]).await?;
    db.tick().await?;

    // No output yet (OnWindowClose)
    let results = db.read_stream("sessions").await?;
    assert_eq!(results.len(), 0);

    // Batch 2: Advance watermark past session end
    // Session end = 500 + 1000 = 1500
    // Watermark needs to be > 1500 (assuming 0 lateness)
    db.write_to_source("events", batch![
        (3000, 100), // Watermark = 3000
    ]).await?;
    db.tick().await?;

    // First session should now be emitted
    let results = db.read_stream("sessions").await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].start, 0);
    assert_eq!(results[0].end, 1500);
    assert_eq!(results[0].total, 30); // 10 + 20
}
```

## Completion Checklist

- [ ] Verify TimerService persistence across batches
- [ ] Re-register timers on restore (if needed)
- [ ] Integrate `on_watermark_advance()` into `process()`
- [ ] Fix timer key format (include key_hash)
- [ ] Update `on_timer()` to parse key_hash + session_id
- [ ] Unit test: timer persistence across checkpoint/restore
- [ ] Integration test: watermark closes session
- [ ] Integration test: EMIT ON WINDOW CLOSE end-to-end

## Dependencies

- Requires F017D (emit strategies)
- Requires investigation of TimerService architecture

## Success Criteria

- Timers fire correctly after operator restore
- Watermark advancement closes sessions at correct time
- `EMIT ON WINDOW CLOSE` only emits when watermark passes
- No sessions "stuck" open due to lost timers

## Notes

- If TimerService is already persistent, much of this work is unnecessary - validate first
- Timer key format change is backward-incompatible - document migration
- Consider adding metrics: `sessions_closed_by_timer`, `sessions_closed_by_watermark`
