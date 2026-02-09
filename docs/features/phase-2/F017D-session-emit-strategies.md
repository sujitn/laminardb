# F017D: Session Window Emit Strategies & Retraction Support

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F017D |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F017C |
| **Owner** | TBD |

## Summary

Ensure all emit strategies (OnUpdate, Changelog, OnWatermark, OnWindowClose, Final) work correctly with session windows. Add proper retraction support for strategies that require it.

## Goals

- `OnUpdate`: Emit after every state change, with retractions on merge
- `Changelog`: Emit Z-set weighted records (Insert/Delete/UpdateBefore/UpdateAfter)
- `OnWatermark`: No intermediate emissions, emit when watermark passes session end
- `OnWindowClose`: Identical to OnWatermark for sessions
- `Final`: Drop late data, only emit finalized results

## Implementation

### Emit Strategy Handling

Most logic implemented in F017C (`emit_for_strategy()`, `emit_retraction()`). This feature adds:

#### 1. Mark Sessions as Emitted

```rust
impl SessionMetadata {
    /// Mark this session as having been emitted.
    pub fn mark_emitted(&mut self) {
        self.emitted = true;
    }
}

// In emit_for_strategy(), after emission:
if let Some(session_meta) = index.get_mut(session.id) {
    session_meta.mark_emitted();
}
```

#### 2. Track Emitted State in Persistent Storage

Session index must be persisted after `mark_emitted()`:

```rust
// After emitting in process()
self.store_session_index(key_hash, index, ctx.state)?;
```

#### 3. OnWatermark / OnWindowClose Emission

Currently stubs - implement in `on_watermark()`:

```rust
impl<A: Aggregator> SessionWindowOperator<A> {
    /// Called when watermark advances (new method).
    pub fn on_watermark_advance(
        &mut self,
        watermark: i64,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();

        // Iterate all keys with sessions (requires prefix scan or tracking)
        for (key_hash, mut index) in self.session_indices.clone() {
            let mut sessions_to_remove = Vec::new();

            for session in &index.sessions {
                let cleanup_time = session.end + self.allowed_lateness_ms;

                // Emit if watermark passed cleanup time and not yet emitted
                if watermark >= cleanup_time && !session.emitted {
                    let acc = self.load_accumulator(session.id, ctx.state);

                    if let Some(event) = self.create_output(session, &acc) {
                        match &self.emit_strategy {
                            EmitStrategy::Changelog => {
                                output.push(Output::Changelog(
                                    ChangelogRecord::insert(event, ctx.processing_time),
                                ));
                            }
                            _ => {
                                output.push(Output::Event(event));
                            }
                        }
                    }

                    sessions_to_remove.push(session.id);
                }
            }

            // Remove emitted sessions
            for id in sessions_to_remove {
                index.remove(id);
                Self::delete_accumulator(id, ctx.state)?;
                self.pending_timers.remove(&id);
            }

            // Update index in state
            self.store_session_index(key_hash, index, ctx.state)?;
        }

        output
    }
}
```

**Integration**: Hook into watermark generator or call explicitly after watermark updates.

#### 4. Final Strategy: Drop Late Data

Already implemented in F017 (line 553):

```rust
if self.emit_strategy.drops_late_data() {
    self.late_data_metrics.record_dropped();
    return output;
}
```

Verify this path is tested.

## Tests

```rust
#[test]
fn test_on_update_emits_intermediate_results() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000),
        CountAggregator::new(),
        Duration::from_millis(0),
    );
    operator.set_emit_strategy(EmitStrategy::OnUpdate);

    let mut state = InMemoryStore::new();
    let mut timers = TimerService::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Event 1: Should emit count=1
    let event1 = create_test_event(100, 1);
    let outputs1 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx)
    };
    assert_eq!(count_events(&outputs1), 1);

    // Event 2 (same session): Should emit count=2
    let event2 = create_test_event(500, 1);
    let outputs2 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx)
    };
    assert_eq!(count_events(&outputs2), 1);
    let result = extract_result(&outputs2[0]);
    assert_eq!(result, 2);
}

#[test]
fn test_on_window_close_suppresses_intermediate() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000),
        CountAggregator::new(),
        Duration::from_millis(0),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut state = InMemoryStore::new();
    let mut timers = TimerService::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Event 1: Should NOT emit (OnWindowClose)
    let event1 = create_test_event(100, 1);
    let outputs1 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx)
    };
    assert_eq!(count_events(&outputs1), 0); // No intermediate emission

    // Event 2: Still no emission
    let event2 = create_test_event(500, 1);
    let outputs2 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx)
    };
    assert_eq!(count_events(&outputs2), 0);

    // Advance watermark past session end + lateness
    let outputs_wm = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_watermark_advance(2000, &mut ctx) // > 500 + 1000 + 0
    };

    // NOW should emit final result
    assert_eq!(count_events(&outputs_wm), 1);
    let result = extract_result(&outputs_wm[0]);
    assert_eq!(result, 2);
}

#[test]
fn test_changelog_emits_weighted_records() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000),
        CountAggregator::new(),
        Duration::from_millis(0),
    );
    operator.set_emit_strategy(EmitStrategy::Changelog);

    let mut state = InMemoryStore::new();
    let mut timers = TimerService::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    let event = create_test_event(100, 1);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx)
    };

    // Should emit ChangelogRecord with Insert operation
    assert_eq!(count_changelog(&outputs), 1);
    match &outputs[0] {
        Output::Changelog(rec) => {
            assert_eq!(rec.operation, CdcOperation::Insert);
            assert_eq!(rec.weight(), 1);
        }
        _ => panic!("Expected Changelog output"),
    }
}

#[test]
fn test_final_strategy_drops_late_data() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000),
        CountAggregator::new(),
        Duration::from_millis(0),
    );
    operator.set_emit_strategy(EmitStrategy::Final);

    let mut state = InMemoryStore::new();
    let mut timers = TimerService::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Advance watermark far ahead
    let early_event = create_test_event(10000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&early_event, &mut ctx);
    }

    // Late event - should be silently dropped (no LateEvent output)
    let late_event = create_test_event(100, 1);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&late_event, &mut ctx)
    };

    assert!(outputs.is_empty()); // Silently dropped
    assert_eq!(operator.late_data_metrics().late_events_dropped(), 1);
}
```

## Completion Checklist

- [ ] `SessionMetadata.emitted` flag tracked correctly
- [ ] Emitted state persisted to StateStore
- [ ] `on_watermark_advance()` method implemented
- [ ] OnUpdate: emits intermediate results ✓
- [ ] OnWatermark/OnWindowClose: no intermediate emissions ✓
- [ ] Changelog: emits weighted records ✓
- [ ] Final: drops late data ✓
- [ ] Tests for all 5 strategies
- [ ] Integration test: end-to-end SQL with EMIT ON WINDOW CLOSE

## Dependencies

- Requires F017C (session merging)
- Blocks integration tests in F017E

## Success Criteria

- All emit strategies produce correct output
- `EMIT ON WINDOW CLOSE` only emits when watermark passes session end
- Late data handled per strategy (retract, side output, or drop)
- Changelog retractions emitted on session merge

## Notes

- `OnWatermark` and `OnWindowClose` are functionally identical for session windows (both wait for watermark)
- `on_watermark_advance()` requires tracking all active keys - consider memory implications for high-cardinality keys
