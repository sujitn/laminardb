# F017C: Session Merging & Overlap Detection

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F017C |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F017B |
| **Owner** | TBD |

## Summary

Implement session merging when events arrive that overlap or bridge multiple existing sessions. This is the core fix for Issue #55 - ensuring sessions accumulate correctly across micro-batch boundaries.

## Goals

- Detect overlapping sessions when new event arrives
- Merge N>1 sessions into a single session
- Merge accumulators from all overlapping sessions
- No premature emission at micro-batch boundaries

## Motivation

**Current Problem** (from Issue #55):
```
Batch 1: Events [0, 10, 20] → Session [0, 50]
Batch 2: Event [100] arrives → Creates NEW session [100, 150], EMITS partial [0, 50]
Batch 3: Event [50] arrives → Should merge both sessions, but only extends one

Result: Two separate sessions instead of merged session
```

**Correct Behavior**:
```
Batch 1: Events [0, 10, 20] → Session A [0, 50]
Batch 2: Event [100] arrives → Session B [100, 150] (separate, gap too large)
Batch 3: Event [50] arrives → Overlaps both A and B → MERGE → Session C [0, 150]
```

## Implementation

### Core Merging Logic

```rust
impl<A: Aggregator> SessionWindowOperator<A> {
    /// Processes a single event, handling session creation, extension, and merging.
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        let event_time = event.timestamp;
        let mut output = OutputVec::new();

        // Update watermark (unchanged)
        let emitted_watermark = ctx.watermark_generator.on_event(event_time);

        // Check if event is late (unchanged)
        let current_wm = ctx.watermark_generator.current_watermark();
        if current_wm > i64::MIN && self.is_late(event_time, current_wm) {
            // Handle late data (existing logic)
            if self.emit_strategy.drops_late_data() {
                self.late_data_metrics.record_dropped();
                return output;
            }
            // ... side output logic
        }

        // Extract key and compute hash
        let key = self.extract_key(event);
        let key_hash = Self::key_hash(&key);

        // Load session index for this key
        let mut index = self.load_session_index(key_hash, ctx.state);

        // Find all overlapping sessions
        let overlapping_ids = index.find_overlapping(event_time, self.gap_ms);

        if overlapping_ids.is_empty() {
            // No overlapping sessions - create new session
            self.create_new_session(&mut index, key, event_time, event, key_hash, ctx, &mut output)?;
        } else {
            // Merge overlapping sessions
            self.merge_sessions(
                &mut index,
                overlapping_ids,
                event_time,
                event,
                key_hash,
                ctx,
                &mut output,
            )?;
        }

        // Store updated session index
        self.store_session_index(key_hash, index, ctx.state)?;

        // Emit watermark if generated
        if let Some(wm) = emitted_watermark {
            output.push(Output::Watermark(wm.timestamp()));
        }

        output
    }

    /// Creates a new session for an event.
    fn create_new_session(
        &mut self,
        index: &mut SessionIndex,
        key: Vec<u8>,
        event_time: i64,
        event: &Event,
        key_hash: u64,
        ctx: &mut OperatorContext,
        output: &mut OutputVec,
    ) -> Result<(), OperatorError> {
        // Generate new session ID
        let session_id = SessionId::generate(&self.operator_id, &self.session_id_counter);

        // Create session metadata
        let mut session = SessionMetadata::new(session_id, event_time, self.gap_ms);

        // Create new accumulator
        let mut acc = self.aggregator.create_accumulator();
        if let Some(value) = self.aggregator.extract(event) {
            acc.add(value);
        }

        // Store accumulator
        Self::store_accumulator(session_id, &acc, ctx.state)?;

        // Add to index
        index.insert(session.clone());

        // Register timer
        self.register_timer(session_id, &session, ctx);

        // Emit based on strategy
        self.emit_for_strategy(&session, &acc, output, ctx);

        Ok(())
    }

    /// Merges overlapping sessions.
    fn merge_sessions(
        &mut self,
        index: &mut SessionIndex,
        overlapping_ids: Vec<SessionId>,
        event_time: i64,
        event: &Event,
        key_hash: u64,
        ctx: &mut OperatorContext,
        output: &mut OutputVec,
    ) -> Result<(), OperatorError> {
        // Collect overlapping sessions
        let mut overlapping_sessions: Vec<SessionMetadata> = overlapping_ids
            .iter()
            .filter_map(|&id| index.get(id).cloned())
            .collect();

        if overlapping_sessions.is_empty() {
            return Ok(()); // Shouldn't happen, but defensive
        }

        // Generate new merged session ID
        let merged_id = SessionId::generate(&self.operator_id, &self.session_id_counter);

        // Compute merged time bounds
        let merged_start = overlapping_sessions
            .iter()
            .map(|s| s.start)
            .min()
            .unwrap_or(event_time);
        let merged_end = overlapping_sessions
            .iter()
            .map(|s| s.end)
            .max()
            .unwrap_or(event_time)
            .max(event_time + self.gap_ms);

        // Determine if any overlapping session was already emitted
        let any_emitted = overlapping_sessions.iter().any(|s| s.emitted);

        // Create merged session
        let mut merged_session = SessionMetadata {
            id: merged_id,
            start: merged_start,
            end: merged_end,
            emitted: false, // Reset - will emit new merged result
            timer_registered: false,
        };

        // Merge accumulators
        let mut merged_acc = self.aggregator.create_accumulator();
        for session in &overlapping_sessions {
            let acc = self.load_accumulator(session.id, ctx.state);
            merged_acc.merge(&acc);
        }

        // Add new event value
        if let Some(value) = self.aggregator.extract(event) {
            merged_acc.add(value);
        }

        // Emit retractions for previously emitted sessions (if OnUpdate or Changelog)
        if self.emit_strategy.generates_retractions() && any_emitted {
            for old_session in &overlapping_sessions {
                if old_session.emitted {
                    let old_acc = self.load_accumulator(old_session.id, ctx.state);
                    self.emit_retraction(old_session, &old_acc, output, ctx);
                }
            }
        }

        // Delete old sessions and accumulators
        for session in &overlapping_sessions {
            index.remove(session.id);
            Self::delete_accumulator(session.id, ctx.state)?;
            self.pending_timers.remove(&session.id);
        }

        // Store merged accumulator
        Self::store_accumulator(merged_id, &merged_acc, ctx.state)?;

        // Add merged session to index
        index.insert(merged_session.clone());

        // Register timer for merged session
        self.register_timer(merged_id, &merged_session, ctx);

        // Emit based on strategy
        self.emit_for_strategy(&merged_session, &merged_acc, output, ctx);

        Ok(())
    }

    /// Emits output based on the configured emit strategy.
    fn emit_for_strategy(
        &mut self,
        session: &SessionMetadata,
        acc: &A::Acc,
        output: &mut OutputVec,
        ctx: &OperatorContext,
    ) {
        match &self.emit_strategy {
            EmitStrategy::OnUpdate => {
                if let Some(event) = self.create_output(session, acc) {
                    output.push(Output::Event(event));
                    // Mark as emitted for retraction tracking
                    if let Some(mut index) = self.session_indices.get_mut(&Self::key_hash(&session.key)) {
                        if let Some(meta) = index.get_mut(session.id) {
                            meta.emitted = true;
                        }
                    }
                }
            }
            EmitStrategy::Changelog => {
                if let Some(event) = self.create_output(session, acc) {
                    let record = ChangelogRecord::insert(event, ctx.processing_time);
                    output.push(Output::Changelog(record));
                    // Mark as emitted
                    if let Some(mut index) = self.session_indices.get_mut(&Self::key_hash(&session.key)) {
                        if let Some(meta) = index.get_mut(session.id) {
                            meta.emitted = true;
                        }
                    }
                }
            }
            // OnWatermark, OnWindowClose, Final: no intermediate emission
            EmitStrategy::OnWatermark
            | EmitStrategy::Periodic(_)
            | EmitStrategy::OnWindowClose
            | EmitStrategy::Final => {}
        }
    }

    /// Emits a retraction for a session (Changelog strategy).
    fn emit_retraction(
        &self,
        session: &SessionMetadata,
        acc: &A::Acc,
        output: &mut OutputVec,
        ctx: &OperatorContext,
    ) {
        if let Some(event) = self.create_output(session, acc) {
            match &self.emit_strategy {
                EmitStrategy::Changelog => {
                    let record = ChangelogRecord::delete(event, ctx.processing_time);
                    output.push(Output::Changelog(record));
                }
                EmitStrategy::OnUpdate => {
                    // For OnUpdate, emit UpdateBefore/UpdateAfter pair
                    let retraction = ChangelogRecord::new(
                        event.clone(),
                        CdcOperation::UpdateBefore,
                        ctx.processing_time,
                    );
                    output.push(Output::Changelog(retraction));
                    // UpdateAfter will be emitted by emit_for_strategy
                }
                _ => {}
            }
        }
    }

    /// Creates output event from session and accumulator.
    fn create_output(&self, session: &SessionMetadata, acc: &A::Acc) -> Option<Event> {
        if acc.is_empty() {
            return None;
        }

        let result = acc.result();
        let result_i64 = result.to_i64();

        let batch = RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            vec![
                Arc::new(Int64Array::from(vec![session.start])),
                Arc::new(Int64Array::from(vec![session.end])),
                Arc::new(Int64Array::from(vec![result_i64])),
            ],
        )
        .ok()?;

        Some(Event::new(session.end, batch))
    }

    /// Registers a timer for session closure.
    fn register_timer(
        &mut self,
        session_id: SessionId,
        session: &SessionMetadata,
        ctx: &mut OperatorContext,
    ) {
        let trigger_time = session.end + self.allowed_lateness_ms;

        // Cancel previous timer if different
        if let Some(&old_time) = self.pending_timers.get(&session_id) {
            if old_time == trigger_time {
                return; // Timer already set
            }
        }

        let timer_key = Self::timer_key(session_id);
        ctx.timers
            .register_timer(trigger_time, Some(timer_key), Some(ctx.operator_index));
        self.pending_timers.insert(session_id, trigger_time);
    }

    /// Generates timer key for a session ID.
    fn timer_key(session_id: SessionId) -> super::TimerKey {
        let mut key = super::TimerKey::new();
        key.push(SESSION_TIMER_PREFIX);
        key.extend_from_slice(&session_id.to_bytes());
        key
    }
}
```

### Accumulator Merge Trait

```rust
/// Extension to the Accumulator trait for merging.
pub trait MergeableAccumulator: Accumulator {
    /// Merges another accumulator into this one.
    ///
    /// This is required for session window merging. After merging,
    /// `self` should contain the combined state of both accumulators.
    fn merge(&mut self, other: &Self);
}

impl MergeableAccumulator for CountAccumulator {
    fn merge(&mut self, other: &Self) {
        self.count += other.count;
    }
}

impl MergeableAccumulator for SumAccumulator {
    fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
    }
}

// Add compile-time check in SessionWindowOperator::new()
impl<A: Aggregator> SessionWindowOperator<A>
where
    A::Acc: MergeableAccumulator,
{
    pub fn new(gap: Duration, aggregator: A, allowed_lateness: Duration) -> Self {
        // ... existing code
    }
}
```

## Tests

### Unit Tests

```rust
#[test]
fn test_session_across_micro_batches() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000),
        CountAggregator::new(),
        Duration::from_millis(0),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Batch 1: Event at t=100
    let event1 = create_test_event(100, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }

    // Simulate micro-batch boundary: clear in-memory cache
    operator.session_indices.clear();

    // Batch 2: Event at t=500 (within gap of previous)
    let event2 = create_test_event(500, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx);
    }

    // Should have ONE session (not two)
    let key_hash = SessionWindowOperator::<CountAggregator>::key_hash(&[]);
    let index = operator.load_session_index(key_hash, &state);
    assert_eq!(index.len(), 1);
    assert_eq!(index.sessions[0].start, 100);
    assert_eq!(index.sessions[0].end, 1500); // 500 + 1000

    // Accumulator should count both events
    let acc = operator.load_accumulator(index.sessions[0].id, &state);
    assert_eq!(acc.result(), 2);
}

#[test]
fn test_late_event_merges_sessions() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000),
        CountAggregator::new(),
        Duration::from_millis(0),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Batch 1: Event at t=0 → Session A [0, 1000]
    let event1 = create_test_event(0, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }

    // Batch 2: Event at t=5000 → Session B [5000, 6000] (gap too large)
    operator.session_indices.clear();
    let event2 = create_test_event(5000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx);
    }

    // Verify two separate sessions
    let key_hash = SessionWindowOperator::<CountAggregator>::key_hash(&[]);
    let index = operator.load_session_index(key_hash, &state);
    assert_eq!(index.len(), 2);

    // Batch 3: Event at t=2500 bridges the gap → should merge both
    operator.session_indices.clear();
    let event3 = create_test_event(2500, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event3, &mut ctx);
    }

    // Should have ONE merged session
    let index = operator.load_session_index(key_hash, &state);
    assert_eq!(index.len(), 1);
    assert_eq!(index.sessions[0].start, 0);
    assert_eq!(index.sessions[0].end, 6000); // max of all ends

    // Accumulator should count all three events
    let acc = operator.load_accumulator(index.sessions[0].id, &state);
    assert_eq!(acc.result(), 3);
}

#[test]
fn test_three_way_session_merge() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(5000), // Large gap for bridging
        SumAggregator::new(0),
        Duration::from_millis(0),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Create three separate sessions
    let events = vec![
        (0, 10),      // Session A [0, 5000]
        (10000, 20),  // Session B [10000, 15000]
        (20000, 30),  // Session C [20000, 25000]
    ];

    for (ts, val) in events {
        operator.session_indices.clear();
        let event = create_test_event(ts, val);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Verify three sessions
    let key_hash = SessionWindowOperator::<SumAggregator>::key_hash(&[]);
    let index = operator.load_session_index(key_hash, &state);
    assert_eq!(index.len(), 3);

    // Event at t=6000 merges A and B
    operator.session_indices.clear();
    let event_merge1 = create_test_event(6000, 100);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event_merge1, &mut ctx);
    }

    let index = operator.load_session_index(key_hash, &state);
    assert_eq!(index.len(), 2); // Two sessions: merged AB, separate C

    // Event at t=16000 merges all three
    operator.session_indices.clear();
    let event_merge2 = create_test_event(16000, 200);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event_merge2, &mut ctx);
    }

    let index = operator.load_session_index(key_hash, &state);
    assert_eq!(index.len(), 1); // All merged
    assert_eq!(index.sessions[0].start, 0);
    assert_eq!(index.sessions[0].end, 25000);

    // Total sum: 10 + 20 + 30 + 100 + 200 = 360
    let acc = operator.load_accumulator(index.sessions[0].id, &state);
    assert_eq!(acc.result(), 360);
}

#[test]
fn test_session_merge_emits_retractions() {
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000),
        CountAggregator::new(),
        Duration::from_millis(0),
    );
    operator.set_emit_strategy(EmitStrategy::Changelog);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Batch 1: Event at t=0 → emits Insert(count=1)
    let event1 = create_test_event(0, 1);
    let outputs1 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx)
    };

    assert_eq!(count_changelog_inserts(&outputs1), 1);

    // Batch 2: Event at t=5000 → emits Insert(count=1)
    operator.session_indices.clear();
    let event2 = create_test_event(5000, 1);
    let outputs2 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx)
    };

    assert_eq!(count_changelog_inserts(&outputs2), 1);

    // Batch 3: Event at t=2500 merges sessions
    operator.session_indices.clear();
    let event3 = create_test_event(2500, 1);
    let outputs3 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event3, &mut ctx)
    };

    // Should emit:
    // 1. Delete for session A (count=1)
    // 2. Delete for session B (count=1)
    // 3. Insert for merged session (count=3)
    assert_eq!(count_changelog_deletes(&outputs3), 2);
    assert_eq!(count_changelog_inserts(&outputs3), 1);

    // Verify final result
    match outputs3.iter().find(|o| matches!(o, Output::Changelog(r) if r.operation == CdcOperation::Insert)) {
        Some(Output::Changelog(rec)) => {
            let result = extract_result_from_event(&rec.event);
            assert_eq!(result, 3);
        }
        _ => panic!("Expected Insert changelog record"),
    }
}
```

## Completion Checklist

- [ ] `MergeableAccumulator` trait defined
- [ ] `merge_sessions()` method implemented
- [ ] `find_overlapping()` returns all overlapping session IDs
- [ ] Accumulator merging for COUNT, SUM, MIN, MAX
- [ ] Retraction emission for OnUpdate and Changelog
- [ ] No premature emission at batch boundaries (verify via test)
- [ ] Unit tests: session across batches, 2-way merge, 3-way merge
- [ ] Unit test: retraction emission
- [ ] Integration test: end-to-end SQL session merge
- [ ] Documentation updates

## Dependencies

- Requires F017B (multi-session support)
- Blocks F017D (emit strategies depend on correct merging)

## Success Criteria

- Events spanning multiple micro-batches correctly merged into single session
- Late data that bridges sessions triggers merge
- No partial results emitted at batch boundaries (unless OnUpdate strategy)
- All accumulators merge correctly
- Retractions emitted when sessions merge (Changelog strategy)

## Notes

- Session merging is **eager**: happens immediately when overlapping event arrives
- Alternative (lazy merging): merge during watermark advancement - more complex, deferred for future
- Not all aggregators support merging (e.g., exact median) - trait bound enforces this at compile time
