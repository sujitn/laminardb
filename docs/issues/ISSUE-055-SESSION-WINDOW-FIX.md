# Issue #55: Session Window Partial Results Across Micro-Batches

> **Status**: Analyzed, Features Specified, Ready for Implementation
> **Priority**: P0 (Critical Correctness Bug)
> **Date Reported**: 2026-02-09
> **Assigned**: TBD

## Summary

SESSION windows emit partial/incorrect results when events for the same session span multiple micro-batches. Instead of accumulating state across batches and emitting once on session close (gap timeout), the system emits intermediate results per micro-batch, producing duplicate/partial outputs.

## Root Cause

**Architectural Mismatch**: Session window operator was designed for continuous reactor-based streaming but is executed via DataFusion micro-batch model.

**Specific Issues**:
1. In-memory `active_sessions` HashMap lost between batches
2. Only one session per key supported (state key: `ses:<key_hash>`)
3. Session merging stubbed out (`#[allow(dead_code)]`)
4. Premature emission at micro-batch boundaries (lines 586-593 in `session_window.rs`)
5. Timer persistence across batches uncertain
6. No support for multiple concurrent sessions per key

## Impact

- **Correctness**: Wrong aggregation results for session windows
- **User Experience**: Cannot use session windows reliably in production
- **Blocking**: Prevents use cases like user session analytics, activity tracking, anomaly detection

## Fix Plan

Implement 4 hardening features (F017B-E) to correct session window behavior:

### F017B: Session State Refactoring
**Effort**: 3-5 days | **Status**: 📝 Draft

- Change state layout from single-session to session-index per key
- Support multiple concurrent sessions: `six:<key_hash>` → `Vec<SessionMetadata>`
- Per-session accumulators: `sac:<session_id>` → `A::Acc`
- Session ID generation with uniqueness guarantees

**Spec**: [F017B-session-state-refactoring.md](../features/phase-2/F017B-session-state-refactoring.md)

### F017C: Session Merging & Overlap Detection
**Effort**: 3-5 days | **Status**: 📝 Draft

- Detect overlapping sessions when new event arrives
- Merge N>1 sessions into single session
- Merge accumulators from all overlapping sessions
- Emit retractions for previously-emitted sessions (Changelog strategy)
- **This is the core fix for Issue #55**

**Spec**: [F017C-session-merging.md](../features/phase-2/F017C-session-merging.md)

### F017D: Session Emit Strategies & Retraction Support
**Effort**: 1-2 days | **Status**: 📝 Draft

- OnUpdate: emit with retractions on merge
- Changelog: Z-set weighted records (Insert/Delete/Update)
- OnWatermark/OnWindowClose: only emit when watermark passes
- Final: drop late data, suppress all intermediate results

**Spec**: [F017D-session-emit-strategies.md](../features/phase-2/F017D-session-emit-strategies.md)

### F017E: Watermark Closure & Timer Persistence
**Effort**: 1-2 days | **Status**: 📝 Draft

- Verify timer persistence across micro-batch boundaries
- Re-register timers on operator restore if needed
- Integrate watermark-driven closure into process loop
- Ensure sessions close at correct time

**Spec**: [F017E-watermark-closure-timers.md](../features/phase-2/F017E-watermark-closure-timers.md)

## Implementation Sequence

```
Week 1:
  PR #1: F017B - State Refactoring (foundation)
  PR #2: F017C - Session Merging (core fix)

Week 2:
  PR #3: F017D - Emit Strategies (correctness)
  PR #4: F017E - Timer Persistence (defensive)
  PR #5: Integration Tests & Documentation
```

## Success Criteria

- [ ] All existing F017 tests pass (23 tests)
- [ ] New test: session across 5+ micro-batches
- [ ] New test: 2-way session merge
- [ ] New test: 3-way session merge
- [ ] New test: retraction emission on merge
- [ ] Integration test: SQL SESSION with EMIT ON WINDOW CLOSE
- [ ] Property test: incremental == batch recomputation (1000 iterations)
- [ ] Benchmark: <1ms p99 latency with 100 sessions/key
- [ ] No partial results at batch boundaries (verified by test)

## Research

Comprehensive research document analyzing production streaming systems:
- **Document**: [session-window-fix-2026.md](../research/session-window-fix-2026.md)
- **Systems Analyzed**: Apache Flink, RisingWave, Kafka Streams, DBSP/Feldera
- **Academic References**: VLDB 2022-2024 papers on session window correctness

## Related Features

- **F017**: Session Windows (base implementation) - 🔧 Hardening
- **F011B**: EMIT Clause Extension - ✅ Done (provides emit strategies)
- **F063**: Changelog/Retraction - ✅ Done (provides Z-set foundation)
- **F064**: Per-Partition Watermarks - ✅ Done (watermark infrastructure)

## Testing Strategy

### Unit Tests (15+ new tests)
- Session metadata (overlap, merge, extend)
- Session index (insert, remove, find_overlapping)
- Multi-session storage and retrieval
- Session merging (2-way, 3-way)
- Retraction emission
- Emit strategy compliance

### Integration Tests (3+ new tests)
- Session across micro-batches (simulated batch boundaries)
- Late data triggers merge
- End-to-end SQL SESSION with EMIT ON WINDOW CLOSE

### Property-Based Tests
- Incremental processing == batch recomputation (proptest with 1000+ iterations)

### Performance Benchmarks
- Session window throughput (10-10K keys)
- Overlap detection latency (1-1000 sessions/key)
- Memory footprint (session index size)

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Performance regression | Binary search on sorted sessions, benchmark before merge |
| State store bloat | Watermark-based GC, max sessions per key limit |
| Backward compatibility | Version state format, migration guide |
| Accumulator merge unsupported | Compile-time trait bound (`MergeableAccumulator`) |

## References

### Code Locations
- `crates/laminar-core/src/operator/session_window.rs` (primary file)
- `crates/laminar-core/src/operator/window.rs` (shared window types)
- `crates/laminar-db/src/stream_executor.rs` (micro-batch execution)

### Documentation
- Feature specs: `docs/features/phase-2/F017*.md`
- Research: `docs/research/session-window-fix-2026.md`
- Base spec: `docs/features/phase-2/F017-session-windows.md`

### External
- [Apache Flink Session Windows](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/)
- [RisingWave EOWC](https://tutorials.risingwave.com/docs/advanced/eowc/)
- [Kafka Streams Sessions](https://kafka.apache.org/31/javadoc/org/apache/kafka/streams/kstream/SessionWindows.html)
- [DBSP Paper (VLDB 2023)](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)

## Progress Tracking

**Feature Index**: See `docs/features/INDEX.md`
- Phase 2: 34/38 features (89%)
- F017: 🔧 Hardening
- F017B-E: 📝 Draft

**Next Action**: Begin F017B implementation (state refactoring)
