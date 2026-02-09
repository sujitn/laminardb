# Session Window Correctness Fix (Issue #55)

> **Date**: 2026-02-09
> **Author**: Research & Analysis
> **Status**: Implementation Planned

## Executive Summary

Issue #55 identifies critical correctness bugs in LaminarDB's session window implementation (F017) where partial results are emitted across micro-batch boundaries. Root cause: architectural mismatch between the operator's continuous-streaming assumptions and DataFusion's micro-batch execution model.

**Impact**: Session windows produce incorrect results in production use cases.

**Fix Complexity**: Medium - requires state refactoring, session merging logic, and emit strategy corrections across 4 new features (F017B-E).

## Research Summary

### Production Systems Analysis

Analyzed 5 production streaming systems' session window implementations:

1. **Apache Flink** (`MergingWindowAssigner`): State-based merging with RocksDB persistence, eager merge on overlap detection, watermark-driven GC
2. **RisingWave**: Epoch-based micro-batch model similar to LaminarDB, known issues with session windows + EOWC (#17119)
3. **Kafka Streams** (`SessionStore`): Record-by-record merging with versioned state store for out-of-order handling
4. **DBSP/Feldera**: Z-set incremental computation with retractions for session merges
5. **Academic Research** (VLDB 2022-2024): Multi-session-per-key requirement, late data merging challenges

**Key Insight**: All correct implementations support **multiple concurrent sessions per key** and **eager merging** when overlaps detected.

### Current Implementation Issues

| Issue | Severity | Impact |
|-------|----------|--------|
| In-memory `active_sessions` HashMap lost between batches | 🔴 Critical | Sessions fragment across batches |
| Only one session per key (`ses:<key_hash>`) | 🔴 Critical | Cannot handle concurrent sessions |
| Session merging stubbed (#[allow(dead_code)]) | 🔴 Critical | Late data doesn't merge sessions |
| Premature emission (lines 586-593) | 🔴 Critical | Partial results at batch boundaries |
| Timer persistence uncertain | 🟠 High | Sessions may never close |
| No multi-session support | 🔴 Critical | Fundamental architecture flaw |

### Proposed Solution

**4-Feature Fix (F017B-E)**:

1. **F017B: State Refactoring** (3-5 days)
   - Multi-session index per key: `six:<key_hash>` → `Vec<SessionMetadata>`
   - Per-session accumulators: `sac:<session_id>` → `A::Acc`
   - Session ID generation with uniqueness guarantees

2. **F017C: Session Merging** (3-5 days)
   - Overlap detection: check if event's time range intersects any existing session
   - Merge N>1 sessions: combine time bounds, merge accumulators, emit retractions
   - No premature emission (only emit per strategy, not at batch boundaries)

3. **F017D: Emit Strategies** (1-2 days)
   - OnUpdate: emit with retractions on merge
   - Changelog: Z-set weighted records
   - OnWatermark/OnWindowClose: only emit when watermark passes session end

4. **F017E: Timer & Watermark** (1-2 days)
   - Verify timer persistence across micro-batches
   - Re-register timers on restore if needed
   - Integrate watermark-driven closure into process loop

**Total Effort**: 8-14 days

### Algorithm Correctness

**Canonical Session Window Algorithm**:
```
On Event(timestamp, value):
  1. Load session index for key
  2. Find all sessions overlapping [timestamp, timestamp + gap]
  3. If overlapping.is_empty():
       Create new session
     Else:
       Merge all overlapping sessions
       Emit retractions for previously-emitted sessions
       Create merged session
  4. Update accumulator
  5. Register timer for session.end + lateness
  6. Emit per strategy (OnUpdate/Changelog/None)

On Watermark(wm):
  For each session where wm > session.end + lateness:
    If not emitted:
      Emit final result
    Delete session and accumulator

On Timer(session_id):
  If session exists and not emitted:
    Emit final result
    Delete session and accumulator
```

## References

### Academic & Industry Sources

- [Apache Flink Windows Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/)
- [RisingWave EMIT ON WINDOW CLOSE](https://tutorials.risingwave.com/docs/advanced/eowc/)
- [Kafka Streams Session Windows](https://kafka.apache.org/31/javadoc/org/apache/kafka/streams/kstream/SessionWindows.html)
- [DBSP: Incremental Computation (VLDB 2023)](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)
- [Survey of Window Types (VLDB Journal 2022)](https://link.springer.com/article/10.1007/s00778-022-00778-6)
- [Spark Session Window Deep Dive (2022)](https://dataninjago.com/2022/07/24/spark-structured-streaming-deep-dive-8-session-window/)

### Internal References

- Issue #55: SESSION Window Emits Partial Results Across Micro-Batches
- F017: Session Windows (base implementation)
- F017B: Session State Refactoring
- F017C: Session Merging & Overlap Detection
- F017D: Emit Strategies & Retraction Support
- F017E: Watermark-Driven Closure & Timer Persistence

## Risk Assessment

| Risk | Probability | Mitigation |
|------|-------------|------------|
| Performance regression (overlap scan) | Medium | Binary search on sorted sessions, benchmark with 1000+ sessions/key |
| State store bloat | Medium | Watermark-based GC, configurable max sessions per key |
| Accumulator merge incompatibility | Low | Compile-time trait bound (`MergeableAccumulator`) |
| Backward compatibility break | Medium | Version state format, document upgrade path |
| Timer scalability | Low | Use hierarchical timer wheel, batch timer registration |

## Success Metrics

- [ ] All unit tests pass (existing 23 + new 15+)
- [ ] Integration test: session across 5+ micro-batches
- [ ] Integration test: 3-way session merge
- [ ] Property test: incremental == batch recomputation (1000 iterations)
- [ ] Benchmark: <1ms p99 latency with 100 sessions/key
- [ ] No partial results emitted across batches (OnWindowClose strategy)

## Next Steps

1. Create feature specs F017B-E ✅
2. Update F017 status to 🔧 Hardening ✅
3. Implement F017B (state refactoring)
4. Implement F017C (session merging) - **Core fix for Issue #55**
5. Implement F017D (emit strategies)
6. Implement F017E (timer persistence)
7. Integration testing with DataFusion micro-batch execution
8. Performance validation and optimization
9. Documentation and migration guide
