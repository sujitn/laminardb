# F017B: Session State Refactoring (Multi-Session Support)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F017B |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F017 |
| **Owner** | TBD |

## Summary

Refactor session window state layout to support multiple concurrent sessions per key. This is the foundation for correct session window behavior across micro-batch boundaries.

## Goals

- Support multiple sessions per key (not just one)
- Persistent session index survives micro-batch boundaries
- Unique session IDs for individual session tracking
- Backward-compatible state migration

## Motivation

**Current Problem**: F017 stores only one session per key using state keys `ses:<key_hash>` and `sac:<key_hash>`. This prevents:
1. Late data that creates a separate session (gap has passed)
2. Session merging (need to track multiple sessions to merge them)
3. Concurrent sessions for the same key at different time ranges

**Solution**: Store a **session index** per key that lists all active sessions, plus individual session state.

## Implementation

### New Data Structures

```rust
/// Unique identifier for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct SessionId(u64);

impl SessionId {
    /// Generates a new unique session ID.
    pub fn generate(operator_id: &str, counter: &AtomicU64) -> Self {
        let id = counter.fetch_add(1, Ordering::Relaxed);
        // Combine operator_id hash with counter for uniqueness
        let hash = fxhash::hash64(operator_id) ^ id;
        Self(hash)
    }

    /// Converts to bytes for state storage.
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Parses from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 8 {
            return None;
        }
        let id = u64::from_be_bytes(bytes.try_into().ok()?);
        Some(Self(id))
    }
}

/// Metadata for a single session (lightweight, stored in index).
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct SessionMetadata {
    /// Unique ID for this session
    pub id: SessionId,
    /// Session start timestamp (inclusive)
    pub start: i64,
    /// Session end timestamp (exclusive, = last event time + gap)
    pub end: i64,
    /// Has this session been emitted yet? (for retraction tracking)
    pub emitted: bool,
    /// Has a closure timer been registered for this session?
    pub timer_registered: bool,
}

impl SessionMetadata {
    /// Creates a new session metadata.
    pub fn new(id: SessionId, timestamp: i64, gap_ms: i64) -> Self {
        Self {
            id,
            start: timestamp,
            end: timestamp + gap_ms,
            emitted: false,
            timer_registered: false,
        }
    }

    /// Checks if a timestamp overlaps this session's time range.
    ///
    /// A timestamp overlaps if:
    /// - It falls within [start, end + gap_ms)
    /// - OR its potential session [timestamp, timestamp + gap_ms) intersects [start, end + gap_ms)
    pub fn overlaps(&self, timestamp: i64, gap_ms: i64) -> bool {
        // Session range: [self.start, self.end + gap_ms)
        // Event range: [timestamp, timestamp + gap_ms)
        // Overlaps if ranges intersect
        timestamp < self.end + gap_ms && self.start < timestamp + gap_ms
    }

    /// Extends the session to include a new timestamp.
    pub fn extend(&mut self, timestamp: i64, gap_ms: i64) {
        self.start = self.start.min(timestamp);
        self.end = self.end.max(timestamp + gap_ms);
    }

    /// Merges another session into this one.
    pub fn merge(&mut self, other: &SessionMetadata) {
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
        // If either session was emitted, merged session is considered emitted
        // (for retraction tracking)
        self.emitted = self.emitted || other.emitted;
    }
}

/// Index of all active sessions for a key.
///
/// Stored in state as a single serialized structure per key.
/// Sessions are kept sorted by start time for efficient overlap detection.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct SessionIndex {
    /// All active sessions for this key, sorted by start time
    pub sessions: Vec<SessionMetadata>,
}

impl SessionIndex {
    /// Creates an empty session index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a new session to the index.
    pub fn insert(&mut self, session: SessionMetadata) {
        self.sessions.push(session);
        // Keep sorted by start time
        self.sessions.sort_by_key(|s| s.start);
    }

    /// Removes a session by ID.
    pub fn remove(&mut self, id: SessionId) -> Option<SessionMetadata> {
        if let Some(pos) = self.sessions.iter().position(|s| s.id == id) {
            Some(self.sessions.remove(pos))
        } else {
            None
        }
    }

    /// Finds all sessions that overlap with a given timestamp.
    pub fn find_overlapping(&self, timestamp: i64, gap_ms: i64) -> Vec<SessionId> {
        self.sessions
            .iter()
            .filter(|s| s.overlaps(timestamp, gap_ms))
            .map(|s| s.id)
            .collect()
    }

    /// Gets a session by ID.
    pub fn get(&self, id: SessionId) -> Option<&SessionMetadata> {
        self.sessions.iter().find(|s| s.id == id)
    }

    /// Gets a mutable reference to a session by ID.
    pub fn get_mut(&mut self, id: SessionId) -> Option<&mut SessionMetadata> {
        self.sessions.iter_mut().find(|s| s.id == id)
    }

    /// Returns the number of active sessions.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Returns true if there are no active sessions.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}
```

### New State Keys

```rust
/// State key prefix for session index (4 bytes)
const SESSION_INDEX_PREFIX: &[u8; 4] = b"six:";

/// State key prefix for session accumulator by session ID (4 bytes)
const SESSION_ACC_PREFIX: &[u8; 4] = b"sac:";

/// Generates the state key for a session index.
fn session_index_key(key_hash: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..4].copy_from_slice(SESSION_INDEX_PREFIX);
    key[4..12].copy_from_slice(&key_hash.to_be_bytes());
    key
}

/// Generates the state key for a session accumulator.
fn session_acc_key(session_id: SessionId) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..4].copy_from_slice(SESSION_ACC_PREFIX);
    key[4..12].copy_from_slice(&session_id.to_bytes());
    key
}
```

### Updated Operator Structure

```rust
pub struct SessionWindowOperator<A: Aggregator> {
    gap_ms: i64,
    aggregator: A,
    allowed_lateness_ms: i64,

    // NEW: Session ID generator
    session_id_counter: Arc<AtomicU64>,

    // In-memory cache of session indices (volatile, rebuilt from state)
    session_indices: FxHashMap<u64, SessionIndex>,

    // Pending timers by session ID (not key hash)
    pending_timers: FxHashMap<SessionId, i64>,

    emit_strategy: EmitStrategy,
    late_data_config: LateDataConfig,
    late_data_metrics: LateDataMetrics,
    operator_id: String,
    output_schema: SchemaRef,
    key_column: Option<usize>,
    _phantom: PhantomData<A::Acc>,
}
```

### State Access Methods

```rust
impl<A: Aggregator> SessionWindowOperator<A> {
    /// Loads the session index for a key from state.
    fn load_session_index(
        &mut self,
        key_hash: u64,
        state: &dyn StateStore,
    ) -> SessionIndex {
        // Check cache first
        if let Some(index) = self.session_indices.get(&key_hash) {
            return index.clone();
        }

        // Load from state
        let state_key = Self::session_index_key(key_hash);
        let index = state
            .get_typed::<SessionIndex>(&state_key)
            .ok()
            .flatten()
            .unwrap_or_else(SessionIndex::new);

        // Cache it
        self.session_indices.insert(key_hash, index.clone());
        index
    }

    /// Stores the session index for a key to state.
    fn store_session_index(
        &mut self,
        key_hash: u64,
        index: SessionIndex,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let state_key = Self::session_index_key(key_hash);

        if index.is_empty() {
            // Delete empty indices to save space
            state
                .delete(&state_key)
                .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))?;
            self.session_indices.remove(&key_hash);
        } else {
            state
                .put_typed(&state_key, &index)
                .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))?;
            self.session_indices.insert(key_hash, index);
        }

        Ok(())
    }

    /// Loads an accumulator for a session ID.
    fn load_accumulator(&self, session_id: SessionId, state: &dyn StateStore) -> A::Acc {
        let acc_key = Self::session_acc_key(session_id);
        state
            .get_typed::<A::Acc>(&acc_key)
            .ok()
            .flatten()
            .unwrap_or_else(|| self.aggregator.create_accumulator())
    }

    /// Stores an accumulator for a session ID.
    fn store_accumulator(
        session_id: SessionId,
        acc: &A::Acc,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let acc_key = Self::session_acc_key(session_id);
        state
            .put_typed(&acc_key, acc)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))
    }

    /// Deletes an accumulator for a session ID.
    fn delete_accumulator(
        session_id: SessionId,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let acc_key = Self::session_acc_key(session_id);
        state
            .delete(&acc_key)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))
    }
}
```

### Migration Logic

```rust
/// Old session state format (F017)
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
struct LegacySessionState {
    start: i64,
    end: i64,
    key: Vec<u8>,
}

impl<A: Aggregator> SessionWindowOperator<A> {
    /// Migrates old state format to new format during restore.
    fn migrate_legacy_state(&mut self, state: &mut dyn StateStore) -> Result<(), OperatorError> {
        // Scan for old-format session state keys (ses:*)
        const OLD_SESSION_PREFIX: &[u8; 4] = b"ses:";

        // Note: This requires prefix scan support in StateStore
        // For now, skip migration if prefix scan not available
        // Users must drain sessions before upgrading

        // TODO: Implement prefix scan in StateStore trait
        // for (key, value) in state.prefix_scan(OLD_SESSION_PREFIX) {
        //     // Parse old state
        //     // Create new SessionIndex with single session
        //     // Delete old key
        // }

        Ok(())
    }
}
```

## Tests

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_id_generation() {
        let counter = Arc::new(AtomicU64::new(0));
        let id1 = SessionId::generate("op1", &counter);
        let id2 = SessionId::generate("op1", &counter);
        assert_ne!(id1, id2); // Unique IDs
    }

    #[test]
    fn test_session_metadata_overlaps() {
        let session = SessionMetadata::new(SessionId(1), 1000, 5000);
        // [1000, 6000]

        assert!(session.overlaps(1000, 5000)); // Exact start
        assert!(session.overlaps(3000, 5000)); // Within session
        assert!(session.overlaps(5999, 5000)); // Just before end
        assert!(session.overlaps(6000, 5000)); // Event range [6000, 11000] overlaps [1000, 11000]
        assert!(session.overlaps(10999, 5000)); // Event range [10999, 15999] overlaps [1000, 11000]

        assert!(!session.overlaps(11001, 5000)); // Event range [11001, 16001] no overlap
    }

    #[test]
    fn test_session_index_insert_sorted() {
        let mut index = SessionIndex::new();

        index.insert(SessionMetadata::new(SessionId(3), 3000, 1000));
        index.insert(SessionMetadata::new(SessionId(1), 1000, 1000));
        index.insert(SessionMetadata::new(SessionId(2), 2000, 1000));

        // Should be sorted by start time
        assert_eq!(index.sessions[0].start, 1000);
        assert_eq!(index.sessions[1].start, 2000);
        assert_eq!(index.sessions[2].start, 3000);
    }

    #[test]
    fn test_session_index_find_overlapping() {
        let mut index = SessionIndex::new();

        index.insert(SessionMetadata::new(SessionId(1), 1000, 1000)); // [1000, 2000]
        index.insert(SessionMetadata::new(SessionId(2), 5000, 1000)); // [5000, 6000]
        index.insert(SessionMetadata::new(SessionId(3), 10000, 1000)); // [10000, 11000]

        // Event at 1500: overlaps session 1 only
        let overlapping = index.find_overlapping(1500, 1000);
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0], SessionId(1));

        // Event at 3000 with gap=5000: event range [3000, 8000]
        // Overlaps [1000, 2000+1000=3000]? 3000 < 3000 && 1000 < 8000? NO (< not <=)
        // Overlaps [5000, 6000+1000=7000]? 3000 < 7000 && 5000 < 8000? YES
        let overlapping = index.find_overlapping(3000, 5000);
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0], SessionId(2));
    }

    #[test]
    fn test_session_index_remove() {
        let mut index = SessionIndex::new();

        index.insert(SessionMetadata::new(SessionId(1), 1000, 1000));
        index.insert(SessionMetadata::new(SessionId(2), 2000, 1000));

        let removed = index.remove(SessionId(1));
        assert!(removed.is_some());
        assert_eq!(index.len(), 1);
        assert_eq!(index.sessions[0].id, SessionId(2));
    }

    #[test]
    fn test_session_metadata_merge() {
        let mut session1 = SessionMetadata::new(SessionId(1), 1000, 1000);
        let session2 = SessionMetadata::new(SessionId(2), 5000, 1000);

        session1.merge(&session2);

        assert_eq!(session1.start, 1000); // min(1000, 5000)
        assert_eq!(session1.end, 6000); // max(2000, 6000)
    }

    #[test]
    fn test_session_state_key_format() {
        let key_hash = 0x1234_5678_9ABC_DEF0u64;
        let key = SessionWindowOperator::<CountAggregator>::session_index_key(key_hash);

        assert_eq!(&key[..4], b"six:");
        assert_eq!(&key[4..12], &key_hash.to_be_bytes());
    }

    #[test]
    fn test_session_acc_key_format() {
        let session_id = SessionId(0x1234_5678_9ABC_DEF0u64);
        let key = SessionWindowOperator::<CountAggregator>::session_acc_key(session_id);

        assert_eq!(&key[..4], b"sac:");
        assert_eq!(&key[4..12], &session_id.to_bytes());
    }
}
```

### Integration Tests

```rust
#[test]
fn test_multi_session_per_key_storage() {
    let aggregator = CountAggregator::new();
    let mut operator = SessionWindowOperator::new(
        Duration::from_millis(1000), // gap
        aggregator,
        Duration::from_millis(0),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Create first session
    let event1 = create_test_event(100, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }

    // Create second session (after gap)
    let event2 = create_test_event(5000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx);
    }

    // Verify two sessions in index
    let key_hash = SessionWindowOperator::<CountAggregator>::key_hash(&[]);
    let index = operator.load_session_index(key_hash, &state);
    assert_eq!(index.len(), 2);

    // Verify each session has its own accumulator
    let acc1 = operator.load_accumulator(index.sessions[0].id, &state);
    let acc2 = operator.load_accumulator(index.sessions[1].id, &state);
    assert_eq!(acc1.result(), 1);
    assert_eq!(acc2.result(), 1);
}
```

## Completion Checklist

- [ ] `SessionId` type implemented
- [ ] `SessionMetadata` struct with overlap detection
- [ ] `SessionIndex` with sorted sessions
- [ ] State key generation (session index, per-session accumulator)
- [ ] Load/store session index from StateStore
- [ ] Load/store per-session accumulators
- [ ] Update `SessionWindowOperator` to use new structures
- [ ] Legacy state migration (or documented upgrade path)
- [ ] Unit tests (8+ tests)
- [ ] Integration test: multi-session storage
- [ ] Documentation updates

## Dependencies

- Requires F017 (base session window operator)
- Blocks F017C (session merging - needs multi-session support)

## Success Criteria

- Multiple sessions per key can be stored in state
- Session index survives operator drop/recreate (persistent)
- All existing F017 tests still pass
- New tests verify multi-session scenarios

## Notes

- This feature does NOT implement session merging logic - that's F017C
- It ONLY changes state layout to support multiple sessions
- Backward compatibility: Users should drain all sessions before upgrading, OR implement migration logic
