# F-SUB-005: Push Subscription API (Channel-Based)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-005 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SUB-001, F-SUB-003, F-SUB-004 |
| **Blocks** | F-SUB-006 (Callback), F-SUB-007 (Stream) |
| **Created** | 2026-02-01 |

## Summary

The primary push subscription API that returns a `PushSubscription` handle wrapping a `tokio::sync::broadcast::Receiver<ChangeEvent>`. This is the channel-based consumption model - the most flexible and testable of the three API styles. Subscribers call `.recv().await` to receive change events as they're pushed by the dispatcher.

This extends the existing `Sink::subscribe()` API (which returns a poll-based `Subscription<T>`) with a new `subscribe_push()` method that returns a reactive push-based handle.

**Research Reference**: [Reactive Subscriptions Research - Option B: Channel-based](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: `subscribe_push(query)` returns `PushSubscription` with async receiver
- **FR-2**: `PushSubscription::recv()` is async, yields `ChangeEvent`
- **FR-3**: Subscription handle supports `pause()`, `resume()`, `cancel()`
- **FR-4**: Handle lagged receivers gracefully (report count of missed events)
- **FR-5**: Optional initial snapshot delivery on subscribe
- **FR-6**: Subscription persists across dispatcher restarts (re-registration)
- **FR-7**: Drop semantics: dropping `PushSubscription` cancels the subscription

### Non-Functional Requirements

- **NFR-1**: `recv()` wakes within 1us of event broadcast
- **NFR-2**: Zero-copy: `ChangeEvent` uses `Arc<RecordBatch>`, clone is O(1)
- **NFR-3**: Memory bounded: configurable channel buffer size
- **NFR-4**: API ergonomic: works naturally with `while let` and `select!`

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      User Code (Ring 2)                          │
│                                                                 │
│  let sub = pipeline.subscribe_push("SELECT * FROM trades")?;   │
│                                                                 │
│  // Async receive loop                                          │
│  while let Ok(event) = sub.recv().await {                       │
│      match event {                                              │
│          ChangeEvent::Insert { data, .. } => process(data),     │
│          ChangeEvent::Watermark { timestamp } => advance(ts),   │
│          _ => {}                                                │
│      }                                                          │
│  }                                                              │
│                                                                 │
│  // Or with select!                                             │
│  tokio::select! {                                               │
│      event = sub.recv() => handle(event),                       │
│      _ = shutdown.recv() => break,                              │
│  }                                                              │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                  Subscription Infrastructure                     │
│                                                                 │
│  PushSubscription {                                             │
│      id: SubscriptionId,                                        │
│      receiver: broadcast::Receiver<ChangeEvent>,                │
│      registry: Arc<SubscriptionRegistry>,                       │
│  }                                                              │
│                                                                 │
│  Pipeline {                                                     │
│      registry: Arc<SubscriptionRegistry>,                       │
│      notification_hub: Arc<NotificationHub>,                    │
│      // ... existing fields                                     │
│  }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::sync::Arc;
use tokio::sync::broadcast;

/// A push-based subscription handle.
///
/// Receives `ChangeEvent`s as they're pushed by the dispatcher.
/// Dropping this handle automatically cancels the subscription.
///
/// # Usage
///
/// ```rust,ignore
/// let sub = pipeline.subscribe_push("SELECT * FROM trades")?;
///
/// // Async receive
/// while let Ok(event) = sub.recv().await {
///     process(event);
/// }
///
/// // With timeout
/// match tokio::time::timeout(Duration::from_secs(5), sub.recv()).await {
///     Ok(Ok(event)) => process(event),
///     Ok(Err(e)) => handle_error(e),
///     Err(_) => println!("timeout"),
/// }
/// ```
pub struct PushSubscription {
    /// Subscription ID.
    id: SubscriptionId,
    /// Broadcast receiver for change events.
    receiver: broadcast::Receiver<ChangeEvent>,
    /// Registry reference for lifecycle management.
    registry: Arc<SubscriptionRegistry>,
    /// Query that created this subscription.
    query: String,
    /// Whether the subscription is cancelled.
    cancelled: bool,
}

/// Errors from push subscription operations.
#[derive(Debug, thiserror::Error)]
pub enum PushSubscriptionError {
    /// The subscription's source was closed.
    #[error("subscription closed")]
    Closed,
    /// Events were missed due to slow consumption.
    #[error("lagged behind by {0} events")]
    Lagged(u64),
    /// The subscription was cancelled.
    #[error("subscription cancelled")]
    Cancelled,
    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl PushSubscription {
    /// Creates a new push subscription.
    pub(crate) fn new(
        id: SubscriptionId,
        receiver: broadcast::Receiver<ChangeEvent>,
        registry: Arc<SubscriptionRegistry>,
        query: String,
    ) -> Self {
        Self {
            id,
            receiver,
            registry,
            query,
            cancelled: false,
        }
    }

    /// Receives the next change event.
    ///
    /// Awaits until an event is available or the subscription is closed.
    ///
    /// # Errors
    ///
    /// - `PushSubscriptionError::Closed` if the source is dropped
    /// - `PushSubscriptionError::Lagged(n)` if `n` events were missed
    /// - `PushSubscriptionError::Cancelled` if the subscription was cancelled
    pub async fn recv(&mut self) -> Result<ChangeEvent, PushSubscriptionError> {
        if self.cancelled {
            return Err(PushSubscriptionError::Cancelled);
        }

        match self.receiver.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                Err(PushSubscriptionError::Lagged(n))
            }
            Err(broadcast::error::RecvError::Closed) => {
                Err(PushSubscriptionError::Closed)
            }
        }
    }

    /// Tries to receive without blocking.
    ///
    /// Returns `None` if no event is immediately available.
    pub fn try_recv(&mut self) -> Option<Result<ChangeEvent, PushSubscriptionError>> {
        if self.cancelled {
            return Some(Err(PushSubscriptionError::Cancelled));
        }

        match self.receiver.try_recv() {
            Ok(event) => Some(Ok(event)),
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                Some(Err(PushSubscriptionError::Lagged(n)))
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                Some(Err(PushSubscriptionError::Closed))
            }
            Err(broadcast::error::TryRecvError::Empty) => None,
        }
    }

    /// Pauses the subscription.
    ///
    /// While paused, events are either buffered or dropped
    /// depending on the backpressure configuration.
    pub fn pause(&self) -> bool {
        self.registry.pause(self.id)
    }

    /// Resumes a paused subscription.
    pub fn resume(&self) -> bool {
        self.registry.resume(self.id)
    }

    /// Cancels the subscription.
    ///
    /// After cancellation, `recv()` returns `Cancelled`.
    pub fn cancel(&mut self) {
        self.cancelled = true;
        self.registry.cancel(self.id);
    }

    /// Returns the subscription ID.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns the query string for this subscription.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Returns true if the subscription has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// Returns subscription metrics.
    pub fn metrics(&self) -> Option<SubscriptionMetrics> {
        self.registry.metrics(self.id)
    }
}

impl Drop for PushSubscription {
    fn drop(&mut self) {
        if !self.cancelled {
            self.registry.cancel(self.id);
        }
    }
}
```

### Pipeline API Extension

```rust
/// Extension to Pipeline for push-based subscriptions.
impl Pipeline {
    /// Creates a push-based subscription for a query or MV name.
    ///
    /// # Arguments
    ///
    /// * `query` - SQL query string or materialized view name
    ///
    /// # Returns
    ///
    /// A `PushSubscription` handle that receives events via async channel.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let sub = pipeline.subscribe_push(
    ///     "SELECT symbol, price FROM trades WHERE price > 100"
    /// )?;
    ///
    /// while let Ok(event) = sub.recv().await {
    ///     println!("Change: {:?}", event);
    /// }
    /// ```
    pub fn subscribe_push(
        &self,
        query: &str,
    ) -> Result<PushSubscription, PushSubscriptionError> {
        self.subscribe_push_with_config(query, SubscriptionConfig::default())
    }

    /// Creates a push-based subscription with custom configuration.
    pub fn subscribe_push_with_config(
        &self,
        query: &str,
        config: SubscriptionConfig,
    ) -> Result<PushSubscription, PushSubscriptionError> {
        // Resolve query to source_id via MV registry or query planner
        let source_id = self.resolve_source_id(query)
            .map_err(|e| PushSubscriptionError::Internal(e.to_string()))?;

        // Register in subscription registry
        let (id, receiver) = self.registry.create(
            query.to_string(),
            source_id,
            config,
        );

        Ok(PushSubscription::new(
            id,
            receiver,
            Arc::clone(&self.registry),
            query.to_string(),
        ))
    }
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| Streaming Sink | `laminar-core/src/streaming/sink.rs` | Add `subscribe_push()` method |
| Pipeline/Engine | New or `laminar-core/src/pipeline.rs` | Pipeline-level subscribe API |
| MvRegistry | `laminar-core/src/mv/registry.rs` | Query-to-source_id resolution |

### New Files

- `crates/laminar-core/src/subscription/handle.rs` - PushSubscription, PushSubscriptionError

## Test Plan

### Unit Tests

- [ ] `test_push_subscription_recv` - Basic receive
- [ ] `test_push_subscription_try_recv` - Non-blocking receive
- [ ] `test_push_subscription_try_recv_empty` - Empty returns None
- [ ] `test_push_subscription_pause_resume` - State transitions
- [ ] `test_push_subscription_cancel` - Cancel returns error
- [ ] `test_push_subscription_drop_cancels` - Drop triggers cancel
- [ ] `test_push_subscription_lagged` - Lagged error with count
- [ ] `test_push_subscription_closed` - Source dropped
- [ ] `test_push_subscription_id_and_query` - Accessor methods
- [ ] `test_push_subscription_metrics` - Metrics available
- [ ] `test_push_subscription_with_select` - Works with tokio::select!
- [ ] `test_push_subscription_multiple_subscribers` - Multiple subs same query

### Integration Tests

- [ ] `test_end_to_end_push_subscribe` - Insert -> notify -> receive
- [ ] `test_push_subscribe_with_config` - Custom config respected
- [ ] `test_push_subscribe_sql_query` - SQL-based subscription

### Benchmarks

- [ ] `bench_push_subscription_recv_latency` - Target: < 1us from send
- [ ] `bench_push_subscription_throughput` - Target: > 5M events/sec
- [ ] `bench_push_subscription_10_subscribers` - Target: < 2us per event

## Completion Checklist

- [ ] PushSubscription struct with recv/try_recv/pause/resume/cancel
- [ ] PushSubscriptionError enum with thiserror
- [ ] Drop impl that auto-cancels
- [ ] Pipeline::subscribe_push() entry point
- [ ] SubscriptionConfig integration
- [ ] Works with tokio::select!
- [ ] Unit tests passing (12+ tests)
- [ ] Integration tests passing
- [ ] Benchmarks meeting targets
- [ ] Documentation with examples
- [ ] Code reviewed

## Advanced Features (from Research)

### Watch Channel Variant (Latest-Value Semantics)

The research identifies `tokio::sync::watch` as ideal for "current state" subscriptions where
only the latest value matters (e.g., "what is the current portfolio value?"). This is distinct
from the broadcast channel which delivers every event.

```rust
/// A latest-value subscription that only delivers the most recent state.
///
/// Unlike `PushSubscription` (which delivers every event), this only
/// yields when the value changes and always returns the latest state.
///
/// # Usage
///
/// ```rust,ignore
/// let watch = pipeline.subscribe_watch("portfolio_value")?;
///
/// // Only sees latest value, never lags
/// while watch.changed().await.is_ok() {
///     let current = watch.borrow();
///     update_dashboard(current);
/// }
/// ```
pub struct WatchSubscription {
    id: SubscriptionId,
    receiver: watch::Receiver<Option<ChangeEvent>>,
    registry: Arc<SubscriptionRegistry>,
    query: String,
}

impl WatchSubscription {
    /// Waits until the value changes.
    pub async fn changed(&mut self) -> Result<(), PushSubscriptionError> {
        self.receiver.changed().await
            .map_err(|_| PushSubscriptionError::Closed)
    }

    /// Returns a reference to the latest value.
    pub fn borrow(&self) -> watch::Ref<'_, Option<ChangeEvent>> {
        self.receiver.borrow()
    }
}

impl Pipeline {
    /// Creates a watch-based subscription (latest-value only).
    ///
    /// The watch subscription always reflects the most recent state.
    /// It never lags - intermediate values are simply overwritten.
    /// Best for dashboard/monitoring use cases.
    pub fn subscribe_watch(
        &self,
        query: &str,
    ) -> Result<WatchSubscription, PushSubscriptionError> {
        // ...
    }
}
```

**When to use which**:
| Pattern | Channel | Use Case |
|---------|---------|----------|
| `subscribe_push()` | `broadcast` | Every event matters (CDC, audit, analytics) |
| `subscribe_watch()` | `watch` | Only latest state matters (dashboards, monitoring) |

### Subscription Sharing

When multiple consumers subscribe to the same query, they should share a single
broadcast channel rather than creating independent computation pipelines. The
`SubscriptionRegistry` (F-SUB-003) handles this via source_id deduplication:

```rust
// Both subscribers share the same broadcast::Sender
let sub_a = pipeline.subscribe_push("SELECT * FROM trades")?;
let sub_b = pipeline.subscribe_push("SELECT * FROM trades")?;
// Only one notification slot + one broadcast channel is created for this query
```

The registry's `create()` method checks if a broadcast::Sender already exists for
the resolved `source_id`. If so, it calls `tx.subscribe()` to create a new receiver
on the existing channel. This is O(1) and avoids duplicate computation.

### Historical Replay from Checkpoint

Beyond the initial snapshot (FR-5), subscribers may need to replay from a historical
checkpoint position. This enables:
- **Late-joining consumers**: Start from a known epoch, not just "now"
- **Recovery**: Re-subscribe after crash and continue from last committed position
- **Testing**: Replay deterministic event sequences

```rust
/// Options for initial position when subscribing.
#[derive(Debug, Clone)]
pub enum SubscriptionStartPosition {
    /// Start from the current position (default). No historical events.
    Latest,
    /// Deliver a snapshot of current state, then live changes.
    Snapshot,
    /// Replay from a specific epoch (requires WAL retention).
    FromEpoch(u64),
    /// Replay from a specific timestamp (best-effort from WAL).
    FromTimestamp(i64),
}

impl Pipeline {
    pub fn subscribe_push_from(
        &self,
        query: &str,
        start: SubscriptionStartPosition,
    ) -> Result<PushSubscription, PushSubscriptionError> {
        // ...
    }
}
```

**Note**: `FromEpoch` and `FromTimestamp` require WAL retention and are bounded by
the checkpoint truncation policy. If the requested position has been truncated,
returns `PushSubscriptionError::PositionUnavailable`.

### Exactly-Once Delivery

For pipelines requiring exactly-once semantics (e.g., feeding downstream sinks),
subscriptions can participate in the epoch-based commit protocol:

```rust
/// Exactly-once subscription that tracks committed epochs.
///
/// Events are delivered speculatively (at-least-once). The subscriber
/// commits epochs to acknowledge processing. On failure, uncommitted
/// epochs are replayed from the WAL.
pub struct ExactlyOncePushSubscription {
    inner: PushSubscription,
    /// Last committed epoch.
    committed_epoch: u64,
}

impl ExactlyOncePushSubscription {
    /// Commits an epoch, acknowledging all events up to this point.
    pub async fn commit_epoch(&mut self, epoch: u64) -> Result<(), PushSubscriptionError> {
        // Persists epoch to subscription offset store
        // ...
        self.committed_epoch = epoch;
        Ok(())
    }
}
```

This is a **Phase 5 advanced feature** per the research. The base `PushSubscription`
provides at-most-once delivery (lagged events are lost). Exactly-once requires
WAL integration and is deferred to a future feature.

### Subscription Composition (Future)

Combining multiple subscription queries into a single stream is deferred to a
future feature. The natural integration point is F-SUB-007 (Stream API) where
`tokio_stream::StreamExt::merge()` can combine multiple `ChangeEventStream`s:

```rust
// Future API sketch
let trades = pipeline.subscribe_stream("trades")?;
let quotes = pipeline.subscribe_stream("quotes")?;
let merged = tokio_stream::StreamExt::merge(trades, quotes);
```

## References

- [F-STREAM-006: Subscription](../streaming/F-STREAM-006-subscription.md) (existing poll API)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [tokio broadcast channel docs](https://docs.rs/tokio/latest/tokio/sync/broadcast/)
- [tokio watch channel docs](https://docs.rs/tokio/latest/tokio/sync/watch/)
