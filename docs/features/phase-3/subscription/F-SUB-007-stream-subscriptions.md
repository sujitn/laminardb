# F-SUB-007: Stream Subscriptions (Async Stream)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-007 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | S (1-3 days) |
| **Dependencies** | F-SUB-005 (Push Subscription API) |
| **Created** | 2026-02-01 |

## Summary

Provides a `subscribe_stream()` API that returns an `impl Stream<Item = ChangeEvent>`, enabling idiomatic Rust async stream consumption with combinators like `.filter()`, `.map()`, `.take()`, and `.buffer_unordered()`. Wraps the channel-based `PushSubscription` (F-SUB-005) in a `tokio_stream`-compatible async Stream adapter.

This is the most Rust-idiomatic consumption pattern and integrates naturally with the broader tokio/async ecosystem.

**Research Reference**: [Reactive Subscriptions Research - Option C: Stream-based](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: `subscribe_stream()` returns `impl Stream<Item = ChangeEvent>`
- **FR-2**: Stream terminates when source closes or subscription is cancelled
- **FR-3**: Lagged events are skipped with a warning (continues streaming)
- **FR-4**: Stream is `Unpin` and cancel-safe (works with `select!`)
- **FR-5**: Companion `subscribe_stream_with_errors()` returns `Result<ChangeEvent, _>`
- **FR-6**: Filter/map combinators work: `stream.filter(|e| e.has_data())`

### Non-Functional Requirements

- **NFR-1**: Stream poll latency < 2us from event broadcast
- **NFR-2**: No additional allocation per event (re-uses broadcast channel)
- **NFR-3**: Pin-safe for use in async contexts
- **NFR-4**: Minimal overhead vs direct channel recv (~50-100ns wrapper cost)

## Technical Design

### Data Structures

> **Implementation Note**: The `tokio_stream::wrappers::BroadcastStream` crate
> already wraps `broadcast::Receiver` as a `Stream`. We use it internally
> rather than implementing `Stream::poll_next` manually, which avoids the
> **busy-spin bug** that occurs when calling `cx.waker().wake_by_ref()` in a
> `Pending` branch (this causes 100% CPU usage because the waker is
> immediately re-invoked without any actual readiness signal).

```rust
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;

/// Async stream wrapper for push subscriptions.
///
/// Implements `Stream<Item = ChangeEvent>`, silently skipping lagged events.
/// Uses `BroadcastStream` internally for correct async wakeup semantics.
///
/// # Usage
///
/// ```rust,ignore
/// use tokio_stream::StreamExt;
///
/// let mut stream = pipeline.subscribe_stream("SELECT * FROM trades")?;
///
/// // Idiomatic stream consumption
/// while let Some(event) = stream.next().await {
///     match event {
///         ChangeEvent::Insert { data, .. } => process(data),
///         ChangeEvent::Watermark { timestamp } => advance(timestamp),
///         _ => {}
///     }
/// }
///
/// // With combinators
/// let inserts = pipeline.subscribe_stream("SELECT * FROM trades")?
///     .filter(|e| e.event_type() == EventType::Insert)
///     .take(100);
///
/// tokio::pin!(inserts);
/// while let Some(event) = inserts.next().await {
///     process(event);
/// }
/// ```
#[pin_project::pin_project]
pub struct ChangeEventStream {
    /// Subscription ID for lifecycle management.
    id: SubscriptionId,
    /// Registry reference for pause/resume/cancel.
    registry: Arc<SubscriptionRegistry>,
    /// Query string for diagnostics.
    query: String,
    /// Inner BroadcastStream that handles proper async wakeup.
    #[pin]
    inner: BroadcastStream<ChangeEvent>,
    /// Whether the stream has terminated.
    terminated: bool,
}

impl ChangeEventStream {
    /// Creates a new stream from a push subscription.
    pub(crate) fn new(sub: PushSubscription) -> Self {
        let id = sub.id();
        let registry = sub.registry_clone();
        let query = sub.query().to_string();
        // Extract the broadcast receiver and wrap in BroadcastStream.
        let receiver = sub.into_receiver();
        Self {
            id,
            registry,
            query,
            inner: BroadcastStream::new(receiver),
            terminated: false,
        }
    }

    /// Returns the subscription ID.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns the query string.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Pauses the underlying subscription.
    pub fn pause(&self) -> bool {
        self.registry.pause(self.id)
    }

    /// Resumes the underlying subscription.
    pub fn resume(&self) -> bool {
        self.registry.resume(self.id)
    }

    /// Cancels the underlying subscription.
    pub fn cancel(self: Pin<&mut Self>) {
        let this = self.project();
        *this.terminated = true;
        this.registry.cancel(*this.id);
    }
}

impl Stream for ChangeEventStream {
    type Item = ChangeEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.terminated {
            return Poll::Ready(None);
        }

        // Delegate to BroadcastStream which handles async wakeup correctly.
        // BroadcastStream yields Result<T, BroadcastStreamRecvError>.
        // We silently skip lagged errors and terminate on closed.
        loop {
            match this.inner.poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => return Poll::Ready(Some(event)),
                Poll::Ready(Some(Err(_lagged))) => {
                    // BroadcastStreamRecvError::Lagged — skip and try again.
                    tracing::debug!("Stream subscription lagged, skipping missed events");
                    continue;
                }
                Poll::Ready(None) => {
                    *this.terminated = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for ChangeEventStream {
    fn drop(&mut self) {
        if !self.terminated {
            self.registry.cancel(self.id);
        }
    }
}

/// Stream wrapper that also yields errors.
///
/// Use this when you need to handle lagged/error conditions explicitly
/// rather than silently skipping them.
#[pin_project::pin_project]
pub struct ChangeEventResultStream {
    /// Subscription ID.
    id: SubscriptionId,
    /// Registry reference.
    registry: Arc<SubscriptionRegistry>,
    /// Inner BroadcastStream.
    #[pin]
    inner: BroadcastStream<ChangeEvent>,
    /// Whether the stream has terminated.
    terminated: bool,
}

impl ChangeEventResultStream {
    pub(crate) fn new(sub: PushSubscription) -> Self {
        let id = sub.id();
        let registry = sub.registry_clone();
        let receiver = sub.into_receiver();
        Self {
            id,
            registry,
            inner: BroadcastStream::new(receiver),
            terminated: false,
        }
    }
}

impl Stream for ChangeEventResultStream {
    type Item = Result<ChangeEvent, PushSubscriptionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.terminated {
            return Poll::Ready(None);
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(Some(Err(lagged))) => {
                Poll::Ready(Some(Err(PushSubscriptionError::Lagged(lagged.0 as u64))))
            }
            Poll::Ready(None) => {
                *this.terminated = true;
                Poll::Ready(None);
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ChangeEventResultStream {
    fn drop(&mut self) {
        if !self.terminated {
            self.registry.cancel(self.id);
        }
    }
}
```

> **Why not manual `poll_next` with `try_recv`?**
>
> The original design used `self.inner.receiver.try_recv()` with
> `cx.waker().wake_by_ref()` in the `Empty` branch. This creates a
> **busy-spin loop**: the waker fires immediately, `poll_next` is called
> again, `try_recv` returns `Empty` again, and the cycle repeats at 100%
> CPU. `BroadcastStream` correctly integrates with tokio's async machinery
> — it only wakes the task when new data is actually available.

### Pipeline API Extension

```rust
impl Pipeline {
    /// Creates an async Stream subscription.
    ///
    /// Returns a `Stream<Item = ChangeEvent>` that yields change events.
    /// Lagged events are silently skipped; the stream terminates when
    /// the source is closed.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = pipeline.subscribe_stream("ohlc_1s")?;
    ///
    /// while let Some(event) = stream.next().await {
    ///     process(event);
    /// }
    /// ```
    pub fn subscribe_stream(
        &self,
        query: &str,
    ) -> Result<ChangeEventStream, PushSubscriptionError> {
        let sub = self.subscribe_push(query)?;
        Ok(ChangeEventStream::new(sub))
    }

    /// Creates an async Stream that also yields errors.
    ///
    /// Use this when you need to handle lagged events explicitly.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = pipeline.subscribe_stream_with_errors("trades")?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(event) => process(event),
    ///         Err(PushSubscriptionError::Lagged(n)) => {
    ///             eprintln!("Missed {} events", n);
    ///         }
    ///         Err(e) => break,
    ///     }
    /// }
    /// ```
    pub fn subscribe_stream_with_errors(
        &self,
        query: &str,
    ) -> Result<ChangeEventResultStream, PushSubscriptionError> {
        let sub = self.subscribe_push(query)?;
        Ok(ChangeEventResultStream::new(sub))
    }
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| Pipeline | `laminar-core/src/subscription/handle.rs` | Add stream methods |
| PushSubscription | `laminar-core/src/subscription/handle.rs` | Expose receiver field |

### New Files

- `crates/laminar-core/src/subscription/stream.rs` - ChangeEventStream, ChangeEventResultStream

### Dependencies

- `tokio-stream` crate (add to Cargo.toml if not present)
- `pin-project-lite` for safe pinning (optional)

## Test Plan

### Unit Tests

- [ ] `test_stream_receives_events` - Basic stream consumption
- [ ] `test_stream_terminates_on_close` - Stream ends when source closes
- [ ] `test_stream_cancel` - Cancel stops stream
- [ ] `test_stream_drop_cancels` - Drop auto-cancels
- [ ] `test_stream_filter_combinator` - .filter() works
- [ ] `test_stream_map_combinator` - .map() works
- [ ] `test_stream_take_combinator` - .take(n) works
- [ ] `test_stream_with_select` - Works with tokio::select!
- [ ] `test_result_stream_yields_errors` - Lagged error surfaces
- [ ] `test_result_stream_terminates_on_close` - Clean termination
- [ ] `test_stream_pause_resume` - Lifecycle through stream handle

### Integration Tests

- [ ] `test_stream_end_to_end` - Push -> stream consumption
- [ ] `test_stream_multiple_consumers` - Multiple streams same query

### Benchmarks

- [ ] `bench_stream_next_latency` - Target: < 2us
- [ ] `bench_stream_throughput` - Target: > 3M events/sec

## Completion Checklist

- [ ] ChangeEventStream implementing Stream<Item = ChangeEvent>
- [ ] ChangeEventResultStream implementing Stream<Item = Result<...>>
- [ ] Pipeline::subscribe_stream() entry point
- [ ] Pipeline::subscribe_stream_with_errors() entry point
- [ ] Pin-safe and cancel-safe
- [ ] Works with tokio_stream combinators
- [ ] Drop-based cleanup
- [ ] Unit tests passing (11+ tests)
- [ ] Benchmarks meeting targets
- [ ] Documentation with combinator examples
- [ ] Code reviewed

## Advanced Features (from Research)

### Subscription Composition (Merge Streams)

The research identifies "Subscription composition (combine multiple queries)" as
a Phase 5 feature. The Stream API is the natural integration point since
`tokio_stream` already provides merge/select combinators:

```rust
use tokio_stream::StreamExt;

// Merge multiple subscription streams into one
let trades = pipeline.subscribe_stream("trades")?;
let quotes = pipeline.subscribe_stream("quotes")?;

// Option 1: tokio_stream merge (interleaves events by readiness)
let merged = tokio_stream::StreamExt::merge(trades, quotes);
tokio::pin!(merged);
while let Some(event) = merged.next().await {
    process(event);
}

// Option 2: select! for prioritized consumption
tokio::pin!(trades);
tokio::pin!(quotes);
loop {
    tokio::select! {
        Some(event) = trades.next() => handle_trade(event),
        Some(event) = quotes.next() => handle_quote(event),
        else => break,
    }
}
```

A dedicated `Pipeline::subscribe_composed()` API could provide optimized
multi-source subscription with a single registry entry:

```rust
impl Pipeline {
    /// Subscribes to multiple sources as a single merged stream.
    ///
    /// More efficient than merging individual streams because it uses
    /// a single notification slot and dispatcher route.
    pub fn subscribe_composed(
        &self,
        queries: &[&str],
    ) -> Result<ChangeEventStream, PushSubscriptionError> {
        // ...
    }
}
```

### Alternative: async_stream Macro

For simpler internal implementation, the `async_stream` crate provides a
macro-based approach that avoids manual Stream trait implementation:

```rust
use async_stream::stream;

fn make_stream(
    mut sub: PushSubscription,
) -> impl Stream<Item = ChangeEvent> {
    stream! {
        loop {
            match sub.recv().await {
                Ok(event) => yield event,
                Err(PushSubscriptionError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    }
}
```

The `BroadcastStream` wrapper is preferred for the primary implementation
because it avoids the `async_stream` dependency and has lower overhead.
However, `async_stream` may be useful for testing or specialized stream
adapters.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio-stream` | `BroadcastStream` wrapper, `StreamExt` combinators |
| `pin-project` | Safe pin projection for `ChangeEventStream` / `ChangeEventResultStream` |
| `async-stream` | Optional: macro-based stream construction for tests |

## References

- [F-SUB-005: Push Subscription API](F-SUB-005-push-subscription-api.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [tokio-stream docs](https://docs.rs/tokio-stream/)
- [BroadcastStream docs](https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.BroadcastStream.html)
- [pin-project docs](https://docs.rs/pin-project/)
- [async-stream crate](https://docs.rs/async-stream/)
