# F-SUB-006: Callback Subscriptions

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-006 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | S (1-3 days) |
| **Dependencies** | F-SUB-005 (Push Subscription API) |
| **Created** | 2026-02-01 |

## Summary

Provides a callback-based subscription API where users register a callback function/trait object that is invoked on every change event. This is the lowest-latency consumption pattern as it avoids channel overhead. Internally wraps the channel-based `PushSubscription` (F-SUB-005) and spawns a tokio task that calls the callback for each received event.

**Research Reference**: [Reactive Subscriptions Research - Option A: Callback-based](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: `SubscriptionCallback` trait with `on_change`, `on_error`, `on_complete`
- **FR-2**: `subscribe_with_callback()` API on Pipeline
- **FR-3**: Closure-based shorthand: `subscribe_fn(query, |event| { ... })`
- **FR-4**: Callback runs on a dedicated tokio task (not on dispatcher thread)
- **FR-5**: Subscription handle for lifecycle management (pause/resume/cancel)
- **FR-6**: Error in callback does not crash the dispatcher

### Non-Functional Requirements

- **NFR-1**: Callback invocation within 2us of event broadcast
- **NFR-2**: Callback panic is caught and converted to `on_error`
- **NFR-3**: No additional channel allocation (wraps existing broadcast channel)

## Technical Design

### Data Structures

```rust
use std::sync::Arc;
use arrow::array::RecordBatch;

/// Callback trait for push-based subscriptions.
///
/// Implement this trait to receive change events via callback.
///
/// # Example
///
/// ```rust,ignore
/// struct MyHandler;
///
/// impl SubscriptionCallback for MyHandler {
///     fn on_change(&self, event: ChangeEvent) {
///         match event {
///             ChangeEvent::Insert { data, .. } => println!("Insert: {} rows", data.num_rows()),
///             ChangeEvent::Delete { data, .. } => println!("Delete: {} rows", data.num_rows()),
///             _ => {}
///         }
///     }
///
///     fn on_error(&self, error: PushSubscriptionError) {
///         eprintln!("Subscription error: {}", error);
///     }
///
///     fn on_complete(&self) {
///         println!("Subscription closed");
///     }
/// }
/// ```
pub trait SubscriptionCallback: Send + Sync + 'static {
    /// Called for each change event.
    fn on_change(&self, event: ChangeEvent);

    /// Called when an error occurs.
    ///
    /// Default implementation logs the error. Override for custom handling.
    fn on_error(&self, error: PushSubscriptionError) {
        tracing::warn!("Subscription error: {}", error);
    }

    /// Called when the subscription is closed (source dropped or cancelled).
    ///
    /// Default implementation is a no-op.
    fn on_complete(&self) {}
}

/// Handle for a callback-based subscription.
///
/// Provides lifecycle management for the callback subscription.
/// Dropping this handle cancels the subscription and stops the callback task.
///
/// The handle and the callback task share the same `SubscriptionEntry` in
/// the registry (via `SubscriptionId`), so `pause()` / `cancel()` on the
/// handle directly affects the task's event delivery.
pub struct CallbackSubscriptionHandle {
    /// Subscription ID (shared with the task).
    id: SubscriptionId,
    /// Registry reference for lifecycle management.
    registry: Arc<SubscriptionRegistry>,
    /// Task handle for the callback runner.
    task: tokio::task::JoinHandle<()>,
    /// Cancellation token for clean task shutdown.
    cancel_token: tokio_util::sync::CancellationToken,
}

impl CallbackSubscriptionHandle {
    /// Pauses the subscription (events buffered/dropped per config).
    pub fn pause(&self) -> bool {
        self.registry.pause(self.id)
    }

    /// Resumes the subscription.
    pub fn resume(&self) -> bool {
        self.registry.resume(self.id)
    }

    /// Cancels the subscription and stops the callback task.
    pub fn cancel(self) {
        self.cancel_token.cancel();
        self.registry.cancel(self.id);
    }

    /// Returns the subscription ID.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns subscription metrics.
    pub fn metrics(&self) -> Option<SubscriptionMetrics> {
        self.registry.metrics(self.id)
    }
}

impl Drop for CallbackSubscriptionHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.registry.cancel(self.id);
    }
}
```

### Pipeline API Extension

```rust
impl Pipeline {
    /// Creates a callback-based subscription.
    ///
    /// The callback is invoked for each change event on a dedicated
    /// tokio task. The callback must be `Send + Sync + 'static`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let handle = pipeline.subscribe_with_callback(
    ///     "SELECT * FROM trades",
    ///     MyTradeHandler::new(),
    /// )?;
    ///
    /// // Later...
    /// handle.cancel();
    /// ```
    pub fn subscribe_with_callback<C: SubscriptionCallback>(
        &self,
        query: &str,
        callback: C,
    ) -> Result<CallbackSubscriptionHandle, PushSubscriptionError> {
        // Create a single subscription — the handle shares it via the
        // registry (using the subscription ID), NOT a second channel.
        let sub_id = {
            let sub = self.subscribe_push(query)?;
            sub.id()
        };

        // Re-register the same subscription ID for the task receiver.
        // The registry returns a new broadcast::Receiver on the same
        // underlying Sender, so pause/cancel on the handle affects
        // the task's subscription (they share one SubscriptionEntry).
        let (task_receiver, handle_receiver) = self.registry
            .create_callback_pair(sub_id)?;

        let callback = Arc::new(callback);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let cancel_child = cancel_token.child_token();

        let task = tokio::spawn({
            let callback = Arc::clone(&callback);
            let mut receiver = task_receiver;
            async move {
                loop {
                    tokio::select! {
                        result = receiver.recv() => {
                            match result {
                                Ok(event) => {
                                    // Catch panics in user callback
                                    let cb = Arc::clone(&callback);
                                    let result = std::panic::catch_unwind(
                                        std::panic::AssertUnwindSafe(|| cb.on_change(event))
                                    );
                                    if let Err(panic) = result {
                                        let msg = format!("callback panicked: {:?}", panic);
                                        callback.on_error(
                                            PushSubscriptionError::Internal(msg)
                                        );
                                    }
                                }
                                Err(PushSubscriptionError::Lagged(n)) => {
                                    callback.on_error(PushSubscriptionError::Lagged(n));
                                    // Continue receiving after lag
                                }
                                Err(e) => {
                                    callback.on_error(e);
                                    callback.on_complete();
                                    break;
                                }
                            }
                        }
                        _ = cancel_child.cancelled() => {
                            callback.on_complete();
                            break;
                        }
                    }
                }
            }
        });

        Ok(CallbackSubscriptionHandle {
            id: sub_id,
            registry: Arc::clone(&self.registry),
            task,
            cancel_token,
        })
    }

    /// Creates a closure-based subscription (convenience method).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let handle = pipeline.subscribe_fn("SELECT * FROM trades", |event| {
    ///     println!("Got event: {:?}", event.event_type());
    /// })?;
    /// ```
    pub fn subscribe_fn<F>(
        &self,
        query: &str,
        f: F,
    ) -> Result<CallbackSubscriptionHandle, PushSubscriptionError>
    where
        F: Fn(ChangeEvent) + Send + Sync + 'static,
    {
        struct FnCallback<F>(F);
        impl<F: Fn(ChangeEvent) + Send + Sync + 'static> SubscriptionCallback for FnCallback<F> {
            fn on_change(&self, event: ChangeEvent) {
                (self.0)(event);
            }
        }

        self.subscribe_with_callback(query, FnCallback(f))
    }
}
```

> **Design Note (Fixed)**: The original design created two independent
> `PushSubscription`s — one moved into the task, one for the handle. This
> meant `handle.pause()` / `handle.cancel()` controlled a different
> subscription than the one actually receiving events. The corrected design
> uses a shared `SubscriptionEntry` in the registry: the task gets a
> broadcast `Receiver`, and the handle controls the same entry via its
> `SubscriptionId`. A `CancellationToken` provides clean task shutdown.

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| Pipeline | `laminar-core/src/subscription/handle.rs` | Add callback methods |
| PushSubscription | `laminar-core/src/subscription/handle.rs` | Used internally |

### New Files

- `crates/laminar-core/src/subscription/callback.rs` - SubscriptionCallback trait, CallbackSubscriptionHandle

## Test Plan

### Unit Tests

- [ ] `test_callback_receives_events` - Events delivered to callback
- [ ] `test_callback_on_error` - Error callback invoked on lag
- [ ] `test_callback_on_complete` - Complete callback on close
- [ ] `test_callback_panic_caught` - Panic in callback -> on_error
- [ ] `test_callback_handle_pause_resume` - Lifecycle through handle
- [ ] `test_callback_handle_cancel` - Cancel stops task
- [ ] `test_callback_handle_drop_cancels` - Drop auto-cancels
- [ ] `test_subscribe_fn` - Closure shorthand
- [ ] `test_callback_ordering` - Events delivered in order

### Integration Tests

- [ ] `test_callback_end_to_end` - Insert -> callback invoked

### Benchmarks

- [ ] `bench_callback_latency` - Target: < 2us from event send to callback
- [ ] `bench_callback_throughput` - Target: > 2M callbacks/sec

## Completion Checklist

- [ ] SubscriptionCallback trait defined
- [ ] CallbackSubscriptionHandle with lifecycle management
- [ ] subscribe_with_callback() on Pipeline
- [ ] subscribe_fn() convenience method
- [ ] Panic catching in callback runner
- [ ] Drop-based cleanup
- [ ] Unit tests passing (9+ tests)
- [ ] Benchmarks meeting targets
- [ ] Documentation with examples
- [ ] Code reviewed

## Advanced Features (from Research)

### Async Callback Variant

The base `SubscriptionCallback` trait has synchronous `on_change`. For async
processing (e.g., writing to a database, HTTP calls), users must spawn their
own tasks or use internal channels. An async variant provides native support:

```rust
/// Async callback trait for subscriptions requiring async processing.
///
/// The callback task awaits each `on_change` before processing the next
/// event, providing natural backpressure. If the callback is slower than
/// the event rate, the broadcast channel absorbs the difference (up to
/// buffer capacity, then backpressure strategy applies).
///
/// # Example
///
/// ```rust,ignore
/// struct AsyncDbWriter { pool: PgPool }
///
/// #[async_trait]
/// impl AsyncSubscriptionCallback for AsyncDbWriter {
///     async fn on_change(&self, event: ChangeEvent) {
///         sqlx::query("INSERT INTO audit ...")
///             .execute(&self.pool).await.unwrap();
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait AsyncSubscriptionCallback: Send + Sync + 'static {
    /// Called for each change event (async).
    async fn on_change(&self, event: ChangeEvent);

    /// Called when an error occurs.
    async fn on_error(&self, error: PushSubscriptionError) {
        tracing::warn!("Subscription error: {}", error);
    }

    /// Called when the subscription is closed.
    async fn on_complete(&self) {}
}

impl Pipeline {
    /// Creates an async callback-based subscription.
    pub fn subscribe_with_async_callback<C: AsyncSubscriptionCallback>(
        &self,
        query: &str,
        callback: C,
    ) -> Result<CallbackSubscriptionHandle, PushSubscriptionError> {
        // Similar to subscribe_with_callback but the task loop uses
        // callback.on_change(event).await instead of sync invocation.
        // Panic catching uses tokio::task::spawn + JoinHandle for safety.
        // ...
    }
}
```

### Demand-Based Flow Control

The Reactive Streams specification (research Section 2) uses `request(n)` for
demand signaling. While LaminarDB's default is buffer-based backpressure
(F-SUB-008), callbacks can optionally participate in demand-based flow:

```rust
/// Extended callback trait with demand signaling.
///
/// The callback must call `demand.request(n)` to receive the next N events.
/// The dispatcher will not push more events until demand is available.
/// This implements the core Reactive Streams contract.
pub trait DemandCallback: Send + Sync + 'static {
    /// Called with a demand handle that the callback must use to request events.
    fn on_subscribe(&self, demand: DemandHandle);

    /// Called for each event (only when demand > 0).
    fn on_change(&self, event: ChangeEvent);

    fn on_error(&self, error: PushSubscriptionError) {}
    fn on_complete(&self) {}
}

/// Handle for requesting demand.
pub struct DemandHandle {
    pending: Arc<AtomicU64>,
}

impl DemandHandle {
    /// Request `n` more events.
    pub fn request(&self, n: u64) {
        self.pending.fetch_add(n, Ordering::Release);
    }
}
```

This is a **Phase 5 advanced feature** per the research. The standard callback
API (with buffer-based backpressure from F-SUB-008) covers most use cases.

### Batched Callback

For high-throughput scenarios where per-event callback overhead is significant,
a batched variant reduces invocation frequency (cross-reference F-SUB-008):

```rust
/// Batched callback trait — receives groups of events.
///
/// The dispatcher accumulates events and delivers batches per the
/// `BatchConfig` (max_batch_size / max_batch_delay from F-SUB-008).
/// Reduces per-event overhead by 5-10x for high-throughput sources.
pub trait BatchedSubscriptionCallback: Send + Sync + 'static {
    /// Called with a batch of events.
    fn on_batch(&self, batch: ChangeEventBatch);

    fn on_error(&self, error: PushSubscriptionError) {
        tracing::warn!("Subscription error: {}", error);
    }

    fn on_complete(&self) {}
}
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio-util` | `CancellationToken` for clean task shutdown |
| `async-trait` | `AsyncSubscriptionCallback` (optional) |

## References

- [F-SUB-005: Push Subscription API](F-SUB-005-push-subscription-api.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [Observer Pattern in Rust](https://refactoring.guru/design-patterns/observer/rust/example)
- [Reactive Streams Specification](https://www.reactive-streams.org/)
