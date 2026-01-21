# F014: SPSC Queue Communication

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F014 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F013 |
| **Owner** | TBD |

## Summary

Implement lock-free single-producer single-consumer (SPSC) queues for inter-core communication. SPSC queues enable zero-lock data transfer between reactor threads.

## Goals

- Lock-free bounded queues
- Minimal cache-line bouncing
- Backpressure signaling
- Batch operations for efficiency

## Technical Design

```rust
pub struct SpscQueue<T> {
    buffer: Box<[MaybeUninit<T>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

impl<T> SpscQueue<T> {
    pub fn new(capacity: usize) -> Self;
    pub fn push(&self, item: T) -> Result<(), T>;
    pub fn pop(&self) -> Option<T>;
    pub fn push_batch(&self, items: &mut Vec<T>) -> usize;
    pub fn is_full(&self) -> bool;
}
```

## Benchmarks

- [ ] `bench_push_pop` - Target: < 50ns
- [ ] `bench_batch_push` - Target: < 10ns/item
- [ ] No false sharing verified

## Completion Checklist

- [ ] Lock-free implementation
- [ ] Batch operations working
- [ ] Benchmarks passing
- [ ] Memory ordering correct
