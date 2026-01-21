# F013: Thread-Per-Core Architecture

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F013 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | XL (2-3 weeks) |
| **Dependencies** | F001 |
| **Owner** | TBD |

## Summary

Implement thread-per-core architecture where each CPU core runs a dedicated reactor with its own state partition. This enables linear scaling and eliminates lock contention.

## Goals

- One reactor thread per CPU core
- Partition state by key across cores
- Lock-free inter-core communication
- CPU pinning for cache efficiency

## Technical Design

```rust
pub struct ThreadPerCoreRuntime {
    cores: Vec<CoreHandle>,
    router: KeyRouter,
}

pub struct CoreHandle {
    reactor: Reactor,
    inbox: SpscReceiver<Event>,
    outbox: SpscSender<Output>,
    core_id: usize,
}

impl ThreadPerCoreRuntime {
    pub fn new(num_cores: usize) -> Self;
    pub fn submit(&self, event: Event);
    pub fn run(&mut self);
}
```

## Benchmarks

- [ ] Linear scaling with cores (>80% efficiency)
- [ ] No lock contention (0 mutex waits)
- [ ] < 1μs inter-core latency

## Completion Checklist

- [ ] Multi-reactor working
- [ ] Key partitioning correct
- [ ] Scaling benchmarks passing
- [ ] CPU pinning verified
