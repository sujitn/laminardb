---
paths:
  - "crates/laminar-core/**/*.rs"
  - "crates/laminar-sql/**/*.rs"
---

# Performance Requirements

## Ring 0 (Hot Path) Rules

Code in the hot path MUST:

### 1. Zero Allocations

```rust
// BAD: Allocates on every call
fn process(&self, event: &Event) -> Vec<u8> {
    event.payload.to_vec()  // Heap allocation!
}

// GOOD: Returns reference or uses pre-allocated buffer
fn process<'a>(&'a self, event: &'a Event) -> &'a [u8] {
    &event.payload
}

// GOOD: Uses arena allocator
fn process(&self, event: &Event, arena: &Arena) -> &[u8] {
    arena.alloc_slice_copy(&event.payload)
}
```

### 2. No Locks

```rust
// BAD: Mutex on hot path
fn get(&self, key: &[u8]) -> Option<&[u8]> {
    let guard = self.data.lock().unwrap();  // Blocks!
    guard.get(key).cloned()
}

// GOOD: Lock-free data structure
fn get(&self, key: &[u8]) -> Option<&[u8]> {
    self.data.get(key)  // Lock-free HashMap
}

// GOOD: SPSC queue for cross-thread communication
fn send(&self, event: Event) {
    self.queue.push(event);  // Lock-free SPSC
}
```

### 3. Predictable Branching

```rust
// BAD: Unpredictable branch
fn process(&self, event: &Event) {
    if event.is_rare_condition() {  // Mispredicted 50% of the time
        self.handle_rare(event);
    } else {
        self.handle_common(event);
    }
}

// GOOD: Likely/unlikely hints
fn process(&self, event: &Event) {
    if std::intrinsics::likely(event.is_common()) {
        self.handle_common(event);
    } else {
        self.handle_rare(event);
    }
}
```

### 4. Cache-Friendly Access

```rust
// BAD: Random access pattern
fn sum_values(&self, indices: &[usize]) -> i64 {
    indices.iter().map(|&i| self.data[i]).sum()
}

// GOOD: Sequential access
fn sum_values(&self) -> i64 {
    self.data.iter().sum()
}

// GOOD: Struct of arrays for better cache utilization
struct Events {
    timestamps: Vec<u64>,
    values: Vec<i64>,
    // Instead of Vec<Event { timestamp, value }>
}
```

## Unsafe Code Requirements

All `unsafe` blocks MUST have a `// SAFETY:` comment explaining:

1. Why unsafe is necessary
2. What invariants are being upheld
3. Why the code is actually safe

```rust
// SAFETY: We know the buffer has at least 8 bytes because we checked
// the length above. The pointer is valid for the lifetime of the slice.
unsafe {
    let value = std::ptr::read_unaligned(buffer.as_ptr() as *const u64);
}
```

## Benchmark Requirements

All hot path code MUST have benchmarks:

```rust
#[bench]
fn bench_state_lookup(b: &mut Bencher) {
    let store = create_test_store();
    b.iter(|| {
        black_box(store.get(b"key"))
    });
}
```

Target latencies:
- State lookup: < 500ns
- Event processing: < 1μs
- Batch emit: < 10μs

## Profiling Checklist

Before merging hot path changes:

- [ ] Run `cargo bench` - no regression > 5%
- [ ] Check with `perf stat` - IPC > 2.0
- [ ] Verify with `perf record` - no unexpected hot spots
- [ ] Memory profiling - no allocations in hot path
