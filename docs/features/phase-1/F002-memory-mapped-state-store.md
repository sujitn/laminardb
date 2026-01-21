# F002: Memory-Mapped State Store

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F002 |
| **Status** | 📝 Draft |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F001 |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-20 |

## Summary

A high-performance key-value state store implementation using memory-mapped files for persistence and FxHashMap for fast lookups. This provides the primary state backend for streaming operators, optimized for sub-microsecond access times.

## Goals

- Achieve < 500ns p99 lookup latency
- Support both in-memory and persistent modes
- Provide efficient prefix scanning for window state
- Enable zero-copy reads where possible

## Non-Goals

- Distributed state (single-node only)
- Transactions across multiple keys
- Secondary indexes

## Technical Design

### Architecture

Located in `crates/laminar-core/src/state/mmap.rs`, implements the `StateStore` trait.

```rust
pub struct MmapStateStore {
    // In-memory hash map for fast lookups
    map: FxHashMap<Vec<u8>, ValueEntry>,
    // Memory-mapped file for persistence (optional)
    mmap: Option<MmapMut>,
    // Arena allocator for values
    arena: Bump,
}

struct ValueEntry {
    offset: usize,  // Offset in mmap or arena
    len: usize,
    version: u64,
}
```

### API/Interface

```rust
impl StateStore for MmapStateStore {
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn prefix_scan(&self, prefix: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])>;
}
```

### Benchmarks

- [ ] `bench_get` - Target: < 500ns p99
- [ ] `bench_put` - Target: < 1μs p99
- [ ] `bench_prefix_scan` - Target: < 10μs for 100 keys

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
