---
paths:
  - "**/tests/**/*.rs"
  - "**/*_test.rs"
  - "**/benches/**/*.rs"
---

# Testing Conventions

## Test Organization

```
crates/laminar-core/
├── src/
│   ├── state/
│   │   ├── mod.rs
│   │   └── tests.rs      # Unit tests (private API)
│   └── lib.rs
├── tests/
│   └── integration.rs    # Integration tests (public API)
└── benches/
    └── state_bench.rs    # Benchmarks
```

## Unit Tests

- Place in `tests.rs` submodule or `#[cfg(test)]` module
- Test one behavior per test function
- Use descriptive test names: `test_{function}_{scenario}_{expected}`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_existing_key_returns_value() {
        let store = StateStore::new();
        store.put(b"key", b"value");
        assert_eq!(store.get(b"key"), Some(b"value".as_slice()));
    }

    #[test]
    fn test_get_missing_key_returns_none() {
        let store = StateStore::new();
        assert_eq!(store.get(b"missing"), None);
    }
}
```

## Integration Tests

- Test public API behavior end-to-end
- Use realistic data sizes and patterns
- Test failure modes and recovery

## Property-Based Testing

Use `proptest` for:
- Serialization roundtrips
- State machine invariants
- Edge cases discovery

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn roundtrip_serialization(value: WindowState) {
        let bytes = value.serialize();
        let decoded = WindowState::deserialize(&bytes)?;
        prop_assert_eq!(value, decoded);
    }
}
```

## Benchmarks

Use `criterion` with:
- Statistical rigor (default iterations)
- Baseline comparisons
- Multiple input sizes

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_state_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_lookup");
    
    for size in [100, 1_000, 10_000, 100_000] {
        let store = create_store_with_entries(size);
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b, _| {
                b.iter(|| {
                    black_box(store.get(b"key_500"))
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_state_lookup);
criterion_main!(benches);
```

## Test Fixtures

- Use factory functions for test data
- Place shared fixtures in `tests/common/mod.rs`
- Avoid test interdependencies

```rust
// tests/common/mod.rs
pub fn create_test_event(id: u64) -> Event {
    Event {
        id,
        timestamp: EventTime::now(),
        payload: vec![0u8; 100],
    }
}

pub fn create_test_batch(size: usize) -> Vec<Event> {
    (0..size).map(create_test_event).collect()
}
```

## Mocking

Use `mockall` for trait mocking:

```rust
#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait Storage {
    fn read(&self, key: &[u8]) -> Result<Vec<u8>, StorageError>;
    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
}

#[test]
fn test_cache_miss_reads_from_storage() {
    let mut mock = MockStorage::new();
    mock.expect_read()
        .with(eq(b"key"))
        .times(1)
        .returning(|_| Ok(b"value".to_vec()));
    
    let cache = Cache::new(mock);
    assert_eq!(cache.get(b"key"), Ok(b"value".to_vec()));
}
```

## Test Coverage

Target: >80% line coverage for non-benchmark code

```bash
# Generate coverage report
cargo llvm-cov --html
open target/llvm-cov/html/index.html
```
