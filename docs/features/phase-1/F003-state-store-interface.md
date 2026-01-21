# F003: State Store Interface

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F003 |
| **Status** | 📝 Draft |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F001 |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-20 |

## Summary

Define the core `StateStore` trait that all state backend implementations must satisfy. This abstraction allows operators to work with any state backend while enabling different implementations for different use cases.

## Technical Design

### API/Interface

```rust
/// Core trait for key-value state storage
pub trait StateStore: Send {
    /// Get a value by key
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    
    /// Store a key-value pair
    fn put(&mut self, key: &[u8], value: &[u8]);
    
    /// Delete a key
    fn delete(&mut self, key: &[u8]);
    
    /// Scan all keys with a given prefix
    fn prefix_scan(&self, prefix: &[u8]) -> Box<dyn Iterator<Item = (&[u8], &[u8])> + '_>;
    
    /// Create a snapshot for checkpointing
    fn snapshot(&self) -> StateSnapshot;
    
    /// Restore from a snapshot
    fn restore(&mut self, snapshot: StateSnapshot);
    
    /// Get approximate size in bytes
    fn size_bytes(&self) -> usize;
}
```

## Completion Checklist

- [ ] Trait defined
- [ ] Documentation complete
- [ ] Example implementation
