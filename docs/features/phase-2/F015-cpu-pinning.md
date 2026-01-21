# F015: CPU Pinning

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F015 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F013 |
| **Owner** | TBD |

## Summary

Pin reactor threads to specific CPU cores for cache efficiency and predictable performance. Includes NUMA-aware allocation.

## Goals

- Pin threads to cores using core_affinity
- NUMA-aware memory allocation
- Real-time scheduling priority (optional)
- Core isolation support

## Technical Design

```rust
pub fn pin_to_core(core_id: usize) -> Result<()> {
    core_affinity::set_for_current(CoreId { id: core_id })
}

pub fn allocate_on_node(node: usize, size: usize) -> *mut u8 {
    // NUMA-aware allocation
}
```

## Completion Checklist

- [ ] Core pinning working
- [ ] NUMA allocation implemented
- [ ] RT priority option
- [ ] Benchmarks show improvement
