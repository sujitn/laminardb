# F022: Incremental Checkpointing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F022 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F008 |
| **Owner** | TBD |

## Summary

Only checkpoint changed state since the last checkpoint. Incremental checkpointing reduces checkpoint time and storage for large state.

## Goals

- Track dirty pages/keys
- Delta checkpoints
- Compaction of checkpoint chain
- Fast recovery from incrementals

## Technical Design

```rust
pub struct IncrementalCheckpoint {
    base_id: u64,
    delta_id: u64,
    changed_keys: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl CheckpointManager {
    pub fn create_incremental(&self) -> Result<IncrementalCheckpoint> {
        let changed = self.state.get_dirty_keys();
        // Only write changed keys
    }
    
    pub fn compact(&self, base: u64, through: u64) -> Result<Checkpoint> {
        // Merge incrementals into new base
    }
}
```

## Completion Checklist

- [ ] Delta tracking working
- [ ] Compaction implemented
- [ ] Recovery from incrementals tested
- [ ] Storage reduction verified
