# F008: Basic Checkpointing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F008 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F007 |
| **Owner** | TBD |

## Summary

Implement periodic checkpointing of operator state. Checkpoints capture a consistent snapshot of all state, enabling fast recovery without replaying the entire WAL.

## Goals

- Periodic state snapshots
- Atomic checkpoint commits
- Recovery to latest checkpoint
- Cleanup of old checkpoints

## Technical Design

```rust
pub struct CheckpointManager {
    interval: Duration,
    checkpoint_dir: PathBuf,
    max_retained: usize,
}

pub struct Checkpoint {
    pub id: u64,
    pub timestamp: u64,
    pub wal_position: u64,
    pub source_offsets: HashMap<String, u64>,
    pub state_path: PathBuf,
}

impl CheckpointManager {
    pub async fn create_checkpoint(&self, state: &StateStore) -> Result<Checkpoint>;
    pub fn latest_checkpoint(&self) -> Option<Checkpoint>;
    pub fn cleanup_old(&self, keep: usize) -> Result<()>;
}
```

## Completion Checklist

- [ ] Checkpoint creation working
- [ ] Recovery from checkpoint tested
- [ ] Cleanup implemented
- [ ] Integration tests passing
