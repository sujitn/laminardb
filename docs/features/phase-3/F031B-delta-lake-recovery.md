# F031B: Delta Lake Recovery & Exactly-Once I/O

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F031B |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | M (3-4 days) |
| **Dependencies** | F031A (Delta Lake I/O), F023 (Exactly-Once Sinks) |
| **Blocks** | None |
| **Blocked By** | F031A (`deltalake` crate integration) |
| **Created** | 2026-02-02 |

## Summary

Implement crash recovery and exactly-once verification for the Delta Lake Sink. On `open()`, the sink resolves the last committed epoch by scanning `txn` actions in the Delta log, enabling idempotent epoch replay. On `commit_epoch()`, the sink atomically writes a `txn` action that binds each LaminarDB epoch to exactly one Delta Lake version.

F031 implemented the epoch skip logic (`begin_epoch` / `commit_epoch` with `last_committed_epoch` comparison). F031B wires that logic to the Delta log so the epoch state survives process restarts.

## Requirements

### Functional Requirements

- **FR-1**: `open()` scans `_delta_log/` for `txn` actions matching the `writer_id` to find the last committed epoch
- **FR-2**: On recovery, epochs up to `last_committed_epoch` are skipped (existing F031 logic)
- **FR-3**: `commit_epoch()` atomically writes `txn` action with `app_id = writer_id` and `version = epoch`
- **FR-4**: Handle Delta log compaction (checkpoint files) — `txn` actions must survive checkpointing
- **FR-5**: Handle concurrent writers: reject conflicting `txn` for same `app_id` at different epoch
- **FR-6**: Recovery time < 5 seconds (scan only latest checkpoint + subsequent commits)
- **FR-7**: Detect and report "gap" scenarios (epoch N committed but epoch N-1 missing)

### Non-Functional Requirements

- **NFR-1**: Recovery scan is bounded: read from last checkpoint, not from version 0
- **NFR-2**: `txn` scan must be O(checkpoint_count), not O(total_versions)
- **NFR-3**: Writer ID collision detection (two sinks with same writer_id = error)

## Technical Design

### Recovery Flow

```
Process restart
    |
    v
open()
    |
    v
open_table_with_storage_options()
    |
    v
scan_txn_actions(writer_id)
    |
    v
last_committed_epoch = max(txn.version where txn.app_id == writer_id)
    |
    v
Ready (skip epochs <= last_committed_epoch)
```

### Key Implementation

```rust
impl DeltaLakeSink {
    /// Resolves the last committed epoch from the Delta log.
    ///
    /// Scans `txn` protocol actions for this sink's `writer_id`.
    /// Uses Delta checkpoints to bound the scan.
    async fn resolve_last_committed_epoch(
        table: &DeltaTable,
        writer_id: &str,
    ) -> Result<u64, ConnectorError> {
        // Delta checkpoints contain all txn actions up to that version.
        // We only need to scan from the last checkpoint forward.
        let snapshot = table.snapshot()
            .map_err(|e| ConnectorError::CheckpointError(
                format!("failed to get table snapshot: {e}")
            ))?;

        // Find txn action for our writer_id.
        for txn in snapshot.app_transaction_version() {
            if txn.0 == writer_id {
                return Ok(txn.1 as u64);
            }
        }

        // No previous epoch found — this is a fresh start.
        Ok(0)
    }
}
```

### Commit Protocol

```
begin_epoch(N)
    |
    v
[epoch N <= last_committed_epoch?] --yes--> skip (idempotent)
    |no
    v
write_batch() * N  (buffer Parquet data)
    |
    v
commit_epoch(N)
    |
    v
flush remaining buffer -> Parquet files
    |
    v
CommitBuilder::new()
    .with_actions([add_file_1, add_file_2, ..., txn(writer_id, N)])
    .build()
    |
    v
Atomic Delta commit (version V)
    |
    v
last_committed_epoch = N
delta_version = V
```

### Conflict Resolution

```rust
/// Handles optimistic concurrency conflicts in Delta Lake commits.
///
/// If another writer committed between our snapshot and commit attempt,
/// Delta Lake raises a conflict. We retry with the latest snapshot.
async fn commit_with_retry(
    &mut self,
    epoch: u64,
    max_retries: u32,
) -> Result<(), ConnectorError> {
    for attempt in 0..max_retries {
        match self.try_commit(epoch).await {
            Ok(()) => return Ok(()),
            Err(ConnectorError::TransactionError(msg))
                if msg.contains("conflict") && attempt < max_retries - 1 =>
            {
                // Refresh table state and retry.
                self.table.as_mut().unwrap().update().await
                    .map_err(|e| ConnectorError::TransactionError(
                        format!("table refresh failed: {e}")
                    ))?;
                tracing::warn!(
                    epoch, attempt,
                    "Delta commit conflict, retrying"
                );
            }
            Err(e) => return Err(e),
        }
    }
    Err(ConnectorError::TransactionError(
        format!("commit failed after {max_retries} retries")
    ))
}
```

## Test Plan

### Unit Tests

- [ ] `test_resolve_epoch_fresh_table` - Returns 0 for new table
- [ ] `test_resolve_epoch_with_history` - Finds correct epoch from txn actions
- [ ] `test_resolve_epoch_multiple_writers` - Ignores other writer IDs
- [ ] `test_commit_writes_txn_action` - txn action present in Delta log
- [ ] `test_epoch_skip_after_recovery` - Recovered epoch is skipped correctly
- [ ] `test_gap_detection` - Non-sequential epochs detected

### Integration Tests

- [ ] `test_crash_recovery_no_duplicates` - Write, kill, recover, verify no duplicates
- [ ] `test_recovery_from_checkpoint` - Recovery after Delta checkpoint compaction
- [ ] `test_concurrent_writer_conflict` - Two writers conflict and retry
- [ ] `test_recovery_time_under_5s` - Recovery completes within SLA

## Completion Checklist

- [ ] `resolve_last_committed_epoch()` scanning Delta log txn actions
- [ ] `commit_with_retry()` with optimistic concurrency handling
- [ ] `txn` action written on every `commit_epoch()`
- [ ] Recovery from Delta checkpoints (not scanning from version 0)
- [ ] Writer ID collision detection
- [ ] Unit tests passing (6+ tests)
- [ ] Integration tests passing (4+ tests)
- [ ] Recovery time benchmark < 5s
- [ ] Documentation updated

## References

- [F031: Delta Lake Sink](F031-delta-lake-sink.md)
- [F031A: Delta Lake I/O](F031A-delta-lake-io.md)
- [F023: Exactly-Once Sinks](../../phase-2/F023-exactly-once-sinks.md)
- [Delta Lake Protocol - Transaction Identifiers](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers)
- [delta-rs app_transaction_version API](https://docs.rs/deltalake/latest/deltalake/struct.DeltaTableState.html)
