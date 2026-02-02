# F031C: Delta Lake Compaction & Maintenance

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F031C |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | M (3-4 days) |
| **Dependencies** | F031A (Delta Lake I/O) |
| **Blocks** | None |
| **Blocked By** | F031A (`deltalake` crate integration) |
| **Created** | 2026-02-02 |

## Summary

Implement background compaction, Z-ORDER optimization, Delta Lake checkpoint creation, and VACUUM for the Delta Lake Sink. These maintenance operations run in Ring 1 (background) and never block the write path.

F031 parses `CompactionConfig` from SQL WITH clauses. F031C wires that config to actual `deltalake` optimize/vacuum operations running on a background task.

## Requirements

### Functional Requirements

- **FR-1**: OPTIMIZE compaction — merge small Parquet files into target-sized files (default 128 MB)
- **FR-2**: Z-ORDER compaction — co-locate rows by specified columns for query performance
- **FR-3**: Delta Lake checkpoint creation every N commits (configurable, default 10)
- **FR-4**: VACUUM — remove old Parquet files beyond retention period (default 7 days)
- **FR-5**: Trigger compaction when file count exceeds `min_files_for_compaction` threshold
- **FR-6**: Run compaction/vacuum on background tokio task (Ring 1), never blocking writes
- **FR-7**: Compaction metrics: files_compacted, bytes_before, bytes_after, duration
- **FR-8**: Graceful shutdown — wait for in-progress compaction before close

### Non-Functional Requirements

- **NFR-1**: Compaction must not lock the table (Delta Lake supports concurrent writes + compact)
- **NFR-2**: VACUUM must respect retention period (never delete files newer than threshold)
- **NFR-3**: Background task should yield to writes (lower priority than commit)
- **NFR-4**: Compaction should be skippable (configurable off)

## Technical Design

### Architecture

```
Ring 1 Write Path                    Ring 1 Background
+-------------------------+         +---------------------------+
| commit_epoch()          |         | CompactionManager         |
|   - write Parquet files |-------->| - check_interval timer    |
|   - commit Delta log    |  notify | - count pending files     |
|   - increment counter   |         | - trigger OPTIMIZE if >=N |
+-------------------------+         | - run Z-ORDER if columns  |
                                    | - create checkpoint if N  |
                                    | - run VACUUM if due       |
                                    +---------------------------+
```

### Key Data Structures

```rust
/// Manages background compaction for a Delta Lake table.
pub struct CompactionManager {
    /// Compaction configuration.
    config: CompactionConfig,
    /// Number of commits since last compaction check.
    commits_since_check: u64,
    /// Number of commits since last Delta checkpoint.
    commits_since_checkpoint: u64,
    /// Last vacuum run timestamp.
    last_vacuum: Option<Instant>,
    /// Compaction metrics.
    metrics: CompactionMetrics,
    /// Background task handle.
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Compaction metrics.
#[derive(Debug, Default)]
pub struct CompactionMetrics {
    pub compactions_run: AtomicU64,
    pub files_compacted: AtomicU64,
    pub bytes_before_compaction: AtomicU64,
    pub bytes_after_compaction: AtomicU64,
    pub checkpoints_created: AtomicU64,
    pub vacuums_run: AtomicU64,
    pub files_vacuumed: AtomicU64,
}

impl CompactionManager {
    /// Called after each commit_epoch() to check if maintenance is needed.
    pub async fn on_commit(
        &mut self,
        table: &mut DeltaTable,
        delta_version: u64,
    ) -> Result<(), ConnectorError> {
        self.commits_since_check += 1;
        self.commits_since_checkpoint += 1;

        // Create Delta checkpoint if interval reached.
        if self.commits_since_checkpoint >= self.config.checkpoint_interval {
            self.create_checkpoint(table, delta_version).await?;
            self.commits_since_checkpoint = 0;
        }

        // Check if compaction needed.
        if self.config.enabled
            && self.commits_since_check >= self.config.check_interval_commits
        {
            self.maybe_compact(table).await?;
            self.commits_since_check = 0;
        }

        Ok(())
    }

    /// Runs OPTIMIZE compaction if file count exceeds threshold.
    async fn maybe_compact(
        &mut self,
        table: &mut DeltaTable,
    ) -> Result<(), ConnectorError> { ... }

    /// Creates a Delta Lake checkpoint (Parquet metadata file).
    async fn create_checkpoint(
        &mut self,
        table: &DeltaTable,
        version: u64,
    ) -> Result<(), ConnectorError> { ... }

    /// Runs VACUUM to remove old files beyond retention.
    pub async fn vacuum(
        &mut self,
        table: &mut DeltaTable,
    ) -> Result<(), ConnectorError> { ... }
}
```

### Integration with DeltaLakeSink

```rust
impl DeltaLakeSink {
    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // ... existing commit logic ...

        // Trigger background maintenance check.
        if let Some(ref mut compaction) = self.compaction_manager {
            compaction.on_commit(
                self.table.as_mut().unwrap(),
                self.delta_version,
            ).await?;
        }

        Ok(())
    }
}
```

## Test Plan

### Unit Tests

- [ ] `test_compaction_trigger_threshold` - Compaction triggers at min_files
- [ ] `test_compaction_disabled` - No compaction when disabled
- [ ] `test_checkpoint_interval` - Checkpoint created every N commits
- [ ] `test_vacuum_retention` - Files within retention not deleted
- [ ] `test_z_order_columns` - Z-ORDER uses configured columns
- [ ] `test_compaction_metrics` - Metrics updated after compaction

### Integration Tests

- [ ] `test_optimize_merges_files` - Small files merged into larger
- [ ] `test_checkpoint_creates_parquet` - Checkpoint file exists in _delta_log
- [ ] `test_vacuum_removes_old_files` - Old files cleaned up
- [ ] `test_concurrent_write_and_compact` - Writes continue during compaction

## Completion Checklist

- [ ] `CompactionManager` with `on_commit()` lifecycle hook
- [ ] OPTIMIZE compaction via `deltalake::optimize::OptimizeBuilder`
- [ ] Z-ORDER via `OptimizeBuilder::with_type(OptimizeType::ZOrder)`
- [ ] Delta checkpoint creation via `deltalake::checkpoints`
- [ ] VACUUM via `deltalake::vacuum::VacuumBuilder`
- [ ] `CompactionMetrics` for observability
- [ ] Background task with graceful shutdown
- [ ] Unit tests passing (6+ tests)
- [ ] Integration tests passing (4+ tests)
- [ ] Documentation updated

## References

- [F031: Delta Lake Sink](F031-delta-lake-sink.md)
- [F031A: Delta Lake I/O](F031A-delta-lake-io.md)
- [delta-rs optimize API](https://docs.rs/deltalake/latest/deltalake/operations/optimize/index.html)
- [delta-rs vacuum API](https://docs.rs/deltalake/latest/deltalake/operations/vacuum/index.html)
- [Delta Lake Checkpoint Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints)
