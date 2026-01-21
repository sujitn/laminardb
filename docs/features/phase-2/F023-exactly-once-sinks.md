# F023: Exactly-Once Sinks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F023 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F008 |
| **Owner** | TBD |

## Summary

Ensure exactly-once delivery semantics to sinks by tracking committed offsets atomically with output. Prevents duplicates on recovery.

## Goals

- Atomic offset + data commit
- Idempotent writes
- Transaction support where available
- Deduplication fallback

## Technical Design

```rust
pub trait ExactlyOnceSink {
    async fn begin_transaction(&mut self) -> Result<TxId>;
    async fn write(&mut self, tx: TxId, batch: RecordBatch) -> Result<()>;
    async fn commit(&mut self, tx: TxId, offsets: &Offsets) -> Result<()>;
    async fn rollback(&mut self, tx: TxId) -> Result<()>;
}

pub struct IdempotentSink {
    inner: Box<dyn Sink>,
    dedup_store: DeduplicationStore,
}

impl IdempotentSink {
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let ids: Vec<_> = batch.column("_id").collect();
        let new_ids = self.dedup_store.filter_unseen(&ids);
        
        if !new_ids.is_empty() {
            self.inner.write(batch.filter(&new_ids)).await?;
            self.dedup_store.mark_seen(&new_ids);
        }
        Ok(())
    }
}
```

## Completion Checklist

- [ ] Transactional sinks working
- [ ] Idempotent fallback implemented
- [ ] Recovery tested (no duplicates)
- [ ] Multiple sink types supported
