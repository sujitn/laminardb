# F020: Lookup Joins

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F020 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F003 |
| **Owner** | TBD |

## Summary

Join streaming data with reference tables (dimension tables). Lookup joins enrich events with data from slowly-changing external tables.

## Goals

- Cached reference table lookups
- Configurable cache TTL
- Async cache refresh
- Support multiple backends (Redis, PostgreSQL)

## Technical Design

```rust
pub struct LookupJoinOperator {
    cache: LruCache<Vec<u8>, RecordBatch>,
    loader: Box<dyn TableLoader>,
    ttl: Duration,
}

impl LookupJoinOperator {
    pub async fn lookup(&mut self, key: &[u8]) -> Option<RecordBatch> {
        if let Some(cached) = self.cache.get(key) {
            if !cached.is_expired(self.ttl) {
                return Some(cached.value.clone());
            }
        }
        
        let loaded = self.loader.load(key).await?;
        self.cache.put(key.to_vec(), CacheEntry::new(loaded.clone()));
        Some(loaded)
    }
}
```

## SQL Syntax

```sql
SELECT o.*, c.name, c.tier
FROM orders o
JOIN customers c  -- Reference table
    ON o.customer_id = c.id;
```

## Completion Checklist

- [ ] Cache working
- [ ] TTL refresh implemented
- [ ] Multiple backends supported
- [ ] Benchmarks passing
