# F005: DataFusion Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F005 |
| **Status** | 📝 Draft |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F001 |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-20 |

## Summary

Integrate Apache DataFusion as the SQL query engine for LaminarDB. DataFusion provides query parsing, planning, and optimization while we provide streaming-specific extensions and execution.

## Goals

- Use DataFusion for SQL parsing and planning
- Implement custom streaming table providers
- Support zero-copy data exchange via Arrow
- Enable predicate and projection pushdown

## Technical Design

### Custom Table Provider

```rust
pub struct StreamingTableProvider {
    schema: SchemaRef,
    source: Arc<dyn StreamSource>,
}

#[async_trait]
impl TableProvider for StreamingTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingScanExec::new(
            self.source.clone(),
            self.schema.clone(),
            projection.cloned(),
        )))
    }
}
```

## Completion Checklist

- [ ] Table provider implemented
- [ ] Streaming execution plan
- [ ] Filter pushdown working
- [ ] Benchmarks passing
