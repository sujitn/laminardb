---
name: arrow-datafusion
description: Apache Arrow and DataFusion integration patterns for LaminarDB. Use when working with query execution, schemas, record batches, or the DataFusion query engine.
---

# Arrow & DataFusion Skill

## Arrow Fundamentals

### Schema Definition

```rust
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

fn create_order_schema() -> Schema {
    Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(10, 2), false),
        Field::new(
            "order_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("status", DataType::Utf8, true),
    ])
}
```

### Zero-Copy Record Batches

```rust
use arrow::array::{Int64Array, RecordBatch};
use std::sync::Arc;

fn create_batch(schema: &Schema, ids: Vec<i64>, amounts: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(amounts)),
        ],
    )
    .unwrap()
}

// Zero-copy slice
fn slice_batch(batch: &RecordBatch, offset: usize, length: usize) -> RecordBatch {
    batch.slice(offset, length)  // No memory copy!
}
```

### Type Mapping

| SQL Type | Arrow Type | Rust Type |
|----------|------------|-----------|
| BIGINT | Int64 | i64 |
| INTEGER | Int32 | i32 |
| DOUBLE | Float64 | f64 |
| DECIMAL(p,s) | Decimal128(p,s) | i128 |
| VARCHAR | Utf8 | String |
| TIMESTAMP | Timestamp(unit, tz) | i64 |
| BINARY | Binary | Vec<u8> |
| BOOLEAN | Boolean | bool |

## DataFusion Integration

### Session Context Setup

```rust
use datafusion::prelude::*;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};

async fn create_context() -> SessionContext {
    let runtime_config = RuntimeConfig::new()
        .with_memory_limit(1024 * 1024 * 1024, 1.0);  // 1GB limit
    
    let runtime_env = RuntimeEnv::new(runtime_config).unwrap();
    
    let config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(num_cpus::get());
    
    SessionContext::new_with_config_rt(config, Arc::new(runtime_env))
}
```

### Custom Table Provider

```rust
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use async_trait::async_trait;

pub struct StreamingTable {
    schema: SchemaRef,
    source: Arc<dyn StreamSource>,
}

#[async_trait]
impl TableProvider for StreamingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingScan::new(
            self.source.clone(),
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
        )))
    }
}
```

### Custom Execution Plan

```rust
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, 
    Partitioning, SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct StreamingScan {
    source: Arc<dyn StreamSource>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl ExecutionPlan for StreamingScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.source.stream(self.projection.clone())
    }
}
```

### Custom Physical Expression

```rust
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug)]
pub struct WatermarkExpr {
    timestamp_col: Arc<dyn PhysicalExpr>,
    delay: Duration,
}

impl PhysicalExpr for WatermarkExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let timestamps = self.timestamp_col.evaluate(batch)?;
        // Subtract delay from each timestamp
        // ...
    }
}
```

## Performance Patterns

### Batch Processing

```rust
// Process in batches for efficiency
const BATCH_SIZE: usize = 8192;

async fn process_stream(mut stream: SendableRecordBatchStream) -> Result<()> {
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        
        // Process batch - amortizes overhead
        for i in 0..batch.num_rows() {
            // Access columnar data efficiently
        }
    }
    Ok(())
}
```

### Memory Management

```rust
// Use memory pools for controlled allocation
use datafusion::execution::memory_pool::GreedyMemoryPool;

let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 512));  // 512MB

// Track memory usage
let reservation = pool.register().try_grow(1024)?;
// ... use memory
drop(reservation);  // Returns memory to pool
```

### Filter Pushdown

```rust
// Push filters to source for efficiency
fn supports_filter_pushdown(&self, filter: &Expr) -> Result<bool> {
    match filter {
        Expr::BinaryExpr { left, op, right } => {
            // Support simple comparisons on indexed columns
            matches!(op, Operator::Eq | Operator::Lt | Operator::Gt)
        }
        _ => false,
    }
}
```
