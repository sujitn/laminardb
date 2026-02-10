//! Live `DataFusion` table provider backed by the `TableStore`.
//!
//! Unlike `MemTable` (which is a static snapshot), [`ReferenceTableProvider`]
//! reads directly from the `TableStore` on each `scan()` call, so queries
//! always see the latest data without needing to deregister/re-register.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;

use crate::table_backend::{ScanOffset, DEFAULT_CHUNK_SIZE};
use crate::table_store::TableStore;

/// A `DataFusion` table provider that reads live data from `TableStore`.
///
/// Registered once at CREATE TABLE time and never needs re-registration —
/// each `scan()` fetches the current snapshot from the backing store.
pub(crate) struct ReferenceTableProvider {
    table_name: String,
    schema: SchemaRef,
    table_store: Arc<parking_lot::Mutex<TableStore>>,
}

impl ReferenceTableProvider {
    /// Create a new provider for the given table.
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        table_store: Arc<parking_lot::Mutex<TableStore>>,
    ) -> Self {
        Self {
            table_name,
            schema,
            table_store,
        }
    }
}

/// A physical execution plan for scanning a `TableStore` table in chunks.
#[derive(Debug)]
pub(crate) struct ReferenceTableExec {
    table_name: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    table_store: Arc<parking_lot::Mutex<TableStore>>,
    properties: PlanProperties,
}

impl ReferenceTableExec {
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        table_store: Arc<parking_lot::Mutex<TableStore>>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(p) => Arc::new(schema.project(p).unwrap()),
            None => schema.clone(),
        };

        let properties = PlanProperties::new(
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            table_name,
            schema,
            projection,
            table_store,
            properties,
        }
    }
}

impl DisplayAs for ReferenceTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReferenceTableExec: table={}", self.table_name)
    }
}

impl ExecutionPlan for ReferenceTableExec {
    fn name(&self) -> &'static str {
        "ReferenceTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let is_persistent = self.table_store.lock().is_persistent(&self.table_name);

        if is_persistent {
            Ok(Box::pin(ReferenceTableStream::new_persistent(
                self.table_name.clone(),
                self.schema.clone(),
                self.projection.clone(),
                self.table_store.clone(),
            )))
        } else {
            // For in-memory tables, we collect all individual row batch references once
            // to avoid repeated lock acquisitions and potential O(N^2) sorting issues.
            let row_batches = {
                let ts = self.table_store.lock();
                ts.to_all_row_batches(&self.table_name)
                    .unwrap_or_default()
            };
            Ok(Box::pin(ReferenceTableStream::new_in_memory(
                self.schema.clone(),
                self.projection.clone(),
                row_batches,
            )))
        }
    }
}

/// A stream that pulls chunks from `TableStore` one by one.
pub(crate) struct ReferenceTableStream {
    table_name: Option<String>,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    table_store: Option<Arc<parking_lot::Mutex<TableStore>>>,
    offset: ScanOffset,
    // For in-memory tables
    row_batches: Vec<RecordBatch>,
    row_index: usize,
    finished: bool,
}

impl ReferenceTableStream {
    /// Create a stream for a persistent (RocksDB) table.
    pub fn new_persistent(
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        table_store: Arc<parking_lot::Mutex<TableStore>>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(p) => Arc::new(schema.project(p).unwrap()),
            None => schema.clone(),
        };
        Self {
            table_name: Some(table_name),
            full_schema: schema,
            projected_schema,
            projection,
            table_store: Some(table_store),
            offset: ScanOffset::Start,
            row_batches: Vec::new(),
            row_index: 0,
            finished: false,
        }
    }

    /// Create a stream for an in-memory table.
    pub fn new_in_memory(
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        row_batches: Vec<RecordBatch>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(p) => Arc::new(schema.project(p).unwrap()),
            None => schema.clone(),
        };
        Self {
            table_name: None,
            full_schema: schema,
            projected_schema,
            projection,
            table_store: None,
            offset: ScanOffset::End,
            row_batches,
            row_index: 0,
            finished: false,
        }
    }
}

impl Stream for ReferenceTableStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // Case 1: In-memory table (already have refs)
        if !self.row_batches.is_empty() {
            let start = self.row_index;
            if start >= self.row_batches.len() {
                self.finished = true;
                return Poll::Ready(None);
            }

            let end = (start + DEFAULT_CHUNK_SIZE).min(self.row_batches.len());
            let chunk = &self.row_batches[start..end];
            let batch = arrow::compute::concat_batches(&self.full_schema, chunk.iter())
                .map_err(|e| DataFusionError::External(format!("concat batches: {e}").into()))?;

            // Apply projection if provided
            let batch = if let Some(p) = &self.projection {
                batch.project(p).map_err(|e| DataFusionError::ArrowError(e, None))?
            } else {
                batch
            };

            self.row_index = end;
            if end >= self.row_batches.len() {
                self.finished = true;
            }
            return Poll::Ready(Some(Ok(batch)));
        }

        // Case 2: Persistent table (pull paged)
        if let (Some(name), Some(ts_arc)) = (&self.table_name, &self.table_store) {
            let result = {
                let ts = ts_arc.lock();
                ts.to_record_batch_paged(name, self.offset.clone(), DEFAULT_CHUNK_SIZE)
            };

            match result {
                Some((batch, next_offset)) => {
                    self.offset = next_offset;
                    if self.offset == ScanOffset::End {
                        self.finished = true;
                        if batch.num_rows() == 0 {
                            return Poll::Ready(None);
                        }
                    }

                    // Apply projection if provided
                    let batch = if let Some(p) = &self.projection {
                        batch.project(p).map_err(|e| DataFusionError::ArrowError(e, None))?
                    } else {
                        batch
                    };

                    return Poll::Ready(Some(Ok(batch)));
                }
                None => {
                    self.finished = true;
                    return Poll::Ready(None);
                }
            }
        }

        self.finished = true;
        Poll::Ready(None)
    }
}

impl RecordBatchStream for ReferenceTableStream {
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
}

#[async_trait]
impl TableProvider for ReferenceTableProvider {
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = ReferenceTableExec::new(
            self.table_name.clone(),
            self.schema.clone(),
            projection.cloned(),
            self.table_store.clone(),
        );

        Ok(Arc::new(exec))
    }
}

impl std::fmt::Debug for ReferenceTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReferenceTableProvider")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i32], names: &[&str], prices: &[f64]) -> arrow::array::RecordBatch {
        arrow::array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_provider_schema() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        assert_eq!(provider.schema(), test_schema());
    }

    #[test]
    fn test_provider_table_type() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn test_provider_scan_empty() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        {
            let mut store = ts.lock();
            store.create_table("test", test_schema(), "id").unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn test_provider_scan_with_data() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        {
            let mut store = ts.lock();
            store.create_table("test", test_schema(), "id").unwrap();
            store
                .upsert("test", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
                .unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_provider_reads_live_data() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        {
            let mut store = ts.lock();
            store.create_table("test", test_schema(), "id").unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts.clone());
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        // First query: empty
        let df = ctx.sql("SELECT count(*) AS cnt FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0);

        // Insert data
        {
            let mut store = ts.lock();
            store
                .upsert("test", &make_batch(&[1], &["A"], &[1.0]))
                .unwrap();
        }

        // Second query: should see the new row without re-registration
        let df = ctx.sql("SELECT count(*) AS cnt FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 1);
    }

    #[test]
    fn test_provider_debug() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let debug = format!("{provider:?}");
        assert!(debug.contains("ReferenceTableProvider"));
        assert!(debug.contains("test"));
    }
}
