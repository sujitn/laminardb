
use std::sync::Arc;
use arrow::array::{Int32Array, RecordBatch, StringArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use laminar_db::table_store::TableStore;
use datafusion::prelude::SessionContext;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
    ]))
}

fn make_batch(id: i32, name: &str, price: f64) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(Float64Array::from(vec![price])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_streaming_provider() {
    let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
    {
        let mut store = ts.lock();
        store.create_table("t", test_schema(), "id").unwrap();

        // Insert 1500 rows. With DEFAULT_CHUNK_SIZE = 1024, this should result in multiple batches in the stream.
        let num_rows = 1500;
        for i in 0..num_rows {
            let batch = make_batch(i, "item", i as f64);
            store.upsert("t", &batch).unwrap();
        }
    }

    let provider = laminar_db::table_provider::ReferenceTableProvider::new(
        "t".to_string(),
        test_schema(),
        ts.clone(),
    );

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let batches = df.collect().await.unwrap();

    // Verify total row count
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1500);

    // If our streaming implementation is working, we should have multiple batches from DataFusion
    // although DataFusion might coalesce them. But ReferenceTableExec should have produced at least 2.
    // In our case, MemTable (which we replaced) would have returned what we gave it.
    // ReferenceTableExec returns one batch per poll_next.
    // Since we have 1500 rows and chunk size is 1024, we should have at least 2 batches.
    assert!(batches.len() >= 2, "Should have at least 2 batches, got {}", batches.len());
}
