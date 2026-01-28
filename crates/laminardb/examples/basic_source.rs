//! Basic source example: create a source, push data, and read it back.
//!
//! ```bash
//! cargo run --example basic_source
//! ```

use laminardb::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    // Create a source via SQL
    db.execute(
        "CREATE SOURCE trades (
            symbol VARCHAR,
            price DOUBLE,
            volume BIGINT
        )",
    )
    .await?;

    println!("Created source 'trades'");

    // Get an untyped handle and push an Arrow RecordBatch directly
    let handle = db.source_untyped("trades")?;
    println!("Source: {}", handle.name());
    println!("Schema: {:?}", handle.schema());

    // Build a RecordBatch manually
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Float64Array::from(vec![175.50, 142.30])),
            Arc::new(arrow::array::Int64Array::from(vec![1000, 500])),
        ],
    )?;

    handle.push_arrow(batch)?;
    println!("Pushed 2 records to 'trades'");

    // List sources
    let sources = db.sources();
    println!("\nRegistered sources:");
    for s in &sources {
        println!("  - {} ({} columns)", s.name, s.schema.fields().len());
    }

    db.close();
    println!("\nDatabase closed.");
    Ok(())
}
