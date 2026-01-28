//! Metadata example: SHOW SOURCES, SHOW SINKS, DESCRIBE.
//!
//! ```bash
//! cargo run --example show_describe
//! ```

use laminardb::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    // Create several sources
    db.execute("CREATE SOURCE orders (order_id BIGINT, customer VARCHAR, amount DOUBLE)")
        .await?;
    db.execute(
        "CREATE SOURCE events (
            event_type VARCHAR,
            payload VARCHAR,
            ts TIMESTAMP,
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        )",
    )
    .await?;

    // Create a sink
    db.execute("CREATE SINK output FROM orders").await?;

    // SHOW SOURCES
    let result = db.execute("SHOW SOURCES").await?;
    if let ExecuteResult::Metadata(batch) = result {
        println!("=== SHOW SOURCES ===");
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("  {}", names.value(i));
        }
    }

    // SHOW SINKS
    let result = db.execute("SHOW SINKS").await?;
    if let ExecuteResult::Metadata(batch) = result {
        println!("\n=== SHOW SINKS ===");
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("  {}", names.value(i));
        }
    }

    // DESCRIBE
    let result = db.execute("DESCRIBE orders").await?;
    if let ExecuteResult::Metadata(batch) = result {
        println!("\n=== DESCRIBE orders ===");
        let col_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let col_types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let col_nullable = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!(
                "  {} : {} (nullable: {})",
                col_names.value(i),
                col_types.value(i),
                col_nullable.value(i)
            );
        }
    }

    // DESCRIBE source with watermark
    let result = db.execute("DESCRIBE events").await?;
    if let ExecuteResult::Metadata(batch) = result {
        println!("\n=== DESCRIBE events ===");
        let col_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let col_types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("  {} : {}", col_names.value(i), col_types.value(i));
        }
    }

    // Also show the source info via Rust API
    let sources = db.sources();
    println!("\n=== Source Info (Rust API) ===");
    for s in &sources {
        println!(
            "  {} — {} columns, watermark: {:?}",
            s.name,
            s.schema.fields().len(),
            s.watermark_column
        );
    }

    db.close();
    Ok(())
}
