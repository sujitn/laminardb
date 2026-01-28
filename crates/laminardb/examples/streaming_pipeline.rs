//! Streaming pipeline example: source → push data → sink subscription.
//!
//! Demonstrates the end-to-end flow of creating a source,
//! pushing data through it, and consuming results via the
//! streaming subscription API.
//!
//! ```bash
//! cargo run --example streaming_pipeline
//! ```

use std::time::Duration;

use laminardb::prelude::*;

#[derive(Record)]
struct Tick {
    symbol: String,
    price: f64,
    #[event_time]
    ts: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    // Create source
    db.execute(
        "CREATE SOURCE ticks (
            symbol VARCHAR,
            price DOUBLE,
            ts BIGINT
        )",
    )
    .await?;

    println!("=== Streaming Pipeline ===\n");

    // Get a typed source handle
    let source = db.source::<Tick>("ticks")?;
    println!("Source '{}' ready ({} columns)", source.name(), source.schema().fields().len());

    // Push a batch of records
    let ticks = vec![
        Tick { symbol: "AAPL".into(), price: 175.00, ts: 1000 },
        Tick { symbol: "GOOG".into(), price: 140.00, ts: 1001 },
        Tick { symbol: "AAPL".into(), price: 175.50, ts: 1002 },
        Tick { symbol: "MSFT".into(), price: 380.00, ts: 1003 },
        Tick { symbol: "GOOG".into(), price: 141.20, ts: 1004 },
        Tick { symbol: "AAPL".into(), price: 176.00, ts: 1005 },
    ];

    let count = source.push_batch(ticks);
    println!("Pushed {} ticks", count);

    // Demonstrate watermark tracking
    source.watermark(1005);
    println!("Watermark advanced to {}", source.current_watermark());

    // Show queries (none yet)
    let queries = db.queries();
    println!("\nActive queries: {}", queries.len());

    // Demonstrate config options
    let config = LaminarConfig {
        default_buffer_size: 4096,
        default_backpressure: BackpressureStrategy::DropOldest,
        storage_dir: None,
    };
    let db2 = LaminarDB::open_with_config(config)?;
    db2.execute("CREATE SOURCE fast_source (id INT, value DOUBLE)")
        .await?;
    println!("\nOpened second DB with custom config (buffer=4096, backpressure=DropOldest)");

    // Demonstrate debug format
    println!("\nDB state: {:?}", db);

    // Graceful shutdown
    db.close();
    db2.close();
    println!("\nAll databases closed.");

    // Verify shutdown prevents operations
    let result = db.execute("CREATE SOURCE test (id INT)").await;
    match result {
        Err(DbError::Shutdown) => println!("Post-shutdown execute correctly rejected."),
        _ => println!("Unexpected result after shutdown"),
    }

    // Show duration types available in prelude
    let _timeout = Duration::from_secs(5);
    println!("\nPrelude provides Duration, Arc, RecordBatch, Schema, etc.");

    Ok(())
}
