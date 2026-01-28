//! Derive macro example: use `#[derive(Record)]` and `#[derive(FromRecordBatch)]`
//! to eliminate boilerplate when working with the streaming API.
//!
//! ```bash
//! cargo run --example derive_macros
//! ```

use laminardb::prelude::*;

/// A trade event with derive macros.
#[derive(Record)]
struct Trade {
    symbol: String,
    price: f64,
    volume: i64,
    #[event_time]
    timestamp: i64,
}

/// Output row deserialized from query results.
#[derive(FromRecordBatch, Debug)]
#[allow(dead_code)]
struct TradeRow {
    symbol: String,
    price: f64,
    volume: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    // Create source matching the Trade struct
    db.execute(
        "CREATE SOURCE trades (
            symbol VARCHAR,
            price DOUBLE,
            volume BIGINT,
            timestamp BIGINT
        )",
    )
    .await?;

    // Get a typed source handle
    let source = db.source::<Trade>("trades")?;
    println!("Source '{}' ready", source.name());

    // Push typed records — no manual RecordBatch construction needed
    source.push(Trade {
        symbol: "AAPL".into(),
        price: 175.50,
        volume: 1000,
        timestamp: 1706400000000,
    })?;

    source.push(Trade {
        symbol: "GOOG".into(),
        price: 142.30,
        volume: 500,
        timestamp: 1706400001000,
    })?;

    println!("Pushed 2 typed records");

    // Schema is auto-generated from the struct
    let schema = Trade::schema();
    println!("\nAuto-generated schema:");
    for field in schema.fields() {
        println!("  {} : {:?} (nullable: {})", field.name(), field.data_type(), field.is_nullable());
    }

    // Demonstrate FromRecordBatch deserialization
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["MSFT", "AMZN"])),
            Arc::new(arrow::array::Float64Array::from(vec![380.10, 178.25])),
            Arc::new(arrow::array::Int64Array::from(vec![200, 750])),
        ],
    )?;

    let rows = TradeRow::from_batch_all(&batch);
    println!("\nDeserialized {} rows:", rows.len());
    for row in &rows {
        println!("  {:?}", row);
    }

    db.close();
    Ok(())
}
