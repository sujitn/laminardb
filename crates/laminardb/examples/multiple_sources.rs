//! Multiple sources example: create, replace, drop, and use IF NOT EXISTS.
//!
//! ```bash
//! cargo run --example multiple_sources
//! ```

use laminardb::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    // Create multiple sources
    db.execute("CREATE SOURCE orders (order_id BIGINT, amount DOUBLE)")
        .await?;
    db.execute("CREATE SOURCE clicks (url VARCHAR, user_id BIGINT, ts BIGINT)")
        .await?;
    db.execute("CREATE SOURCE payments (payment_id BIGINT, order_id BIGINT, status VARCHAR)")
        .await?;

    println!("Created {} sources:", db.sources().len());
    for s in db.sources() {
        println!("  - {}", s.name);
    }

    // IF NOT EXISTS — no error on duplicate
    db.execute("CREATE SOURCE IF NOT EXISTS orders (order_id BIGINT, amount DOUBLE)")
        .await?;
    println!("\nCREATE IF NOT EXISTS 'orders' — no error");

    // OR REPLACE — replaces existing schema
    db.execute("CREATE OR REPLACE SOURCE orders (order_id BIGINT, amount DOUBLE, customer VARCHAR)")
        .await?;
    let orders_schema = db
        .sources()
        .iter()
        .find(|s| s.name == "orders")
        .map(|s| s.schema.fields().len());
    println!(
        "OR REPLACE 'orders' — now has {} columns",
        orders_schema.unwrap()
    );

    // DROP SOURCE
    db.execute("DROP SOURCE clicks").await?;
    println!("\nDropped 'clicks'. Sources remaining: {}", db.sources().len());
    for s in db.sources() {
        println!("  - {}", s.name);
    }

    // DROP SOURCE IF EXISTS — no error
    db.execute("DROP SOURCE IF EXISTS nonexistent").await?;
    println!("\nDROP IF EXISTS 'nonexistent' — no error");

    // DROP SOURCE that doesn't exist — error
    let result = db.execute("DROP SOURCE nonexistent").await;
    match result {
        Err(DbError::SourceNotFound(name)) => {
            println!("DROP 'nonexistent' — error: source '{}' not found", name);
        }
        other => println!("Unexpected: {:?}", other),
    }

    // Push data to remaining sources
    let orders = db.source_untyped("orders")?;
    let batch = RecordBatch::try_new(
        orders.schema().clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::Float64Array::from(vec![99.99, 149.50, 25.00])),
            Arc::new(arrow::array::StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )?;
    orders.push_arrow(batch)?;
    println!("\nPushed 3 orders");

    db.close();
    println!("Database closed.");
    Ok(())
}
