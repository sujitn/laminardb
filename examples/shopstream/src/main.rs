//! ShopStream: E-Commerce Analytics Demo for LaminarDB.
//!
//! Demonstrates all Phase 3 features in a realistic e-commerce scenario:
//!
//! - **F-DB-001**: Builder API with config vars
//! - **F-SQL-005**: Multi-statement SQL execution
//! - **F-SQL-006**: Config variable `${VAR}` substitution
//! - **F-SQL-001**: Extended Source DDL (FROM KAFKA syntax)
//! - **F-SQL-002**: Extended Sink DDL (INTO KAFKA + WHERE)
//! - **F-SQL-003**: CREATE STREAM named pipelines
//! - **F-SQL-004**: CREATE TABLE + INSERT INTO
//! - **F-DERIVE-001**: Record + FromRecordBatch derive macros
//! - **F-DB-002**: Named stream subscriptions
//! - **F-DB-003**: Pipeline lifecycle (start/shutdown)
//! - **F-DB-004**: Connector Manager
//!
//! # Running
//!
//! ## Embedded mode (default, no external dependencies):
//! ```bash
//! cargo run -p shopstream-demo
//! ```
//!
//! ## Kafka mode (requires Docker):
//! ```bash
//! docker-compose -f examples/shopstream/docker-compose.yml up -d
//! bash examples/shopstream/scripts/setup-kafka.sh
//! SHOPSTREAM_MODE=kafka cargo run -p shopstream-demo --features kafka
//! ```

mod generator;
mod types;

use std::time::Instant;

use laminar_db::{ExecuteResult, LaminarDB};

use types::{ClickEvent, InventoryUpdate, OrderEvent};

/// SQL files embedded at compile time.
const TABLES_SQL: &str = include_str!("../sql/tables.sql");
const SOURCES_SQL: &str = include_str!("../sql/sources.sql");
const STREAMS_SQL: &str = include_str!("../sql/streams.sql");
const SINKS_SQL: &str = include_str!("../sql/sinks.sql");

/// Kafka-mode SQL files (richer schemas, FROM KAFKA / INTO KAFKA).
#[cfg(feature = "kafka")]
const SOURCES_KAFKA_SQL: &str = include_str!("../sql/sources_kafka.sql");
#[cfg(feature = "kafka")]
const STREAMS_KAFKA_SQL: &str = include_str!("../sql/streams_kafka.sql");
#[cfg(feature = "kafka")]
const SINKS_KAFKA_SQL: &str = include_str!("../sql/sinks_kafka.sql");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("SHOPSTREAM_MODE").unwrap_or_else(|_| "embedded".into());

    match mode.as_str() {
        #[cfg(feature = "kafka")]
        "kafka" => run_kafka_mode().await,
        _ => run_embedded_mode().await,
    }
}

/// Embedded mode: in-memory sources, synthetic data, no external dependencies.
async fn run_embedded_mode() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ShopStream: E-Commerce Analytics Demo (Embedded Mode) ===");
    println!();

    // ── Step 1: Build LaminarDB with config vars ──────────────────────
    println!("[1/7] Building LaminarDB with config variables...");
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", "localhost:9092")
        .config_var("GROUP_ID", "shopstream-demo")
        .config_var("ENVIRONMENT", "development")
        .buffer_size(65536)
        .build()
        .await?;

    println!("  Pipeline state: {}", db.pipeline_state());

    // ── Step 2: Create reference tables ───────────────────────────────
    println!("[2/7] Creating reference tables (multi-statement SQL)...");
    let t0 = Instant::now();
    db.execute(TABLES_SQL).await?;
    println!("  Created users + products tables in {:?}", t0.elapsed());

    verify_table_counts(&db).await;

    // ── Step 3: Create streaming sources ──────────────────────────────
    println!("[3/7] Creating streaming sources (multi-statement SQL)...");
    db.execute(SOURCES_SQL).await?;
    println!(
        "  Sources: {:?}",
        db.sources().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 4: Create streaming pipelines ────────────────────────────
    println!("[4/7] Creating streaming pipelines...");
    db.execute(STREAMS_SQL).await?;
    println!("  Streams created via multi-statement execution");

    // ── Step 5: Create sinks ──────────────────────────────────────────
    println!("[5/7] Creating sinks...");
    db.execute(SINKS_SQL).await?;
    println!(
        "  Sinks: {:?}",
        db.sinks().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 6: Start pipeline ────────────────────────────────────────
    println!("[6/7] Starting pipeline...");
    db.start().await?;
    println!("  Pipeline state: {}", db.pipeline_state());

    // ── Step 7: Generate and push data ────────────────────────────────
    println!("[7/7] Generating and pushing synthetic events...");
    println!();

    let base_ts = chrono::Utc::now().timestamp_millis();

    let click_source = db.source::<ClickEvent>("clickstream")?;
    let order_source = db.source::<OrderEvent>("orders")?;
    let inventory_source = db.source::<InventoryUpdate>("inventory_updates")?;

    let clicks = generator::generate_clicks(50, base_ts);
    let orders = generator::generate_orders(10, base_ts);
    let inventory = generator::generate_inventory(5, base_ts);

    let click_count = click_source.push_batch(clicks);
    let order_count = order_source.push_batch(orders);
    let inv_count = inventory_source.push_batch(inventory);

    println!("  Pushed {} clickstream events", click_count);
    println!("  Pushed {} order events", order_count);
    println!("  Pushed {} inventory updates", inv_count);

    click_source.watermark(base_ts + 10_000);
    order_source.watermark(base_ts + 10_000);
    inventory_source.watermark(base_ts + 10_000);

    // ── Summary ───────────────────────────────────────────────────────
    print_summary(&db).await;
    print_reference_queries(&db).await;

    // ── Shutdown ──────────────────────────────────────────────────────
    println!();
    println!("Shutting down pipeline...");
    db.shutdown().await?;
    println!("  Pipeline state: {}", db.pipeline_state());
    println!();
    println!("=== ShopStream Demo Complete ===");

    Ok(())
}

/// Kafka mode: reads from Kafka topics, writes to Kafka output topics.
#[cfg(feature = "kafka")]
async fn run_kafka_mode() -> Result<(), Box<dyn std::error::Error>> {
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::FutureProducer;

    let brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let group_id =
        std::env::var("GROUP_ID").unwrap_or_else(|_| "shopstream-demo".into());
    let gen_rate: usize = std::env::var("GEN_RATE")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .unwrap_or(100);

    println!("=== ShopStream: E-Commerce Analytics Demo (Kafka Mode) ===");
    println!();
    println!("  Kafka brokers:  {brokers}");
    println!("  Consumer group: {group_id}");
    println!("  Generator rate: {gen_rate} events/batch");
    println!();

    // ── Step 1: Build LaminarDB ───────────────────────────────────────
    println!("[1/7] Building LaminarDB with Kafka config...");
    let db = LaminarDB::builder()
        .config_var("KAFKA_BROKERS", &brokers)
        .config_var("GROUP_ID", &group_id)
        .config_var("ENVIRONMENT", "kafka-demo")
        .buffer_size(65536)
        .build()
        .await?;

    // ── Step 2: Create reference tables ───────────────────────────────
    println!("[2/7] Creating reference tables...");
    db.execute(TABLES_SQL).await?;

    // ── Step 3: Create Kafka sources ──────────────────────────────────
    println!("[3/7] Creating Kafka sources (FROM KAFKA)...");
    db.execute(SOURCES_KAFKA_SQL).await?;
    println!(
        "  Sources: {:?}",
        db.sources().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 4: Create streaming pipelines ────────────────────────────
    println!("[4/7] Creating streaming pipelines...");
    db.execute(STREAMS_KAFKA_SQL).await?;

    // ── Step 5: Create Kafka sinks ────────────────────────────────────
    println!("[5/7] Creating Kafka sinks (INTO KAFKA + WHERE)...");
    db.execute(SINKS_KAFKA_SQL).await?;
    println!(
        "  Sinks: {:?}",
        db.sinks().iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    // ── Step 6: Start pipeline ────────────────────────────────────────
    println!("[6/7] Starting Kafka pipeline...");
    db.start().await?;
    println!("  Pipeline state: {}", db.pipeline_state());

    // ── Step 7: Generate data and produce to Kafka ────────────────────
    println!("[7/7] Producing synthetic events to Kafka...");
    println!();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let base_ts = chrono::Utc::now().timestamp_millis();

    let clicks = generator::generate_kafka_clicks(gen_rate, base_ts);
    let orders = generator::generate_kafka_orders(gen_rate / 5, base_ts);
    let inventory = generator::generate_kafka_inventory(gen_rate / 10, base_ts);

    let click_count =
        generator::produce_to_kafka(&producer, "clickstream", &clicks).await?;
    let order_count =
        generator::produce_to_kafka(&producer, "orders", &orders).await?;
    let inv_count = generator::produce_to_kafka(
        &producer,
        "inventory_updates",
        &inventory,
    )
    .await?;

    println!("  Produced {} clickstream events to Kafka", click_count);
    println!("  Produced {} order events to Kafka", order_count);
    println!("  Produced {} inventory updates to Kafka", inv_count);

    // ── Live dashboard loop ───────────────────────────────────────────
    println!();
    println!("Pipeline running. Press Ctrl+C to stop.");
    println!();

    let start_time = Instant::now();
    let mut cycle = 0u64;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!();
                println!("Ctrl+C received, shutting down...");
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                cycle += 1;
                let uptime = start_time.elapsed();
                println!(
                    "=== ShopStream Live [cycle {}] | Uptime: {}s ===",
                    cycle,
                    uptime.as_secs()
                );
                println!(
                    "  Pipeline: {} | Sources: {} | Sinks: {}",
                    db.pipeline_state(),
                    db.source_count(),
                    db.sink_count(),
                );

                // Generate more events periodically
                let ts = chrono::Utc::now().timestamp_millis();
                let batch = generator::generate_kafka_clicks(gen_rate / 10, ts);
                let n = generator::produce_to_kafka(
                    &producer, "clickstream", &batch,
                ).await.unwrap_or(0);
                println!("  Generated {n} more clickstream events");
                println!();
            }
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────────
    println!("Shutting down pipeline...");
    db.shutdown().await?;
    println!("  Pipeline state: {}", db.pipeline_state());
    println!();
    println!("=== ShopStream Demo Complete ===");

    Ok(())
}

// ── Helper functions ──────────────────────────────────────────────────

async fn verify_table_counts(db: &LaminarDB) {
    let result = db.execute("SELECT COUNT(*) as cnt FROM users").await;
    if let Ok(ExecuteResult::Query(mut q)) = result {
        if let Ok(rows) = q.subscribe::<CountResult>() {
            if let Ok(batch) = rows.recv_timeout(std::time::Duration::from_secs(2)) {
                for r in &batch {
                    println!("  users table: {} rows", r.cnt);
                }
            }
        }
    }

    let result = db.execute("SELECT COUNT(*) as cnt FROM products").await;
    if let Ok(ExecuteResult::Query(mut q)) = result {
        if let Ok(rows) = q.subscribe::<CountResult>() {
            if let Ok(batch) = rows.recv_timeout(std::time::Duration::from_secs(2)) {
                for r in &batch {
                    println!("  products table: {} rows", r.cnt);
                }
            }
        }
    }
}

async fn print_summary(db: &LaminarDB) {
    println!();
    println!("=== Pipeline Summary ===");
    println!("  Sources:  {}", db.source_count());
    println!("  Sinks:    {}", db.sink_count());
    println!("  State:    {}", db.pipeline_state());
    println!();

    let result = db.execute("SHOW SOURCES").await;
    if let Ok(ExecuteResult::Metadata(batch)) = result {
        println!("  SHOW SOURCES: {} rows", batch.num_rows());
    }

    let result = db.execute("SHOW STREAMS").await;
    if let Ok(ExecuteResult::Metadata(batch)) = result {
        println!("  SHOW STREAMS: {} rows", batch.num_rows());
    }

    let result = db.execute("SHOW SINKS").await;
    if let Ok(ExecuteResult::Metadata(batch)) = result {
        println!("  SHOW SINKS:   {} rows", batch.num_rows());
    }
}

async fn print_reference_queries(db: &LaminarDB) {
    println!();
    println!("=== Reference Table Queries ===");

    let result = db
        .execute("SELECT user_id, tier, lifetime_value FROM users WHERE tier = 'gold'")
        .await;
    if let Ok(ExecuteResult::Query(mut q)) = result {
        if let Ok(rows) = q.subscribe::<UserRow>() {
            if let Ok(batch) = rows.recv_timeout(std::time::Duration::from_secs(2)) {
                println!("  Gold tier users:");
                for user in &batch {
                    println!(
                        "    {} | tier={} | ltv=${:.2}",
                        user.user_id, user.tier, user.lifetime_value
                    );
                }
            }
        }
    }

    let result = db
        .execute(
            "SELECT name, category, price FROM products WHERE category = 'Electronics'",
        )
        .await;
    if let Ok(ExecuteResult::Query(mut q)) = result {
        if let Ok(rows) = q.subscribe::<ProductRow>() {
            if let Ok(batch) = rows.recv_timeout(std::time::Duration::from_secs(2)) {
                println!("  Electronics products:");
                for prod in &batch {
                    println!(
                        "    {} | {} | ${:.2}",
                        prod.name, prod.category, prod.price
                    );
                }
            }
        }
    }
}

// ── Helper types for ad-hoc queries ──────────────────────────────────

#[derive(Debug, laminar_derive::FromRow)]
struct CountResult {
    cnt: i64,
}

#[derive(Debug, laminar_derive::FromRow)]
struct UserRow {
    user_id: String,
    tier: String,
    lifetime_value: f64,
}

#[derive(Debug, laminar_derive::FromRow)]
struct ProductRow {
    name: String,
    category: String,
    price: f64,
}
