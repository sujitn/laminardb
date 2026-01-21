---
name: connectors
description: Source and sink connector patterns including Kafka, CDC (Change Data Capture), and lookup joins. Use when implementing connectors to external systems.
---

# Connectors Skill

## Connector Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Connector Framework                          │
├─────────────────────────────────────────────────────────────────┤
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐      │
│   │   Source    │────▶│  Transform  │────▶│    Sink     │      │
│   │  Connector  │     │   (SQL)     │     │  Connector  │      │
│   └─────────────┘     └─────────────┘     └─────────────┘      │
│         │                                       │               │
│         ▼                                       ▼               │
│   ┌─────────────┐                        ┌─────────────┐       │
│   │   Offset    │                        │  Idempotent │       │
│   │  Tracking   │                        │   Writer    │       │
│   └─────────────┘                        └─────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

## Source Connector Trait

```rust
use async_trait::async_trait;
use arrow::record_batch::RecordBatch;

#[async_trait]
pub trait SourceConnector: Send + Sync {
    fn schema(&self) -> SchemaRef;
    async fn start(&mut self, offsets: HashMap<String, u64>) -> Result<()>;
    async fn poll(&mut self, timeout: Duration) -> Result<Option<SourceBatch>>;
    async fn commit(&mut self, offsets: HashMap<String, u64>) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

pub struct SourceBatch {
    pub records: RecordBatch,
    pub offsets: HashMap<String, u64>,
    pub watermark: Option<i64>,
}
```

## Kafka Source

```rust
use rdkafka::consumer::{Consumer, StreamConsumer};

pub struct KafkaSource {
    consumer: StreamConsumer,
    topics: Vec<String>,
    schema: SchemaRef,
}

#[async_trait]
impl SourceConnector for KafkaSource {
    async fn poll(&mut self, timeout: Duration) -> Result<Option<SourceBatch>> {
        let mut records = Vec::new();
        let mut offsets = HashMap::new();
        
        while let Ok(msg) = self.consumer.recv().await {
            records.push(self.deserialize(msg.payload())?);
            offsets.insert(
                format!("{}:{}", msg.topic(), msg.partition()),
                msg.offset() as u64
            );
            if records.len() >= MAX_BATCH_SIZE { break; }
        }
        
        Ok(Some(SourceBatch { records: batch, offsets, watermark: None }))
    }
}
```

## CDC Source (PostgreSQL)

```rust
pub struct PostgresCdcSource {
    client: Client,
    slot_name: String,
    publication: String,
}

pub enum CdcOperation {
    Insert { after: Row },
    Update { before: Option<Row>, after: Row },
    Delete { before: Row },
}
```

## Lookup Table

```rust
pub struct LookupTable {
    cache: HashMap<Vec<u8>, CacheEntry>,
    ttl: Duration,
    loader: Box<dyn TableLoader>,
}

impl LookupTable {
    pub async fn lookup(&mut self, keys: &[Vec<u8>]) -> Result<RecordBatch> {
        // Check cache, load missing keys
    }
}
```

## Sink Connector Trait

```rust
#[async_trait]
pub trait SinkConnector: Send + Sync {
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn begin_transaction(&mut self) -> Result<TransactionId>;
    async fn commit(&mut self, tx_id: TransactionId, offsets: HashMap<String, u64>) -> Result<()>;
}
```

## Configuration

```toml
[connectors.kafka]
bootstrap_servers = "localhost:9092"
group_id = "laminardb-consumer"

[connectors.postgres_cdc]
connection_string = "host=localhost dbname=mydb"
slot_name = "laminardb_slot"

[connectors.lookup]
cache_ttl = "5m"
max_cache_size = "100MB"
```
