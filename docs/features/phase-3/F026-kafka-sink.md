# F026: Kafka Sink Connector

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F026 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F023 |
| **Owner** | TBD |

## Summary

Produce events to Apache Kafka topics with exactly-once semantics using Kafka transactions.

## Goals

- Transactional writes (exactly-once)
- Configurable partitioning
- Multiple serializers
- Batching for throughput

## Technical Design

```rust
pub struct KafkaSink {
    producer: FutureProducer,
    config: KafkaSinkConfig,
    serializer: Box<dyn Serializer>,
}

impl ExactlyOnceSink for KafkaSink {
    async fn begin_transaction(&mut self) -> Result<TxId> {
        self.producer.begin_transaction()?;
        Ok(TxId::new())
    }
    
    async fn commit(&mut self, tx: TxId, offsets: &Offsets) -> Result<()> {
        self.producer.send_offsets_to_transaction(offsets)?;
        self.producer.commit_transaction()?;
        Ok(())
    }
}
```

## SQL Syntax

```sql
CREATE SINK TABLE order_results (
    order_id BIGINT,
    status VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'order-results',
    'bootstrap.servers' = 'localhost:9092',
    format = 'json'
);
```

## Completion Checklist

- [ ] Basic production working
- [ ] Transactions (exactly-once) working
- [ ] Partitioning configurable
- [ ] Batching optimized
