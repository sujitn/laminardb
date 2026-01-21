# F025: Kafka Source Connector

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F025 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F001 |
| **Owner** | TBD |

## Summary

Consume events from Apache Kafka topics. Supports consumer groups, offset tracking, and exactly-once integration.

## Goals

- Multi-partition consumption
- Consumer group coordination
- Manual offset commits (for exactly-once)
- Multiple deserializers (JSON, Avro, Protobuf)

## Technical Design

```rust
pub struct KafkaSource {
    consumer: StreamConsumer,
    config: KafkaSourceConfig,
    deserializer: Box<dyn Deserializer>,
}

pub struct KafkaSourceConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub auto_offset_reset: OffsetReset,
    pub max_poll_records: usize,
}
```

## SQL Syntax

```sql
CREATE SOURCE TABLE orders (
    order_id BIGINT,
    amount DECIMAL(10, 2),
    ts TIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'orders',
    'bootstrap.servers' = 'localhost:9092',
    format = 'json'
);
```

## Completion Checklist

- [ ] Basic consumption working
- [ ] Consumer groups coordinating
- [ ] Offset tracking correct
- [ ] Multiple formats supported
