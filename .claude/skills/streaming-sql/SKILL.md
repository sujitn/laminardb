---
name: streaming-sql
description: LaminarDB SQL extensions for streaming queries including windows, watermarks, EMIT clauses, and late data handling. Use when writing or reviewing streaming SQL queries.
---

# Streaming SQL Skill

## LaminarDB SQL Extensions

### Creating Streaming Tables

```sql
-- Source table from Kafka
CREATE SOURCE TABLE orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10, 2),
    order_time TIMESTAMP,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'orders',
    bootstrap.servers = 'localhost:9092',
    format = 'json'
);

-- Sink table to PostgreSQL
CREATE SINK TABLE order_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_orders BIGINT,
    total_amount DECIMAL(10, 2)
) WITH (
    connector = 'jdbc',
    url = 'jdbc:postgresql://localhost:5432/analytics',
    table = 'order_stats'
);
```

### Window Functions

```sql
-- Tumbling window (non-overlapping, fixed size)
SELECT
    TUMBLE_START(order_time, INTERVAL '1' HOUR) as window_start,
    TUMBLE_END(order_time, INTERVAL '1' HOUR) as window_end,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR);

-- Sliding/Hopping window (overlapping)
SELECT
    HOP_START(order_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as window_start,
    HOP_END(order_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as window_end,
    AVG(amount) as avg_amount
FROM orders
GROUP BY HOP(order_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR);

-- Session window (gap-based)
SELECT
    SESSION_START(order_time, INTERVAL '30' MINUTE) as session_start,
    SESSION_END(order_time, INTERVAL '30' MINUTE) as session_end,
    customer_id,
    COUNT(*) as orders_in_session
FROM orders
GROUP BY SESSION(order_time, INTERVAL '30' MINUTE), customer_id;
```

### EMIT Clause (Output Control)

```sql
-- Emit on watermark (default, most efficient)
SELECT ...
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)
EMIT ON WATERMARK;

-- Emit periodically for real-time updates
SELECT ...
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)
EMIT EVERY INTERVAL '10' SECOND;

-- Emit on every change (highest overhead)
SELECT ...
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)
EMIT ON UPDATE;
```

### Late Data Handling

```sql
-- Allow late data up to 1 hour after watermark
SELECT
    TUMBLE_START(order_time, INTERVAL '1' HOUR) as window_start,
    COUNT(*) as order_count
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)
ALLOW LATENESS INTERVAL '1' HOUR;

-- Handling late data with side output
SELECT
    TUMBLE_START(order_time, INTERVAL '1' HOUR) as window_start,
    COUNT(*) as order_count
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)
LATE DATA TO late_orders;
```

### Watermarks

Watermarks track event-time progress:

```sql
-- Bounded out-of-orderness watermark
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND

-- Periodic watermark (for ordered data)
WATERMARK FOR event_time AS event_time

-- Custom watermark expression
WATERMARK FOR event_time AS 
    CASE 
        WHEN is_heartbeat THEN event_time 
        ELSE event_time - INTERVAL '10' SECOND 
    END
```

### Joins

```sql
-- Stream-stream join (windowed)
SELECT 
    o.order_id,
    o.amount,
    p.payment_status
FROM orders o
JOIN payments p
    ON o.order_id = p.order_id
    AND p.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR;

-- Stream-table join (lookup)
SELECT
    o.order_id,
    o.amount,
    c.customer_name,
    c.tier
FROM orders o
JOIN customers c  -- Reference table
    ON o.customer_id = c.customer_id;

-- Temporal join (versioned lookup)
SELECT
    o.order_id,
    o.amount,
    r.rate
FROM orders o
JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time r
    ON o.currency = r.currency;
```

## Best Practices

1. **Always define watermarks** for event-time processing
2. **Use appropriate window sizes** - too small creates overhead, too large increases latency
3. **Consider late data** - define ALLOW LATENESS based on your SLA
4. **Prefer EMIT ON WATERMARK** unless real-time updates needed
5. **Use lookup joins** for enrichment instead of stream-stream joins when possible
