-- Kafka-backed sinks for ShopStream.
-- Each sink writes to a Kafka output topic.
-- WHERE clauses filter data before writing.

CREATE SINK session_output
FROM session_activity
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'session-analytics'
) FORMAT JSON;

CREATE SINK order_output
FROM order_totals
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'sales-kpis'
) FORMAT JSON;

CREATE SINK fraud_alerts
FROM fraud_detection
WHERE event_count > 20
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'fraud-alerts'
) FORMAT JSON;

CREATE SINK inventory_alerts
FROM low_stock_alerts
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'inventory-alerts'
) FORMAT JSON;

CREATE SINK product_trends
FROM product_activity
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'trending-products'
) FORMAT JSON;

CREATE SINK revenue_output
FROM revenue_by_category
INTO KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'revenue-by-category'
) FORMAT JSON;
