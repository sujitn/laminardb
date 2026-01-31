-- Streaming pipelines for Kafka-backed ShopStream.
-- These are the same queries as the embedded mode, plus additional
-- analytics that take advantage of the richer Kafka schemas.

-- 1. Session activity: aggregate clicks per user session
CREATE STREAM session_activity AS
SELECT
    session_id,
    user_id,
    COUNT(*) as event_count,
    MAX(ts) as last_event_ts
FROM clickstream
GROUP BY session_id, user_id;

-- 2. Order totals: revenue aggregation per user
CREATE STREAM order_totals AS
SELECT
    user_id,
    COUNT(*) as order_count,
    SUM(total_amount) as gross_revenue,
    AVG(total_amount) as avg_order_value
FROM orders
GROUP BY user_id;

-- 3. Product popularity: track which products are being viewed/ordered
CREATE STREAM product_activity AS
SELECT
    product_id,
    event_type,
    COUNT(*) as event_count
FROM clickstream
WHERE product_id IS NOT NULL
GROUP BY product_id, event_type;

-- 4. Fraud detection: sessions with rapid bursts of events
CREATE STREAM fraud_detection AS
SELECT
    session_id,
    user_id,
    COUNT(*) as event_count,
    MAX(ts) - MIN(ts) as session_duration_ms
FROM clickstream
GROUP BY session_id, user_id;

-- 5. Low stock alerts: inventory below reorder point
CREATE STREAM low_stock_alerts AS
SELECT
    product_id,
    warehouse_id,
    new_quantity,
    reorder_point,
    ts
FROM inventory_updates
WHERE new_quantity < reorder_point;

-- 6. Revenue by category: join clickstream with orders
CREATE STREAM revenue_by_category AS
SELECT
    c.category,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_revenue
FROM orders o
JOIN clickstream c ON o.product_id = c.product_id
WHERE c.category IS NOT NULL
GROUP BY c.category;
