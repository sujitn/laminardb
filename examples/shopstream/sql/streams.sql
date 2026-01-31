-- Streaming pipelines for the ShopStream demo.
-- Each CREATE STREAM defines a named continuous query.

-- 1. Session activity: aggregate clicks per user session
CREATE STREAM session_activity AS
SELECT
    session_id,
    user_id,
    COUNT(*) as event_count
FROM clickstream
GROUP BY session_id, user_id;

-- 2. Order totals: revenue aggregation
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
