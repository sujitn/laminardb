-- Sinks for the ShopStream demo.
-- In embedded mode these are in-memory sinks (subscribed via the API).
-- In production, replace with INTO KAFKA (...) connectors.

CREATE SINK session_output FROM session_activity;
CREATE SINK order_output FROM order_totals;
CREATE SINK product_output FROM product_activity;
