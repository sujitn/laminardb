-- Streaming sources for the ShopStream demo.
-- In embedded mode these are in-memory sources fed by the data generator.
-- In production, replace with FROM KAFKA (...) connectors.

CREATE SOURCE clickstream (
    event_id        VARCHAR NOT NULL,
    user_id         VARCHAR NOT NULL,
    session_id      VARCHAR NOT NULL,
    event_type      VARCHAR NOT NULL,
    product_id      VARCHAR,
    page_url        VARCHAR,
    ts              BIGINT NOT NULL
);

CREATE SOURCE orders (
    order_id        VARCHAR NOT NULL,
    user_id         VARCHAR NOT NULL,
    product_id      VARCHAR NOT NULL,
    quantity        BIGINT NOT NULL,
    total_amount    DOUBLE NOT NULL,
    payment_method  VARCHAR NOT NULL,
    ts              BIGINT NOT NULL
);

CREATE SOURCE inventory_updates (
    product_id      VARCHAR NOT NULL,
    warehouse_id    VARCHAR NOT NULL,
    quantity_change BIGINT NOT NULL,
    new_quantity    BIGINT NOT NULL,
    ts              BIGINT NOT NULL
);
