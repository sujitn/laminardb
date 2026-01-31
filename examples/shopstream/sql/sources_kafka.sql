-- Kafka-backed streaming sources for ShopStream.
-- Requires a running Kafka broker (see docker-compose.yml).
-- Config variables (${VAR}) are resolved at runtime by LaminarDB.

CREATE SOURCE clickstream FROM KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'clickstream',
    'group.id' = '${GROUP_ID}',
    'auto.offset.reset' = 'latest'
) FORMAT JSON SCHEMA (
    event_id        VARCHAR NOT NULL,
    user_id         VARCHAR NOT NULL,
    session_id      VARCHAR NOT NULL,
    event_type      VARCHAR NOT NULL,
    product_id      VARCHAR,
    category        VARCHAR,
    page_url        VARCHAR,
    referrer        VARCHAR,
    device_type     VARCHAR,
    browser         VARCHAR,
    country         VARCHAR,
    region          VARCHAR,
    city            VARCHAR,
    price           DOUBLE,
    ts              BIGINT NOT NULL
);

CREATE SOURCE orders FROM KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'orders',
    'group.id' = '${GROUP_ID}',
    'auto.offset.reset' = 'latest'
) FORMAT JSON SCHEMA (
    order_id        VARCHAR NOT NULL,
    user_id         VARCHAR NOT NULL,
    product_id      VARCHAR NOT NULL,
    quantity        BIGINT NOT NULL,
    unit_price      DOUBLE NOT NULL,
    total_amount    DOUBLE NOT NULL,
    discount_pct    DOUBLE,
    payment_method  VARCHAR NOT NULL,
    shipping_method VARCHAR,
    status          VARCHAR NOT NULL,
    ts              BIGINT NOT NULL
);

CREATE SOURCE inventory_updates FROM KAFKA (
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    'topic' = 'inventory_updates',
    'group.id' = '${GROUP_ID}',
    'auto.offset.reset' = 'latest'
) FORMAT JSON SCHEMA (
    product_id      VARCHAR NOT NULL,
    warehouse_id    VARCHAR NOT NULL,
    quantity_change BIGINT NOT NULL,
    new_quantity    BIGINT NOT NULL,
    reason          VARCHAR,
    supplier_id     VARCHAR,
    cost_per_unit   DOUBLE,
    reorder_point   BIGINT,
    ts              BIGINT NOT NULL
);
