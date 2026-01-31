-- Reference/dimension tables for the ShopStream demo.
-- These are static lookup tables used for enrichment joins.

CREATE TABLE users (
    user_id         VARCHAR,
    email           VARCHAR,
    tier            VARCHAR,
    lifetime_value  DOUBLE,
    country         VARCHAR
);

INSERT INTO users VALUES
    ('user_001', 'alice@example.com',   'gold',   5000.00, 'US'),
    ('user_002', 'bob@example.com',     'silver', 1500.00, 'UK'),
    ('user_003', 'carol@example.com',   'gold',   8200.00, 'US'),
    ('user_004', 'dave@example.com',    'bronze',  300.00, 'DE'),
    ('user_005', 'eve@example.com',     'silver', 2100.00, 'FR');

CREATE TABLE products (
    product_id  VARCHAR,
    name        VARCHAR,
    category    VARCHAR,
    price       DOUBLE
);

INSERT INTO products VALUES
    ('prod_001', 'Wireless Headphones', 'Electronics', 79.99),
    ('prod_002', 'Running Shoes',       'Apparel',    129.99),
    ('prod_003', 'Coffee Maker',        'Home',        49.99),
    ('prod_004', 'Yoga Mat',            'Fitness',     29.99),
    ('prod_005', 'Laptop Stand',        'Electronics', 39.99),
    ('prod_006', 'Water Bottle',        'Fitness',     14.99),
    ('prod_007', 'Desk Lamp',           'Home',        24.99),
    ('prod_008', 'Backpack',            'Apparel',     59.99);
