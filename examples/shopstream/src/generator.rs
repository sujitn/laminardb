//! Synthetic e-commerce event generator for the ShopStream demo.

use rand::Rng;

use crate::types::{ClickEvent, InventoryUpdate, OrderEvent};

const USER_IDS: &[&str] = &[
    "user_001", "user_002", "user_003", "user_004", "user_005",
];

const PRODUCT_IDS: &[&str] = &[
    "prod_001", "prod_002", "prod_003", "prod_004",
    "prod_005", "prod_006", "prod_007", "prod_008",
];

const EVENT_TYPES: &[&str] = &[
    "page_view", "product_view", "add_to_cart", "checkout", "search",
];

const PAGES: &[&str] = &[
    "/", "/products", "/cart", "/checkout", "/search", "/account",
];

const PAYMENT_METHODS: &[&str] = &["credit_card", "paypal", "apple_pay", "bank_transfer"];

const WAREHOUSES: &[&str] = &["wh-east", "wh-west", "wh-central"];

const PRICES: &[f64] = &[79.99, 129.99, 49.99, 29.99, 39.99, 14.99, 24.99, 59.99];

#[cfg(feature = "kafka")]
const CATEGORIES: &[&str] = &[
    "Electronics", "Apparel", "Home", "Fitness",
    "Electronics", "Fitness", "Home", "Apparel",
];

#[cfg(feature = "kafka")]
const DEVICES: &[&str] = &["desktop", "mobile", "tablet"];

#[cfg(feature = "kafka")]
const BROWSERS: &[&str] = &["Chrome", "Firefox", "Safari", "Edge"];

#[cfg(feature = "kafka")]
const COUNTRIES: &[&str] = &["US", "UK", "DE", "FR", "JP"];

#[cfg(feature = "kafka")]
const SHIPPING: &[&str] = &["standard", "express", "overnight"];

#[cfg(feature = "kafka")]
const REASONS: &[&str] = &["restock", "return", "adjustment", "sale"];

/// Generate a batch of clickstream events (embedded mode).
pub fn generate_clicks(count: usize, base_ts: i64) -> Vec<ClickEvent> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let event_type = EVENT_TYPES[rng.gen_range(0..EVENT_TYPES.len())];
            let has_product = event_type == "product_view" || event_type == "add_to_cart";
            ClickEvent {
                event_id: format!("evt_{:06}", base_ts as u64 + i as u64),
                user_id: USER_IDS[rng.gen_range(0..USER_IDS.len())].to_string(),
                session_id: format!("sess_{:04}", rng.gen_range(1..=20)),
                event_type: event_type.to_string(),
                product_id: if has_product {
                    Some(PRODUCT_IDS[rng.gen_range(0..PRODUCT_IDS.len())].to_string())
                } else {
                    None
                },
                page_url: Some(PAGES[rng.gen_range(0..PAGES.len())].to_string()),
                ts: base_ts + i as i64 * 100, // 100ms apart
            }
        })
        .collect()
}

/// Generate a batch of order events (embedded mode).
pub fn generate_orders(count: usize, base_ts: i64) -> Vec<OrderEvent> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let prod_idx = rng.gen_range(0..PRODUCT_IDS.len());
            let qty = rng.gen_range(1..=5_i64);
            OrderEvent {
                order_id: format!("ord_{:06}", base_ts as u64 + i as u64),
                user_id: USER_IDS[rng.gen_range(0..USER_IDS.len())].to_string(),
                product_id: PRODUCT_IDS[prod_idx].to_string(),
                quantity: qty,
                total_amount: PRICES[prod_idx] * qty as f64,
                payment_method: PAYMENT_METHODS[rng.gen_range(0..PAYMENT_METHODS.len())]
                    .to_string(),
                ts: base_ts + i as i64 * 500, // 500ms apart
            }
        })
        .collect()
}

/// Generate a batch of inventory update events (embedded mode).
pub fn generate_inventory(count: usize, base_ts: i64) -> Vec<InventoryUpdate> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let change: i64 = rng.gen_range(-10..=50);
            InventoryUpdate {
                product_id: PRODUCT_IDS[rng.gen_range(0..PRODUCT_IDS.len())].to_string(),
                warehouse_id: WAREHOUSES[rng.gen_range(0..WAREHOUSES.len())].to_string(),
                quantity_change: change,
                new_quantity: (100 + change).max(0),
                ts: base_ts + i as i64 * 1000, // 1s apart
            }
        })
        .collect()
}

// ── Kafka mode: JSON event generators ─────────────────────────────

/// Generate rich clickstream events for Kafka (15 fields).
#[cfg(feature = "kafka")]
pub fn generate_kafka_clicks(
    count: usize,
    base_ts: i64,
) -> Vec<crate::types::KafkaClickEvent> {
    use crate::types::KafkaClickEvent;
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let event_type = EVENT_TYPES[rng.gen_range(0..EVENT_TYPES.len())];
            let has_product = event_type == "product_view" || event_type == "add_to_cart";
            let prod_idx = rng.gen_range(0..PRODUCT_IDS.len());
            KafkaClickEvent {
                event_id: format!("evt_{:06}", base_ts as u64 + i as u64),
                user_id: USER_IDS[rng.gen_range(0..USER_IDS.len())].to_string(),
                session_id: format!("sess_{:04}", rng.gen_range(1..=20)),
                event_type: event_type.to_string(),
                product_id: if has_product {
                    Some(PRODUCT_IDS[prod_idx].to_string())
                } else {
                    None
                },
                category: if has_product {
                    Some(CATEGORIES[prod_idx].to_string())
                } else {
                    None
                },
                page_url: Some(PAGES[rng.gen_range(0..PAGES.len())].to_string()),
                referrer: if rng.gen_bool(0.3) {
                    Some("https://google.com".to_string())
                } else {
                    None
                },
                device_type: DEVICES[rng.gen_range(0..DEVICES.len())].to_string(),
                browser: BROWSERS[rng.gen_range(0..BROWSERS.len())].to_string(),
                country: COUNTRIES[rng.gen_range(0..COUNTRIES.len())].to_string(),
                region: None,
                city: None,
                price: if has_product {
                    Some(PRICES[prod_idx])
                } else {
                    None
                },
                ts: base_ts + i as i64 * 100,
            }
        })
        .collect()
}

/// Generate rich order events for Kafka (11 fields).
#[cfg(feature = "kafka")]
pub fn generate_kafka_orders(
    count: usize,
    base_ts: i64,
) -> Vec<crate::types::KafkaOrderEvent> {
    use crate::types::KafkaOrderEvent;
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let prod_idx = rng.gen_range(0..PRODUCT_IDS.len());
            let qty = rng.gen_range(1..=5_i64);
            let unit_price = PRICES[prod_idx];
            let discount = if rng.gen_bool(0.2) {
                Some(rng.gen_range(5.0..25.0))
            } else {
                None
            };
            let total = unit_price * qty as f64
                * (1.0 - discount.unwrap_or(0.0) / 100.0);
            KafkaOrderEvent {
                order_id: format!("ord_{:06}", base_ts as u64 + i as u64),
                user_id: USER_IDS[rng.gen_range(0..USER_IDS.len())].to_string(),
                product_id: PRODUCT_IDS[prod_idx].to_string(),
                quantity: qty,
                unit_price,
                total_amount: (total * 100.0).round() / 100.0,
                discount_pct: discount,
                payment_method: PAYMENT_METHODS[rng.gen_range(0..PAYMENT_METHODS.len())]
                    .to_string(),
                shipping_method: Some(
                    SHIPPING[rng.gen_range(0..SHIPPING.len())].to_string(),
                ),
                status: "completed".to_string(),
                ts: base_ts + i as i64 * 500,
            }
        })
        .collect()
}

/// Generate rich inventory events for Kafka (9 fields).
#[cfg(feature = "kafka")]
pub fn generate_kafka_inventory(
    count: usize,
    base_ts: i64,
) -> Vec<crate::types::KafkaInventoryUpdate> {
    use crate::types::KafkaInventoryUpdate;
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let change: i64 = rng.gen_range(-10..=50);
            let prod_idx = rng.gen_range(0..PRODUCT_IDS.len());
            KafkaInventoryUpdate {
                product_id: PRODUCT_IDS[prod_idx].to_string(),
                warehouse_id: WAREHOUSES[rng.gen_range(0..WAREHOUSES.len())].to_string(),
                quantity_change: change,
                new_quantity: (100 + change).max(0),
                reason: Some(REASONS[rng.gen_range(0..REASONS.len())].to_string()),
                supplier_id: if rng.gen_bool(0.5) {
                    Some(format!("sup_{:03}", rng.gen_range(1..=10)))
                } else {
                    None
                },
                cost_per_unit: Some(PRICES[prod_idx] * 0.6),
                reorder_point: Some(rng.gen_range(10..=50)),
                ts: base_ts + i as i64 * 1000,
            }
        })
        .collect()
}

/// Produce a batch of JSON events to a Kafka topic.
#[cfg(feature = "kafka")]
pub async fn produce_to_kafka<T: serde::Serialize>(
    producer: &rdkafka::producer::FutureProducer,
    topic: &str,
    events: &[T],
) -> Result<usize, Box<dyn std::error::Error>> {
    use rdkafka::producer::FutureRecord;

    let mut count = 0;
    for event in events {
        let json = serde_json::to_string(event)?;
        let record = FutureRecord::<(), _>::to(topic).payload(&json);
        producer
            .send(record, std::time::Duration::from_secs(5))
            .await
            .map_err(|(e, _)| e)?;
        count += 1;
    }
    Ok(count)
}
