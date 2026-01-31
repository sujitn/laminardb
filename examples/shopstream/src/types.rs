//! Typed structs for ShopStream events and analytics results.

#![allow(dead_code)]

use laminar_derive::{FromRow, Record};

// ── Input event types (pushed into sources) ───────────────────────

/// A clickstream event from the e-commerce frontend.
#[derive(Debug, Clone, Record)]
pub struct ClickEvent {
    pub event_id: String,
    pub user_id: String,
    pub session_id: String,
    pub event_type: String,
    pub product_id: Option<String>,
    pub page_url: Option<String>,
    #[event_time]
    pub ts: i64,
}

/// An order placed by a customer.
#[derive(Debug, Clone, Record)]
pub struct OrderEvent {
    pub order_id: String,
    pub user_id: String,
    pub product_id: String,
    pub quantity: i64,
    pub total_amount: f64,
    pub payment_method: String,
    #[event_time]
    pub ts: i64,
}

/// An inventory level change in a warehouse.
#[derive(Debug, Clone, Record)]
pub struct InventoryUpdate {
    pub product_id: String,
    pub warehouse_id: String,
    pub quantity_change: i64,
    pub new_quantity: i64,
    #[event_time]
    pub ts: i64,
}

// ── Output analytics types (read from query results) ──────────────

/// Aggregated session activity.
#[derive(Debug, Clone, FromRow)]
pub struct SessionActivity {
    pub session_id: String,
    pub user_id: String,
    pub event_count: i64,
}

/// Aggregated order totals per user.
#[derive(Debug, Clone, FromRow)]
pub struct OrderTotals {
    pub user_id: String,
    pub order_count: i64,
    pub gross_revenue: f64,
    pub avg_order_value: f64,
}

/// Product activity aggregation.
#[derive(Debug, Clone, FromRow)]
pub struct ProductActivity {
    pub product_id: String,
    pub event_type: String,
    pub event_count: i64,
}

// ── Kafka-mode: Rich event types for JSON serialization ───────────

/// Rich clickstream event for Kafka mode (15 fields).
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct KafkaClickEvent {
    pub event_id: String,
    pub user_id: String,
    pub session_id: String,
    pub event_type: String,
    pub product_id: Option<String>,
    pub category: Option<String>,
    pub page_url: Option<String>,
    pub referrer: Option<String>,
    pub device_type: String,
    pub browser: String,
    pub country: String,
    pub region: Option<String>,
    pub city: Option<String>,
    pub price: Option<f64>,
    pub ts: i64,
}

/// Rich order event for Kafka mode (11 fields).
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct KafkaOrderEvent {
    pub order_id: String,
    pub user_id: String,
    pub product_id: String,
    pub quantity: i64,
    pub unit_price: f64,
    pub total_amount: f64,
    pub discount_pct: Option<f64>,
    pub payment_method: String,
    pub shipping_method: Option<String>,
    pub status: String,
    pub ts: i64,
}

/// Rich inventory event for Kafka mode (9 fields).
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct KafkaInventoryUpdate {
    pub product_id: String,
    pub warehouse_id: String,
    pub quantity_change: i64,
    pub new_quantity: i64,
    pub reason: Option<String>,
    pub supplier_id: Option<String>,
    pub cost_per_unit: Option<f64>,
    pub reorder_point: Option<i64>,
    pub ts: i64,
}
