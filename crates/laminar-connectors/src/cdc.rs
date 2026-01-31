//! CDC (Change Data Capture) connectors for databases.
//!
//! Provides CDC source connectors that stream row-level changes
//! from databases into LaminarDB using logical replication.

/// PostgreSQL logical replication CDC source connector (F027).
#[cfg(feature = "postgres-cdc")]
pub mod postgres;