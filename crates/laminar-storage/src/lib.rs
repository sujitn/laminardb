//! # `LaminarDB` Storage
//!
//! Durability layer for `LaminarDB` - WAL, checkpointing, and lakehouse integration.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Write-ahead log implementation - WAL for durability and exactly-once semantics
pub mod wal;

/// WAL-backed state store - Combines MmapStateStore with WAL for durability
pub mod wal_state_store;

/// Checkpointing for state persistence - Incremental and full checkpointing
pub mod checkpoint;

/// Lakehouse format integration - Delta Lake and Iceberg sink support
pub mod lakehouse;

// Re-export key types
pub use wal::{WriteAheadLog, WalEntry, WalError};
pub use wal_state_store::WalStateStore;