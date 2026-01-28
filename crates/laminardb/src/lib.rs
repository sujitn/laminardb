//! # `LaminarDB`
//!
//! An embedded streaming database. SQLite for stream processing.
//!
//! `LaminarDB` combines the deployment simplicity of SQLite with the streaming
//! capabilities of Apache Flink, delivering sub-microsecond latency for
//! real-time data processing.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use laminardb::prelude::*;
//!
//! #[derive(Record)]
//! struct Trade {
//!     symbol: String,
//!     price: f64,
//!     #[event_time]
//!     timestamp: i64,
//! }
//!
//! #[derive(FromRecordBatch)]
//! struct AvgPrice {
//!     symbol: String,
//!     avg_price: f64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let db = LaminarDB::open()?;
//!
//!     db.execute("CREATE SOURCE trades (
//!         symbol VARCHAR, price DOUBLE, timestamp BIGINT,
//!         WATERMARK FOR timestamp AS timestamp - INTERVAL '1' SECOND
//!     )").await?;
//!
//!     let trades = db.source::<Trade>("trades")?;
//!     trades.push(Trade {
//!         symbol: "AAPL".into(),
//!         price: 175.50,
//!         timestamp: 1706400000000,
//!     })?;
//!
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]
#![warn(clippy::all)]

// Re-export the database facade
pub use laminar_db::*;

// Re-export derive macros
pub use laminar_derive::{FromRecordBatch, Record};

// Re-export core streaming types
pub use laminar_core::streaming::{self, BackpressureStrategy, WaitStrategy};

// Re-export ASOF join types for configuration
pub use laminar_core::operator::asof_join::{AsofDirection, AsofJoinConfig, AsofJoinType};

/// Commonly used types, traits, and macros.
///
/// ```rust,ignore
/// use laminardb::prelude::*;
/// ```
pub mod prelude {
    // Database
    pub use laminar_db::{DbError, ExecuteResult, LaminarConfig, LaminarDB, QueryHandle};
    pub use laminar_db::{QueryInfo, SinkInfo, SourceHandle, SourceInfo, UntypedSourceHandle};

    // Derive macros
    pub use laminar_derive::{FromRecordBatch, Record};

    // Streaming
    pub use laminar_core::streaming::{BackpressureStrategy, Record as RecordTrait, WaitStrategy};

    // Arrow (commonly needed)
    pub use arrow::array::RecordBatch;
    pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    // Standard library re-exports for convenience
    pub use std::sync::Arc;
    pub use std::time::Duration;
}
