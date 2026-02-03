//! Connector SDK - developer tooling and operational resilience.
//!
//! The SDK provides higher-level utilities built on top of the core connector
//! traits (`SourceConnector`, `SinkConnector`):
//!
//! - **Retry & Resilience**: [`RetryPolicy`], [`CircuitBreaker`]
//! - **Rate Limiting**: [`RateLimiter`], [`TokenBucket`], [`LeakyBucket`]
//! - **Test Harness**: [`ConnectorTestHarness`] for connector validation
//! - **Builders**: Fluent APIs for connector construction
//! - **Schema Discovery**: [`infer_schema_from_samples`]
//!
//! # Example: Using Retry Policy
//!
//! ```rust,ignore
//! use laminar_connectors::sdk::{RetryPolicy, with_retry};
//!
//! let policy = RetryPolicy::exponential(3, Duration::from_millis(100));
//! let result = with_retry(&policy, || async {
//!     // Operation that might fail transiently
//!     Ok::<_, ConnectorError>(42)
//! }).await;
//! ```
//!
//! # Example: Rate Limiting
//!
//! ```rust,ignore
//! use laminar_connectors::sdk::{RateLimiter, TokenBucket};
//!
//! let limiter = TokenBucket::new(1000.0, 100); // 1000/sec, burst of 100
//! limiter.acquire().await;
//! // Proceed with rate-limited operation
//! ```

pub mod builder;
pub mod defaults;
pub mod harness;
pub mod rate_limit;
pub mod retry;
pub mod schema;

// Re-exports
pub use builder::{SinkConnectorBuilder, SourceConnectorBuilder};
pub use defaults::register_all_connectors;
pub use harness::{
    ConnectorTestHarness, SinkTestMetrics, SourceTestMetrics, TestResult, TestSinkConnector,
    TestSourceConnector,
};
pub use rate_limit::{LeakyBucket, RateLimiter, TokenBucket};
pub use retry::{with_retry, CircuitBreaker, CircuitState, RetryPolicy};
pub use schema::{infer_schema_from_samples, SchemaDiscoveryHints};
