//! Fluent builder APIs for connector construction.
//!
//! Provides builder patterns for configuring connectors with retry policies,
//! rate limiters, and circuit breakers.

use std::sync::Arc;

use crate::config::ConnectorConfig;
use crate::connector::{SinkConnector, SourceConnector};
use crate::error::ConnectorError;

use super::rate_limit::RateLimiter;
use super::retry::{CircuitBreaker, RetryPolicy};

/// Builder for configuring source connectors.
///
/// # Example
///
/// ```rust,ignore
/// let source = SourceConnectorBuilder::new(KafkaSource::new())
///     .config(config)
///     .retry_policy(RetryPolicy::exponential(3, Duration::from_millis(100)))
///     .rate_limit(TokenBucket::new(1000.0, 100))
///     .build();
/// ```
pub struct SourceConnectorBuilder<C> {
    connector: C,
    config: ConnectorConfig,
    retry_policy: Option<RetryPolicy>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
    circuit_breaker: Option<CircuitBreaker>,
}

impl<C: SourceConnector> SourceConnectorBuilder<C> {
    /// Creates a new builder with the given connector.
    #[must_use]
    pub fn new(connector: C) -> Self {
        Self {
            connector,
            config: ConnectorConfig::default(),
            retry_policy: None,
            rate_limiter: None,
            circuit_breaker: None,
        }
    }

    /// Sets the connector configuration.
    #[must_use]
    pub fn config(mut self, config: ConnectorConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the retry policy.
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Sets the rate limiter.
    #[must_use]
    pub fn rate_limit<R: RateLimiter + 'static>(mut self, limiter: R) -> Self {
        self.rate_limiter = Some(Arc::new(limiter));
        self
    }

    /// Sets the circuit breaker.
    #[must_use]
    pub fn circuit_breaker(mut self, breaker: CircuitBreaker) -> Self {
        self.circuit_breaker = Some(breaker);
        self
    }

    /// Builds the configured source connector.
    #[must_use]
    pub fn build(self) -> ConfiguredSource<C> {
        ConfiguredSource {
            connector: self.connector,
            config: self.config,
            retry_policy: self.retry_policy.unwrap_or_default(),
            rate_limiter: self.rate_limiter,
            circuit_breaker: self.circuit_breaker,
        }
    }
}

/// A source connector with attached configuration.
pub struct ConfiguredSource<C> {
    /// The underlying connector.
    pub connector: C,
    /// The configuration.
    pub config: ConnectorConfig,
    /// Retry policy for operations.
    pub retry_policy: RetryPolicy,
    /// Optional rate limiter.
    pub rate_limiter: Option<Arc<dyn RateLimiter>>,
    /// Optional circuit breaker.
    pub circuit_breaker: Option<CircuitBreaker>,
}

impl<C: SourceConnector> ConfiguredSource<C> {
    /// Opens the connector.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if opening fails.
    pub async fn open(&mut self) -> Result<(), ConnectorError> {
        self.connector.open(&self.config).await
    }

    /// Returns a reference to the underlying connector.
    #[must_use]
    pub fn connector(&self) -> &C {
        &self.connector
    }

    /// Returns a mutable reference to the underlying connector.
    pub fn connector_mut(&mut self) -> &mut C {
        &mut self.connector
    }

    /// Returns the retry policy.
    #[must_use]
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    /// Returns the rate limiter if configured.
    #[must_use]
    pub fn rate_limiter(&self) -> Option<&Arc<dyn RateLimiter>> {
        self.rate_limiter.as_ref()
    }

    /// Returns the circuit breaker if configured.
    #[must_use]
    pub fn circuit_breaker(&self) -> Option<&CircuitBreaker> {
        self.circuit_breaker.as_ref()
    }
}

/// Builder for configuring sink connectors.
///
/// # Example
///
/// ```rust,ignore
/// let sink = SinkConnectorBuilder::new(PostgresSink::new())
///     .config(config)
///     .retry_policy(RetryPolicy::exponential(3, Duration::from_millis(100)))
///     .rate_limit(TokenBucket::new(1000.0, 100))
///     .build();
/// ```
pub struct SinkConnectorBuilder<C> {
    connector: C,
    config: ConnectorConfig,
    retry_policy: Option<RetryPolicy>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
    circuit_breaker: Option<CircuitBreaker>,
}

impl<C: SinkConnector> SinkConnectorBuilder<C> {
    /// Creates a new builder with the given connector.
    #[must_use]
    pub fn new(connector: C) -> Self {
        Self {
            connector,
            config: ConnectorConfig::default(),
            retry_policy: None,
            rate_limiter: None,
            circuit_breaker: None,
        }
    }

    /// Sets the connector configuration.
    #[must_use]
    pub fn config(mut self, config: ConnectorConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the retry policy.
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Sets the rate limiter.
    #[must_use]
    pub fn rate_limit<R: RateLimiter + 'static>(mut self, limiter: R) -> Self {
        self.rate_limiter = Some(Arc::new(limiter));
        self
    }

    /// Sets the circuit breaker.
    #[must_use]
    pub fn circuit_breaker(mut self, breaker: CircuitBreaker) -> Self {
        self.circuit_breaker = Some(breaker);
        self
    }

    /// Builds the configured sink connector.
    #[must_use]
    pub fn build(self) -> ConfiguredSink<C> {
        ConfiguredSink {
            connector: self.connector,
            config: self.config,
            retry_policy: self.retry_policy.unwrap_or_default(),
            rate_limiter: self.rate_limiter,
            circuit_breaker: self.circuit_breaker,
        }
    }
}

/// A sink connector with attached configuration.
pub struct ConfiguredSink<C> {
    /// The underlying connector.
    pub connector: C,
    /// The configuration.
    pub config: ConnectorConfig,
    /// Retry policy for operations.
    pub retry_policy: RetryPolicy,
    /// Optional rate limiter.
    pub rate_limiter: Option<Arc<dyn RateLimiter>>,
    /// Optional circuit breaker.
    pub circuit_breaker: Option<CircuitBreaker>,
}

impl<C: SinkConnector> ConfiguredSink<C> {
    /// Opens the connector.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if opening fails.
    pub async fn open(&mut self) -> Result<(), ConnectorError> {
        self.connector.open(&self.config).await
    }

    /// Returns a reference to the underlying connector.
    #[must_use]
    pub fn connector(&self) -> &C {
        &self.connector
    }

    /// Returns a mutable reference to the underlying connector.
    pub fn connector_mut(&mut self) -> &mut C {
        &mut self.connector
    }

    /// Returns the retry policy.
    #[must_use]
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    /// Returns the rate limiter if configured.
    #[must_use]
    pub fn rate_limiter(&self) -> Option<&Arc<dyn RateLimiter>> {
        self.rate_limiter.as_ref()
    }

    /// Returns the circuit breaker if configured.
    #[must_use]
    pub fn circuit_breaker(&self) -> Option<&CircuitBreaker> {
        self.circuit_breaker.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::sdk::rate_limit::TokenBucket;
    use crate::testing::{MockSinkConnector, MockSourceConnector};

    #[test]
    fn test_source_builder_basic() {
        let source = MockSourceConnector::new();
        let configured = SourceConnectorBuilder::new(source).build();

        // Default retry policy should be applied
        assert_eq!(configured.retry_policy.max_attempts, 3);
    }

    #[test]
    fn test_source_builder_with_config() {
        let source = MockSourceConnector::new();
        let mut config = ConnectorConfig::new("mock");
        config.set("key", "value");

        let configured = SourceConnectorBuilder::new(source).config(config).build();

        assert_eq!(configured.config.get("key"), Some("value"));
    }

    #[test]
    fn test_source_builder_with_retry() {
        let source = MockSourceConnector::new();
        let policy = RetryPolicy::exponential(5, Duration::from_millis(200));

        let configured = SourceConnectorBuilder::new(source)
            .retry_policy(policy)
            .build();

        assert_eq!(configured.retry_policy.max_attempts, 5);
        assert_eq!(
            configured.retry_policy.initial_backoff,
            Duration::from_millis(200)
        );
    }

    #[test]
    fn test_source_builder_with_rate_limit() {
        let source = MockSourceConnector::new();
        let limiter = TokenBucket::new(1000.0, 100);

        let configured = SourceConnectorBuilder::new(source)
            .rate_limit(limiter)
            .build();

        assert!(configured.rate_limiter.is_some());
    }

    #[test]
    fn test_source_builder_with_circuit_breaker() {
        let source = MockSourceConnector::new();
        let breaker = CircuitBreaker::new(5, Duration::from_secs(30));

        let configured = SourceConnectorBuilder::new(source)
            .circuit_breaker(breaker)
            .build();

        assert!(configured.circuit_breaker.is_some());
    }

    #[tokio::test]
    async fn test_configured_source_open() {
        let source = MockSourceConnector::new();
        let mut configured = SourceConnectorBuilder::new(source)
            .config(ConnectorConfig::new("mock"))
            .build();

        configured.open().await.unwrap();
    }

    #[test]
    fn test_sink_builder_basic() {
        let sink = MockSinkConnector::new();
        let configured = SinkConnectorBuilder::new(sink).build();

        assert_eq!(configured.retry_policy.max_attempts, 3);
    }

    #[test]
    fn test_sink_builder_with_all_options() {
        let sink = MockSinkConnector::new();
        let policy = RetryPolicy::fixed(10, Duration::from_secs(1));
        let limiter = TokenBucket::new(500.0, 50);
        let breaker = CircuitBreaker::new(3, Duration::from_secs(60));

        let mut config = ConnectorConfig::new("mock");
        config.set("table", "test_table");

        let configured = SinkConnectorBuilder::new(sink)
            .config(config)
            .retry_policy(policy)
            .rate_limit(limiter)
            .circuit_breaker(breaker)
            .build();

        assert_eq!(configured.config.get("table"), Some("test_table"));
        assert_eq!(configured.retry_policy.max_attempts, 10);
        assert!(configured.rate_limiter.is_some());
        assert!(configured.circuit_breaker.is_some());
    }

    #[tokio::test]
    async fn test_configured_sink_open() {
        let sink = MockSinkConnector::new();
        let mut configured = SinkConnectorBuilder::new(sink)
            .config(ConnectorConfig::new("mock"))
            .build();

        configured.open().await.unwrap();
    }
}
