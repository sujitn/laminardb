//! Metrics for the connector bridge layer.
//!
//! Provides atomic counters for source bridges, sink bridges, and the
//! overall bridge runtime. All counters use relaxed ordering since they
//! are advisory and do not participate in synchronization.

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for a [`DagSourceBridge`](super::DagSourceBridge).
#[derive(Debug, Default)]
pub struct SourceBridgeMetrics {
    /// Number of batches polled from the source connector.
    pub batches_polled: AtomicU64,
    /// Total events ingested into the DAG executor.
    pub events_ingested: AtomicU64,
    /// Total bytes ingested (approximate, from `RecordBatch` memory size).
    pub bytes_ingested: AtomicU64,
    /// Number of polls that returned no data.
    pub poll_empty_count: AtomicU64,
    /// Number of errors encountered during polling.
    pub errors: AtomicU64,
}

impl SourceBridgeMetrics {
    /// Creates a new zeroed metrics instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        self.batches_polled.store(0, Ordering::Relaxed);
        self.events_ingested.store(0, Ordering::Relaxed);
        self.bytes_ingested.store(0, Ordering::Relaxed);
        self.poll_empty_count.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }
}

/// Metrics for a [`DagSinkBridge`](super::DagSinkBridge).
#[derive(Debug, Default)]
pub struct SinkBridgeMetrics {
    /// Number of batches written to the sink connector.
    pub batches_written: AtomicU64,
    /// Total events sent to the sink.
    pub events_output: AtomicU64,
    /// Total bytes written (approximate, from `RecordBatch` memory size).
    pub bytes_output: AtomicU64,
    /// Number of epochs successfully committed.
    pub epochs_committed: AtomicU64,
    /// Number of epochs rolled back.
    pub epochs_rolled_back: AtomicU64,
    /// Number of errors encountered during writing.
    pub errors: AtomicU64,
}

impl SinkBridgeMetrics {
    /// Creates a new zeroed metrics instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        self.batches_written.store(0, Ordering::Relaxed);
        self.events_output.store(0, Ordering::Relaxed);
        self.bytes_output.store(0, Ordering::Relaxed);
        self.epochs_committed.store(0, Ordering::Relaxed);
        self.epochs_rolled_back.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }
}

/// Metrics for the [`ConnectorBridgeRuntime`](super::ConnectorBridgeRuntime).
#[derive(Debug, Default)]
pub struct BridgeRuntimeMetrics {
    /// Number of process cycles completed.
    pub cycles_completed: AtomicU64,
    /// Total events ingested across all sources.
    pub total_events_in: AtomicU64,
    /// Total events output across all sinks.
    pub total_events_out: AtomicU64,
    /// Number of checkpoints completed.
    pub checkpoints_completed: AtomicU64,
    /// Duration of the last checkpoint in microseconds.
    pub last_checkpoint_duration_us: AtomicU64,
    /// Number of recovery operations performed.
    pub recoveries: AtomicU64,
}

impl BridgeRuntimeMetrics {
    /// Creates a new zeroed metrics instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        self.cycles_completed.store(0, Ordering::Relaxed);
        self.total_events_in.store(0, Ordering::Relaxed);
        self.total_events_out.store(0, Ordering::Relaxed);
        self.checkpoints_completed.store(0, Ordering::Relaxed);
        self.last_checkpoint_duration_us.store(0, Ordering::Relaxed);
        self.recoveries.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_metrics_default_zero() {
        let m = SourceBridgeMetrics::new();
        assert_eq!(m.batches_polled.load(Ordering::Relaxed), 0);
        assert_eq!(m.events_ingested.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_ingested.load(Ordering::Relaxed), 0);
        assert_eq!(m.poll_empty_count.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_sink_metrics_default_zero() {
        let m = SinkBridgeMetrics::new();
        assert_eq!(m.batches_written.load(Ordering::Relaxed), 0);
        assert_eq!(m.events_output.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_output.load(Ordering::Relaxed), 0);
        assert_eq!(m.epochs_committed.load(Ordering::Relaxed), 0);
        assert_eq!(m.epochs_rolled_back.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_runtime_metrics_default_zero() {
        let m = BridgeRuntimeMetrics::new();
        assert_eq!(m.cycles_completed.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_events_in.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_events_out.load(Ordering::Relaxed), 0);
        assert_eq!(m.checkpoints_completed.load(Ordering::Relaxed), 0);
        assert_eq!(m.last_checkpoint_duration_us.load(Ordering::Relaxed), 0);
        assert_eq!(m.recoveries.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_metrics_increment_and_reset() {
        let m = SourceBridgeMetrics::new();
        m.batches_polled.fetch_add(5, Ordering::Relaxed);
        m.events_ingested.fetch_add(100, Ordering::Relaxed);
        m.bytes_ingested.fetch_add(4096, Ordering::Relaxed);
        m.poll_empty_count.fetch_add(2, Ordering::Relaxed);
        m.errors.fetch_add(1, Ordering::Relaxed);

        assert_eq!(m.batches_polled.load(Ordering::Relaxed), 5);
        assert_eq!(m.events_ingested.load(Ordering::Relaxed), 100);
        assert_eq!(m.bytes_ingested.load(Ordering::Relaxed), 4096);
        assert_eq!(m.poll_empty_count.load(Ordering::Relaxed), 2);
        assert_eq!(m.errors.load(Ordering::Relaxed), 1);

        m.reset();

        assert_eq!(m.batches_polled.load(Ordering::Relaxed), 0);
        assert_eq!(m.events_ingested.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_ingested.load(Ordering::Relaxed), 0);
        assert_eq!(m.poll_empty_count.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_sink_metrics_increment_and_reset() {
        let m = SinkBridgeMetrics::new();
        m.batches_written.fetch_add(3, Ordering::Relaxed);
        m.events_output.fetch_add(50, Ordering::Relaxed);
        m.epochs_committed.fetch_add(2, Ordering::Relaxed);
        m.epochs_rolled_back.fetch_add(1, Ordering::Relaxed);

        assert_eq!(m.batches_written.load(Ordering::Relaxed), 3);
        assert_eq!(m.epochs_committed.load(Ordering::Relaxed), 2);
        assert_eq!(m.epochs_rolled_back.load(Ordering::Relaxed), 1);

        m.reset();
        assert_eq!(m.batches_written.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_runtime_metrics_increment_and_reset() {
        let m = BridgeRuntimeMetrics::new();
        m.cycles_completed.fetch_add(10, Ordering::Relaxed);
        m.total_events_in.fetch_add(1000, Ordering::Relaxed);
        m.total_events_out.fetch_add(900, Ordering::Relaxed);
        m.checkpoints_completed.fetch_add(3, Ordering::Relaxed);
        m.last_checkpoint_duration_us.store(500, Ordering::Relaxed);
        m.recoveries.fetch_add(1, Ordering::Relaxed);

        assert_eq!(m.cycles_completed.load(Ordering::Relaxed), 10);
        assert_eq!(m.total_events_in.load(Ordering::Relaxed), 1000);
        assert_eq!(m.last_checkpoint_duration_us.load(Ordering::Relaxed), 500);

        m.reset();
        assert_eq!(m.cycles_completed.load(Ordering::Relaxed), 0);
        assert_eq!(m.last_checkpoint_duration_us.load(Ordering::Relaxed), 0);
    }
}
