//! Common metrics for Lakehouse sink connectors.
//!
//! Provides shared atomic counters for tracking statistics across different
//! table formats (Delta Lake, Iceberg, Hudi, Paimon).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for Lakehouse sink connector statistics.
#[derive(Debug)]
pub struct LakehouseSinkMetrics {
    /// Total rows flushed to storage (Parquet/ORC/etc.).
    pub rows_flushed: AtomicU64,

    /// Total bytes written to storage.
    pub bytes_written: AtomicU64,

    /// Total number of flush operations.
    pub flush_count: AtomicU64,

    /// Total number of commits (transactions/snapshots).
    pub commits: AtomicU64,

    /// Total errors encountered.
    pub errors_total: AtomicU64,

    /// Total epochs rolled back.
    pub epochs_rolled_back: AtomicU64,

    /// Total changelog DELETE operations processed.
    pub changelog_deletes: AtomicU64,
}

impl LakehouseSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows_flushed: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            epochs_rolled_back: AtomicU64::new(0),
            changelog_deletes: AtomicU64::new(0),
        }
    }

    /// Records a successful flush of `records` rows totaling `bytes`.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.rows_flushed.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful commit (transaction/snapshot).
    pub fn record_commit(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a write or I/O error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Records changelog DELETE operations processed.
    pub fn record_deletes(&self, count: u64) {
        self.changelog_deletes.fetch_add(count, Ordering::Relaxed);
    }

    /// Populates standard `ConnectorMetrics` fields and adds common custom metrics with a prefix.
    ///
    /// The `prefix` is used for custom metrics keys, e.g., `"{prefix}.flush_count"`.
    #[allow(clippy::cast_precision_loss)]
    pub fn populate_metrics(&self, metrics: &mut ConnectorMetrics, prefix: &str) {
        metrics.records_total = self.rows_flushed.load(Ordering::Relaxed);
        metrics.bytes_total = self.bytes_written.load(Ordering::Relaxed);
        metrics.errors_total = self.errors_total.load(Ordering::Relaxed);

        metrics.add_custom(
            format!("{prefix}.flush_count"),
            self.flush_count.load(Ordering::Relaxed) as f64,
        );
        metrics.add_custom(
            format!("{prefix}.commits"),
            self.commits.load(Ordering::Relaxed) as f64,
        );
        metrics.add_custom(
            format!("{prefix}.epochs_rolled_back"),
            self.epochs_rolled_back.load(Ordering::Relaxed) as f64,
        );
        metrics.add_custom(
            format!("{prefix}.changelog_deletes"),
            self.changelog_deletes.load(Ordering::Relaxed) as f64,
        );
    }
}

impl Default for LakehouseSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}
