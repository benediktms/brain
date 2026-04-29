//! Runtime metrics for observability.
//!
//! All recording methods are lock-free (`AtomicU64`) except `LatencyRing::record`
//! which takes a `Mutex<Vec<u64>>` — acceptable since it's bounded at 1000 entries
//! and only called on the write path.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

const RING_CAPACITY: usize = 1000;

/// Fixed-size ring buffer of latency samples in microseconds.
pub struct LatencyRing {
    samples: Mutex<Vec<u64>>,
    total: AtomicU64,
}

impl LatencyRing {
    fn new() -> Self {
        Self {
            samples: Mutex::new(Vec::with_capacity(RING_CAPACITY)),
            total: AtomicU64::new(0),
        }
    }

    /// Record a duration sample.
    pub fn record(&self, d: Duration) {
        let micros = d.as_micros() as u64;
        let mut buf = self.samples.lock().unwrap();
        let count = self.total.fetch_add(1, Ordering::Relaxed) as usize;
        if buf.len() < RING_CAPACITY {
            buf.push(micros);
        } else {
            buf[count % RING_CAPACITY] = micros;
        }
    }

    /// Compute p50 and p95 from current samples (nearest-rank method).
    /// Returns `(0, 0)` if empty.
    pub fn percentiles(&self) -> (u64, u64) {
        let buf = self.samples.lock().unwrap();
        if buf.is_empty() {
            return (0, 0);
        }
        let mut sorted = buf.clone();
        sorted.sort_unstable();
        let n = sorted.len();
        let p50 = sorted[((n as f64 * 0.50).ceil() as usize).saturating_sub(1)];
        let p95 = sorted[((n as f64 * 0.95).ceil() as usize).saturating_sub(1)];
        (p50, p95)
    }

    /// Total number of samples ever recorded.
    pub fn count(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// Centralized runtime metrics (Send + Sync via atomics + Mutex).
pub struct Metrics {
    pub indexing_latency: LatencyRing,
    pub query_latency: LatencyRing,
    pub stale_hashes_prevented: AtomicU64,
    pub indexing_errors: AtomicU64,
    pub query_errors: AtomicU64,
    pub queue_depth: AtomicU64,
    pub lancedb_unoptimized_rows: AtomicU64,
    started_at: Instant,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            indexing_latency: LatencyRing::new(),
            query_latency: LatencyRing::new(),
            stale_hashes_prevented: AtomicU64::new(0),
            indexing_errors: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            lancedb_unoptimized_rows: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    pub fn record_index_latency(&self, d: Duration) {
        self.indexing_latency.record(d);
    }

    pub fn record_query_latency(&self, d: Duration) {
        self.query_latency.record(d);
    }

    pub fn record_stale_hash_prevented(&self) {
        self.stale_hashes_prevented.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_queue_depth(&self, n: u64) {
        self.queue_depth.store(n, Ordering::Relaxed);
    }

    pub fn set_lancedb_unoptimized_rows(&self, n: u64) {
        self.lancedb_unoptimized_rows.store(n, Ordering::Relaxed);
    }

    /// Produce a serializable snapshot of current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let (idx_p50, idx_p95) = self.indexing_latency.percentiles();
        let (qry_p50, qry_p95) = self.query_latency.percentiles();

        MetricsSnapshot {
            uptime_seconds: self.started_at.elapsed().as_secs(),
            indexing_latency: LatencySnapshot {
                p50_us: idx_p50,
                p95_us: idx_p95,
                total_samples: self.indexing_latency.count(),
            },
            query_latency: LatencySnapshot {
                p50_us: qry_p50,
                p95_us: qry_p95,
                total_samples: self.query_latency.count(),
            },
            stale_hashes_prevented: self.stale_hashes_prevented.load(Ordering::Relaxed),
            indexing_errors: self.indexing_errors.load(Ordering::Relaxed),
            query_errors: self.query_errors.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            lancedb_unoptimized_rows: self.lancedb_unoptimized_rows.load(Ordering::Relaxed),
            dual_store_stuck_files: 0, // filled by caller from SQLite
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    pub uptime_seconds: u64,
    pub indexing_latency: LatencySnapshot,
    pub query_latency: LatencySnapshot,
    pub stale_hashes_prevented: u64,
    pub indexing_errors: u64,
    pub query_errors: u64,
    pub queue_depth: u64,
    pub lancedb_unoptimized_rows: u64,
    pub dual_store_stuck_files: u64,
}

#[derive(Debug, Serialize)]
pub struct LatencySnapshot {
    pub p50_us: u64,
    pub p95_us: u64,
    pub total_samples: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ring_returns_zero_percentiles() {
        let ring = LatencyRing::new();
        assert_eq!(ring.percentiles(), (0, 0));
        assert_eq!(ring.count(), 0);
    }

    #[test]
    fn single_sample() {
        let ring = LatencyRing::new();
        ring.record(Duration::from_micros(42));
        assert_eq!(ring.percentiles(), (42, 42));
        assert_eq!(ring.count(), 1);
    }

    #[test]
    fn multiple_samples_sorted() {
        let ring = LatencyRing::new();
        for v in [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000] {
            ring.record(Duration::from_micros(v));
        }
        let (p50, p95) = ring.percentiles();
        assert_eq!(p50, 500); // ceil(10*0.50)-1 = 4 → sorted[4]
        assert_eq!(p95, 1000); // ceil(10*0.95)-1 = 9 → sorted[9]
    }

    #[test]
    fn two_samples_percentiles() {
        let ring = LatencyRing::new();
        ring.record(Duration::from_micros(100));
        ring.record(Duration::from_micros(200));
        let (p50, p95) = ring.percentiles();
        assert_eq!(p50, 100); // ceil(2*0.50)-1 = 0
        assert_eq!(p95, 200); // ceil(2*0.95)-1 = 1
    }

    #[test]
    fn ring_wraps_at_capacity() {
        let ring = LatencyRing::new();
        // Fill past capacity
        for i in 0..1500u64 {
            ring.record(Duration::from_micros(i));
        }
        assert_eq!(ring.count(), 1500);
        let buf = ring.samples.lock().unwrap();
        assert_eq!(buf.len(), RING_CAPACITY);
    }

    #[test]
    fn metrics_snapshot_reflects_recordings() {
        let m = Metrics::new();
        m.record_index_latency(Duration::from_micros(100));
        m.record_index_latency(Duration::from_micros(200));
        m.record_stale_hash_prevented();
        m.record_stale_hash_prevented();
        m.set_queue_depth(5);
        m.set_lancedb_unoptimized_rows(42);

        let snap = m.snapshot();
        assert!(snap.uptime_seconds < 5);
        assert_eq!(snap.indexing_latency.total_samples, 2);
        assert_eq!(snap.stale_hashes_prevented, 2);
        assert_eq!(snap.queue_depth, 5);
        assert_eq!(snap.lancedb_unoptimized_rows, 42);
        assert_eq!(snap.dual_store_stuck_files, 0);
    }
}
