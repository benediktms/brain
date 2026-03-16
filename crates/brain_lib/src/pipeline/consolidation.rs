use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::ports::{SummaryReader, SummaryWriter};
use crate::summarizer::{Summarize, summarize_async};

/// Default idle threshold: 5 minutes of no file events before consolidation starts.
const DEFAULT_IDLE_THRESHOLD: Duration = Duration::from_secs(300);
/// Default batch size per consolidation run.
const DEFAULT_BATCH_SIZE: usize = 20;

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Idle-triggered background scheduler that summarizes chunks lacking ML summaries.
///
/// Modeled after `OptimizeScheduler` in `store.rs`: uses an `AtomicU64` for
/// lock-free last-event timestamp updates, and a `Mutex<Instant>` guard to
/// prevent concurrent runs and track last-run time.
pub struct ConsolidationScheduler {
    /// Unix seconds timestamp of the last file event. Lock-free updates from
    /// the watch loop; read inside `maybe_consolidate` to gate on idle time.
    last_event_ts: Arc<AtomicU64>,
    /// Execution guard: `try_lock` prevents concurrent runs; the held `Instant`
    /// records when the previous run completed.
    guard: Mutex<Instant>,
    /// Minimum idle time (no file events) before consolidation triggers.
    idle_threshold: Duration,
    /// Maximum number of chunks to summarize per run.
    batch_size: usize,
}

impl ConsolidationScheduler {
    /// Create a scheduler with default settings.
    ///
    /// `last_event_ts` should be the same `Arc<AtomicU64>` updated by the
    /// watch loop via `record_file_event` whenever a file event arrives.
    pub fn new(last_event_ts: Arc<AtomicU64>) -> Self {
        Self {
            last_event_ts,
            guard: Mutex::new(Instant::now()),
            idle_threshold: DEFAULT_IDLE_THRESHOLD,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Create with explicit settings (useful for tests).
    pub fn with_config(
        last_event_ts: Arc<AtomicU64>,
        idle_threshold: Duration,
        batch_size: usize,
    ) -> Self {
        Self {
            last_event_ts,
            guard: Mutex::new(Instant::now()),
            idle_threshold,
            batch_size,
        }
    }

    /// Record that a file event just occurred. Called from the watch loop event
    /// branch to stamp the current Unix seconds timestamp.
    pub fn record_file_event(&self) {
        self.last_event_ts.store(now_unix_secs(), Ordering::Relaxed);
    }

    /// Returns how many seconds have elapsed since the last file event.
    /// Used for observability / dashboard metrics.
    pub fn seconds_since_last_event(&self) -> u64 {
        let last = self.last_event_ts.load(Ordering::Relaxed);
        let now = now_unix_secs();
        now.saturating_sub(last)
    }

    /// Check idle threshold and run a summarization batch if conditions are met.
    ///
    /// Returns the number of summaries generated (0 if skipped).
    ///
    /// - Skips if the system has been active within `idle_threshold`.
    /// - Skips (non-blocking) if another run is already in progress.
    /// - Yields mid-batch if new file events arrive (re-indexes take priority).
    pub async fn maybe_consolidate(
        &self,
        db: &(impl SummaryReader + SummaryWriter),
        summarizer: &Arc<dyn Summarize>,
    ) -> crate::error::Result<usize> {
        // Gate 1: idle threshold — skip if file activity is recent.
        if self.seconds_since_last_event() < self.idle_threshold.as_secs() {
            return Ok(0);
        }

        // Gate 2: concurrency guard — skip if a previous run is still going.
        let Ok(_guard) = self.guard.try_lock() else {
            return Ok(0);
        };

        // Find chunks that have no ML summary from this summarizer backend.
        let chunks = db.find_chunks_lacking_summary(summarizer.backend_name(), self.batch_size)?;

        if chunks.is_empty() {
            return Ok(0);
        }

        let ts_at_start = self.last_event_ts.load(Ordering::Relaxed);
        let mut count = 0usize;

        for (chunk_id, content) in chunks {
            // Yield to the indexer if new file events have arrived since we started.
            let current_ts = self.last_event_ts.load(Ordering::Relaxed);
            if current_ts != ts_at_start {
                debug!(
                    processed = count,
                    "consolidation: yielding to indexer — new file events detected"
                );
                break;
            }

            match summarize_async(summarizer, content).await {
                Ok(summary) => {
                    match db.store_ml_summary(&chunk_id, &summary, summarizer.backend_name()) {
                        Ok(_) => {
                            count += 1;
                        }
                        Err(e) => {
                            warn!(
                                chunk_id = %chunk_id,
                                error = %e,
                                "consolidation: failed to store summary, skipping chunk"
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        chunk_id = %chunk_id,
                        error = %e,
                        "consolidation: summarizer failed for chunk, skipping"
                    );
                }
            }
        }

        if count > 0 {
            info!(processed = count, "consolidation: summarized chunks");
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use super::*;
    use crate::db::Db;
    use crate::db::summaries::store_ml_summary;
    use crate::summarizer::MockSummarizer;

    fn insert_chunk(db: &Db, chunk_id: &str, file_id: &str, content: &str) {
        db.with_write_conn(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO files (file_id, path, indexing_state) VALUES (?1, ?1, 'idle')",
                [file_id],
            )?;
            conn.execute(
                "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content, heading_path, byte_start, byte_end, token_estimate)
                 VALUES (?1, ?2, 0, '', ?3, '', 0, 0, 0)",
                rusqlite::params![chunk_id, file_id, content],
            )?;
            Ok(())
        })
        .unwrap();
    }

    #[tokio::test]
    async fn record_file_event_updates_timestamp() {
        let ts = Arc::new(AtomicU64::new(0));
        let scheduler = ConsolidationScheduler::new(Arc::clone(&ts));

        scheduler.record_file_event();
        let elapsed = scheduler.seconds_since_last_event();
        // Should be 0 or 1 seconds (just recorded)
        assert!(
            elapsed <= 1,
            "elapsed should be <= 1s after recording, got {elapsed}"
        );
    }

    #[tokio::test]
    async fn maybe_consolidate_skips_when_idle_threshold_not_met() {
        let ts = Arc::new(AtomicU64::new(now_unix_secs())); // very recent event
        // Use a 1-hour idle threshold so we definitely don't trigger
        let scheduler =
            ConsolidationScheduler::with_config(Arc::clone(&ts), Duration::from_secs(3600), 10);

        let db = Db::open_in_memory().unwrap();
        let summarizer: Arc<dyn Summarize> = Arc::new(MockSummarizer);

        let count = scheduler.maybe_consolidate(&db, &summarizer).await.unwrap();
        assert_eq!(count, 0, "should skip because idle threshold not met");
    }

    #[tokio::test]
    async fn maybe_consolidate_processes_chunks_and_stores_summaries() {
        // Set last_event_ts to far in the past so idle threshold is met.
        let ts = Arc::new(AtomicU64::new(0));
        let scheduler = ConsolidationScheduler::with_config(
            Arc::clone(&ts),
            Duration::from_secs(1), // 1s threshold — easily met with ts=0
            10,
        );

        let db = Db::open_in_memory().unwrap();
        insert_chunk(&db, "chunk:1", "file:1", "content of chunk one");
        insert_chunk(&db, "chunk:2", "file:1", "content of chunk two");

        let summarizer: Arc<dyn Summarize> = Arc::new(MockSummarizer);

        let count = scheduler.maybe_consolidate(&db, &summarizer).await.unwrap();
        assert_eq!(count, 2, "should summarize both chunks");

        // Verify summaries are stored
        let map = db
            .with_read_conn(|conn| {
                crate::db::summaries::get_ml_summaries_for_chunks(conn, &["chunk:1", "chunk:2"])
            })
            .unwrap();
        assert!(
            map.contains_key("chunk:1"),
            "summary for chunk:1 should be stored"
        );
        assert!(
            map.contains_key("chunk:2"),
            "summary for chunk:2 should be stored"
        );
    }

    #[tokio::test]
    async fn maybe_consolidate_skips_already_summarized_chunks() {
        let ts = Arc::new(AtomicU64::new(0));
        let scheduler =
            ConsolidationScheduler::with_config(Arc::clone(&ts), Duration::from_secs(1), 10);

        let db = Db::open_in_memory().unwrap();
        insert_chunk(&db, "chunk:1", "file:1", "content one");

        // Pre-store a summary for chunk:1
        db.with_write_conn(|conn| store_ml_summary(conn, "chunk:1", "existing summary", "mock"))
            .unwrap();

        let summarizer: Arc<dyn Summarize> = Arc::new(MockSummarizer);

        let count = scheduler.maybe_consolidate(&db, &summarizer).await.unwrap();
        assert_eq!(count, 0, "chunk:1 already has a summary — nothing to do");
    }
}
