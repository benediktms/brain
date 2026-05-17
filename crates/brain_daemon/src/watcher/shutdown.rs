//! Shutdown bookkeeping for the watcher supervisor — outcome struct,
//! reason enum, and the bounded drain helper.

use std::time::Duration;

use brain_lib::pipeline::IndexPipeline;
use brain_lib::prelude::WorkQueue;

/// Outcome of the watch shutdown sequence.
pub struct ShutdownOutcome {
    /// Whether shutdown completed all phases cleanly.
    pub clean: bool,
    /// Number of work-queue items that were not processed.
    pub dropped_items: usize,
}

/// Why the event loop exited.
pub enum ShutdownReason {
    /// The watcher channel closed (watcher dropped or errored).
    ChannelClosed,
    /// Received SIGINT (Ctrl+C) or SIGTERM.
    Signal,
}

/// Drain remaining work-queue items through the pipeline within a timeout.
///
/// Returns `Ok(processed_count)` on success, or `Err(remaining_count)` if the
/// timeout expires before all items are processed.
pub async fn drain_with_timeout(
    pipeline: &IndexPipeline,
    work_queue: &mut WorkQueue,
    timeout: Duration,
) -> std::result::Result<usize, usize> {
    let result = tokio::time::timeout(timeout, async {
        let (renames, index_paths, delete_paths) = work_queue.drain_batch();
        let mut processed = 0;

        for (from, to) in &renames {
            if let Err(e) = pipeline.rename_file(from, to).await {
                tracing::warn!(error = %e, "error handling rename during drain");
            }
            processed += 1;
        }

        for p in &delete_paths {
            if let Err(e) = pipeline.delete_file(p).await {
                tracing::warn!(error = %e, "error handling delete during drain");
            }
            processed += 1;
        }

        if !index_paths.is_empty() {
            if let Err(e) = pipeline.index_files_batch(&index_paths).await {
                tracing::warn!(error = %e, "error in batch index during drain");
            }
            processed += index_paths.len();
        }

        processed
    })
    .await;

    match result {
        Ok(processed) => Ok(processed),
        Err(_) => Err(work_queue.len()),
    }
}
