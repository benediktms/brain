use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use brain_lib::config::paths::normalize_note_paths_lenient;
use brain_lib::prelude::*;
use tracing::info;

// The daemon (daemon.rs) uses libc and sends SIGTERM — unix-only.
use tokio::signal::unix::SignalKind;

/// Outcome of the watch shutdown sequence.
#[allow(dead_code)]
pub struct ShutdownOutcome {
    /// Whether shutdown completed all phases cleanly.
    pub clean: bool,
    /// Number of work-queue items that were not processed.
    pub dropped_items: usize,
}

/// Why the event loop exited.
enum ShutdownReason {
    /// The watcher channel closed (watcher dropped or errored).
    ChannelClosed,
    /// Received SIGINT (Ctrl+C) or SIGTERM.
    Signal,
}

/// Watch a directory for changes and re-index incrementally.
pub async fn run(
    notes_path: PathBuf,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<ShutdownOutcome> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    let note_dirs = normalize_note_paths_lenient(&[notes_path], &cwd);

    info!("starting brain watch on {:?}", note_dirs);

    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;

    // Full scan on startup to catch offline changes
    let stats = pipeline.full_scan(&note_dirs).await?;
    info!(
        indexed = stats.indexed,
        skipped = stats.skipped,
        deleted = stats.deleted,
        "startup scan complete"
    );

    // Compact fragments created during full scan
    pipeline.store().optimizer().force_optimize().await;

    // Set up file watcher
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let _watcher = brain_lib::watcher::BrainWatcher::new(&note_dirs, tx)?;

    info!("watching for changes... (press Ctrl+C to stop)");

    // Work queue with file_id coalescing and bounded capacity.
    let mut work_queue = WorkQueue::default();

    // Periodic tick to check time-elapsed optimize trigger during quiet periods.
    // The check is near-free (two atomic loads + one mutex read); the 5min
    // threshold is evaluated inside should_optimize().
    let mut optimize_tick = tokio::time::interval(Duration::from_secs(60));

    // SIGTERM handler — the daemon sends SIGTERM on `brain stop` (daemon.rs:75)
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    // SIGUSR1 handler — dump metrics snapshot to stderr
    let mut sigusr1 = tokio::signal::unix::signal(SignalKind::user_defined1())
        .expect("failed to register SIGUSR1 handler");

    // Event loop: batch-drain, coalesce, and dispatch
    let shutdown_reason = loop {
        // Update queue depth + lancedb pending rows at top of each iteration
        pipeline.metrics().set_queue_depth(rx.len() as u64);
        pipeline
            .metrics()
            .set_lancedb_unoptimized_rows(pipeline.store().optimizer().pending_count());

        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(first) => {
                        // 1. Drain: collect first + any ready events (50ms window)
                        work_queue.push(first);
                        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
                        while let Ok(Some(evt)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                            work_queue.push(evt);
                        }

                        // 2. Coalesce via work queue (deduped, bounded)
                        let (renames, index_paths, delete_paths) = work_queue.drain_batch();

                        // 3. Process renames
                        for (from, to) in &renames {
                            if let Err(e) = pipeline.rename_file(from, to).await {
                                tracing::warn!(error = %e, "error handling rename");
                            }
                        }

                        // 4. Process deletes
                        for p in &delete_paths {
                            if let Err(e) = pipeline.delete_file(p).await {
                                tracing::warn!(error = %e, "error handling delete");
                            }
                        }

                        // 5. Batch-index all changed/created files
                        if !index_paths.is_empty()
                            && let Err(e) = pipeline.index_files_batch(&index_paths).await
                        {
                            tracing::warn!(error = %e, "error in batch index");
                        }

                        // 6. Check row-count trigger after each batch
                        pipeline.store().optimizer().maybe_optimize().await;
                    }
                    None => {
                        info!("watcher channel closed, shutting down");
                        break ShutdownReason::ChannelClosed;
                    }
                }
            }
            _ = optimize_tick.tick() => {
                // Check time-elapsed trigger during quiet periods
                pipeline.store().optimizer().maybe_optimize().await;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("received Ctrl+C, shutting down");
                break ShutdownReason::Signal;
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                break ShutdownReason::Signal;
            }
            _ = sigusr1.recv() => {
                let mut snapshot = pipeline.metrics().snapshot();
                // Enrich with stuck-file count via brain_lib's Db wrapper
                let stuck_files = pipeline.db()
                    .with_read_conn(brain_lib::db::files::find_stuck_files)
                    .unwrap_or_default();
                snapshot.dual_store_stuck_files = stuck_files.len() as u64;
                eprintln!("{}", serde_json::to_string_pretty(&snapshot).unwrap_or_default());
            }
        }
    };

    // ── Shutdown sequence ──────────────────────────────────────────

    // Phase 1: Stop watcher — no new events enter the channel.
    info!("shutdown phase 1/5: stopping file watcher");
    drop(_watcher);

    // Phase 2: Drain pending work (signal shutdown only).
    let mut dropped_items: usize = 0;
    let mut force_shutdown = false;

    if matches!(shutdown_reason, ShutdownReason::Signal) {
        info!("shutdown phase 2/5: draining pending work queue (10s timeout)");

        // Collect any remaining channel events into the work queue.
        while let Ok(evt) = rx.try_recv() {
            work_queue.push(evt);
        }

        if !work_queue.is_empty() {
            let queued = work_queue.len();
            info!(queued, "processing remaining queued items");

            // Race drain processing against a second Ctrl+C for force-shutdown.
            let drain_result = tokio::select! {
                result = drain_with_timeout(&pipeline, &mut work_queue, Duration::from_secs(10)) => {
                    result
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("received second Ctrl+C, force-shutting down");
                    force_shutdown = true;
                    Err(work_queue.len())
                }
            };

            match drain_result {
                Ok(processed) => {
                    info!(processed, "drain complete");
                }
                Err(remaining) => {
                    dropped_items = remaining;
                    tracing::warn!(dropped_items, "drain incomplete, items dropped");
                }
            }
        } else {
            info!("no pending items to drain");
        }
    } else {
        info!("shutdown phase 2/5: channel closed, skipping drain");
    }

    if !force_shutdown {
        // Phase 3: SQLite WAL checkpoint.
        info!("shutdown phase 3/5: checkpointing SQLite WAL");
        if let Err(e) = pipeline.db().wal_checkpoint() {
            tracing::warn!(error = %e, "WAL checkpoint failed");
        }

        // Phase 4: LanceDB optimize.
        info!("shutdown phase 4/5: optimizing LanceDB");
        pipeline.store().optimizer().force_optimize().await;
    } else {
        info!("shutdown phases 3-5: skipped (force shutdown)");
    }

    // Phase 5: Done.
    let clean = !force_shutdown && dropped_items == 0;
    info!(
        clean,
        dropped_items,
        "shutdown phase 5/5: shutdown complete"
    );

    Ok(ShutdownOutcome {
        clean,
        dropped_items,
    })
}

/// Drain remaining work-queue items through the pipeline within a timeout.
///
/// Returns `Ok(processed_count)` on success, or `Err(remaining_count)` if the
/// timeout expires before all items are processed.
async fn drain_with_timeout(
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
