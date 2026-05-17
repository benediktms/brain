use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use brain_lib::config::paths::normalize_note_paths_lenient;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::pipeline::embed_poll;
use brain_lib::pipeline::job_worker;
use brain_lib::pipeline::recurring_jobs;
use brain_lib::prelude::*;
use tracing::info;

// The daemon (daemon.rs) uses libc and sends SIGTERM — unix-only.
use tokio::signal::unix::SignalKind;

use brain_daemon::watcher::shutdown::{ShutdownOutcome, ShutdownReason, drain_with_timeout};

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

    let mut pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;

    // Resolve LLM provider: env vars first, then DB-backed credentials.
    let brain_home = brain_lib::config::brain_home().unwrap_or_else(|_| PathBuf::from("."));
    if let Some(provider) =
        brain_lib::llm::resolve_provider_with_db(pipeline.provider_store(), &brain_home)
    {
        pipeline.set_summarizer(Arc::from(provider));
    }

    // Startup self-heal: if LanceDB is missing, reset embedded_at so all
    // tasks and chunks will be re-embedded on the next EmbedPollSweep job.
    embed_poll::self_heal_if_lance_missing(pipeline.embedding_resetter(), pipeline.store()).await;

    // Startup compaction: merge any historical LanceDB fragments left over
    // from prior runs. The in-memory pending_mutations counter resets on
    // restart, so without this call `maybe_optimize` would never trigger
    // for pre-existing fragment debt — fragments would accumulate forever.
    pipeline.store().optimizer().startup_compact().await;

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

    // Embedding is now handled by the EmbedPollSweep recurring job.

    // Summarization job poll: reaps stuck jobs, processes ready jobs, GC's old ones.
    // 60s (was 30s); 30s spawned concurrent tokio tasks too aggressively on
    // low-activity brains. 60s is sufficient for job processing latency.
    let mut summarize_poll_interval = tokio::time::interval(Duration::from_secs(60));

    // In-memory lock set: prevents the reaper from resetting jobs that are
    // still actively running in a tokio::spawn task.
    let active_jobs = job_worker::ActiveJobs::new(job_worker::DEFAULT_MAX_CONCURRENT_JOBS);

    // SIGTERM handler — the daemon sends SIGTERM on `brain stop`.
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    // SIGUSR1 handler — dump metrics snapshot to stderr
    let mut sigusr1 = tokio::signal::unix::signal(SignalKind::user_defined1())
        .expect("failed to register SIGUSR1 handler");

    // Event loop: batch-drain, coalesce, and dispatch
    let shutdown_reason = loop {
        // Update queue depth + lancedb pending rows + compaction failure
        // counter at top of each iteration. Polling the failure counter
        // (incremented inside run_optimize on Err) surfaces silent compaction
        // failures via `brain status` rather than only in warn-level logs.
        pipeline.metrics().set_queue_depth(rx.len() as u64);
        pipeline
            .metrics()
            .set_lancedb_unoptimized_rows(pipeline.store().optimizer().pending_count());
        pipeline
            .metrics()
            .set_lancedb_optimize_failures(pipeline.store().optimizer().optimize_failure_count());

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
            _ = summarize_poll_interval.tick() => {
                // Reconcile recurring singleton jobs (idempotent).
                let brain_infos = vec![recurring_jobs::BrainInfo {
                    brain_id: String::new(),
                }];
                if let Err(e) = recurring_jobs::reconcile_recurring_jobs(pipeline.job_queue(), &brain_infos) {
                    tracing::warn!(error = %e, "reconcile_recurring_jobs failed");
                }

                if let Err(e) = job_worker::reap_stuck_jobs_filtered(pipeline.job_queue(), &active_jobs) {
                    tracing::warn!(error = %e, "reap_stuck_jobs failed");
                }
                let pipeline_db = pipeline.clone_db_for_spawn();
                let n = job_worker::process_jobs(
                    &pipeline_db,
                    pipeline.store(),
                    pipeline.embedder(),
                    &active_jobs,
                    20,
                ).await;
                if n > 0 {
                    info!(processed = n, "jobs dispatched");
                }
                let protected = recurring_jobs::protected_kinds();
                if let Err(e) = pipeline.gc_completed_jobs(7 * 86400, &protected) {
                    tracing::warn!(error = %e, "gc_completed_jobs failed");
                }
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
                let stuck_files = pipeline.find_stuck_files().unwrap_or_default();
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
        if let Err(e) = pipeline.wal_checkpoint() {
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
        dropped_items, "shutdown phase 5/5: shutdown complete"
    );

    Ok(ShutdownOutcome {
        clean,
        dropped_items,
    })
}

// ── Multi-brain support ────────────────────────────────────────────────────

/// Watch all registered brains from the global config for changes and
/// re-index incrementally.
///
/// Thin shim over `brain_daemon::watcher::Supervisor::bootstrap_and_run`. The
/// cli path doesn't dispatch `ControlMessage`; the control channel is wired up
/// by `brain-daemon`'s RPC layer instead. `control_tx` is kept alive for the
/// supervisor's entire lifetime so the receiver never returns `None` and the
/// supervisor's `ChannelClosed` shutdown arm never fires here. Shutdown is
/// driven by the supervisor's own signal handlers.
pub async fn run_multi() -> Result<ShutdownOutcome> {
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(64);
    let outcome = brain_daemon::watcher::Supervisor::bootstrap_and_run(control_rx)
        .await
        .map_err(|e| anyhow::anyhow!("supervisor: {e}"))?;
    drop(control_tx);
    Ok(outcome)
}
