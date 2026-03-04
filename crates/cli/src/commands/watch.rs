use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use brain_lib::prelude::*;
use brain_lib::watcher::coalesce_events;
use tracing::info;

// The daemon (daemon.rs) uses libc and sends SIGTERM — unix-only.
use tokio::signal::unix::SignalKind;

/// Watch a directory for changes and re-index incrementally.
pub async fn run(
    notes_path: PathBuf,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<()> {
    info!("starting brain watch on {}", notes_path.display());

    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;

    // Full scan on startup to catch offline changes
    let stats = pipeline
        .full_scan(std::slice::from_ref(&notes_path))
        .await?;
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
    let _watcher = brain_lib::watcher::BrainWatcher::new(&[notes_path], tx)?;

    info!("watching for changes... (press Ctrl+C to stop)");

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
    loop {
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
                        let mut raw = vec![first];
                        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
                        while let Ok(Some(evt)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                            raw.push(evt);
                        }

                        // 2. Coalesce
                        let (renames, index_paths, delete_paths) = coalesce_events(raw);

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
                        break;
                    }
                }
            }
            _ = optimize_tick.tick() => {
                // Check time-elapsed trigger during quiet periods
                pipeline.store().optimizer().maybe_optimize().await;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("received Ctrl+C, shutting down");
                pipeline.store().optimizer().force_optimize().await;
                break;
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                pipeline.store().optimizer().force_optimize().await;
                break;
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
    }

    Ok(())
}
