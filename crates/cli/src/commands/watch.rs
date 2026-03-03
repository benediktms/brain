use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use brain_lib::prelude::*;
use brain_lib::watcher::coalesce_events;
use tracing::info;

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

    // Event loop: batch-drain, coalesce, and dispatch
    loop {
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
                // Final optimize before exit
                pipeline.store().optimizer().force_optimize().await;
                break;
            }
        }
    }

    Ok(())
}
