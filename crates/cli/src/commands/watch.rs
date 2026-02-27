use std::path::PathBuf;

use anyhow::Result;
use brain_lib::prelude::*;
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

    // Set up file watcher
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let _watcher = brain_lib::watcher::BrainWatcher::new(&[notes_path], tx)?;

    info!("watching for changes... (press Ctrl+C to stop)");

    // Event loop: process file events or shutdown on signal
    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(evt) => {
                        if let Err(e) = pipeline.handle_event(evt).await {
                            tracing::warn!(error = %e, "error handling file event");
                        }
                    }
                    None => {
                        info!("watcher channel closed, shutting down");
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("received Ctrl+C, shutting down");
                break;
            }
        }
    }

    Ok(())
}
