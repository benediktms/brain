use std::path::PathBuf;

use anyhow::Result;
use brain_lib::config::paths::normalize_note_paths;
use brain_lib::prelude::*;
use tracing::info;

/// Index the given notes directory into the LanceDB database.
///
/// This will be rewritten in Step 5 to use IndexPipeline with SQLite-backed
/// incremental indexing. For now it uses the pipeline stub.
pub async fn run(
    notes_path: PathBuf,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<()> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    let note_dirs = normalize_note_paths(&[notes_path], &cwd)?;

    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let stats = pipeline.full_scan(&note_dirs).await?;

    // Compact all fragments created during the full scan
    pipeline.store().optimizer().force_optimize().await;

    info!(
        indexed = stats.indexed,
        skipped = stats.skipped,
        deleted = stats.deleted,
        errors = stats.errors,
        "indexing complete"
    );

    Ok(())
}
