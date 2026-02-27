use std::path::PathBuf;

use anyhow::Result;
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
    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let stats = pipeline.full_scan(&[notes_path]).await?;

    info!(
        indexed = stats.indexed,
        skipped = stats.skipped,
        deleted = stats.deleted,
        errors = stats.errors,
        "indexing complete"
    );

    Ok(())
}
