use std::path::PathBuf;

use anyhow::Result;
use brain_lib::prelude::*;

/// Re-index all files (clears content hashes, forces full re-embed).
pub async fn run_full(
    notes_path: PathBuf,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<()> {
    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let stats = pipeline.reindex_full(&[notes_path]).await?;

    println!(
        "Reindex complete: {} indexed, {} skipped, {} deleted, {} errors",
        stats.indexed, stats.skipped, stats.deleted, stats.errors
    );

    Ok(())
}

/// Re-index a single file.
pub async fn run_file(
    file_path: PathBuf,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<()> {
    let abs_path = file_path.canonicalize().unwrap_or(file_path.clone());
    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let indexed = pipeline.reindex_file(&abs_path).await?;

    if indexed {
        println!("Re-indexed: {}", abs_path.display());
    } else {
        println!(
            "File unchanged (empty after re-check): {}",
            abs_path.display()
        );
    }

    Ok(())
}
