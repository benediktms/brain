use std::path::PathBuf;

use anyhow::Result;
use brain_lib::ports::MaintenanceOps;
use brain_lib::prelude::*;

/// Re-index all files (clears content hashes, forces full re-embed).
pub async fn run_full(
    notes_path: PathBuf,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<()> {
    let mut pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let brain_name = db_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    if let Ok((_, brain_id)) = brain_lib::config::resolve_brain_with_fallback(None, &brain_name) {
        pipeline.set_brain_id(brain_id);
    }
    let stats = pipeline.reindex_full(&[notes_path]).await?;

    // Rebuild FTS5 summaries index after full reindex
    let summaries_count = pipeline.db().reindex_summaries_fts()?;

    println!(
        "Reindex complete: {} indexed, {} skipped, {} deleted, {} errors",
        stats.indexed, stats.skipped, stats.deleted, stats.errors
    );
    println!("FTS summaries reindexed: {summaries_count} summaries");

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
    let mut pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let brain_name = db_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    if let Ok((_, brain_id)) = brain_lib::config::resolve_brain_with_fallback(None, &brain_name) {
        pipeline.set_brain_id(brain_id);
    }
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
