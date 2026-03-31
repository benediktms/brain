use std::path::PathBuf;

use anyhow::Result;
use brain_lib::prelude::*;

/// Run diagnostic checks on the index and print a report.
pub async fn run(
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
    let report = pipeline.doctor(&[notes_path]).await?;

    println!("{report}");

    if !report.is_healthy() {
        std::process::exit(1);
    }

    Ok(())
}
