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
    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let report = pipeline.doctor(&[notes_path]).await?;

    println!("{report}");

    if !report.is_healthy() {
        std::process::exit(1);
    }

    Ok(())
}
