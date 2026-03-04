use std::path::PathBuf;

use anyhow::Result;
use brain_lib::prelude::*;

/// Run vacuum: purge old deleted files, SQLite VACUUM, LanceDB optimize.
pub async fn run(
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
    older_than_days: u32,
) -> Result<()> {
    let pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;
    let stats = pipeline.vacuum(older_than_days).await?;

    println!(
        "Vacuum complete: {} deleted files purged",
        stats.purged_files
    );
    println!("SQLite VACUUM and LanceDB optimize done.");

    Ok(())
}
