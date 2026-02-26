use std::path::PathBuf;

use anyhow::Result;
use core::prelude::*;
use tracing::info;

/// Index the given notes directory into the LanceDB database.
pub async fn run(notes_path: PathBuf, model_dir: PathBuf, db_path: PathBuf) -> Result<()> {
    let start = std::time::Instant::now();

    info!("scanning {}", notes_path.display());
    let files = scan_brain(&[notes_path]);
    info!(file_count = files.len(), "files found");

    let embedder = Embedder::load(&model_dir)?;

    info!("opening store at {}", db_path.display());
    let store = Store::open_or_create(&db_path).await?;

    let mut total_chunks = 0;
    for file in &files {
        let chunks = chunk_text(&file.content);
        if chunks.is_empty() {
            continue;
        }

        let texts: Vec<&str> = chunks.iter().map(|c| c.content.as_str()).collect();
        let embeddings = embedder.embed_batch(&texts)?;

        let chunk_pairs: Vec<(usize, &str)> =
            chunks.iter().map(|c| (c.ord, c.content.as_str())).collect();

        store
            .insert_chunks(
                file.path.to_str().unwrap_or("unknown"),
                &chunk_pairs,
                &embeddings,
            )
            .await?;

        total_chunks += chunks.len();
    }

    let elapsed = start.elapsed();
    info!(
        total_chunks,
        elapsed_ms = elapsed.as_millis(),
        "indexing complete"
    );

    Ok(())
}
