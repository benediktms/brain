use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use brain_lib::embedder::{Embed, Embedder};
use brain_lib::metrics::Metrics;
use brain_lib::prelude::*;
use brain_lib::query_pipeline::QueryPipeline;

/// Query the knowledge base using full hybrid search (vector + FTS + ranking).
pub async fn run(
    query: String,
    top_k: usize,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<()> {
    let embedder = Embedder::load(&model_dir)?;
    let embedder_arc: Arc<dyn Embed> = Arc::new(embedder);
    let store = Store::open_or_create(&db_path).await?;
    let store_reader = brain_lib::store::StoreReader::from_store(&store);
    let db = brain_lib::db::Db::open(&sqlite_path)?;

    let metrics = Arc::new(Metrics::new());
    let pipeline = QueryPipeline::new(&db, &store_reader, &embedder_arc, &metrics);
    let search_result = pipeline.search(&query, "auto", 800, top_k).await?;

    if search_result.results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    for (rank, stub) in search_result.results.iter().enumerate() {
        println!("#{} [score: {:.4}]", rank + 1, stub.hybrid_score);
        if !stub.heading_path.is_empty() {
            println!("  file: {} | {}", stub.file_path, stub.heading_path);
        } else {
            println!("  file: {}", stub.file_path);
        }
        println!("  {}", stub.title);
        if !stub.summary_2sent.is_empty() {
            println!("  {}", stub.summary_2sent);
        }
        println!();
    }

    Ok(())
}
