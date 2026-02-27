use std::path::PathBuf;

use anyhow::Result;
use brain_lib::prelude::*;

/// Query the LanceDB database for top-k results for the given input.
pub async fn run(query: String, top_k: usize, model_dir: PathBuf, db_path: PathBuf) -> Result<()> {
    let embedder = Embedder::load(&model_dir)?;
    let store = Store::open_or_create(&db_path).await?;

    let query_embedding = embedder.embed_batch(&[query.as_str()])?;
    let results = store.query(&query_embedding[0], top_k).await?;

    if results.is_empty() {
        println!("No results found.");
    } else {
        for (rank, result) in results.iter().enumerate() {
            let snippet: String = result.content.chars().take(200).collect();
            let score = result
                .score
                .map(|s| format!("{s:.4}"))
                .unwrap_or_else(|| "n/a".to_string());

            println!("#{} [score: {}]", rank + 1, score);
            println!("  file: {} (chunk {})", result.file_path, result.chunk_ord);
            println!("  {snippet}");
            println!();
        }
    }

    Ok(())
}
