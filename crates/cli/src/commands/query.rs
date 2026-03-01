use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use brain_lib::prelude::*;

/// Query the LanceDB database for top-k results for the given input.
pub async fn run(
    query: String,
    top_k: usize,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<()> {
    let embedder = Embedder::load(&model_dir)?;
    let store = Store::open_or_create(&db_path).await?;
    let db = brain_lib::db::Db::open(&sqlite_path)?;

    let query_embedding = embedder.embed_batch(&[query.as_str()])?;
    let results = store.query(&query_embedding[0], top_k).await?;

    if results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    // Enrich results with heading_path from SQLite
    let chunk_ids: Vec<String> = results.iter().map(|r| r.chunk_id.clone()).collect();
    let chunk_rows =
        db.with_conn(|conn| brain_lib::db::chunks::get_chunks_by_ids(conn, &chunk_ids));
    let heading_map: HashMap<String, String> = chunk_rows
        .map(|rows| {
            rows.into_iter()
                .map(|r| (r.chunk_id.clone(), r.heading_path.clone()))
                .collect()
        })
        .unwrap_or_default();

    for (rank, result) in results.iter().enumerate() {
        let snippet: String = result.content.chars().take(200).collect();
        let score = result
            .score
            .map(|s| format!("{s:.4}"))
            .unwrap_or_else(|| "n/a".to_string());
        let heading = heading_map.get(&result.chunk_id).filter(|h| !h.is_empty());

        println!("#{} [score: {}]", rank + 1, score);
        if let Some(heading_path) = heading {
            println!(
                "  file: {} (chunk {}) | {}",
                result.file_path, result.chunk_ord, heading_path
            );
        } else {
            println!("  file: {} (chunk {})", result.file_path, result.chunk_ord);
        }
        println!("  {snippet}");
        println!();
    }

    Ok(())
}
