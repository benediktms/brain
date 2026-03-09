use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use brain_lib::embedder::{Embed, Embedder};
use brain_lib::metrics::Metrics;
use brain_lib::prelude::*;
use brain_lib::query_pipeline::QueryPipeline;
use brain_lib::ranking::resolve_intent;

/// Parameters for a CLI query invocation.
pub struct QueryParams {
    pub query: String,
    pub top_k: usize,
    pub intent: String,
    pub budget: usize,
    pub verbose: bool,
    pub model_dir: PathBuf,
    pub db_path: PathBuf,
    pub sqlite_path: PathBuf,
}

/// Query the knowledge base using full hybrid search (vector + FTS + ranking).
pub async fn run(params: QueryParams) -> Result<()> {
    let embedder = Embedder::load(&params.model_dir)?;
    let embedder_arc: Arc<dyn Embed> = Arc::new(embedder);
    let store = Store::open_or_create(&params.db_path).await?;
    let store_reader = brain_lib::store::StoreReader::from_store(&store);
    let db = brain_lib::db::Db::open(&params.sqlite_path)?;

    let metrics = Arc::new(Metrics::new());
    let pipeline = QueryPipeline::new(&db, &store_reader, &embedder_arc, &metrics);

    let profile = resolve_intent(&params.intent);

    let search_result = if params.verbose {
        pipeline
            .search_with_scores(
                &params.query,
                &params.intent,
                params.budget,
                params.top_k,
                &[],
            )
            .await?
    } else {
        pipeline
            .search(
                &params.query,
                &params.intent,
                params.budget,
                params.top_k,
                &[],
            )
            .await?
    };

    if search_result.results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    println!(
        "Hybrid search | intent: {profile:?} | {}/{} results within {}-token budget\n",
        search_result.results.len(),
        search_result.total_available,
        params.budget,
    );

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
        if let Some(ref scores) = stub.signal_scores {
            println!(
                "  signals: vec={:.3} kw={:.3} rec={:.3} links={:.3} tags={:.3} imp={:.3}",
                scores.vector,
                scores.keyword,
                scores.recency,
                scores.links,
                scores.tag_match,
                scores.importance,
            );
        }
        println!();
    }

    Ok(())
}
