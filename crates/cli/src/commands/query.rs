use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use brain_lib::config::{list_brain_keys, load_brain_toml, open_remote_search_context};
use brain_lib::embedder::{Embed, Embedder};
use brain_lib::metrics::Metrics;
use brain_lib::prelude::*;
use brain_lib::query_pipeline::{FederatedPipeline, QueryPipeline};
use brain_lib::ranking::resolve_intent;
use brain_lib::retrieval::SearchResult;

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
    /// Brain names/IDs to search. Empty = local brain only. "all" = all registered brains.
    pub brains: Vec<String>,
}

/// Format a `SearchResult` into the human-readable CLI output string.
pub fn format_search_results(result: &SearchResult, intent: &str, budget: usize) -> String {
    if result.results.is_empty() {
        return "No results found.".to_string();
    }

    let profile = resolve_intent(intent);
    let mut output = format!(
        "Hybrid search | intent: {profile:?} | {}/{} results within {}-token budget\n\n",
        result.results.len(),
        result.total_available,
        budget,
    );

    for (rank, stub) in result.results.iter().enumerate() {
        output.push_str(&format!(
            "#{} [score: {:.4}]\n",
            rank + 1,
            stub.hybrid_score
        ));
        if !stub.heading_path.is_empty() {
            if let Some(ref brain) = stub.brain_name {
                output.push_str(&format!(
                    "  file: [{}] {} | {}\n",
                    brain, stub.file_path, stub.heading_path
                ));
            } else {
                output.push_str(&format!(
                    "  file: {} | {}\n",
                    stub.file_path, stub.heading_path
                ));
            }
        } else if let Some(ref brain) = stub.brain_name {
            output.push_str(&format!("  file: [{}] {}\n", brain, stub.file_path));
        } else {
            output.push_str(&format!("  file: {}\n", stub.file_path));
        }
        output.push_str(&format!("  {}\n", stub.title));
        if !stub.summary_2sent.is_empty() {
            output.push_str(&format!("  {}\n", stub.summary_2sent));
        }
        if let Some(ref scores) = stub.signal_scores {
            output.push_str(&format!(
                "  signals: vec={:.3} kw={:.3} rec={:.3} links={:.3} tags={:.3} imp={:.3}\n",
                scores.vector,
                scores.keyword,
                scores.recency,
                scores.links,
                scores.tag_match,
                scores.importance,
            ));
        }
        output.push('\n');
    }

    output
}

/// Run a query using a pre-built pipeline, returning the formatted output as a `String`.
///
/// This is the testable core of the query command. It accepts an already-constructed
/// `QueryPipeline` so that tests can inject a `MockEmbedder` and a `TempDir`-based
/// store without touching real model weights or the filesystem.
pub async fn run_with_pipeline(
    params: &QueryParams,
    pipeline: &QueryPipeline<'_>,
) -> Result<String> {
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

    Ok(format_search_results(
        &search_result,
        &params.intent,
        params.budget,
    ))
}

/// Query the knowledge base using full hybrid search (vector + FTS + ranking).
pub async fn run(params: QueryParams) -> Result<()> {
    let embedder = Embedder::load(&params.model_dir)?;
    let embedder_arc: Arc<dyn Embed> = Arc::new(embedder);
    let metrics = Arc::new(Metrics::new());

    if params.brains.is_empty() {
        // Single-brain path (backward compatible).
        let store = Store::open_or_create(&params.db_path).await?;
        let store_reader = brain_lib::store::StoreReader::from_store(&store);
        let db = brain_lib::db::Db::open(&params.sqlite_path)?;
        let pipeline = QueryPipeline::new(&db, &store_reader, &embedder_arc, &metrics);
        let output = run_with_pipeline(&params, &pipeline).await?;
        print!("{output}");
        return Ok(());
    }

    // Federated path.
    let brain_home = brain_lib::config::brain_home()
        .map_err(|e| anyhow::anyhow!("cannot determine brain home: {e}"))?;

    // Determine which brain names to search.
    let brain_keys: Vec<String> = if params.brains.iter().any(|b| b == "all") {
        list_brain_keys(&brain_home)?
            .into_iter()
            .map(|(name, _id)| name)
            .collect()
    } else {
        params.brains.clone()
    };

    // Determine the local brain name from cwd brain.toml.
    let local_brain_name = {
        let cwd = std::env::current_dir()?;
        brain_lib::config::find_brain_root(&cwd)
            .and_then(|root| {
                load_brain_toml(&root.join(".brain"))
                    .ok()
                    .map(|t| t.name)
            })
            .unwrap_or_else(|| "local".to_string())
    };

    // Open local brain stores.
    let local_store = Store::open_or_create(&params.db_path).await?;
    let local_store_reader = brain_lib::store::StoreReader::from_store(&local_store);
    let local_db = brain_lib::db::Db::open(&params.sqlite_path)?;

    // Open remote search contexts for all requested brains that are not the local brain.
    let mut remotes = Vec::new();
    for key in &brain_keys {
        if key == &local_brain_name {
            // Local brain is already included via local_db / local_store_reader.
            continue;
        }
        match open_remote_search_context(&brain_home, key, &params.model_dir, &embedder_arc)
            .await?
        {
            Some(ctx) => remotes.push(ctx),
            None => {
                eprintln!("warning: brain '{key}' not found in registry, skipping");
            }
        }
    }

    let federated = FederatedPipeline {
        local_db: &local_db,
        local_store: &local_store_reader,
        local_brain_name: local_brain_name.clone(),
        remotes,
        embedder: &embedder_arc,
        metrics: &metrics,
    };

    let search_result = federated
        .search(
            &params.query,
            &params.intent,
            params.budget,
            params.top_k,
            &[],
        )
        .await?;

    let output = format_search_results(&search_result, &params.intent, params.budget);
    print!("{output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_lib::retrieval::{MemoryStub, SearchResult};
    use clap::Parser;
    use crate::cli::Cli;

    fn make_stub(rank: usize, score: f64, file: &str, title: &str) -> MemoryStub {
        MemoryStub {
            memory_id: format!("chunk-{rank}"),
            title: title.to_string(),
            summary_2sent: String::new(),
            hybrid_score: score,
            file_path: file.to_string(),
            heading_path: String::new(),
            token_estimate: 10,
            signal_scores: None,
            kind: "note".to_string(),
            brain_name: None,
        }
    }

    #[test]
    fn format_empty_result() {
        let result = SearchResult {
            budget_tokens: 1000,
            used_tokens_est: 0,
            num_results: 0,
            total_available: 0,
            results: vec![],
            fusion_confidence: None,
        };
        let output = format_search_results(&result, "lookup", 1000);
        assert_eq!(output, "No results found.");
    }

    #[test]
    fn format_single_result() {
        let stub = make_stub(1, 0.9123, "notes/foo.md", "My Note");
        let result = SearchResult {
            budget_tokens: 500,
            used_tokens_est: 10,
            num_results: 1,
            total_available: 1,
            results: vec![stub],
            fusion_confidence: None,
        };
        let output = format_search_results(&result, "lookup", 500);
        assert!(output.contains("#1 [score: 0.9123]"), "got: {output}");
        assert!(output.contains("notes/foo.md"), "got: {output}");
        assert!(output.contains("My Note"), "got: {output}");
        assert!(output.contains("1/1 results"), "got: {output}");
        assert!(output.contains("500-token budget"), "got: {output}");
    }

    #[test]
    fn format_result_with_heading_path() {
        let mut stub = make_stub(1, 0.8, "notes/bar.md", "Section Title");
        stub.heading_path = "## Introduction".to_string();
        let result = SearchResult {
            budget_tokens: 500,
            used_tokens_est: 10,
            num_results: 1,
            total_available: 1,
            results: vec![stub],
            fusion_confidence: None,
        };
        let output = format_search_results(&result, "lookup", 500);
        assert!(
            output.contains("notes/bar.md | ## Introduction"),
            "got: {output}"
        );
    }

    #[test]
    fn format_result_with_summary() {
        let mut stub = make_stub(1, 0.7, "notes/baz.md", "Some Title");
        stub.summary_2sent = "This is the summary. It has two sentences.".to_string();
        let result = SearchResult {
            budget_tokens: 500,
            used_tokens_est: 10,
            num_results: 1,
            total_available: 1,
            results: vec![stub],
            fusion_confidence: None,
        };
        let output = format_search_results(&result, "lookup", 500);
        assert!(output.contains("This is the summary."), "got: {output}");
    }

    #[test]
    fn format_result_with_brain_name() {
        let mut stub = make_stub(1, 0.85, "notes/remote.md", "Remote Note");
        stub.brain_name = Some("work".to_string());
        let result = SearchResult {
            budget_tokens: 500,
            used_tokens_est: 10,
            num_results: 1,
            total_available: 1,
            results: vec![stub],
            fusion_confidence: None,
        };
        let output = format_search_results(&result, "lookup", 500);
        assert!(output.contains("[work] notes/remote.md"), "got: {output}");
    }

    #[test]
    fn format_result_with_brain_name_and_heading() {
        let mut stub = make_stub(1, 0.85, "notes/remote.md", "Remote Note");
        stub.brain_name = Some("personal".to_string());
        stub.heading_path = "## Section".to_string();
        let result = SearchResult {
            budget_tokens: 500,
            used_tokens_est: 10,
            num_results: 1,
            total_available: 1,
            results: vec![stub],
            fusion_confidence: None,
        };
        let output = format_search_results(&result, "lookup", 500);
        assert!(
            output.contains("[personal] notes/remote.md | ## Section"),
            "got: {output}"
        );
    }

    #[test]
    fn cli_parse_query_no_brain_flag() {
        let cli = Cli::try_parse_from(["brain", "query", "hello"]).unwrap();
        if let crate::cli::Command::Query { brains, .. } = cli.command {
            assert!(brains.is_empty());
        } else {
            panic!("expected Query command");
        }
    }

    #[test]
    fn cli_parse_query_single_brain_flag() {
        let cli = Cli::try_parse_from(["brain", "query", "hello", "--brain", "work"]).unwrap();
        if let crate::cli::Command::Query { brains, .. } = cli.command {
            assert_eq!(brains, vec!["work"]);
        } else {
            panic!("expected Query command");
        }
    }

    #[test]
    fn cli_parse_query_multiple_brain_flags() {
        let cli =
            Cli::try_parse_from(["brain", "query", "hello", "--brain", "work", "--brain", "personal"])
                .unwrap();
        if let crate::cli::Command::Query { brains, .. } = cli.command {
            assert_eq!(brains, vec!["work", "personal"]);
        } else {
            panic!("expected Query command");
        }
    }

    #[test]
    fn cli_parse_query_brain_all() {
        let cli = Cli::try_parse_from(["brain", "query", "hello", "--brain", "all"]).unwrap();
        if let crate::cli::Command::Query { brains, .. } = cli.command {
            assert_eq!(brains, vec!["all"]);
        } else {
            panic!("expected Query command");
        }
    }
}
