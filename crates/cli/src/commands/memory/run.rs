use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
use serde_json::json;

use brain_lib::db::summaries::Episode;
use brain_lib::embedder::{Embed, Embedder, embed_batch_async};
use brain_lib::metrics::Metrics;
use brain_lib::ports::{EpisodeReader, EpisodeWriter, ReflectionWriter};
use brain_lib::prelude::*;
use brain_lib::query_pipeline::{QueryPipeline, SearchParams};
use brain_lib::search_service::SearchService;
use brain_lib::store::StoreReader;
use brain_lib::stores::BrainStores;

use crate::markdown_table::MarkdownTable;

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

pub struct MemoryCtx {
    pub(crate) stores: BrainStores,
    pub(crate) search: SearchService,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) json: bool,
}

impl MemoryCtx {
    pub async fn new(
        sqlite_db: &Path,
        lance_db: &Path,
        model_dir: &Path,
        json: bool,
    ) -> Result<Self> {
        let stores = BrainStores::from_path(sqlite_db, Some(lance_db))?;
        let embedder: Arc<dyn Embed> = Arc::new(Embedder::load(model_dir)?);
        let store = Store::open_or_create(lance_db).await?;
        let search = SearchService {
            store: StoreReader::from_store(&store),
            embedder,
        };
        let metrics = Arc::new(Metrics::new());
        Ok(Self {
            stores,
            search,
            metrics,
            json,
        })
    }
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------

pub struct SearchParams2 {
    pub query: String,
    pub k: usize,
    pub intent: String,
    pub budget: usize,
    pub tags: Vec<String>,
    pub brains: Vec<String>,
}

pub async fn search(ctx: &MemoryCtx, params: SearchParams2) -> Result<()> {
    let search_params = SearchParams::new(
        &params.query,
        &params.intent,
        params.budget,
        params.k,
        &params.tags,
    );

    let result = if params.brains.is_empty() {
        let pipeline = QueryPipeline::new(
            ctx.stores.db(),
            &ctx.search.store,
            &ctx.search.embedder,
            &ctx.metrics,
        );
        pipeline.search(&search_params).await?
    } else {
        use brain_lib::config::{list_brain_keys, open_remote_search_context};
        use brain_lib::query_pipeline::FederatedPipeline;

        let brain_keys: Vec<String> = if params.brains.iter().any(|b| b == "all") {
            list_brain_keys(&ctx.stores.brain_home)?
                .into_iter()
                .map(|(name, _id)| name)
                .collect()
        } else {
            params.brains.clone()
        };

        let mut brains: Vec<(String, Option<StoreReader>)> = Vec::new();
        brains.push((ctx.stores.brain_name.clone(), Some(ctx.search.store.clone())));

        for key in &brain_keys {
            if key == &ctx.stores.brain_name {
                continue;
            }
            match open_remote_search_context(
                &ctx.stores.brain_home,
                key,
                Path::new(""),
                &ctx.search.embedder,
            )
            .await?
            {
                Some(remote) => {
                    brains.push((remote.brain_name, remote.store));
                }
                None => {
                    eprintln!("warning: brain '{key}' not found in registry, skipping");
                }
            }
        }

        let federated = FederatedPipeline {
            db: ctx.stores.db(),
            brains,
            embedder: &ctx.search.embedder,
            metrics: &ctx.metrics,
        };
        federated.search(&search_params).await?
    };

    if ctx.json {
        let results_json: Vec<serde_json::Value> = result
            .results
            .iter()
            .map(|stub| {
                let mut v = json!({
                    "memory_id": stub.memory_id,
                    "title": stub.title,
                    "summary": stub.summary_2sent,
                    "score": stub.hybrid_score,
                    "file_path": stub.file_path,
                    "heading_path": stub.heading_path,
                    "kind": stub.kind,
                });
                if let Some(ref bn) = stub.brain_name {
                    v["brain_name"] = json!(bn);
                }
                v
            })
            .collect();
        let out = json!({
            "budget_tokens": result.budget_tokens,
            "used_tokens_est": result.used_tokens_est,
            "result_count": result.num_results,
            "total_available": result.total_available,
            "results": results_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if result.results.is_empty() {
            println!("No results found.");
            return Ok(());
        }
        let mut table = MarkdownTable::new(vec!["ID", "TITLE", "KIND", "SCORE"]);
        for stub in &result.results {
            table.add_row(vec![
                stub.memory_id.clone(),
                stub.title.clone(),
                stub.kind.clone(),
                format!("{:.4}", stub.hybrid_score),
            ]);
        }
        print!("{table}");
        println!();
        println!(
            "{}/{} results | intent: {} | {}-token budget",
            result.num_results, result.total_available, params.intent, params.budget
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// expand
// ---------------------------------------------------------------------------

pub async fn expand(ctx: &MemoryCtx, memory_ids: &[String], budget: usize) -> Result<()> {
    let pipeline = QueryPipeline::new(
        ctx.stores.db(),
        &ctx.search.store,
        &ctx.search.embedder,
        &ctx.metrics,
    );
    let result = pipeline.expand(memory_ids, budget).await?;

    if ctx.json {
        let memories_json: Vec<serde_json::Value> = result
            .memories
            .iter()
            .map(|m| {
                json!({
                    "memory_id": m.memory_id,
                    "content": m.content,
                    "file_path": m.file_path,
                    "heading_path": m.heading_path,
                    "byte_start": m.byte_start,
                    "byte_end": m.byte_end,
                    "truncated": m.truncated,
                })
            })
            .collect();
        let out = json!({
            "budget_tokens": result.budget_tokens,
            "used_tokens_est": result.used_tokens_est,
            "memories": memories_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if result.memories.is_empty() {
            println!("No memories found for the given IDs.");
            return Ok(());
        }
        for m in &result.memories {
            println!("=== {} ===", m.memory_id);
            if !m.file_path.is_empty() {
                println!("File: {}", m.file_path);
            }
            if !m.heading_path.is_empty() {
                println!("Section: {}", m.heading_path);
            }
            println!();
            println!("{}", m.content);
            if m.truncated {
                println!("[truncated]");
            }
            println!();
        }
        println!(
            "{} memor{} expanded | {}/{} tokens used",
            result.memories.len(),
            if result.memories.len() == 1 { "y" } else { "ies" },
            result.used_tokens_est,
            result.budget_tokens,
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// write_episode
// ---------------------------------------------------------------------------

pub struct WriteEpisodeParams {
    pub goal: String,
    pub actions: String,
    pub outcome: String,
    pub tags: Vec<String>,
    pub importance: f64,
    /// Optional writable LanceDB store for best-effort embedding.
    pub lance_db: Option<std::path::PathBuf>,
}

pub async fn write_episode(ctx: &MemoryCtx, params: WriteEpisodeParams) -> Result<()> {
    let embed_content = format!(
        "Goal: {}\nActions: {}\nOutcome: {}",
        params.goal, params.actions, params.outcome
    );

    let episode = Episode {
        brain_id: ctx.stores.brain_id.clone(),
        goal: params.goal.clone(),
        actions: params.actions.clone(),
        outcome: params.outcome.clone(),
        tags: params.tags.clone(),
        importance: params.importance,
    };

    let summary_id = ctx.stores.db().store_episode(&episode)?;

    // Best-effort embedding.
    if let Some(ref lance_path) = params.lance_db {
        match Store::open_or_create(lance_path).await {
            Ok(store) => {
                match embed_batch_async(&ctx.search.embedder, vec![embed_content.clone()]).await {
                    Ok(vecs) => {
                        if let Some(vec) = vecs.into_iter().next() {
                            if let Err(e) =
                                store.upsert_summary(&summary_id, &embed_content, &vec).await
                            {
                                eprintln!("warning: failed to embed episode (best-effort): {e}");
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("warning: failed to generate embedding for episode (best-effort): {e}");
                    }
                }
            }
            Err(e) => {
                eprintln!("warning: failed to open LanceDB for embedding (best-effort): {e}");
            }
        }
    }

    if ctx.json {
        let out = json!({
            "status": "stored",
            "summary_id": summary_id,
            "goal": params.goal,
            "tags": params.tags,
            "importance": params.importance,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Episode stored: {summary_id}");
        println!("  Goal:       {}", params.goal);
        println!("  Importance: {}", params.importance);
        if !params.tags.is_empty() {
            println!("  Tags:       {}", params.tags.join(", "));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// reflect_prepare
// ---------------------------------------------------------------------------

pub struct ReflectPrepareParams {
    pub topic: String,
    pub budget: usize,
    pub brains: Vec<String>,
}

pub async fn reflect_prepare(ctx: &MemoryCtx, params: ReflectPrepareParams) -> Result<()> {
    let episodes = if params.brains.is_empty() {
        ctx.stores
            .db()
            .list_episodes(10, &ctx.stores.brain_id)
            .unwrap_or_default()
    } else if params.brains.iter().any(|b| b == "all") {
        ctx.stores.db().list_episodes(10, "").unwrap_or_default()
    } else {
        ctx.stores
            .db()
            .list_episodes_multi_brain(10, &params.brains)
            .unwrap_or_default()
    };

    let pipeline = QueryPipeline::new(
        ctx.stores.db(),
        &ctx.search.store,
        &ctx.search.embedder,
        &ctx.metrics,
    );
    let result = pipeline
        .reflect_with_episodes(params.topic.clone(), params.budget, episodes)
        .await?;

    if ctx.json {
        let episode_sources: Vec<serde_json::Value> = result
            .episodes
            .iter()
            .map(|ep| {
                json!({
                    "type": "episode",
                    "summary_id": ep.summary_id,
                    "title": ep.title,
                    "content": ep.content,
                    "tags": ep.tags,
                    "importance": ep.importance,
                })
            })
            .collect();
        let chunks: Vec<serde_json::Value> = result
            .search_result
            .results
            .iter()
            .map(|stub| {
                json!({
                    "memory_id": stub.memory_id,
                    "title": stub.title,
                    "summary": stub.summary_2sent,
                    "score": stub.hybrid_score,
                    "file_path": stub.file_path,
                    "heading_path": stub.heading_path,
                })
            })
            .collect();
        let out = json!({
            "mode": "prepare",
            "topic": result.topic,
            "budget_tokens": result.budget_tokens,
            "source_count": episode_sources.len(),
            "episodes": episode_sources,
            "related_chunks": {
                "result_count": result.search_result.num_results,
                "used_tokens_est": result.search_result.used_tokens_est,
                "results": chunks,
            },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Reflect prepare | topic: {}", result.topic);
        println!();
        if result.episodes.is_empty() {
            println!("No recent episodes found.");
        } else {
            println!("## Recent Episodes ({})", result.episodes.len());
            for ep in &result.episodes {
                println!(
                    "  [{}] {} (importance: {})",
                    ep.summary_id,
                    ep.title.as_deref().unwrap_or("<untitled>"),
                    ep.importance
                );
            }
        }
        println!();
        if result.search_result.results.is_empty() {
            println!("No related chunks found.");
        } else {
            println!("## Related Chunks ({})", result.search_result.num_results);
            let mut table = MarkdownTable::new(vec!["ID", "TITLE", "SCORE"]);
            for stub in &result.search_result.results {
                table.add_row(vec![
                    stub.memory_id.clone(),
                    stub.title.clone(),
                    format!("{:.4}", stub.hybrid_score),
                ]);
            }
            print!("{table}");
        }
        println!();
        println!(
            "Use `brain memory reflect --commit --title ... --content ... --source-ids <ids>` to store a reflection."
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// reflect_commit
// ---------------------------------------------------------------------------

pub struct ReflectCommitParams {
    pub title: String,
    pub content: String,
    pub source_ids: Vec<String>,
    pub tags: Vec<String>,
    pub importance: f64,
    pub lance_db: Option<std::path::PathBuf>,
}

pub async fn reflect_commit(ctx: &MemoryCtx, params: ReflectCommitParams) -> Result<()> {
    if params.title.is_empty() {
        bail!("--title is required for commit mode");
    }
    if params.content.is_empty() {
        bail!("--content is required for commit mode");
    }
    if params.source_ids.is_empty() {
        bail!("--source-ids is required for commit mode");
    }

    let importance = params.importance.clamp(0.0, 1.0);

    // Validate source_ids.
    let found = ctx.stores.db().get_summaries_by_ids(&params.source_ids)?;
    let found_ids: HashSet<&str> = found.iter().map(|r| r.summary_id.as_str()).collect();
    for id in &params.source_ids {
        if !found_ids.contains(id.as_str()) {
            bail!("source_id not found: {id}");
        }
    }

    let summary_id = ctx.stores.db().store_reflection(
        &params.title,
        &params.content,
        &params.source_ids,
        &params.tags,
        importance,
        &ctx.stores.brain_id,
    )?;

    // Best-effort embedding.
    if let Some(ref lance_path) = params.lance_db {
        match Store::open_or_create(lance_path).await {
            Ok(store) => {
                let embed_content = params.content.clone();
                match embed_batch_async(&ctx.search.embedder, vec![embed_content.clone()]).await {
                    Ok(vecs) => {
                        if let Some(vec) = vecs.into_iter().next() {
                            if let Err(e) =
                                store.upsert_summary(&summary_id, &embed_content, &vec).await
                            {
                                eprintln!("warning: failed to embed reflection (best-effort): {e}");
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("warning: failed to generate embedding for reflection (best-effort): {e}");
                    }
                }
            }
            Err(e) => {
                eprintln!("warning: failed to open LanceDB for embedding (best-effort): {e}");
            }
        }
    }

    if ctx.json {
        let out = json!({
            "mode": "commit",
            "status": "stored",
            "summary_id": summary_id,
            "title": params.title,
            "source_count": params.source_ids.len(),
            "importance": importance,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Reflection stored: {summary_id}");
        println!("  Title:        {}", params.title);
        println!("  Sources:      {}", params.source_ids.len());
        println!("  Importance:   {importance}");
        if !params.tags.is_empty() {
            println!("  Tags:         {}", params.tags.join(", "));
        }
    }

    Ok(())
}
