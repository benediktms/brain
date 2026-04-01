use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
use serde_json::json;

use brain_lib::embedder::{Embed, Embedder, embed_batch_async};
use brain_lib::metrics::Metrics;
use brain_lib::ports::{EpisodeReader, EpisodeWriter, ReflectionWriter};
use brain_lib::prelude::*;
use brain_lib::query_pipeline::{QueryPipeline, SearchParams};
use brain_lib::search_service::SearchService;
use brain_lib::stores::BrainStores;
use brain_lib::uri::SynapseUri;
use brain_persistence::db::summaries::Episode;
use brain_persistence::store::StoreReader;

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
    pub explain: bool,
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
        if params.explain {
            pipeline.search_with_scores(&search_params).await?
        } else {
            pipeline.search(&search_params).await?
        }
    } else {
        use brain_lib::config::{list_brain_keys, open_remote_search_context};
        use brain_lib::query_pipeline::FederatedPipeline;

        let brain_keys: Vec<String> = if params.brains.iter().any(|b| b == "all") {
            list_brain_keys(ctx.stores.db())?
                .into_iter()
                .map(|(name, _id)| name)
                .collect()
        } else {
            params.brains.clone()
        };

        let mut brains: Vec<(String, Option<StoreReader>)> = Vec::new();
        brains.push((
            ctx.stores.brain_name.clone(),
            Some(ctx.search.store.clone()),
        ));

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
        federated.search(&search_params, false).await?
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
                let uri_brain = stub.brain_name.as_deref().unwrap_or(&ctx.stores.brain_name);
                let uri = match stub.kind.as_str() {
                    "episode" => SynapseUri::for_episode(uri_brain, &stub.memory_id),
                    "reflection" => SynapseUri::for_reflection(uri_brain, &stub.memory_id),
                    "procedure" => SynapseUri::for_procedure(uri_brain, &stub.memory_id),
                    "record" => SynapseUri::for_record(uri_brain, &stub.memory_id),
                    "task" | "task-outcome" => SynapseUri::for_task(uri_brain, &stub.memory_id),
                    _ => SynapseUri::for_memory(uri_brain, &stub.memory_id),
                };
                v["uri"] = json!(uri.to_string());
                if let Some(ref bn) = stub.brain_name {
                    v["brain_name"] = json!(bn);
                }
                if let Some(ref ss) = stub.signal_scores {
                    v["signals"] = json!({
                        "sim_vector": ss.vector,
                        "bm25": ss.keyword,
                        "recency": ss.recency,
                        "links": ss.links,
                        "tag_match": ss.tag_match,
                        "importance": ss.importance,
                    });
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
        let headers = if params.explain {
            vec!["ID", "TITLE", "KIND", "SCORE", "VECTOR", "BM25"]
        } else {
            vec!["ID", "TITLE", "KIND", "SCORE"]
        };
        let mut table = MarkdownTable::new(headers);
        for stub in &result.results {
            if params.explain {
                let (vector, bm25) = stub
                    .signal_scores
                    .as_ref()
                    .map(|ss| (format!("{:.4}", ss.vector), format!("{:.4}", ss.keyword)))
                    .unwrap_or_else(|| ("-".to_string(), "-".to_string()));
                table.add_row(vec![
                    stub.memory_id.clone(),
                    stub.title.clone(),
                    stub.kind.clone(),
                    format!("{:.4}", stub.hybrid_score),
                    vector,
                    bm25,
                ]);
            } else {
                table.add_row(vec![
                    stub.memory_id.clone(),
                    stub.title.clone(),
                    stub.kind.clone(),
                    format!("{:.4}", stub.hybrid_score),
                ]);
            }
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
            if result.memories.len() == 1 {
                "y"
            } else {
                "ies"
            },
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
                        if let Some(vec) = vecs.into_iter().next()
                            && let Err(e) = store
                                .upsert_summary(
                                    &summary_id,
                                    &embed_content,
                                    &ctx.stores.brain_id,
                                    &vec,
                                )
                                .await
                        {
                            eprintln!("warning: failed to embed episode (best-effort): {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "warning: failed to generate embedding for episode (best-effort): {e}"
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("warning: failed to open LanceDB for embedding (best-effort): {e}");
            }
        }
    }

    let uri = SynapseUri::for_episode(&ctx.stores.brain_name, &summary_id).to_string();
    if ctx.json {
        let out = json!({
            "status": "stored",
            "summary_id": summary_id,
            "uri": uri,
            "goal": params.goal,
            "tags": params.tags,
            "importance": params.importance,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Episode stored: {summary_id}");
        println!("  URI:        {uri}");
        println!("  Goal:       {}", params.goal);
        println!("  Importance: {}", params.importance);
        if !params.tags.is_empty() {
            println!("  Tags:       {}", params.tags.join(", "));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// write_procedure
// ---------------------------------------------------------------------------

pub struct WriteProcedureParams {
    pub title: String,
    pub steps: String,
    pub tags: Vec<String>,
    pub importance: f64,
    /// Optional writable LanceDB store for best-effort embedding.
    pub lance_db: Option<std::path::PathBuf>,
}

pub async fn write_procedure(ctx: &MemoryCtx, params: WriteProcedureParams) -> Result<()> {
    use brain_lib::ports::ProcedureWriter;

    let embed_content = format!("{}\n\n{}", params.title, params.steps);

    let summary_id = ctx.stores.db().store_procedure(
        &params.title,
        &params.steps,
        &params.tags,
        params.importance,
        &ctx.stores.brain_id,
    )?;

    // Best-effort embedding.
    if let Some(ref lance_path) = params.lance_db {
        match Store::open_or_create(lance_path).await {
            Ok(store) => {
                match embed_batch_async(&ctx.search.embedder, vec![embed_content.clone()]).await {
                    Ok(vecs) => {
                        if let Some(vec) = vecs.into_iter().next()
                            && let Err(e) = store
                                .upsert_summary(
                                    &summary_id,
                                    &embed_content,
                                    &ctx.stores.brain_id,
                                    &vec,
                                )
                                .await
                        {
                            eprintln!("warning: failed to embed procedure (best-effort): {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "warning: failed to generate embedding for procedure (best-effort): {e}"
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("warning: failed to open LanceDB for embedding (best-effort): {e}");
            }
        }
    }

    let uri = SynapseUri::for_procedure(&ctx.stores.brain_name, &summary_id).to_string();
    if ctx.json {
        let out = json!({
            "status": "stored",
            "summary_id": summary_id,
            "uri": uri,
            "title": params.title,
            "tags": params.tags,
            "importance": params.importance,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Procedure stored: {summary_id}");
        println!("  URI:        {uri}");
        println!("  Title:      {}", params.title);
        println!("  Importance: {}", params.importance);
        if !params.tags.is_empty() {
            println!("  Tags:       {}", params.tags.join(", "));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// consolidate
// ---------------------------------------------------------------------------

pub async fn consolidate(
    ctx: &MemoryCtx,
    limit: usize,
    gap_seconds: i64,
    auto_summarize: bool,
) -> Result<()> {
    use brain_lib::consolidation::{consolidate_episodes, enqueue_cluster_summarization};
    use brain_lib::ports::EpisodeReader;

    let episodes = ctx
        .stores
        .db()
        .list_episodes(limit, &ctx.stores.brain_id)
        .unwrap_or_default();

    let result = consolidate_episodes(episodes, gap_seconds);
    let jobs_enqueued = if auto_summarize {
        enqueue_cluster_summarization(ctx.stores.db(), &result.clusters, &ctx.stores.brain_id)?
    } else {
        0
    };

    if ctx.json {
        let clusters_json: Vec<serde_json::Value> = result
            .clusters
            .iter()
            .map(|c| {
                json!({
                    "episode_ids": c.episode_ids,
                    "episode_count": c.episodes.len(),
                    "suggested_title": c.suggested_title,
                    "summary": c.summary,
                    "oldest_ts": c.episodes.iter().map(|e| e.created_at).min(),
                    "newest_ts": c.episodes.iter().map(|e| e.created_at).max(),
                })
            })
            .collect();
        let out = json!({
            "cluster_count": clusters_json.len(),
            "jobs_enqueued": jobs_enqueued,
            "clusters": clusters_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if result.clusters.is_empty() {
            println!("No clusters found. No episodes to consolidate.");
            return Ok(());
        }
        println!("{} cluster(s) found:", result.clusters.len());
        println!();
        for (i, cluster) in result.clusters.iter().enumerate() {
            println!("Cluster {} — {} episode(s)", i + 1, cluster.episodes.len());
            println!("  Title:    {}", cluster.suggested_title);
            println!("  Summary:  {}", cluster.summary);
            println!("  IDs:      {}", cluster.episode_ids.join(", "));
            if let (Some(oldest), Some(newest)) = (
                cluster.episodes.iter().map(|e| e.created_at).min(),
                cluster.episodes.iter().map(|e| e.created_at).max(),
            ) {
                println!("  Range:    {oldest} → {newest}");
            }
            println!();
        }
        if auto_summarize {
            println!("Enqueued {jobs_enqueued} async consolidation job(s).");
            println!();
        }
        println!("Use `brain memory reflect --commit` to synthesize a cluster into a reflection.");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// summarize_scope
// ---------------------------------------------------------------------------

pub async fn summarize_scope(
    ctx: &MemoryCtx,
    scope_type: &str,
    scope_value: &str,
    regenerate: bool,
    async_llm: bool,
) -> Result<()> {
    use brain_lib::hierarchy::{
        DerivedSummary, ScopeType, generate_scope_summary_with_options, get_scope_summary,
    };

    let st = match scope_type {
        "directory" => ScopeType::Directory,
        "tag" => ScopeType::Tag,
        other => bail!("Invalid scope_type '{other}'. Must be 'directory' or 'tag'."),
    };

    let mut llm_pending = false;

    let summary: DerivedSummary = if regenerate {
        let generation =
            generate_scope_summary_with_options(ctx.stores.db(), &st, scope_value, async_llm)?;
        llm_pending = generation.llm_pending;
        get_scope_summary(ctx.stores.db(), &st, scope_value)?.ok_or_else(|| {
            anyhow::anyhow!(
                "Generated summary '{}' not found after insert",
                generation.id
            )
        })?
    } else {
        match get_scope_summary(ctx.stores.db(), &st, scope_value)? {
            Some(s) => s,
            None => {
                let generation = generate_scope_summary_with_options(
                    ctx.stores.db(),
                    &st,
                    scope_value,
                    async_llm,
                )?;
                llm_pending = generation.llm_pending;
                get_scope_summary(ctx.stores.db(), &st, scope_value)?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Generated summary '{}' not found after insert",
                        generation.id
                    )
                })?
            }
        }
    };

    if ctx.json {
        let out = json!({
            "scope_type": summary.scope_type,
            "scope_value": summary.scope_value,
            "content": summary.content,
            "stale": summary.stale,
            "llm_pending": llm_pending,
            "generated_at": summary.generated_at,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Scope summary | {}:{}",
            summary.scope_type, summary.scope_value
        );
        if summary.stale {
            println!("  [stale — use --regenerate to refresh]");
        }
        if llm_pending {
            println!("  [async LLM refresh queued]");
        }
        println!("  Generated: {}", summary.generated_at);
        println!();
        println!("{}", summary.content);
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
                        if let Some(vec) = vecs.into_iter().next()
                            && let Err(e) = store
                                .upsert_summary(
                                    &summary_id,
                                    &embed_content,
                                    &ctx.stores.brain_id,
                                    &vec,
                                )
                                .await
                        {
                            eprintln!("warning: failed to embed reflection (best-effort): {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "warning: failed to generate embedding for reflection (best-effort): {e}"
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("warning: failed to open LanceDB for embedding (best-effort): {e}");
            }
        }
    }

    let uri = SynapseUri::for_reflection(&ctx.stores.brain_name, &summary_id).to_string();
    if ctx.json {
        let out = json!({
            "mode": "commit",
            "status": "stored",
            "summary_id": summary_id,
            "uri": uri,
            "title": params.title,
            "source_count": params.source_ids.len(),
            "importance": importance,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Reflection stored: {summary_id}");
        println!("  URI:        {uri}");
        println!("  Title:        {}", params.title);
        println!("  Sources:      {}", params.source_ids.len());
        println!("  Importance:   {importance}");
        if !params.tags.is_empty() {
            println!("  Tags:         {}", params.tags.join(", "));
        }
    }

    Ok(())
}
