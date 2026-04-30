use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
use serde_json::json;

use brain_lib::embedder::{Embed, Embedder, embed_batch_async};
use brain_lib::metrics::Metrics;
use brain_lib::prelude::*;
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

fn resolve_stores_for_cwd() -> Result<BrainStores> {
    let cwd = std::env::current_dir()?;
    let root = brain_lib::config::find_brain_root(&cwd)
        .ok_or_else(|| anyhow::anyhow!("no .brain marker found from cwd"))?;
    let toml = brain_lib::config::load_brain_toml(&root.join(".brain"))?;
    BrainStores::from_brain(&toml.name).map_err(anyhow::Error::from)
}

impl MemoryCtx {
    pub async fn new(
        sqlite_db: &Path,
        lance_db: &Path,
        model_dir: &Path,
        json: bool,
    ) -> Result<Self> {
        // Prefer cwd-marker resolution so `brain_name` is the registered name
        // (required by `retrieve` to reopen stores via `from_brain`). The
        // path-based fallback works for everything except retrieve, but yields
        // an empty `brain_name` under the unified `~/.brain/` layout.
        let stores = resolve_stores_for_cwd().or_else(|_e| {
            BrainStores::from_path(sqlite_db, Some(lance_db)).map_err(anyhow::Error::from)
        })?;
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

    let summary_id = ctx.stores.store_episode(&episode)?;

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
    let embed_content = format!("{}\n\n{}", params.title, params.steps);

    let summary_id = ctx.stores.store_procedure(
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

    let episodes = ctx
        .stores
        .list_episodes(limit, &ctx.stores.brain_id)
        .unwrap_or_default();

    let result = consolidate_episodes(episodes, gap_seconds);
    let jobs_enqueued = if auto_summarize {
        enqueue_cluster_summarization(&ctx.stores, &result.clusters, &ctx.stores.brain_id)?
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

    let stores = &ctx.stores;

    let summary: DerivedSummary = if regenerate {
        let generation = generate_scope_summary_with_options(stores, &st, scope_value, async_llm)?;
        llm_pending = generation.llm_pending;
        get_scope_summary(stores, &st, scope_value)?.ok_or_else(|| {
            anyhow::anyhow!(
                "Generated summary '{}' not found after insert",
                generation.id
            )
        })?
    } else {
        match get_scope_summary(stores, &st, scope_value)? {
            Some(s) => s,
            None => {
                let generation =
                    generate_scope_summary_with_options(stores, &st, scope_value, async_llm)?;
                llm_pending = generation.llm_pending;
                get_scope_summary(stores, &st, scope_value)?.ok_or_else(|| {
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
// retrieve
// ---------------------------------------------------------------------------

pub struct RetrieveParams {
    pub query: Option<String>,
    pub uri: Option<String>,
    pub lod: String,
    pub count: u64,
    pub strategy: String,
    pub brains: Vec<String>,
    pub time_scope: Option<String>,
    pub time_after: Option<i64>,
    pub time_before: Option<i64>,
    pub tags: Vec<String>,
    pub tags_require: Vec<String>,
    pub tags_exclude: Vec<String>,
    pub kinds: Vec<String>,
    pub explain: bool,
}

pub async fn retrieve(ctx: &MemoryCtx, params: RetrieveParams) -> Result<()> {
    use brain_lib::mcp::McpContext;
    use brain_lib::stores::BrainStores;

    let has_query = params.query.as_ref().is_some_and(|q| !q.trim().is_empty());
    let has_uri = params.uri.is_some();
    if has_query && has_uri {
        bail!("Provide QUERY or --uri, not both");
    }
    if !has_query && !has_uri {
        bail!("Either a QUERY positional argument or --uri is required");
    }

    // BrainStores and SearchService don't implement Clone, so we reopen stores
    // from the registry while reusing the already-loaded embedder + StoreReader.
    let stores = BrainStores::from_brain(&ctx.stores.brain_name)?;
    let search = SearchService {
        store: ctx.search.store.clone(),
        embedder: ctx.search.embedder.clone(),
    };
    let mcp_ctx = McpContext::from_stores(stores, Some(search), None, ctx.metrics.clone());

    retrieve_with_mcp_ctx(&mcp_ctx, ctx.json, params).await
}

/// Inner helper: dispatch + render. Accepts a pre-built McpContext so tests
/// can bypass the global registry lookup in `retrieve()`.
pub(crate) async fn retrieve_with_mcp_ctx(
    mcp_ctx: &brain_lib::mcp::McpContext,
    json: bool,
    params: RetrieveParams,
) -> Result<()> {
    use brain_lib::mcp::tools::ToolRegistry;

    let json_params = serde_json::json!({
        "query": params.query,
        "uri": params.uri,
        "lod": params.lod,
        "count": params.count,
        "strategy": params.strategy,
        "brains": params.brains,
        "time_scope": params.time_scope,
        "time_after": params.time_after,
        "time_before": params.time_before,
        "tags": params.tags,
        "tags_require": params.tags_require,
        "tags_exclude": params.tags_exclude,
        "kinds": params.kinds,
        "explain": params.explain,
    });

    let registry = ToolRegistry::new();
    let call_result = registry
        .dispatch("memory.retrieve", json_params, mcp_ctx)
        .await;

    if call_result.is_error == Some(true) {
        let msg = call_result
            .content
            .first()
            .map(|c| c.text.as_str())
            .unwrap_or("retrieve failed");
        bail!("{msg}");
    }

    let text = call_result
        .content
        .first()
        .map(|c| c.text.as_str())
        .unwrap_or("{}");

    let response: serde_json::Value =
        serde_json::from_str(text).unwrap_or_else(|_| serde_json::json!({"raw": text}));

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    let results = response["results"].as_array();
    let Some(results) = results else {
        println!("No results.");
        return Ok(());
    };
    if results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    let headers = if params.explain {
        vec![
            "URI",
            "KIND",
            "SCORE",
            "LOD",
            "EXPANSION_REASON",
            "SLOT",
            "SIGNALS",
        ]
    } else {
        vec!["URI", "KIND", "SCORE", "LOD", "EXPANSION_REASON", "SLOT"]
    };
    let mut table = MarkdownTable::new(headers);
    for entry in results {
        let uri = entry["uri"].as_str().unwrap_or("-");
        let kind = entry["kind"].as_str().unwrap_or("-");
        let score = entry["score"]
            .as_f64()
            .map(|s| format!("{s:.4}"))
            .unwrap_or_else(|| "-".to_string());
        let lod = entry["lod"].as_str().unwrap_or("-");
        let expansion_reason = entry["expansion_reason"].as_str().unwrap_or("-");
        let slot = entry["lod_plan_slot"]
            .as_u64()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_string());

        if params.explain {
            let signals = if let Some(sig) = entry["signals"].as_object() {
                sig.iter()
                    .filter_map(|(k, v)| v.as_f64().map(|f| format!("{k}:{f:.3}")))
                    .collect::<Vec<_>>()
                    .join(" ")
            } else {
                "-".to_string()
            };
            table.add_row(vec![
                uri.to_string(),
                kind.to_string(),
                score,
                lod.to_string(),
                expansion_reason.to_string(),
                slot,
                signals,
            ]);
        } else {
            table.add_row(vec![
                uri.to_string(),
                kind.to_string(),
                score,
                lod.to_string(),
                expansion_reason.to_string(),
                slot,
            ]);
        }
    }
    print!("{table}");
    println!();
    let result_count = response["result_count"]
        .as_u64()
        .unwrap_or(results.len() as u64);
    let lod_requested = response["lod_requested"].as_str().unwrap_or(&params.lod);
    println!(
        "{result_count} result(s) | lod: {lod_requested} | strategy: {}",
        params.strategy
    );

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
            .list_episodes(10, &ctx.stores.brain_id)
            .unwrap_or_default()
    } else if params.brains.iter().any(|b| b == "all") {
        ctx.stores.list_episodes(10, "").unwrap_or_default()
    } else {
        ctx.stores
            .list_episodes_multi_brain(10, &params.brains)
            .unwrap_or_default()
    };

    let pipeline = ctx
        .stores
        .query_pipeline(&ctx.search.store, &ctx.search.embedder, &ctx.metrics);
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
    let found = ctx.stores.get_summaries_by_ids(&params.source_ids)?;
    let found_ids: HashSet<&str> = found.iter().map(|r| r.summary_id.as_str()).collect();
    for id in &params.source_ids {
        if !found_ids.contains(id.as_str()) {
            bail!("source_id not found: {id}");
        }
    }

    let summary_id = ctx.stores.store_reflection(
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

#[cfg(test)]
mod retrieve_tests {
    use std::sync::Arc;

    use brain_lib::embedder::{Embed, MockEmbedder};
    use brain_lib::mcp::McpContext;
    use brain_lib::mcp::tools::ToolRegistry;
    use brain_lib::metrics::Metrics;
    use brain_lib::pipeline::IndexPipeline;
    use brain_lib::search_service::SearchService;
    use brain_lib::stores::BrainStores;
    use brain_persistence::store::{Store, StoreReader};
    use tempfile::TempDir;

    // Two-phase build: (1) index into LanceDB, (2) open a fresh StoreReader so
    // the reader sees all committed chunks. Mirrors the pattern from
    // crates/brain_lib/tests/retrieve_acceptance_tests.rs.
    //
    // Note: the production `retrieve()` CLI function calls
    // `BrainStores::from_brain()` which opens the global registry, making it
    // untestable in an in-memory context. Tests therefore exercise the MCP tool
    // layer directly (same approach as the predecessor acceptance tests). See
    // production bug note in handoff.

    struct TestCtx {
        _tmp: TempDir,
        mcp_ctx: McpContext,
        registry: ToolRegistry,
        db: brain_persistence::db::Db,
    }

    async fn make_ctx(notes: &[(&str, &str)], embedder: Arc<dyn Embed>) -> TestCtx {
        let tmp = TempDir::new().expect("create tempdir");
        let notes_dir = tmp.path().join("notes");
        std::fs::create_dir_all(&notes_dir).unwrap();

        let db_path = tmp.path().join("brain.db");
        let lance_path = tmp.path().join("lancedb");

        let db = brain_persistence::db::Db::open(&db_path).expect("open db");
        db.upsert_brain(&brain_persistence::db::schema::BrainUpsert {
            brain_id: "test-brain-id",
            name: "test-brain",
            prefix: "TST",
            roots_json: "[]",
            notes_json: "[]",
            aliases_json: "[]",
            archived: false,
        })
        .expect("register brain");

        for (name, content) in notes {
            std::fs::write(notes_dir.join(name), content).unwrap();
        }

        {
            let store = Store::open_or_create(&lance_path)
                .await
                .expect("open lance");
            let mut pipeline =
                IndexPipeline::with_embedder(db.clone(), store, Arc::clone(&embedder))
                    .await
                    .expect("build pipeline");
            pipeline.set_brain_id("test-brain-id".to_string());
            let stats = pipeline.full_scan(&[notes_dir]).await.expect("full_scan");
            assert_eq!(stats.errors, 0, "indexing had errors");
            assert!(
                stats.indexed >= notes.len(),
                "expected ≥{} chunks indexed, got {}",
                notes.len(),
                stats.indexed
            );
        }

        let brain_data_dir = tmp.path().join("brains").join("test-brain");
        std::fs::create_dir_all(&brain_data_dir).unwrap();
        let stores =
            BrainStores::from_dbs(db.clone(), "test-brain-id", &brain_data_dir, tmp.path())
                .expect("build BrainStores");

        let writable_store = Store::open_or_create(&lance_path)
            .await
            .expect("open writable LanceDB");
        let store_reader = StoreReader::from_store(&writable_store);
        let search = SearchService {
            store: store_reader,
            embedder: Arc::clone(&embedder),
        };

        let mcp_ctx = McpContext {
            stores,
            search: Some(search),
            writable_store: Some(writable_store),
            metrics: Arc::new(Metrics::new()),
        };

        TestCtx {
            _tmp: tmp,
            mcp_ctx,
            registry: ToolRegistry::new(),
            db,
        }
    }

    async fn make_ctx_mock(notes: &[(&str, &str)]) -> TestCtx {
        make_ctx(notes, Arc::new(MockEmbedder) as Arc<dyn Embed>).await
    }

    fn five_chunks() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                "chunk_a.md",
                "# Alpha\n\nAlpha is about memory retrieval and semantic indexing.",
            ),
            (
                "chunk_b.md",
                "# Beta\n\nBeta covers vector embeddings and nearest-neighbour search.",
            ),
            (
                "chunk_c.md",
                "# Gamma\n\nGamma discusses hybrid search combining BM25 and cosine similarity.",
            ),
            (
                "chunk_d.md",
                "# Delta\n\nDelta explains ranking signals: recency, importance, backlinks.",
            ),
            (
                "chunk_e.md",
                "# Epsilon\n\nEpsilon is about query pipelines and LOD resolution.",
            ),
        ]
    }

    // ── 1. query mode ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn retrieve_query_mode_returns_results() {
        let ctx = make_ctx_mock(&five_chunks()).await;

        let result = ctx
            .registry
            .dispatch(
                "memory.retrieve",
                serde_json::json!({ "query": "memory retrieval semantic", "lod": "L0", "count": 5 }),
                &ctx.mcp_ctx,
            )
            .await;

        assert_ne!(
            result.is_error,
            Some(true),
            "tool error: {}",
            result.content[0].text
        );
        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("valid JSON");

        let count = parsed["result_count"]
            .as_u64()
            .expect("result_count present");
        assert!(count > 0, "expected non-zero result_count, got {count}");

        let results = parsed["results"].as_array().expect("results is array");
        assert!(!results.is_empty(), "results array must not be empty");

        for item in results {
            assert!(item["uri"].is_string(), "missing uri: {item}");
            assert!(!item["uri"].as_str().unwrap().is_empty(), "empty uri");
            assert!(
                item["expansion_reason"].is_string(),
                "missing expansion_reason: {item}"
            );
            assert!(
                item["lod_plan_slot"].is_number(),
                "missing lod_plan_slot: {item}"
            );
        }
    }

    // ── 2. URI mode ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn retrieve_uri_mode_returns_single_chunk() {
        let ctx = make_ctx_mock(&[("single.md", "# Single\n\nA single chunk for URI test.")]).await;

        let brain_id = ctx.mcp_ctx.brain_id().to_string();
        let chunk_id = ctx
            .db
            .with_read_conn(|conn| {
                let mut stmt =
                    conn.prepare("SELECT chunk_id FROM chunks WHERE brain_id = ?1 LIMIT 1")?;
                let id: String = stmt.query_row([&brain_id], |row| row.get(0))?;
                Ok(id)
            })
            .expect("get chunk_id from DB");

        let uri = format!("synapse://test-brain/memory/{chunk_id}");

        let result = ctx
            .registry
            .dispatch(
                "memory.retrieve",
                serde_json::json!({ "uri": uri }),
                &ctx.mcp_ctx,
            )
            .await;

        assert_ne!(
            result.is_error,
            Some(true),
            "tool error: {}",
            result.content[0].text
        );
        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("valid JSON");

        let results = parsed["results"].as_array().expect("results is array");
        assert_eq!(results.len(), 1, "URI mode must return exactly 1 result");

        let item = &results[0];
        assert_eq!(
            item["expansion_reason"].as_str().unwrap_or(""),
            "uri_direct",
            "URI mode must have expansion_reason=uri_direct: {item}"
        );
        assert_eq!(
            item["lod_plan_slot"].as_u64().unwrap_or(99),
            0,
            "URI mode lod_plan_slot must be 0: {item}"
        );
    }

    // ── helper: cheap MemoryCtx for validation-only tests ────────────────────
    //
    // The XOR bail! branches in retrieve() fire before BrainStores::from_brain,
    // so we only need a structurally valid MemoryCtx — not a globally-registered
    // one. Uses pub(crate) field access (same crate).

    async fn make_memory_ctx_for_validation() -> (TempDir, super::MemoryCtx) {
        let tmp = TempDir::new().expect("create tempdir");
        let db_path = tmp.path().join("brain.db");
        let lance_path = tmp.path().join("lancedb");

        let db = brain_persistence::db::Db::open(&db_path).expect("open db");
        let brain_data_dir = tmp.path().join("brains").join("stub-brain");
        std::fs::create_dir_all(&brain_data_dir).unwrap();
        let stores = BrainStores::from_dbs(db, "stub-id", &brain_data_dir, tmp.path())
            .expect("build BrainStores");

        let writable_store = Store::open_or_create(&lance_path)
            .await
            .expect("open lance for validation ctx");
        let store_reader = StoreReader::from_store(&writable_store);
        let search = SearchService {
            store: store_reader,
            embedder: Arc::new(MockEmbedder) as Arc<dyn Embed>,
        };

        let ctx = super::MemoryCtx {
            stores,
            search,
            metrics: Arc::new(Metrics::new()),
            json: false,
        };
        (tmp, ctx)
    }

    // ── 4. XOR: both query and uri provided ───────────────────────────────────

    #[tokio::test]
    async fn retrieve_rejects_query_and_uri_together() {
        let (_tmp, ctx) = make_memory_ctx_for_validation().await;
        let result = super::retrieve(
            &ctx,
            super::RetrieveParams {
                query: Some("some query".to_string()),
                uri: Some("synapse://brain/memory/x".to_string()),
                lod: "L0".to_string(),
                count: 5,
                strategy: "auto".to_string(),
                brains: vec![],
                time_scope: None,
                time_after: None,
                time_before: None,
                tags: vec![],
                tags_require: vec![],
                tags_exclude: vec![],
                kinds: vec![],
                explain: false,
            },
        )
        .await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not both"),
            "expected 'not both' in error, got: {err}"
        );
    }

    // ── 5. XOR: neither query nor uri provided ────────────────────────────────

    #[tokio::test]
    async fn retrieve_rejects_neither_query_nor_uri() {
        let (_tmp, ctx) = make_memory_ctx_for_validation().await;
        let result = super::retrieve(
            &ctx,
            super::RetrieveParams {
                query: None,
                uri: None,
                lod: "L0".to_string(),
                count: 5,
                strategy: "auto".to_string(),
                brains: vec![],
                time_scope: None,
                time_after: None,
                time_before: None,
                tags: vec![],
                tags_require: vec![],
                tags_exclude: vec![],
                kinds: vec![],
                explain: false,
            },
        )
        .await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("required"),
            "expected 'required' in error, got: {err}"
        );
    }

    // ── 3. JSON output mode ──────────────────────────────────────────────────

    #[tokio::test]
    async fn retrieve_json_mode_emits_json_payload() {
        let ctx = make_ctx_mock(&five_chunks()).await;

        let result = ctx
            .registry
            .dispatch(
                "memory.retrieve",
                serde_json::json!({
                    "query": "vector embeddings search",
                    "lod": "L0",
                    "count": 5,
                    "explain": true
                }),
                &ctx.mcp_ctx,
            )
            .await;

        assert_ne!(
            result.is_error,
            Some(true),
            "tool error: {}",
            result.content[0].text
        );
        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("valid JSON");

        assert!(parsed.get("result_count").is_some(), "missing result_count");
        let results = parsed["results"].as_array().expect("results is array");

        for item in results {
            assert!(
                item.get("expansion_reason").is_some(),
                "missing expansion_reason: {item}"
            );
            assert!(
                item.get("lod_plan_slot").is_some(),
                "missing lod_plan_slot: {item}"
            );
        }
    }
}
