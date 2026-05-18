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
#[allow(unused_imports)]
use brain_persistence::sql::SqlResultExt;
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
    use brain_lib::ipc::sync_client::sync_tools_call;

    let has_query = params.query.as_ref().is_some_and(|q| !q.trim().is_empty());
    let has_uri = params.uri.is_some();
    if has_query && has_uri {
        bail!("Provide --query or --uri, not both");
    }
    if !has_query && !has_uri {
        bail!("Either a QUERY positional argument or --uri is required");
    }

    // Route through the daemon via sync RPC — the daemon owns all stores/search.
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

    let result = sync_tools_call("memory.retrieve", &ctx.stores.brain_name, json_params)
        .map_err(|e| anyhow::anyhow!("memory.retrieve failed: {e}"))?;

    retrieve_render(ctx.json, params, result)
}

/// Render a memory.retrieve result from the daemon RPC response.
pub(crate) fn retrieve_render(
    json: bool,
    params: RetrieveParams,
    result: serde_json::Value,
) -> Result<()> {
    // The sync_tools_call result is the raw JSON value from the daemon.
    // If it has an `isError` field at top level, propagate it.
    if result.get("isError").and_then(|v| v.as_bool()) == Some(true) {
        let msg = result
            .pointer("/content/0/text")
            .and_then(|v| v.as_str())
            .unwrap_or("retrieve failed");
        bail!("{msg}");
    }

    let response = result;

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

// ---------------------------------------------------------------------------
// remote helpers
// ---------------------------------------------------------------------------

/// Convert a user-supplied importance value into the wire-format
/// millis-u32. Clamps to [0.0, 1.0] so out-of-range values can't
/// silently saturate the `as u32` cast into a misleading "successful
/// 5000-millis write" — the daemon caps importance at 1.0 anyway, so
/// clamping client-side keeps the wire-format and storage semantics
/// aligned.
fn importance_to_millis(v: f64) -> u32 {
    (v.clamp(0.0, 1.0) * 1000.0) as u32
}

pub fn write_episode_remote(params: WriteEpisodeParams, json: bool) -> anyhow::Result<()> {
    use brain_rpc::domain::{MemoryWriteEpisodeParams, Request, Response};
    let mut client = crate::commands::rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::MemoryWriteEpisode {
            params: MemoryWriteEpisodeParams {
                goal: params.goal.clone(),
                actions: params.actions.clone(),
                outcome: params.outcome.clone(),
                tags: params.tags.clone(),
                importance_millis: importance_to_millis(params.importance),
                // CLI doesn't yet expose thread-extension; episodes
                // created via `brain memory write-episode` stand alone.
                continues: None,
            },
        })
        .map_err(|e| anyhow::anyhow!("MemoryWriteEpisode rpc failed: {e}"))?;
    let (summary_id, uri) = match resp {
        Response::MemoryWriteEpisode { summary_id, uri } => (summary_id, uri),
        other => anyhow::bail!("unexpected response to MemoryWriteEpisode: {other:?}"),
    };
    if json {
        let out = serde_json::json!({
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

pub fn write_procedure_remote(params: WriteProcedureParams, json: bool) -> anyhow::Result<()> {
    use brain_rpc::domain::{MemoryWriteProcedureParams, Request, Response};
    let mut client = crate::commands::rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::MemoryWriteProcedure {
            params: MemoryWriteProcedureParams {
                title: params.title.clone(),
                steps: params.steps.clone(),
                tags: params.tags.clone(),
                importance_millis: importance_to_millis(params.importance),
            },
        })
        .map_err(|e| anyhow::anyhow!("MemoryWriteProcedure rpc failed: {e}"))?;
    let (summary_id, uri) = match resp {
        Response::MemoryWriteProcedure { summary_id, uri } => (summary_id, uri),
        other => anyhow::bail!("unexpected response to MemoryWriteProcedure: {other:?}"),
    };
    if json {
        let out = serde_json::json!({
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

pub fn retrieve_remote(params: RetrieveParams, json: bool) -> anyhow::Result<()> {
    use brain_rpc::domain::{MemoryRetrieveParams, Request, Response};
    let mut client = crate::commands::rpc_client::connect_daemon()?;
    let strategy = params.strategy.clone();
    let lod = params.lod.clone();
    let explain = params.explain;
    let resp = client
        .call(Request::MemoryRetrieve {
            params: MemoryRetrieveParams {
                query: params.query,
                uri: params.uri,
                lod,
                count: params.count,
                strategy,
                brains: params.brains,
                time_scope: params.time_scope,
                time_after: params.time_after,
                time_before: params.time_before,
                tags: params.tags,
                tags_require: params.tags_require,
                tags_exclude: params.tags_exclude,
                kinds: params.kinds,
                explain,
            },
        })
        .map_err(|e| anyhow::anyhow!("MemoryRetrieve rpc failed: {e}"))?;
    let result_json = match resp {
        Response::MemoryRetrieve { result_json } => result_json,
        other => anyhow::bail!("unexpected response to MemoryRetrieve: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_str(&result_json)
        .unwrap_or_else(|_| serde_json::json!({"raw": result_json}));
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
    let headers = if explain {
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
    let mut table = crate::markdown_table::MarkdownTable::new(headers);
    for entry in results {
        let uri = entry["uri"].as_str().unwrap_or("-");
        let kind = entry["kind"].as_str().unwrap_or("-");
        let score = entry["score"]
            .as_f64()
            .map(|s| format!("{s:.4}"))
            .unwrap_or_else(|| "-".to_string());
        let lod_val = entry["lod"].as_str().unwrap_or("-");
        let expansion_reason = entry["expansion_reason"].as_str().unwrap_or("-");
        let slot = entry["lod_plan_slot"]
            .as_u64()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_string());
        if explain {
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
                lod_val.to_string(),
                expansion_reason.to_string(),
                slot,
                signals,
            ]);
        } else {
            table.add_row(vec![
                uri.to_string(),
                kind.to_string(),
                score,
                lod_val.to_string(),
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
    let lod_requested = response["lod_requested"]
        .as_str()
        .unwrap_or(response["lod"].as_str().unwrap_or("-"));
    let strategy_val = response["strategy"].as_str().unwrap_or("-");
    println!("{result_count} result(s) | lod: {lod_requested} | strategy: {strategy_val}");
    Ok(())
}

pub fn consolidate_remote(
    limit: usize,
    gap_seconds: i64,
    auto_summarize: bool,
    json: bool,
) -> anyhow::Result<()> {
    use brain_rpc::domain::{MemoryConsolidateParams, Request, Response};
    let mut client = crate::commands::rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::MemoryConsolidate {
            params: MemoryConsolidateParams {
                limit,
                gap_seconds,
                auto_summarize,
            },
        })
        .map_err(|e| anyhow::anyhow!("MemoryConsolidate rpc failed: {e}"))?;
    let result_json = match resp {
        Response::MemoryConsolidate { result_json } => result_json,
        other => anyhow::bail!("unexpected response to MemoryConsolidate: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_str(&result_json)
        .unwrap_or_else(|_| serde_json::json!({"raw": result_json}));
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let clusters = response["clusters"].as_array();
    match clusters.map(|v| v.as_slice()) {
        None | Some([]) => {
            println!("No clusters found. No episodes to consolidate.");
        }
        Some(clusters) => {
            println!("{} cluster(s) found:", clusters.len());
            println!();
            for (i, cluster) in clusters.iter().enumerate() {
                let episode_count = cluster["episode_count"].as_u64().unwrap_or(0);
                let title = cluster["suggested_title"].as_str().unwrap_or("<untitled>");
                let summary = cluster["summary"].as_str().unwrap_or("");
                let ids = cluster["episode_ids"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                println!("Cluster {} — {} episode(s)", i + 1, episode_count);
                println!("  Title:    {title}");
                println!("  Summary:  {summary}");
                println!("  IDs:      {ids}");
                println!();
            }
            if auto_summarize {
                let jobs = response["jobs_enqueued"].as_u64().unwrap_or(0);
                println!("Enqueued {jobs} async consolidation job(s).");
                println!();
            }
            println!(
                "Use `brain memory reflect --commit` to synthesize a cluster into a reflection."
            );
        }
    }
    Ok(())
}

pub fn summarize_scope_remote(
    scope_type: &str,
    scope_value: &str,
    regenerate: bool,
    async_llm: bool,
    json: bool,
) -> anyhow::Result<()> {
    use brain_rpc::domain::{MemorySummarizeScopeParams, Request, Response};
    let mut client = crate::commands::rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::MemorySummarizeScope {
            params: MemorySummarizeScopeParams {
                scope_type: scope_type.to_string(),
                scope_value: scope_value.to_string(),
                regenerate,
                async_llm,
            },
        })
        .map_err(|e| anyhow::anyhow!("MemorySummarizeScope rpc failed: {e}"))?;
    let result_json = match resp {
        Response::MemorySummarizeScope { result_json } => result_json,
        other => anyhow::bail!("unexpected response to MemorySummarizeScope: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_str(&result_json)
        .unwrap_or_else(|_| serde_json::json!({"raw": result_json}));
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let st = response["scope_type"].as_str().unwrap_or(scope_type);
    let sv = response["scope_value"].as_str().unwrap_or(scope_value);
    println!("Scope summary | {st}:{sv}");
    if response["stale"].as_bool().unwrap_or(false) {
        println!("  [stale — use --regenerate to refresh]");
    }
    if response["llm_pending"].as_bool().unwrap_or(false) {
        println!("  [async LLM refresh queued]");
    }
    if let Some(ts) = response["generated_at"].as_str() {
        println!("  Generated: {ts}");
    }
    println!();
    if let Some(content) = response["content"].as_str() {
        println!("{content}");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn reflect_remote(
    commit: bool,
    topic: Option<String>,
    budget: usize,
    brains: Vec<String>,
    title: Option<String>,
    content: Option<String>,
    source_ids: Vec<String>,
    tags: Vec<String>,
    importance: Option<f64>,
    json: bool,
) -> anyhow::Result<()> {
    use brain_rpc::domain::{MemoryReflectParams, Request, Response};
    let mut client = crate::commands::rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::MemoryReflect {
            params: MemoryReflectParams {
                commit,
                topic,
                budget,
                brains,
                title,
                content,
                source_ids,
                tags,
                importance_millis: importance.map(importance_to_millis),
            },
        })
        .map_err(|e| anyhow::anyhow!("MemoryReflect rpc failed: {e}"))?;
    let result_json = match resp {
        Response::MemoryReflect { result_json } => result_json,
        other => anyhow::bail!("unexpected response to MemoryReflect: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_str(&result_json)
        .unwrap_or_else(|_| serde_json::json!({"raw": result_json}));
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    // Render human-readable output from the JSON response.
    if commit {
        let summary_id = response["summary_id"].as_str().unwrap_or("-");
        let uri = response["uri"].as_str().unwrap_or("-");
        let title_val = response["title"].as_str().unwrap_or("-");
        let sources = response["source_count"].as_u64().unwrap_or(0);
        let imp = response["importance"].as_f64().unwrap_or(0.0);
        println!("Reflection stored: {summary_id}");
        println!("  URI:        {uri}");
        println!("  Title:        {title_val}");
        println!("  Sources:      {sources}");
        println!("  Importance:   {imp}");
    } else {
        let topic_val = response["topic"].as_str().unwrap_or("-");
        println!("Reflect prepare | topic: {topic_val}");
        println!();
        let episodes = response["episodes"].as_array();
        match episodes.map(|v| v.as_slice()) {
            None | Some([]) => println!("No recent episodes found."),
            Some(eps) => {
                println!("## Recent Episodes ({})", eps.len());
                for ep in eps {
                    let id = ep["summary_id"].as_str().unwrap_or("-");
                    let title_ep = ep["title"].as_str().unwrap_or("<untitled>");
                    let imp = ep["importance"].as_f64().unwrap_or(0.0);
                    println!("  [{id}] {title_ep} (importance: {imp})");
                }
            }
        }
        println!();
        let chunks = response["related_chunks"]["results"].as_array();
        match chunks.map(|v| v.as_slice()) {
            None | Some([]) => println!("No related chunks found."),
            Some(cks) => {
                println!("## Related Chunks ({})", cks.len());
                let mut table =
                    crate::markdown_table::MarkdownTable::new(vec!["ID", "TITLE", "SCORE"]);
                for stub in cks {
                    let mid = stub["memory_id"].as_str().unwrap_or("-");
                    let t = stub["title"].as_str().unwrap_or("-");
                    let score = stub["score"]
                        .as_f64()
                        .map(|s| format!("{s:.4}"))
                        .unwrap_or_else(|| "-".to_string());
                    table.add_row(vec![mid.to_string(), t.to_string(), score]);
                }
                print!("{table}");
            }
        }
        println!();
        println!(
            "Use `brain memory reflect --commit --title ... --content ... --source-ids <ids>` to store a reflection."
        );
    }
    Ok(())
}
