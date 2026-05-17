//! `memory.reflect` — gather episode sources via vector search
//! (prepare mode), or persist a synthesized reflection linked to
//! source episodes (commit mode).
//!
//! `prepare` composes brain_retrieval's QueryPipeline with the
//! caller's-brain episode listing. `commit` writes a row of
//! `kind = 'reflection'` into the `summaries` table, linked to its
//! source episode IDs.

use std::collections::HashSet;

use brain_core::error::{BrainCoreError, Result};
use brain_core::uri::SynapseUri;
use brain_persistence::db::summaries::{self, SummaryRow};
use brain_persistence::sql::SqlResultExt;
use brain_retrieval::query_pipeline::QueryPipeline;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::context::SemanticContext;

fn default_mode() -> String {
    "prepare".to_string()
}

fn default_budget() -> u64 {
    2000
}

/// Typed params covering both `prepare` and `commit` modes. Unused
/// fields default to empty / 0 so the JSON shape stays permissive.
#[derive(Deserialize, Debug, Clone)]
pub struct ReflectParams {
    #[serde(default = "default_mode")]
    pub mode: String,
    // -- prepare fields --
    #[serde(default)]
    pub topic: String,
    #[serde(default = "default_budget")]
    pub budget_tokens: u64,
    /// Brain names/IDs to include. Empty = current brain. "all" = all brains.
    #[serde(default)]
    pub brains: Vec<String>,
    // -- commit fields --
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub source_ids: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub importance: Option<f64>,
}

/// Dispatch on `params.mode`. Returns a `Parse`-typed error for an
/// unknown mode string so the MCP wrapper can surface it verbatim.
pub async fn run_as_json(
    ctx: &SemanticContext<'_>,
    params: ReflectParams,
) -> Result<Value> {
    match params.mode.as_str() {
        "prepare" => prepare_as_json(ctx, params).await,
        "commit" => commit_as_json(ctx, params),
        other => Err(BrainCoreError::Parse(format!(
            "Invalid mode: '{other}'. Must be 'prepare' or 'commit'"
        ))),
    }
}

/// Prepare mode: load episodes for the requested brain scope, run
/// the retrieval pipeline for additional source chunks, and shape the
/// MCP wire envelope.
pub async fn prepare_as_json(
    ctx: &SemanticContext<'_>,
    params: ReflectParams,
) -> Result<Value> {
    let store = ctx
        .store
        .ok_or_else(|| BrainCoreError::Embedding(MEMORY_UNAVAILABLE.into()))?;
    let embedder = ctx
        .embedder
        .ok_or_else(|| BrainCoreError::Embedding(MEMORY_UNAVAILABLE.into()))?;

    if params.topic.is_empty() {
        return Err(BrainCoreError::Parse(
            "'topic' is required for prepare mode".into(),
        ));
    }

    let pipeline = QueryPipeline::new(ctx.db, store, embedder, ctx.metrics);

    let episodes: Vec<SummaryRow> = if params.brains.is_empty() {
        let brain_id = ctx.brain_id.to_string();
        ctx.db
            .with_read_conn(move |conn| summaries::list_episodes(conn, 10, &brain_id))
            .into_brain_core()?
    } else if params.brains.iter().any(|b| b == "all") {
        ctx.db
            .with_read_conn(|conn| summaries::list_episodes(conn, 10, ""))
            .into_brain_core()?
    } else {
        let brain_ids: Vec<String> = params.brains.clone();
        ctx.db
            .with_read_conn(move |conn| {
                summaries::list_episodes_multi_brain(conn, 10, &brain_ids)
            })
            .into_brain_core()?
    };

    let reflect_result = pipeline
        .reflect_with_episodes(params.topic.clone(), params.budget_tokens as usize, episodes)
        .await?;

    let episode_sources: Vec<Value> = reflect_result
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

    let related_chunks_json: Vec<Value> = reflect_result
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

    Ok(json!({
        "mode": "prepare",
        "topic": reflect_result.topic,
        "budget_tokens": reflect_result.budget_tokens,
        "source_count": episode_sources.len(),
        "episodes": episode_sources,
        "related_chunks": {
            "budget_tokens": reflect_result.search_result.budget_tokens,
            "used_tokens_est": reflect_result.search_result.used_tokens_est,
            "result_count": reflect_result.search_result.num_results,
            "total_available": reflect_result.search_result.total_available,
            "results": related_chunks_json,
        },
    }))
}

/// Commit mode: validate source_ids exist, persist a reflection row,
/// emit the wire envelope.
pub fn commit_as_json(ctx: &SemanticContext<'_>, params: ReflectParams) -> Result<Value> {
    if params.title.is_empty() {
        return Err(BrainCoreError::Parse(
            "'title' is required for commit mode".into(),
        ));
    }
    if params.content.is_empty() {
        return Err(BrainCoreError::Parse(
            "'content' is required for commit mode".into(),
        ));
    }
    if params.source_ids.is_empty() {
        return Err(BrainCoreError::Parse(
            "'source_ids' is required for commit mode".into(),
        ));
    }

    let importance = params.importance.unwrap_or(1.0).clamp(0.0, 1.0);

    // Batch-validate source_ids in a single round-trip.
    let source_ids = params.source_ids.clone();
    let found = {
        let ids = source_ids.clone();
        ctx.db
            .with_read_conn(move |conn| summaries::get_summaries_by_ids(conn, &ids))
            .into_brain_core()?
    };
    let found_ids: HashSet<&str> = found.iter().map(|r| r.summary_id.as_str()).collect();
    for id in &source_ids {
        if !found_ids.contains(id.as_str()) {
            return Err(BrainCoreError::Parse(format!("source_id not found: {id}")));
        }
    }

    let title = params.title.clone();
    let content = params.content.clone();
    let tags = params.tags.clone();
    let brain_id = ctx.brain_id.to_string();
    let source_ids_owned = source_ids.clone();

    let summary_id = ctx
        .db
        .with_write_conn(move |conn| {
            summaries::store_reflection(
                conn,
                &title,
                &content,
                &source_ids_owned,
                &tags,
                importance,
                &brain_id,
            )
        })
        .into_brain_core()?;

    let uri = SynapseUri::for_reflection(ctx.brain_name, &summary_id).to_string();

    Ok(json!({
        "mode": "commit",
        "status": "stored",
        "summary_id": summary_id,
        "uri": uri,
        "title": params.title,
        "source_count": source_ids.len(),
        "importance": importance,
    }))
}

/// Error message surfaced when the search layer is absent (tasks-only
/// mode). Matches the wording used by the MCP `super::MEMORY_UNAVAILABLE`
/// constant so the wire contract stays byte-identical for callers.
const MEMORY_UNAVAILABLE: &str =
    "Memory tools require the embedding model. Run `brain init` or download the model.";
