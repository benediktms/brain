//! `memory.retrieve` — semantic retrieval composing FTS+vector search,
//! ranking, and LOD resolution. Supports two modes:
//!
//! - **URI mode**: direct content lookup by `synapse://` address. No
//!   ranking, no embedding — straight DB read with LOD resolution.
//! - **Query mode**: hybrid search (vector + FTS) with ranking and
//!   per-result LOD resolution. Single-brain by default; federated
//!   when the caller supplies a pre-resolved brain list.
//!
//! Federated brain resolution itself stays in the MCP wrapper because
//! it depends on the registry surface (`config::open_remote_search_context`)
//! that brain_memory deliberately doesn't link.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use brain_core::error::{BrainCoreError, Result};
use brain_core::uri::{Domain, SynapseUri};
use brain_core::utils::{content_hash, now_ts};
use brain_persistence::db::Db;
use brain_persistence::ports::ChunkMetaReader;
use brain_persistence::store::StoreReader;
use brain_retrieval::VectorSearchStrategy;
use brain_retrieval::lod::LodLevel;
use brain_retrieval::lod_resolver::{
    resolve_lod_batch, resolve_lod_batch_federated, resolve_single_lod,
};
use brain_retrieval::query_pipeline::{FederatedPipeline, QueryPipeline, SearchParams};
use brain_retrieval::ranking::{ExpansionReason, resolve_intent};
use brain_retrieval::retrieval::{MemoryKind, derive_kind};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::context::SemanticContext;

/// Error message surfaced when the search layer is absent (tasks-only
/// mode). Matches the wording used by the MCP `MEMORY_UNAVAILABLE`
/// constant byte-identical so the wire contract is unchanged.
const MEMORY_UNAVAILABLE: &str =
    "Memory tools require the embedding model. Run `brain init` or download the model.";

fn default_lod() -> String {
    "L0".into()
}
fn default_count() -> u64 {
    10
}
fn default_strategy() -> String {
    "auto".into()
}

/// Typed params mirroring the MCP wire shape.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct RetrieveParams {
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default = "default_lod")]
    pub lod: String,
    #[serde(default = "default_count")]
    pub count: u64,
    #[serde(default = "default_strategy")]
    pub strategy: String,
    #[serde(default)]
    pub brain: Option<String>,
    #[serde(default)]
    pub brains: Vec<String>,
    #[serde(default)]
    pub time_scope: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub kinds: Vec<MemoryKind>,
    #[serde(default)]
    pub time_after: Option<i64>,
    #[serde(default)]
    pub time_before: Option<i64>,
    #[serde(default)]
    pub tags_require: Vec<String>,
    #[serde(default)]
    pub tags_exclude: Vec<String>,
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub vector_search_mode: Option<String>,
}

/// Resolved federated brain entry: `(brain_name, brain_id, optional_store)`.
/// A `None` store skips vector search for that brain (FTS only).
pub type FederatedBrain = (String, String, Option<StoreReader>);

/// Parse a `time_scope` string like `"7d"` or `"24h"` into a Unix
/// timestamp (now minus the duration). Returns `None` for malformed
/// input — callers may then fall back to an explicit `time_after`.
fn parse_time_scope(scope: &str) -> Option<i64> {
    let scope = scope.trim();
    if scope.is_empty() {
        return None;
    }
    let (num_str, unit) = if let Some(s) = scope.strip_suffix('d') {
        (s, "d")
    } else if let Some(s) = scope.strip_suffix('h') {
        (s, "h")
    } else {
        return None;
    };
    let n: i64 = num_str.trim().parse().ok()?;
    let seconds = match unit {
        "d" => n * 86_400,
        "h" => n * 3_600,
        _ => return None,
    };
    Some(now_ts() - seconds)
}

/// URI mode: direct content lookup by `synapse://` address with LOD
/// resolution. No embedding, no ranking — just a typed DB read.
///
/// `uri.brain == "_"` resolves to the caller's brain (`ctx.brain_id`);
/// any other brain key goes through `Db::resolve_brain`.
pub fn run_uri_mode_as_json(
    ctx: &SemanticContext<'_>,
    uri_str: &str,
    lod_level: LodLevel,
    explain: bool,
) -> Result<Value> {
    let start = Instant::now();

    let uri = uri_str
        .parse::<SynapseUri>()
        .map_err(|e| BrainCoreError::Parse(format!("Invalid URI {uri_str:?}: {e}")))?;

    let (brain_id, brain_name) = if uri.brain == "_" {
        (ctx.brain_id.to_string(), ctx.brain_name.to_string())
    } else {
        match ctx.db.resolve_brain(&uri.brain) {
            Ok((id, name)) => (id, name),
            Err(e) => {
                return Err(BrainCoreError::Parse(format!(
                    "Unknown brain {:?}: {e}",
                    uri.brain
                )));
            }
        }
    };

    let content: String = match uri.domain {
        Domain::Memory => fetch_chunk_content(ctx.db, uri.id.clone(), uri_str)?,
        Domain::Episode | Domain::Reflection | Domain::Procedure => {
            match ctx.db.get_summary_by_id(&uri.id) {
                Ok(Some(row)) => row.content,
                Ok(None) => {
                    return Err(BrainCoreError::Parse(format!(
                        "Object not found: {uri_str}"
                    )));
                }
                Err(e) => {
                    return Err(BrainCoreError::Parse(format!("DB error: {e}")));
                }
            }
        }
        Domain::Task => fetch_chunk_content(ctx.db, format!("task:{}:0", uri.id), uri_str)?,
        Domain::Record => fetch_chunk_content(ctx.db, format!("record:{}:0", uri.id), uri_str)?,
    };

    let source_hash = content_hash(&content);

    // For task/record domains, the LOD key uses the synthetic chunk_id,
    // not the raw URI — matches what build_object_uri produces in query mode.
    let lod_uri = match uri.domain {
        Domain::Task => {
            SynapseUri::for_task(&brain_name, &format!("task:{}:0", uri.id)).to_string()
        }
        Domain::Record => {
            SynapseUri::for_record(&brain_name, &format!("record:{}:0", uri.id)).to_string()
        }
        _ => uri_str.to_string(),
    };

    let (resolution, lod_diag) = resolve_single_lod(
        ctx.db,
        &lod_uri,
        &content,
        &source_hash,
        lod_level,
        &brain_id,
    );

    let query_time_ms = start.elapsed().as_millis() as u64;

    let kind = match uri.domain {
        Domain::Memory => "note",
        Domain::Episode => "episode",
        Domain::Reflection => "reflection",
        Domain::Procedure => "procedure",
        Domain::Task => "task",
        Domain::Record => "record",
    };

    let title: String = resolution
        .content
        .lines()
        .next()
        .unwrap_or("")
        .trim_start_matches('#')
        .trim()
        .to_string();

    let mut entry = json!({
        "uri": uri_str,
        "kind": kind,
        "lod": resolution.actual_lod.as_str(),
        "lod_fresh": resolution.lod_fresh,
        "title": title,
        "content": resolution.content,
        "score": null,
        "strategy_used": null,
        "generated_at": resolution.generated_at,
        "source_uri": uri_str,
        "explain": null,
        "expansion_reason": ExpansionReason::UriDirect.as_str(),
        "lod_plan_slot": 0,
    });

    if explain {
        entry["signals"] = json!(null);
    }

    Ok(json!({
        "query_time_ms": query_time_ms,
        "lod_requested": lod_level.as_str(),
        "result_count": 1,
        "lod_diagnostics": {
            "lod_hits": lod_diag.lod_hits,
            "lod_misses": lod_diag.lod_misses,
            "lod_generation_enqueued": lod_diag.lod_generation_enqueued,
        },
        "results": [entry],
    }))
}

/// Fetch chunk content by chunk_id, returning a `Parse`-typed
/// "Object not found" error for the empty case to preserve the verbatim
/// MCP wire message.
fn fetch_chunk_content(db: &Db, chunk_id: String, uri_str: &str) -> Result<String> {
    let chunk_ids = vec![chunk_id];
    match ChunkMetaReader::get_chunks_by_ids(db, &chunk_ids) {
        Ok(chunks) => chunks
            .into_iter()
            .next()
            .map(|c| c.content)
            .ok_or_else(|| BrainCoreError::Parse(format!("Object not found: {uri_str}"))),
        Err(e) => Err(BrainCoreError::Parse(format!("DB error: {e}"))),
    }
}

/// Query mode: hybrid search with ranking + LOD resolution.
///
/// `brains` empty → single-brain path using the caller's brain.
/// `brains` non-empty → federated path; the caller's brain MUST be the
/// first entry (matches `FederatedPipeline`'s contract).
pub async fn run_query_as_json(
    ctx: &SemanticContext<'_>,
    params: RetrieveParams,
    brains: Vec<FederatedBrain>,
) -> Result<Value> {
    let store = ctx
        .store
        .ok_or_else(|| BrainCoreError::Embedding(MEMORY_UNAVAILABLE.into()))?;
    let embedder = ctx
        .embedder
        .ok_or_else(|| BrainCoreError::Embedding(MEMORY_UNAVAILABLE.into()))?;

    let lod = LodLevel::parse(&params.lod.to_uppercase()).ok_or_else(|| {
        BrainCoreError::Parse(format!(
            "Invalid lod value {:?}: must be one of L0, L1, L2",
            params.lod
        ))
    })?;

    let query = match params.query.as_deref() {
        Some(q) if !q.trim().is_empty() => q,
        _ => {
            return Err(BrainCoreError::Parse(
                "Either 'query' or 'uri' is required".into(),
            ));
        }
    };

    let mode = match params.vector_search_mode.as_deref() {
        Some(s) => s
            .parse::<VectorSearchStrategy>()
            .map_err(BrainCoreError::Parse)?,
        None => VectorSearchStrategy::default(),
    };

    let time_after = params
        .time_scope
        .as_deref()
        .and_then(parse_time_scope)
        .or(params.time_after);

    let start = Instant::now();

    let is_federated = !brains.is_empty();
    let current_brain_ids = vec![ctx.brain_id.to_string()];
    let fts_brain_ids: Option<&[String]> = if is_federated {
        None
    } else {
        Some(&current_brain_ids)
    };

    // TODO(spike): evaluate enabling graph_expand — retrieve is the
    // strongest candidate since callers expect related context.
    let search_params = SearchParams::new(
        query,
        &params.strategy,
        0,
        params.count as usize,
        &params.tags,
    )
    .with_mode(mode)
    .with_brain_ids(fts_brain_ids)
    .with_brain_id(Some(ctx.brain_id))
    .with_kinds(&params.kinds)
    .with_time_after(time_after)
    .with_time_before(params.time_before)
    .with_tags_require(&params.tags_require)
    .with_tags_exclude(&params.tags_exclude);

    if is_federated {
        run_federated(ctx, &params, &search_params, brains, lod, start, embedder).await
    } else {
        run_single_brain(ctx, &params, &search_params, store, embedder, lod, start).await
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_federated(
    ctx: &SemanticContext<'_>,
    params: &RetrieveParams,
    search_params: &SearchParams<'_>,
    brains: Vec<FederatedBrain>,
    lod: LodLevel,
    start: Instant,
    embedder: &std::sync::Arc<dyn brain_core::ports::Embed>,
) -> Result<Value> {
    let federated = FederatedPipeline {
        db: ctx.db,
        brains,
        embedder,
        metrics: ctx.metrics,
    };

    let fed_result = federated
        .search_ranked_federated(search_params)
        .await
        .map_err(|e| BrainCoreError::Parse(format!("Federated search failed: {e}")))?;

    let ranked = &fed_result.ranked[..fed_result.ranked.len().min(params.count as usize)];

    // Build a brain_name → brain_id cache for L1 enqueue.
    let brain_id_cache: HashMap<String, String> = fed_result
        .chunk_brain
        .values()
        .collect::<HashSet<_>>()
        .into_iter()
        .filter_map(|name| {
            ctx.db
                .resolve_brain(name)
                .ok()
                .map(|(id, _)| (name.clone(), id))
        })
        .collect();

    let (resolutions, lod_diag) = resolve_lod_batch_federated(
        ctx.db,
        ranked,
        lod,
        &fed_result.chunk_brain,
        ctx.brain_name,
        ctx.brain_id,
        &|name| brain_id_cache.get(name).cloned(),
    );

    let query_time_ms = start.elapsed().as_millis() as u64;

    let results_json: Vec<Value> = ranked
        .iter()
        .zip(resolutions.iter())
        .enumerate()
        .map(|(slot, (r, resolution))| {
            let brain_name = fed_result
                .chunk_brain
                .get(&r.chunk_id)
                .map(|s| s.as_str())
                .unwrap_or(ctx.brain_name);
            let kind = derive_kind(&r.chunk_id, r.summary_kind.as_deref());
            let uri = match kind.as_str() {
                "episode" => SynapseUri::for_episode(brain_name, &r.chunk_id),
                "reflection" => SynapseUri::for_reflection(brain_name, &r.chunk_id),
                "procedure" => SynapseUri::for_procedure(brain_name, &r.chunk_id),
                "record" => SynapseUri::for_record(brain_name, &r.chunk_id),
                "task" | "task-outcome" => SynapseUri::for_task(brain_name, &r.chunk_id),
                _ => SynapseUri::for_memory(brain_name, &r.chunk_id),
            };
            let uri_str = uri.to_string();

            let title = if !r.heading_path.is_empty() {
                r.heading_path.clone()
            } else {
                resolution
                    .content
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim_start_matches('#')
                    .trim()
                    .to_string()
            };

            let mut entry = json!({
                "uri": uri_str,
                "kind": kind.as_str(),
                "lod": resolution.actual_lod.as_str(),
                "lod_fresh": resolution.lod_fresh,
                "title": title,
                "content": resolution.content,
                "score": r.hybrid_score,
                "strategy_used": format!("{:?}", resolve_intent(&params.strategy)),
                "generated_at": resolution.generated_at,
                "source_uri": uri_str,
                "brain": brain_name,
                "expansion_reason": r.expansion_reason.as_str(),
                "lod_plan_slot": slot,
            });

            if params.explain {
                entry["signals"] = json!({
                    "sim_vector": r.scores.vector,
                    "bm25": r.scores.keyword,
                    "recency": r.scores.recency,
                    "links": r.scores.links,
                    "tag_match": r.scores.tag_match,
                    "importance": r.scores.importance,
                });
            }

            entry
        })
        .collect();

    Ok(json!({
        "query_time_ms": query_time_ms,
        "lod_requested": lod.as_str(),
        "result_count": results_json.len(),
        "fusion_confidence": fed_result.fusion_confidence.as_ref().map(|fc| json!({
            "confidence": fc.confidence,
            "k": fc.k,
            "overlap": fc.overlap,
        })),
        "pipeline_diagnostics": null,
        "lod_diagnostics": {
            "lod_hits": lod_diag.lod_hits,
            "lod_misses": lod_diag.lod_misses,
            "lod_generation_enqueued": lod_diag.lod_generation_enqueued,
        },
        "results": results_json,
    }))
}

async fn run_single_brain(
    ctx: &SemanticContext<'_>,
    params: &RetrieveParams,
    search_params: &SearchParams<'_>,
    store: &StoreReader,
    embedder: &std::sync::Arc<dyn brain_core::ports::Embed>,
    lod: LodLevel,
    start: Instant,
) -> Result<Value> {
    let pipeline = QueryPipeline::new(ctx.db, store, embedder, ctx.metrics);

    let (ranked, fusion, pipeline_diag) = pipeline
        .search_ranked_with_diagnostics(search_params)
        .await
        .map_err(|e| BrainCoreError::Parse(format!("Search failed: {e}")))?;

    let ranked = &ranked[..ranked.len().min(params.count as usize)];

    let (resolutions, lod_diag) =
        resolve_lod_batch(ctx.db, ranked, lod, ctx.brain_name, ctx.brain_id);

    let query_time_ms = start.elapsed().as_millis() as u64;

    let results_json: Vec<Value> = ranked
        .iter()
        .zip(resolutions.iter())
        .enumerate()
        .map(|(slot, (r, resolution))| {
            let kind = derive_kind(&r.chunk_id, r.summary_kind.as_deref());
            let uri = match kind.as_str() {
                "episode" => SynapseUri::for_episode(ctx.brain_name, &r.chunk_id),
                "reflection" => SynapseUri::for_reflection(ctx.brain_name, &r.chunk_id),
                "procedure" => SynapseUri::for_procedure(ctx.brain_name, &r.chunk_id),
                "record" => SynapseUri::for_record(ctx.brain_name, &r.chunk_id),
                "task" | "task-outcome" => SynapseUri::for_task(ctx.brain_name, &r.chunk_id),
                _ => SynapseUri::for_memory(ctx.brain_name, &r.chunk_id),
            };
            let uri_str = uri.to_string();

            let title = if !r.heading_path.is_empty() {
                r.heading_path.clone()
            } else {
                resolution
                    .content
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim_start_matches('#')
                    .trim()
                    .to_string()
            };

            let mut entry = json!({
                "uri": uri_str,
                "kind": kind.as_str(),
                "lod": resolution.actual_lod.as_str(),
                "lod_fresh": resolution.lod_fresh,
                "title": title,
                "content": resolution.content,
                "score": r.hybrid_score,
                "strategy_used": format!("{:?}", resolve_intent(&params.strategy)),
                "generated_at": resolution.generated_at,
                "source_uri": uri_str,
                "expansion_reason": r.expansion_reason.as_str(),
                "lod_plan_slot": slot,
            });

            if params.explain {
                entry["signals"] = json!({
                    "sim_vector": r.scores.vector,
                    "bm25": r.scores.keyword,
                    "recency": r.scores.recency,
                    "links": r.scores.links,
                    "tag_match": r.scores.tag_match,
                    "importance": r.scores.importance,
                });
            }

            entry
        })
        .collect();

    Ok(json!({
        "query_time_ms": query_time_ms,
        "lod_requested": lod.as_str(),
        "result_count": results_json.len(),
        "pipeline_diagnostics": {
            "vector_candidates": pipeline_diag.vector_candidates,
            "fts_candidates": pipeline_diag.fts_candidates,
            "union_size": pipeline_diag.union_size,
            "reranked": pipeline_diag.reranked,
        },
        "fusion_confidence": {
            "confidence": fusion.confidence,
            "k": fusion.k,
            "overlap": fusion.overlap,
        },
        "lod_diagnostics": {
            "lod_hits": lod_diag.lod_hits,
            "lod_misses": lod_diag.lod_misses,
            "lod_generation_enqueued": lod_diag.lod_generation_enqueued,
        },
        "results": results_json,
    }))
}
