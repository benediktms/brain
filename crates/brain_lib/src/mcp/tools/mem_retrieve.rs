use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::lod::LodLevel;
use crate::lod_resolver::{resolve_lod_batch, resolve_lod_batch_federated, resolve_single_lod};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::SearchParams;
use crate::ranking::resolve_intent;
use crate::retrieval::{MemoryKind, derive_kind};
use crate::uri::{Domain, SynapseUri};
use brain_persistence::store::VectorSearchMode;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    uri: Option<String>,
    #[serde(default = "default_lod")]
    lod: String,
    #[serde(default = "default_count")]
    count: u64,
    #[serde(default = "default_strategy")]
    strategy: String,
    #[serde(default)]
    #[allow(dead_code)]
    brain: Option<String>,
    #[serde(default)]
    brains: Vec<String>,
    #[serde(default)]
    time_scope: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    kinds: Vec<MemoryKind>,
    #[serde(default)]
    time_after: Option<i64>,
    #[serde(default)]
    time_before: Option<i64>,
    #[serde(default)]
    tags_require: Vec<String>,
    #[serde(default)]
    tags_exclude: Vec<String>,
    #[serde(default)]
    explain: bool,
    #[serde(default)]
    vector_search_mode: Option<String>,
}

fn default_lod() -> String {
    "L0".into()
}
fn default_count() -> u64 {
    10
}
fn default_strategy() -> String {
    "auto".into()
}

/// Parse a time_scope string like "7d", "30d" into a Unix timestamp (now - duration).
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
    let now = crate::utils::now_ts();
    Some(now - seconds)
}

async fn handle_uri_mode(
    ctx: &McpContext,
    uri_str: &str,
    lod_level: LodLevel,
    explain: bool,
) -> ToolCallResult {
    let start = Instant::now();

    // Parse the URI.
    let uri = match uri_str.parse::<SynapseUri>() {
        Ok(u) => u,
        Err(e) => return ToolCallResult::error(format!("Invalid URI {uri_str:?}: {e}")),
    };

    let stores = &ctx.stores;

    // Resolve brain: "_" means current brain.
    let (brain_id, brain_name) = if uri.brain == "_" {
        (ctx.brain_id().to_string(), ctx.brain_name().to_string())
    } else {
        match stores.resolve_brain(&uri.brain) {
            Ok((id, name)) => (id, name),
            Err(e) => {
                return ToolCallResult::error(format!("Unknown brain {:?}: {e}", uri.brain));
            }
        }
    };

    // Domain dispatch: fetch content.
    let content: String = match uri.domain {
        Domain::Memory => {
            let chunk_ids = vec![uri.id.clone()];
            match stores.get_chunks_by_ids(&chunk_ids) {
                Ok(chunks) => match chunks.into_iter().next() {
                    Some(c) => c.content,
                    None => return ToolCallResult::error(format!("Object not found: {uri_str}")),
                },
                Err(e) => return ToolCallResult::error(format!("DB error: {e}")),
            }
        }
        Domain::Episode | Domain::Reflection | Domain::Procedure => {
            match stores.get_summary_by_id(&uri.id) {
                Ok(Some(row)) => row.content,
                Ok(None) => return ToolCallResult::error(format!("Object not found: {uri_str}")),
                Err(e) => return ToolCallResult::error(format!("DB error: {e}")),
            }
        }
        Domain::Task => {
            let chunk_ids = vec![format!("task:{}:0", uri.id)];
            match stores.get_chunks_by_ids(&chunk_ids) {
                Ok(chunks) => match chunks.into_iter().next() {
                    Some(c) => c.content,
                    None => return ToolCallResult::error(format!("Object not found: {uri_str}")),
                },
                Err(e) => return ToolCallResult::error(format!("DB error: {e}")),
            }
        }
        Domain::Record => {
            let chunk_ids = vec![format!("record:{}:0", uri.id)];
            match stores.get_chunks_by_ids(&chunk_ids) {
                Ok(chunks) => match chunks.into_iter().next() {
                    Some(c) => c.content,
                    None => return ToolCallResult::error(format!("Object not found: {uri_str}")),
                },
                Err(e) => return ToolCallResult::error(format!("DB error: {e}")),
            }
        }
    };

    let source_hash = crate::utils::content_hash(&content);

    // Build the LOD object_uri matching what build_object_uri produces in query mode.
    // For task/record domains, the LOD key uses the synthetic chunk_id, not the raw URI.
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
        stores,
        &lod_uri,
        &content,
        &source_hash,
        lod_level,
        &brain_id,
    );

    let query_time_ms = start.elapsed().as_millis() as u64;

    // Derive kind string from domain.
    let kind = match uri.domain {
        Domain::Memory => "note",
        Domain::Episode => "episode",
        Domain::Reflection => "reflection",
        Domain::Procedure => "procedure",
        Domain::Task => "task",
        Domain::Record => "record",
    };

    // Derive title: first line of resolved content.
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
    });

    if explain {
        entry["signals"] = json!(null);
    }

    let _ = brain_name; // used for brain resolution; not needed in response

    let response = json!({
        "query_time_ms": query_time_ms,
        "lod_requested": lod_level.as_str(),
        "result_count": 1,
        "lod_diagnostics": {
            "lod_hits": lod_diag.lod_hits,
            "lod_misses": lod_diag.lod_misses,
            "lod_generation_enqueued": lod_diag.lod_generation_enqueued,
        },
        "results": [entry],
    });

    json_response(&response)
}

pub(super) struct MemRetrieve;

impl McpTool for MemRetrieve {
    fn name(&self) -> &'static str {
        "memory.retrieve"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Retrieve memory chunks at a requested level of detail (LOD). Supports two modes: query (semantic search) and URI (direct access by synapse:// address). L0 returns extractive summaries (~100 tokens each), L1 returns LLM-summarized content (~2000 tokens each), L2 returns full source content. Falls back to the next available level when the requested LOD is not yet generated. Provide `query` for semantic search or `uri` for direct access.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query. Provide either query or uri."
                    },
                    "uri": {
                        "type": "string",
                        "description": "Direct access by synapse:// URI (e.g. synapse://brain-name/memory/chunk-id). Provide either query or uri."
                    },
                    "lod": {
                        "type": "string",
                        "enum": ["L0", "L1", "L2"],
                        "description": "Level of detail for returned content. L0: extractive abstract (~100 tokens). L1: LLM summary (~2000 tokens). L2: full source passthrough. Default: L0",
                        "default": "L0"
                    },
                    "count": {
                        "type": "integer",
                        "description": "Maximum number of results. Default: 10",
                        "default": 10
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["lookup", "planning", "reflection", "synthesis", "auto"],
                        "description": "Retrieval strategy — controls ranking weight profile. Default: auto",
                        "default": "auto"
                    },
                    "brain": {
                        "type": "string",
                        "description": "Optional brain name or ID to scope search to a single brain"
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional list of brain names or IDs to search across multiple brains. Use [\"all\"] to search all registered brains."
                    },
                    "time_scope": {
                        "type": "string",
                        "description": "Relative time window, e.g. \"7d\" or \"24h\". Sets time_after to now minus the duration."
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags to boost results via Jaccard similarity"
                    },
                    "kinds": {
                        "type": "array",
                        "items": { "type": "string", "enum": ["note", "episode", "reflection", "procedure", "task", "task-outcome", "record"] },
                        "description": "Filter by result kind. Empty = all kinds."
                    },
                    "time_after": {
                        "type": "integer",
                        "description": "Only results modified/created after this Unix timestamp (seconds)"
                    },
                    "time_before": {
                        "type": "integer",
                        "description": "Only results modified/created before this Unix timestamp (seconds)"
                    },
                    "tags_require": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Require ALL of these tags (AND logic, case-insensitive)"
                    },
                    "tags_exclude": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Exclude results matching ANY of these tags (NOR logic, case-insensitive)"
                    },
                    "explain": {
                        "type": "boolean",
                        "description": "When true, include per-signal score breakdowns in the response. Default: false",
                        "default": false
                    },
                    "vector_search_mode": {
                        "type": "string",
                        "enum": ["exact", "ann_refined", "ann_fast"],
                        "description": "Vector search strategy. Default: ann_refined"
                    }
                }
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let Some(store) = ctx.store() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };
            let Some(embedder) = ctx.embedder() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };

            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            // Validate LOD level.
            let lod = match LodLevel::parse(&params.lod.to_uppercase()) {
                Some(l) => l,
                None => {
                    return ToolCallResult::error(format!(
                        "Invalid lod value {:?}: must be one of L0, L1, L2",
                        params.lod
                    ));
                }
            };

            let has_query = params.query.as_ref().is_some_and(|q| !q.trim().is_empty());
            let has_uri = params.uri.is_some();

            if has_query && has_uri {
                return ToolCallResult::error("Provide 'query' or 'uri', not both");
            }

            // URI mode: direct access by synapse:// URI, no ranking.
            if let Some(ref uri_str) = params.uri {
                return handle_uri_mode(ctx, uri_str, lod, params.explain).await;
            }

            // Query mode: query is required.
            let query = match params.query.as_deref() {
                Some(q) if !q.trim().is_empty() => q,
                _ => return ToolCallResult::error("Either 'query' or 'uri' is required"),
            };

            let mode = match params.vector_search_mode.as_deref() {
                Some(s) => match s.parse::<VectorSearchMode>() {
                    Ok(m) => m,
                    Err(e) => return ToolCallResult::error(e),
                },
                None => VectorSearchMode::default(),
            };

            // Resolve time_scope → time_after (overrides explicit time_after if set).
            let time_after = params
                .time_scope
                .as_deref()
                .and_then(parse_time_scope)
                .or(params.time_after);

            let start = Instant::now();

            // Determine federated vs single-brain.
            let is_federated = !params.brains.is_empty();

            let current_brain_ids = vec![ctx.brain_id().to_string()];
            let fts_brain_ids: Option<&[String]> = if is_federated {
                None
            } else {
                Some(&current_brain_ids)
            };

            // TODO(spike): evaluate enabling graph_expand — retrieve is the
            // strongest candidate since callers expect related context
            let search_params = SearchParams::new(
                query,
                &params.strategy,
                0, // budget_tokens unused by search_ranked_with_diagnostics
                params.count as usize,
                &params.tags,
            )
            .with_mode(mode)
            .with_brain_ids(fts_brain_ids)
            .with_brain_id(Some(ctx.brain_id()))
            .with_kinds(&params.kinds)
            .with_time_after(time_after)
            .with_time_before(params.time_before)
            .with_tags_require(&params.tags_require)
            .with_tags_exclude(&params.tags_exclude);

            if is_federated {
                // Federated path — resolve LOD per-brain using ranked results.
                let brains = match super::build_federated_brains(
                    ctx,
                    store.clone(),
                    embedder,
                    &params.brains,
                )
                .await
                {
                    Ok(b) => b,
                    Err(e) => return ToolCallResult::error(e),
                };

                let federated = ctx
                    .stores
                    .federated_pipeline(brains, embedder, &ctx.metrics);

                let fed_result = match federated.search_ranked_federated(&search_params).await {
                    Ok(r) => r,
                    Err(e) => {
                        return ToolCallResult::error(format!("Federated search failed: {e}"));
                    }
                };

                // Truncate to requested count.
                let ranked =
                    &fed_result.ranked[..fed_result.ranked.len().min(params.count as usize)];

                // Build brain_name → brain_id cache for L1 enqueue.
                let brain_id_cache: std::collections::HashMap<String, String> = fed_result
                    .chunk_brain
                    .values()
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .filter_map(|name| {
                        ctx.stores
                            .resolve_brain(name)
                            .ok()
                            .map(|(id, _)| (name.clone(), id))
                    })
                    .collect();

                // Resolve LOD with per-result brain attribution.
                let (resolutions, lod_diag) = resolve_lod_batch_federated(
                    &ctx.stores,
                    ranked,
                    lod,
                    &fed_result.chunk_brain,
                    ctx.brain_name(),
                    ctx.brain_id(),
                    &|name| brain_id_cache.get(name).cloned(),
                );

                let query_time_ms = start.elapsed().as_millis() as u64;

                let results_json: Vec<Value> = ranked
                    .iter()
                    .zip(resolutions.iter())
                    .map(|(r, resolution)| {
                        let brain_name = fed_result
                            .chunk_brain
                            .get(&r.chunk_id)
                            .map(|s| s.as_str())
                            .unwrap_or(ctx.brain_name());
                        let kind = derive_kind(&r.chunk_id, r.summary_kind.as_deref());
                        let uri = match kind.as_str() {
                            "episode" => SynapseUri::for_episode(brain_name, &r.chunk_id),
                            "reflection" => SynapseUri::for_reflection(brain_name, &r.chunk_id),
                            "procedure" => SynapseUri::for_procedure(brain_name, &r.chunk_id),
                            "record" => SynapseUri::for_record(brain_name, &r.chunk_id),
                            "task" | "task-outcome" => {
                                SynapseUri::for_task(brain_name, &r.chunk_id)
                            }
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

                let response = json!({
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
                });

                return json_response(&response);
            }

            // Single-brain path.
            let pipeline = ctx.stores.query_pipeline(store, embedder, &ctx.metrics);

            let (ranked, fusion, pipeline_diag) = match pipeline
                .search_ranked_with_diagnostics(&search_params)
                .await
            {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
            };

            // Truncate to count.
            let ranked = &ranked[..ranked.len().min(params.count as usize)];

            // Resolve LOD for each result.
            let (resolutions, lod_diag) =
                resolve_lod_batch(&ctx.stores, ranked, lod, ctx.brain_name(), ctx.brain_id());

            let query_time_ms = start.elapsed().as_millis() as u64;

            let results_json: Vec<Value> = ranked
                .iter()
                .zip(resolutions.iter())
                .map(|(r, resolution)| {
                    let kind = derive_kind(&r.chunk_id, r.summary_kind.as_deref());
                    let uri = match kind.as_str() {
                        "episode" => SynapseUri::for_episode(ctx.brain_name(), &r.chunk_id),
                        "reflection" => SynapseUri::for_reflection(ctx.brain_name(), &r.chunk_id),
                        "procedure" => SynapseUri::for_procedure(ctx.brain_name(), &r.chunk_id),
                        "record" => SynapseUri::for_record(ctx.brain_name(), &r.chunk_id),
                        "task" | "task-outcome" => {
                            SynapseUri::for_task(ctx.brain_name(), &r.chunk_id)
                        }
                        _ => SynapseUri::for_memory(ctx.brain_name(), &r.chunk_id),
                    };
                    let uri_str = uri.to_string();

                    // Title: heading_path if non-empty, else first line of content.
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

            let response = json!({
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
            });

            json_response(&response)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use crate::mcp::McpContext;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_missing_query() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("memory.retrieve", json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("required"),
            "got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_neither_query_nor_uri_errors() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.retrieve", json!({ "lod": "L2" }), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("required"),
            "got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_query_and_uri_rejects() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.retrieve",
                json!({ "query": "hello", "uri": "synapse://b/memory/x" }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("not both"),
            "got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_uri_mode_invalid_uri() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.retrieve", json!({ "uri": "not-a-valid-uri" }), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("Invalid URI"),
            "got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_memory_unavailable_no_store() {
        let (tmp, stores) =
            crate::stores::BrainStores::in_memory().expect("checked in test assertions");
        let ctx = McpContext {
            stores,
            search: None,
            writable_store: None,
            metrics: Arc::new(crate::metrics::Metrics::new()),
        };
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.retrieve", json!({ "query": "test" }), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0]
                .text
                .contains("Memory tools are unavailable"),
            "got: {}",
            result.content[0].text
        );
        drop(tmp);
    }

    #[tokio::test]
    async fn test_valid_query_defaults() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.retrieve", json!({ "query": "hello" }), &ctx)
            .await;
        assert!(
            result.is_error.is_none(),
            "should succeed with defaults; got: {}",
            result.content[0].text
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).expect("valid JSON");
        assert_eq!(parsed["lod_requested"], "L0");
        assert_eq!(parsed["result_count"], 0);
        assert!(parsed["results"].is_array());
    }

    #[tokio::test]
    async fn test_invalid_lod_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.retrieve",
                json!({ "query": "hello", "lod": "L9" }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid lod value"));
    }

    #[tokio::test]
    async fn test_lod_l2_accepted() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.retrieve",
                json!({ "query": "hello", "lod": "L2" }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).expect("valid JSON");
        assert_eq!(parsed["lod_requested"], "L2");
    }

    #[tokio::test]
    async fn test_response_shape() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.retrieve", json!({ "query": "anything" }), &ctx)
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).expect("valid JSON");
        assert!(
            parsed.get("query_time_ms").is_some(),
            "missing query_time_ms"
        );
        assert!(
            parsed.get("lod_requested").is_some(),
            "missing lod_requested"
        );
        assert!(parsed.get("result_count").is_some(), "missing result_count");
        assert!(parsed.get("results").is_some(), "missing results");
        assert!(parsed["results"].is_array());
    }
}
