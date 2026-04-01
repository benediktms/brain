use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::lod::LodLevel;
use crate::lod_resolver::resolve_lod_batch;
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::{FederatedPipeline, QueryPipeline, SearchParams};
use crate::ranking::resolve_intent;
use crate::retrieval::{MemoryKind, derive_kind};
use crate::uri::SynapseUri;
use brain_persistence::store::VectorSearchMode;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    query: String,
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

pub(super) struct MemRetrieve;

impl McpTool for MemRetrieve {
    fn name(&self) -> &'static str {
        "memory.retrieve"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Retrieve memory chunks at a requested level of detail (LOD). L0 returns extractive summaries (~100 tokens each), L1 returns LLM-summarized content (~2000 tokens each), L2 returns full source content. Falls back to the next available level when the requested LOD is not yet generated.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
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
                },
                "required": ["query"]
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

            let search_params = SearchParams::new(
                &params.query,
                &params.strategy,
                0, // budget_tokens unused by search_ranked_with_diagnostics
                params.count as usize,
                &params.tags,
            )
            .with_mode(mode)
            .with_brain_ids(fts_brain_ids)
            .with_kinds(&params.kinds)
            .with_time_after(time_after)
            .with_time_before(params.time_before)
            .with_tags_require(&params.tags_require)
            .with_tags_exclude(&params.tags_exclude);

            if is_federated {
                // Federated path — LOD forced to L2 (no per-brain LOD resolution).
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

                let federated = FederatedPipeline {
                    db: ctx.stores.db(),
                    brains,
                    embedder,
                    metrics: &ctx.metrics,
                };

                let search_result = match federated.search(&search_params).await {
                    Ok(r) => r,
                    Err(e) => {
                        return ToolCallResult::error(format!("Federated search failed: {e}"));
                    }
                };

                let query_time_ms = start.elapsed().as_millis() as u64;

                let results_json: Vec<Value> = search_result
                    .results
                    .iter()
                    .map(|stub| {
                        let uri_brain = stub.brain_name.as_deref().unwrap_or(ctx.brain_name());
                        let uri = match stub.kind.as_str() {
                            "episode" => SynapseUri::for_episode(uri_brain, &stub.memory_id),
                            "reflection" => SynapseUri::for_reflection(uri_brain, &stub.memory_id),
                            "procedure" => SynapseUri::for_procedure(uri_brain, &stub.memory_id),
                            "record" => SynapseUri::for_record(uri_brain, &stub.memory_id),
                            "task" | "task-outcome" => {
                                SynapseUri::for_task(uri_brain, &stub.memory_id)
                            }
                            _ => SynapseUri::for_memory(uri_brain, &stub.memory_id),
                        };
                        let uri_str = uri.to_string();
                        json!({
                            "uri": uri_str,
                            "kind": stub.kind,
                            "lod": "L2",
                            "lod_fresh": true,
                            "title": stub.title,
                            "content": stub.summary_2sent,
                            "score": stub.hybrid_score,
                            "strategy_used": format!("{:?}", resolve_intent(&params.strategy)),
                            "generated_at": null,
                            "source_uri": uri_str,
                        })
                    })
                    .collect();

                let response = json!({
                    "query_time_ms": query_time_ms,
                    "lod_requested": lod.as_str(),
                    "result_count": results_json.len(),
                    "results": results_json,
                });

                return json_response(&response);
            }

            // Single-brain path.
            let pipeline = QueryPipeline::new(ctx.stores.db(), store, embedder, &ctx.metrics);

            let (ranked, _fusion, pipeline_diag) = match pipeline
                .search_ranked_with_diagnostics(&search_params)
                .await
            {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
            };

            // Truncate to count.
            let ranked = &ranked[..ranked.len().min(params.count as usize)];

            // Resolve LOD for each result.
            let (resolutions, lod_diag) = resolve_lod_batch(
                ctx.stores.db(),
                ranked,
                lod,
                ctx.brain_name(),
                ctx.brain_id(),
            );

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
