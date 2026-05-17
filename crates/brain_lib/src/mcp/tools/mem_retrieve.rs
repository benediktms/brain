use std::future::Future;
use std::pin::Pin;

use brain_core::error::BrainCoreError;
use serde_json::{Value, json};

use crate::lod::LodLevel;
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

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
            // tasks-only-mode gate: surface MCP's verbatim error string up front.
            if ctx.store().is_none() || ctx.embedder().is_none() {
                // URI mode reads chunks/summaries directly and works without
                // the search layer; gate only the query path here. The
                // brain_memory layer re-checks via SemanticContext when the
                // query path executes.
                if !matches!(params.get("uri"), Some(Value::String(s)) if !s.is_empty()) {
                    return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
                }
            }

            let params: brain_memory::retrieve::RetrieveParams =
                match serde_json::from_value(params) {
                    Ok(p) => p,
                    Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
                };

            // Validate LOD level up front so the error message matches the
            // historical wording.
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

            let sem_ctx = brain_memory::context::SemanticContext {
                db: ctx.stores.inner_db(),
                brain_id: ctx.brain_id(),
                brain_name: ctx.brain_name(),
                store: ctx.store(),
                embedder: ctx.embedder(),
                metrics: &ctx.metrics,
            };

            // URI mode — direct lookup, no search layer required.
            if let Some(ref uri_str) = params.uri {
                return match brain_memory::retrieve::run_uri_mode_as_json(
                    &sem_ctx,
                    uri_str,
                    lod,
                    params.explain,
                ) {
                    Ok(v) => json_response(&v),
                    Err(BrainCoreError::Parse(msg)) => ToolCallResult::error(msg),
                    Err(e) => ToolCallResult::error(format!("Retrieve failed: {e}")),
                };
            }

            // Query mode requires a non-empty query.
            if !has_query {
                return ToolCallResult::error("Either 'query' or 'uri' is required");
            }

            // Federated brain resolution lives in the MCP wrapper because it
            // depends on the registry surface that brain_memory does not link.
            // Empty `brains` list selects the single-brain path inside
            // brain_memory::retrieve.
            let federated_brains = if params.brains.is_empty() {
                Vec::new()
            } else {
                // SAFETY: store + embedder presence already gated above.
                let store = ctx.store().expect("store presence gated above");
                let embedder = ctx.embedder().expect("embedder presence gated above");
                match super::build_federated_brains(ctx, store.clone(), embedder, &params.brains)
                    .await
                {
                    Ok(b) => b,
                    Err(e) => return ToolCallResult::error(e),
                }
            };

            match brain_memory::retrieve::run_query_as_json(&sem_ctx, params, federated_brains)
                .await
            {
                Ok(v) => json_response(&v),
                Err(BrainCoreError::Parse(msg)) => ToolCallResult::error(msg),
                Err(BrainCoreError::Embedding(_)) => {
                    ToolCallResult::error(super::MEMORY_UNAVAILABLE)
                }
                Err(e) => ToolCallResult::error(format!("Retrieve failed: {e}")),
            }
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
