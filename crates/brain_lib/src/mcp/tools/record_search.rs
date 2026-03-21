use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::{FederatedPipeline, QueryPipeline, SearchParams};

use crate::uri::SynapseUri;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    query: String,
    #[serde(default = "default_k")]
    k: u64,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    brains: Vec<String>,
}

fn default_k() -> u64 {
    10
}

fn default_budget() -> u64 {
    800
}

pub(super) struct RecordSearch;

impl McpTool for RecordSearch {
    fn name(&self) -> &'static str {
        "records.search"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Search records (artifacts, snapshots, documents) using semantic + \
                keyword hybrid retrieval. Returns only record-kind results, filtered from the \
                full hybrid pipeline. Use this to find previously saved artifacts, snapshots, \
                reports, and documents by content."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Maximum number of results to return. Default: 10",
                        "default": 10
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 800",
                        "default": 800
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional tags to filter results. Pass as a JSON array, e.g. [\"drone-checkpoint\", \"wave:1\"]"
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional list of brain names or IDs to search across multiple brains. Use [\"all\"] to search all registered brains. When omitted, searches only the current brain."
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

            let k = (params.k as usize).min(100);
            // Over-request to account for post-filter attrition.
            let over_k = k * 3;

            let search_params = SearchParams::new(
                &params.query,
                "lookup",
                params.budget_tokens as usize,
                over_k,
                &params.tags,
            );

            let search_result = if params.brains.is_empty() {
                // Single-brain path.
                let pipeline = QueryPipeline::new(ctx.db(), store, embedder, &ctx.metrics);
                match pipeline.search(&search_params).await {
                    Ok(r) => r,
                    Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
                }
            } else {
                // Federated path — delegate setup to shared helper.
                let brains = match super::build_federated_brains(ctx, store.clone(), embedder, &params.brains).await
                {
                    Ok(b) => b,
                    Err(e) => return ToolCallResult::error(e),
                };

                let federated = FederatedPipeline {
                    db: ctx.db(),
                    brains,
                    embedder,
                    metrics: &ctx.metrics,
                };

                match federated.search(&search_params).await {
                    Ok(r) => r,
                    Err(e) => {
                        return ToolCallResult::error(format!("Federated search failed: {e}"));
                    }
                }
            };

            // Filter to record-kind only, then truncate to k.
            let record_stubs: Vec<_> = search_result
                .results
                .iter()
                .filter(|stub| stub.kind == "record")
                .take(k)
                .collect();

            let used_tokens_est: usize = record_stubs.iter().map(|s| s.token_estimate).sum();
            let num_results = record_stubs.len();

            let results_json: Vec<Value> = record_stubs
                .iter()
                .map(|stub| {
                    // Extract record_id from memory_id: "record:<ID>:<chunk>" → "<ID>"
                    let record_id = stub
                        .memory_id
                        .strip_prefix("record:")
                        .and_then(|s| s.rsplit_once(':').map(|(id, _)| id))
                        .unwrap_or(&stub.memory_id);

                    let mut result_json = json!({
                        "record_id": record_id,
                        "memory_id": stub.memory_id,
                        "title": stub.title,
                        "summary": stub.summary_2sent,
                        "score": stub.hybrid_score,
                        "kind": stub.kind,
                    });
                    let uri_brain = stub.brain_name.as_deref().unwrap_or(ctx.brain_name());
                    let uri = SynapseUri::for_record(uri_brain, record_id).to_string();
                    result_json["uri"] = json!(uri);
                    if let Some(ref bn) = stub.brain_name {
                        result_json["brain_name"] = json!(bn);
                    }
                    result_json
                })
                .collect();

            let response = json!({
                "budget_tokens": params.budget_tokens,
                "used_tokens_est": used_tokens_est,
                "result_count": num_results,
                "total_available": search_result.total_available,
                "results": results_json,
            });

            json_response(&response)
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;
    use super::RecordSearch;
    use crate::mcp::protocol::ToolDefinition;
    use crate::mcp::tools::McpTool;

    /// Verify the tool is registered and dispatches without "Unknown tool" error.
    #[tokio::test]
    async fn test_records_search_exists() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("records.search", json!({"query": "test"}), &ctx)
            .await;

        // Must NOT be "Unknown tool" — the tool must be registered.
        let error_text = &result.content[0].text;
        assert!(
            !error_text.contains("Unknown tool"),
            "records.search is not registered in ToolRegistry; got: {error_text}"
        );
    }

    /// Missing required `query` field should produce a validation error.
    #[tokio::test]
    async fn test_records_search_missing_query() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("records.search", json!({}), &ctx)
            .await;

        assert_eq!(
            result.is_error,
            Some(true),
            "missing query should yield an error"
        );
        assert!(
            result.content[0].text.contains("Invalid parameters"),
            "error message should mention invalid parameters; got: {}",
            result.content[0].text
        );
    }

    /// The tool definition must carry the expected name and required fields.
    #[tokio::test]
    async fn test_records_search_schema() {
        let tool = RecordSearch;
        let def: ToolDefinition = tool.definition();

        assert_eq!(def.name, "records.search");

        let required = def.input_schema.get("required");
        assert!(required.is_some(), "schema must have a 'required' field");

        let required_arr = required.unwrap().as_array().unwrap();
        assert!(
            required_arr.iter().any(|v| v.as_str() == Some("query")),
            "'query' must be in required; got: {required_arr:?}"
        );

        let props = def.input_schema.get("properties").unwrap();
        assert!(props.get("query").is_some(), "schema must include 'query' property");
        assert!(props.get("k").is_some(), "schema must include 'k' property");
        assert!(
            props.get("budget_tokens").is_some(),
            "schema must include 'budget_tokens' property"
        );
        assert!(props.get("tags").is_some(), "schema must include 'tags' property");
        assert!(props.get("brains").is_some(), "schema must include 'brains' property");
    }

    /// Underscore alias must also dispatch correctly.
    #[tokio::test]
    async fn test_records_search_underscore_alias() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("records_search", json!({"query": "test"}), &ctx)
            .await;

        let error_text = &result.content[0].text;
        assert!(
            !error_text.contains("Unknown tool"),
            "records_search alias not registered; got: {error_text}"
        );
    }
}
