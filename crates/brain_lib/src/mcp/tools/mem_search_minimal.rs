use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::QueryPipeline;

use super::McpTool;

#[derive(Deserialize)]
struct Params {
    query: String,
    #[serde(default = "default_intent")]
    intent: String,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
    #[serde(default = "default_k")]
    k: u64,
}

fn default_intent() -> String {
    "auto".into()
}
fn default_budget() -> u64 {
    800
}
fn default_k() -> u64 {
    10
}

pub(super) struct MemSearchMinimal;

impl McpTool for MemSearchMinimal {
    fn name(&self) -> &'static str {
        "memory.search_minimal"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Search the knowledge base and return compact memory stubs within a token budget. Use this first to find relevant memories, then expand specific ones.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "intent": {
                        "type": "string",
                        "enum": ["lookup", "planning", "reflection", "synthesis", "auto"],
                        "description": "Retrieval intent — controls ranking weight profile. Default: auto",
                        "default": "auto"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 800",
                        "default": 800
                    },
                    "k": {
                        "type": "integer",
                        "description": "Maximum number of results. Default: 10",
                        "default": 10
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
            let Some(store) = ctx.store.as_ref() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };
            let Some(embedder) = ctx.embedder.as_ref() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };

            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let pipeline = QueryPipeline::new(&ctx.db, store, embedder, &ctx.metrics);
            let search_result = match pipeline
                .search(
                    &params.query,
                    &params.intent,
                    params.budget_tokens as usize,
                    params.k as usize,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
            };

            ctx.metrics
                .record_search_minimal_tokens(search_result.used_tokens_est);

            let results_json: Vec<Value> = search_result
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

            let response = json!({
                "budget_tokens": search_result.budget_tokens,
                "used_tokens_est": search_result.used_tokens_est,
                "intent_resolved": format!("{:?}", crate::ranking::resolve_intent(&params.intent)),
                "result_count": search_result.num_results,
                "total_available": search_result.total_available,
                "results": results_json
            });

            ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_missing_query() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.search_minimal", json!({}), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
    }
}
