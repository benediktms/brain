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
    topic: String,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
}

fn default_budget() -> u64 {
    2000
}

pub(super) struct MemReflect;

impl McpTool for MemReflect {
    fn name(&self) -> &'static str {
        "memory.reflect"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Retrieve source material for reflection. Returns relevant memories that the LLM can synthesize into a reflection, then call back to store.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic to reflect on"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens for source material. Default: 2000",
                        "default": 2000
                    }
                },
                "required": ["topic"]
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
            let reflect_result = match pipeline
                .reflect(params.topic.clone(), params.budget_tokens as usize)
                .await
            {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Reflect failed: {e}")),
            };

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

            let response = json!({
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
    async fn test_reflect() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({ "topic": "project architecture" });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(result.is_error.is_none());
    }
}
