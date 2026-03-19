use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::QueryPipeline;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    memory_ids: Vec<String>,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
}

fn default_budget() -> u64 {
    2000
}

pub(super) struct MemExpand;

impl McpTool for MemExpand {
    fn name(&self) -> &'static str {
        "memory.expand"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description:
                "Expand memory stubs to full content. Pass memory_ids from search_minimal results."
                    .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "memory_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Memory IDs to expand (from search_minimal results). Pass as a JSON array, e.g. [\"abc123\", \"def456\"]"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 2000",
                        "default": 2000
                    }
                },
                "required": ["memory_ids"]
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

            let pipeline = QueryPipeline::new(ctx.db(), store, embedder, &ctx.metrics);
            let expand_result = match pipeline
                .expand(&params.memory_ids, params.budget_tokens as usize)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    error!(error = %e, "expand failed");
                    return ToolCallResult::error(format!("Expand failed: {e}"));
                }
            };

            ctx.metrics
                .record_expand_tokens(expand_result.used_tokens_est);

            let memories_json: Vec<Value> = expand_result
                .memories
                .iter()
                .map(|m| {
                    json!({
                        "memory_id": m.memory_id,
                        "content": m.content,
                        "file_path": m.file_path,
                        "heading_path": m.heading_path,
                        "byte_start": m.byte_start,
                        "byte_end": m.byte_end,
                        "truncated": m.truncated,
                    })
                })
                .collect();

            let response = json!({
                "budget_tokens": expand_result.budget_tokens,
                "used_tokens_est": expand_result.used_tokens_est,
                "memories": memories_json
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

    #[tokio::test]
    async fn test_missing_ids() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("memory.expand", json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_empty_ids() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({ "memory_ids": [], "budget_tokens": 1000 });
        let result = registry.dispatch("memory.expand", params, &ctx).await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["memories"].as_array().unwrap().len(), 0);
    }
}
