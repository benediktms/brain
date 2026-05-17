use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

pub(super) struct MemConsolidate;

impl McpTool for MemConsolidate {
    fn name(&self) -> &'static str {
        "memory.consolidate"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Group recent episodes by temporal proximity into consolidation clusters.\n\n",
                "Returns clusters of temporally proximate episodes with suggested titles ",
                "and summaries. Clusters are ordered newest-first. Use the output to decide ",
                "which episodes to synthesize into a reflection via `memory.reflect`."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of recent episodes to consider. Default: 50",
                        "default": 50
                    },
                    "brain_id": {
                        "type": "string",
                        "description": "Brain ID to scope episodes to. Empty or omitted = current brain."
                    },
                    "gap_seconds": {
                        "type": "integer",
                        "description": "Gap in seconds that separates two clusters. Default: 3600 (1 hour)",
                        "default": 3600
                    },
                    "auto_summarize": {
                        "type": "boolean",
                        "description": "Enqueue async LLM synthesis jobs for each cluster. Default: false",
                        "default": false
                    }
                },
                "required": []
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let params: brain_memory::consolidate::ConsolidateParams =
                match serde_json::from_value(params) {
                    Ok(p) => p,
                    Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
                };

            match brain_memory::consolidate::run_as_json(
                ctx.stores.inner_db(),
                ctx.brain_id(),
                params,
            ) {
                Ok(v) => json_response(&v),
                Err(e) => ToolCallResult::error(format!("Failed to consolidate: {e}")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_consolidate_empty_returns_ok() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.consolidate", json!({}), &ctx)
            .await;
        assert!(
            result.is_error.is_none(),
            "unexpected error: {:?}",
            result.content
        );
        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["cluster_count"], 0);
        assert_eq!(parsed["jobs_enqueued"], 0);
    }

    #[tokio::test]
    async fn test_consolidate_with_limit_param() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.consolidate", json!({"limit": 10}), &ctx)
            .await;
        assert!(result.is_error.is_none());
    }

    #[tokio::test]
    async fn test_consolidate_auto_summarize_empty_enqueues_no_jobs() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.consolidate", json!({"auto_summarize": true}), &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["jobs_enqueued"], 0);
    }

    #[tokio::test]
    async fn test_consolidate_invalid_params_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        // limit must be an integer — passing a string triggers a deserialization error.
        let result = registry
            .dispatch("memory.consolidate", json!({"limit": "not-a-number"}), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
    }
}
