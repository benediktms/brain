use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    task_ids: Vec<String>,
    #[serde(default = "default_actor")]
    actor: String,
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct SagaRemoveTasks;

impl SagaRemoveTasks {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        match ctx
            .stores
            .sagas
            .remove_tasks(&params.saga_id, params.task_ids, &params.actor)
        {
            Ok(removed) => {
                let response = json!({
                    "saga_id": params.saga_id,
                    "removed": removed,
                });
                json_response(&response)
            }
            Err(e) => ToolCallResult::error(format!("Failed to remove tasks from saga: {e}")),
        }
    }
}

impl McpTool for SagaRemoveTasks {
    fn name(&self) -> &'static str {
        "sagas.remove_tasks"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Remove one or more tasks from a saga. Idempotent: task IDs that are \
                not members of the saga are silently ignored. Returns the count of tasks \
                actually removed. Allowed in any saga status."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": "Bare 26-char ULID saga ID"
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Task IDs to remove from the saga"
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is performing the removal. Default: mcp",
                        "default": "mcp"
                    }
                },
                "required": ["saga_id", "task_ids"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move { self.execute(params, ctx) })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::tests::create_test_context;
    use super::{McpTool, SagaRemoveTasks};
    use crate::mcp::tools::saga_create::SagaCreate;

    async fn call_remove(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaRemoveTasks.call(params, ctx).await
    }

    async fn create_saga(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let result = SagaCreate.call(json!({ "title": title }), ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        parsed["saga_id"].as_str().unwrap().to_string()
    }

    async fn add_tasks(ctx: &crate::mcp::McpContext, saga_id: &str, task_ids: &[&str]) {
        ctx.stores
            .sagas
            .add_tasks(
                saga_id,
                task_ids.iter().map(|s| s.to_string()).collect::<Vec<String>>(),
                "test",
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_remove_existing_tasks() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Remove Test").await;
        add_tasks(&ctx, &saga_id, &["T-001", "T-002", "T-003"]).await;

        let result = call_remove(
            json!({ "saga_id": saga_id, "task_ids": ["T-001", "T-002"] }),
            &ctx,
        )
        .await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["removed"], 2);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_is_noop() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Noop Test").await;

        let result = call_remove(
            json!({ "saga_id": saga_id, "task_ids": ["NONEXISTENT-001"] }),
            &ctx,
        )
        .await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["removed"], 0);
    }

    #[tokio::test]
    async fn test_remove_mixed_batch() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Mixed Test").await;
        add_tasks(&ctx, &saga_id, &["T-001", "T-003"]).await;

        let result = call_remove(
            json!({ "saga_id": saga_id, "task_ids": ["T-001", "T-002", "T-003"] }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["removed"], 2);
    }

    #[tokio::test]
    async fn test_remove_empty_list() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Empty Test").await;

        let result = call_remove(json!({ "saga_id": saga_id, "task_ids": [] }), &ctx).await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["removed"], 0);
    }

    #[tokio::test]
    async fn test_remove_missing_params_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_remove(json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaRemoveTasks.underscore_alias(), "sagas_remove_tasks");
    }
}
