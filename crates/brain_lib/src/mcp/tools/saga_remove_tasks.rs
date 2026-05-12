use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::saga_validation::{
    MAX_TASKS_PER_BATCH, validate_actor, validate_saga_id, validate_task_id,
};
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

        if let Err(msg) = validate_saga_id(&params.saga_id) {
            return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
        }

        let (saga_id, saga_id_short) = match ctx.stores.sagas.resolve_short(&params.saga_id) {
            Ok(pair) => pair,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve saga_id: {e}")),
        };

        if let Err(msg) = validate_actor(&params.actor) {
            return ToolCallResult::error(format!("Invalid actor: {msg}"));
        }
        // Empty `task_ids` is intentionally a no-op (returns removed: 0) per
        // the idempotent semantics of remove_tasks — distinct from add_tasks
        // which rejects empty as a degenerate batch.
        if params.task_ids.len() > MAX_TASKS_PER_BATCH {
            return ToolCallResult::error(format!(
                "task_ids exceeds maximum batch size of {MAX_TASKS_PER_BATCH}"
            ));
        }
        for tid in &params.task_ids {
            if let Err(msg) = validate_task_id(tid) {
                return ToolCallResult::error(format!("Invalid task_id '{tid}': {msg}"));
            }
        }

        // ID resolution lives in `SagaStore::remove_tasks` (mirrors `add_tasks`)
        // so all transports — MCP, CLI, future callers — benefit equally.
        match ctx
            .stores
            .sagas
            .remove_tasks(&saga_id, params.task_ids, &params.actor)
        {
            Ok(removed) => {
                let response = json!({
                    "saga_id": saga_id_short,
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
                actually removed. Allowed in any saga status. \
                Accepts compact `saga-<hex>` IDs (e.g. `saga-3j5`); 26-char ULIDs are still accepted for back-compat."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": "Saga ID — either `saga-<hex>` short form or bare 26-char ULID"
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string", "minLength": 1, "maxLength": 128 },
                        "description": "Task IDs to remove from the saga (empty array is a valid no-op)",
                        "maxItems": 500
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is performing the removal. Default: mcp",
                        "default": "mcp",
                        "maxLength": 64,
                        "pattern": "^[A-Za-z0-9_:-]+$"
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
    use crate::mcp::tools::task_create::TaskCreate;

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

    async fn make_task(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let r = TaskCreate
            .call(json!({ "title": title, "task_type": "feature" }), ctx)
            .await;
        let v: Value = serde_json::from_str(&r.content[0].text).unwrap();
        v["task_id"].as_str().unwrap().to_string()
    }

    async fn add_tasks(ctx: &crate::mcp::McpContext, saga_id: &str, task_ids: &[&str]) {
        let owned: Vec<String> = task_ids.iter().map(|s| s.to_string()).collect();
        ctx.stores.sagas.add_tasks(saga_id, &owned, "test").unwrap();
    }

    #[tokio::test]
    async fn test_remove_existing_tasks() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Remove Test").await;
        let t1 = make_task(&ctx, "T1").await;
        let t2 = make_task(&ctx, "T2").await;
        let t3 = make_task(&ctx, "T3").await;
        add_tasks(&ctx, &saga_id, &[&t1, &t2, &t3]).await;

        let result = call_remove(json!({ "saga_id": saga_id, "task_ids": [t1, t2] }), &ctx).await;
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
        let t1 = make_task(&ctx, "T1").await;
        let t2 = make_task(&ctx, "T2").await;
        let t3 = make_task(&ctx, "T3").await;
        add_tasks(&ctx, &saga_id, &[&t1, &t3]).await;

        let result = call_remove(
            json!({ "saga_id": saga_id, "task_ids": [t1, t2, t3] }),
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
