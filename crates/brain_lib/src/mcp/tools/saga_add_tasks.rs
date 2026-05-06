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

pub(super) struct SagaAddTasks;

impl SagaAddTasks {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if params.task_ids.is_empty() {
            return ToolCallResult::error("task_ids must not be empty");
        }

        match ctx
            .stores
            .sagas
            .add_tasks(&params.saga_id, &params.task_ids, &params.actor)
        {
            Ok(count) => json_response(&json!({
                "saga_id": params.saga_id,
                "added": count,
            })),
            Err(e) => ToolCallResult::error(format!("{e}")),
        }
    }
}

impl McpTool for SagaAddTasks {
    fn name(&self) -> &'static str {
        "sagas.add_tasks"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Atomically add one or more tasks to a saga. All task IDs must resolve \
                (cross-brain short IDs are supported). The saga must not be closed or cancelled. \
                Duplicate adds and unresolvable IDs cause the entire batch to fail."
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
                        "description": "Task IDs to add (full IDs or short hashes, cross-brain aware)",
                        "minItems": 1
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is adding the tasks. Default: mcp",
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
    use super::{McpTool, SagaAddTasks};
    use crate::mcp::tools::saga_create::SagaCreate;
    use crate::mcp::tools::task_create::TaskCreate;

    async fn call_add(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaAddTasks.call(params, ctx).await
    }

    async fn make_saga(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let r = SagaCreate.call(json!({ "title": title }), ctx).await;
        let v: Value = serde_json::from_str(&r.content[0].text).unwrap();
        v["saga_id"].as_str().unwrap().to_string()
    }

    async fn make_task(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let r = TaskCreate
            .call(json!({ "title": title, "task_type": "feature" }), ctx)
            .await;
        let v: Value = serde_json::from_str(&r.content[0].text).unwrap();
        v["task_id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_add_single_task() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S1").await;
        let task_id = make_task(&ctx, "T1").await;

        let result = call_add(json!({ "saga_id": saga_id, "task_ids": [task_id] }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let v: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(v["added"], 1);
        assert_eq!(v["saga_id"], saga_id);
    }

    #[tokio::test]
    async fn test_add_multiple_tasks() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S2").await;
        let t1 = make_task(&ctx, "T1").await;
        let t2 = make_task(&ctx, "T2").await;

        let result = call_add(json!({ "saga_id": saga_id, "task_ids": [t1, t2] }), &ctx).await;
        assert!(result.is_error.is_none());
        let v: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(v["added"], 2);
    }

    #[tokio::test]
    async fn test_duplicate_add_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S3").await;
        let task_id = make_task(&ctx, "T1").await;

        call_add(
            json!({ "saga_id": saga_id, "task_ids": [task_id.clone()] }),
            &ctx,
        )
        .await;

        let result = call_add(json!({ "saga_id": saga_id, "task_ids": [task_id] }), &ctx).await;
        assert_eq!(result.is_error, Some(true), "duplicate should fail");
    }

    #[tokio::test]
    async fn test_nonexistent_saga_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let task_id = make_task(&ctx, "T1").await;

        let result = call_add(
            json!({ "saga_id": "01NONEXISTENT000000000000", "task_ids": [task_id] }),
            &ctx,
        )
        .await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_nonexistent_task_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S4").await;

        let result = call_add(
            json!({ "saga_id": saga_id, "task_ids": ["NONEXISTENT-TASK-ID"] }),
            &ctx,
        )
        .await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_empty_task_ids_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S5").await;

        let result = call_add(json!({ "saga_id": saga_id, "task_ids": [] }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaAddTasks.underscore_alias(), "sagas_add_tasks");
    }
}
