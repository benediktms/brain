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

pub(super) struct SagaAddTasks;

impl SagaAddTasks {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if let Err(msg) = validate_saga_id(&params.saga_id) {
            return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
        }
        if let Err(msg) = validate_actor(&params.actor) {
            return ToolCallResult::error(format!("Invalid actor: {msg}"));
        }
        if params.task_ids.is_empty() {
            return ToolCallResult::error("task_ids must not be empty");
        }
        // M1: cap batch size so an errant agent can't hold the SQLite writer
        // through a million-id transaction.
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
                Already-member tasks and intra-batch duplicates are silently skipped (idempotent). \
                Unresolvable IDs cause the entire batch to fail."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": "Bare 26-char ULID saga ID",
                        "pattern": "^[0-9A-Za-z]{26}$"
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string", "minLength": 1, "maxLength": 128 },
                        "description": "Task IDs to add (full IDs or short hashes, cross-brain aware)",
                        "minItems": 1,
                        "maxItems": 500
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is adding the tasks. Default: mcp",
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
    async fn test_duplicate_add_is_idempotent_noop() {
        // The store now skips already-member task_ids without erroring (see
        // `add_tasks_skips_already_member_no_error` in `crate::sagas`). The
        // MCP layer should reflect that contract: duplicate adds succeed and
        // simply count zero new memberships.
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S3").await;
        let task_id = make_task(&ctx, "T1").await;

        call_add(
            json!({ "saga_id": saga_id, "task_ids": [task_id.clone()] }),
            &ctx,
        )
        .await;

        let result = call_add(json!({ "saga_id": saga_id, "task_ids": [task_id] }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "duplicate add should be a no-op, not an error: {:?}",
            result.content
        );
        let v: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(
            v["added"], 0,
            "duplicate add must not increase membership count"
        );
    }

    #[tokio::test]
    async fn test_nonexistent_saga_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let task_id = make_task(&ctx, "T1").await;

        let result = call_add(
            json!({ "saga_id": "01HXXNONEXISTENT0000000000", "task_ids": [task_id] }),
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
