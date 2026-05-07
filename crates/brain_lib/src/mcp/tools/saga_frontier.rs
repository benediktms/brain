use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::saga_validation::validate_saga_id;
use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    saga_id: String,
}

pub(super) struct SagaFrontier;

impl SagaFrontier {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if let Err(msg) = validate_saga_id(&params.saga_id) {
            return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
        }

        let frontier = match ctx.stores.sagas.frontier(&params.saga_id) {
            Ok(f) => f,
            Err(e) => return ToolCallResult::error(format!("Failed to compute frontier: {e}")),
        };

        let tasks: Vec<Value> = frontier
            .tasks
            .iter()
            .map(|t| {
                json!({
                    "task_id": t.task_id,
                    "title": t.title,
                    "status": t.status,
                    "priority": t.priority,
                    "task_type": t.task_type,
                })
            })
            .collect();

        let brains: Vec<Value> = frontier
            .brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();

        json_response(&json!({
            "saga_id": params.saga_id,
            "saga_status": frontier.status.to_string(),
            "tasks": tasks,
            "brains": brains,
            "total": tasks.len(),
        }))
    }
}

impl McpTool for SagaFrontier {
    fn name(&self) -> &'static str {
        "sagas.frontier"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Return the ready-actionable tasks in a saga (same qualification rules as \
                tasks.next: open/in_progress, no blocked_reason, no unresolved deps, not deferred, \
                not epic), together with the brains those tasks belong to. \
                Planning/closed/cancelled sagas return an empty task list but still populate brains."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": "Saga ID (bare 26-char ULID)",
                        "pattern": "^[0-9A-Za-z]{26}$"
                    }
                },
                "required": ["saga_id"]
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
    use super::{McpTool, SagaFrontier};

    async fn call(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaFrontier.call(params, ctx).await
    }

    #[tokio::test]
    async fn test_frontier_empty_saga() {
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        let r = create.call(json!({ "title": "Empty" }), &ctx).await;
        let p: Value = serde_json::from_str(&r.content[0].text).unwrap();
        let saga_id = p["saga_id"].as_str().unwrap();

        let result = call(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 0);
        assert!(listed["tasks"].as_array().unwrap().is_empty());
        assert!(listed["brains"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_frontier_missing_saga_id_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call(json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaFrontier.underscore_alias(), "sagas_frontier");
    }
}
