use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::sagas::CascadeOutcome;

use super::{McpTool, cascade_results_to_json, json_response};

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    #[serde(default)]
    cascade: bool,
    #[serde(default = "default_actor")]
    actor: String,
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct SagaClose;

impl SagaClose {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // H2: cascade now happens inside SagaStore::close, atomically with the
        // saga's status change. We just consume the per-task results here.
        let (row, cascade_results) =
            match ctx
                .stores
                .sagas
                .close(&params.saga_id, params.cascade, &params.actor)
            {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Failed to close saga: {e}")),
            };

        let cascade_json = cascade_results_to_json(&cascade_results, CascadeOutcome::Closed);

        let response = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
            },
            "cascade": params.cascade,
            "cascade_results": cascade_json,
        });

        json_response(&response)
    }
}

impl McpTool for SagaClose {
    fn name(&self) -> &'static str {
        "sagas.close"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Close a saga (open → closed). Only open sagas can be closed. \
                With cascade=true, member tasks are transitioned to done (best-effort: \
                already-done and already-cancelled tasks are skipped)."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": "Bare 26-char ULID saga ID"
                    },
                    "cascade": {
                        "type": "boolean",
                        "description": "If true, close all member tasks. Default: false",
                        "default": false
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is closing the saga. Default: mcp",
                        "default": "mcp"
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
    use super::{McpTool, SagaClose};
    use crate::mcp::tools::saga_create::SagaCreate;

    async fn call_create(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaCreate.call(params, ctx).await
    }

    async fn call_close(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaClose.call(params, ctx).await
    }

    async fn create_open_saga(ctx: &crate::mcp::McpContext) -> String {
        let result = call_create(json!({ "title": "Test Saga" }), ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let saga_id = parsed["saga_id"].as_str().unwrap().to_string();
        // Force status to open directly so close tests don't depend on start impl
        ctx.stores
            .db_for_tests()
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'open' WHERE saga_id = ?1",
                    [&saga_id],
                )?;
                Ok(())
            })
            .unwrap();
        saga_id
    }

    #[tokio::test]
    async fn test_close_open_saga() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_open_saga(&ctx).await;

        let result = call_close(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["status"], "closed");
        assert!(parsed["saga"]["closed_at"].as_i64().is_some());
    }

    #[tokio::test]
    async fn test_close_planning_saga_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_create(json!({ "title": "Planning Saga" }), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let saga_id = parsed["saga_id"].as_str().unwrap();

        let result = call_close(json!({ "saga_id": saga_id }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_close_already_closed_fails() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_open_saga(&ctx).await;

        call_close(json!({ "saga_id": saga_id }), &ctx).await;
        let result = call_close(json!({ "saga_id": saga_id }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_close_nonexistent_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_close(json!({ "saga_id": "01NONEXISTENT000000000000" }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaClose.underscore_alias(), "sagas_close");
    }
}
