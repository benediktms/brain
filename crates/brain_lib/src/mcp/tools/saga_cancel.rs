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
    #[serde(default)]
    cascade: bool,
    #[serde(default = "default_actor")]
    actor: String,
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct SagaCancel;

impl SagaCancel {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        match ctx
            .stores
            .sagas
            .cancel(&params.saga_id, params.cascade, &params.actor)
        {
            Ok(row) => json_response(&json!({
                "saga_id": row.saga_id,
                "saga": {
                    "saga_id": row.saga_id,
                    "title": row.title,
                    "description": row.description,
                    "status": row.status,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at,
                    "closed_at": row.closed_at,
                }
            })),
            Err(e) => ToolCallResult::error(format!("{e}")),
        }
    }
}

impl McpTool for SagaCancel {
    fn name(&self) -> &'static str {
        "sagas.cancel"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Cancel a saga. Allowed from active states (planning, open). \
                Closed sagas must be reopened before cancelling. Sets closed_at and emits \
                SagaCancelled. With cascade=true, non-terminal member tasks are transitioned \
                to cancelled."
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
                        "description": "If true, cancel non-terminal member tasks. Default: false",
                        "default": false
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is cancelling the saga. Default: mcp",
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
    use super::{McpTool, SagaCancel};
    use crate::mcp::tools::saga_create::SagaCreate;

    async fn call_cancel(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaCancel.call(params, ctx).await
    }

    async fn make_saga(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let r = SagaCreate.call(json!({ "title": title }), ctx).await;
        let v: Value = serde_json::from_str(&r.content[0].text).unwrap();
        v["saga_id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_cancel_planning_saga() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S1").await;

        let result = call_cancel(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let v: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(v["saga"]["status"], "cancelled");
        assert!(!v["saga"]["closed_at"].is_null());
    }

    #[tokio::test]
    async fn test_cancel_already_cancelled_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S2").await;

        call_cancel(json!({ "saga_id": saga_id }), &ctx).await;
        let result = call_cancel(json!({ "saga_id": saga_id }), &ctx).await;
        assert_eq!(result.is_error, Some(true), "double cancel should fail");
        assert!(result.content[0].text.contains("already cancelled"));
    }

    #[tokio::test]
    async fn test_cancel_nonexistent_saga_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_cancel(json!({ "saga_id": "01NONEXISTENT000000000000" }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_cancel_sets_closed_at() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S3").await;

        let result = call_cancel(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(result.is_error.is_none());
        let v: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let closed_at = v["saga"]["closed_at"].as_i64().unwrap_or(0);
        assert!(closed_at > 0, "closed_at must be a positive timestamp");
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaCancel.underscore_alias(), "sagas_cancel");
    }
}
