use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::sagas::compact_saga_id;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::saga_validation::{validate_actor, validate_saga_id};
use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    #[serde(default = "default_actor")]
    actor: String,
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct SagaReopen;

impl SagaReopen {
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

        match ctx.stores.sagas.reopen(&params.saga_id, &params.actor) {
            Ok(row) => {
                let response = json!({
                    "saga_id": compact_saga_id(&row.display_id),
                    "saga": {
                        "saga_id": compact_saga_id(&row.display_id),
                        "title": row.title,
                        "description": row.description,
                        "status": row.status,
                        "created_at": row.created_at,
                        "updated_at": row.updated_at,
                        "closed_at": row.closed_at,
                    }
                });
                json_response(&response)
            }
            Err(e) => ToolCallResult::error(format!("Failed to reopen saga: {e}")),
        }
    }
}

impl McpTool for SagaReopen {
    fn name(&self) -> &'static str {
        "sagas.reopen"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Reopen a closed or cancelled saga, restoring it to 'open' status. \
                Clears closed_at. Rejected from 'planning' or 'open'. \
                Member tasks are unaffected."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": super::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is reopening the saga. Default: mcp",
                        "default": "mcp",
                        "maxLength": 64,
                        "pattern": "^[A-Za-z0-9_:-]+$"
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
    use super::{McpTool, SagaReopen};
    use crate::mcp::tools::saga_create::SagaCreate;
    use crate::sagas::SagaStatus;

    async fn call_reopen(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaReopen.call(params, ctx).await
    }

    async fn create_saga(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let result = SagaCreate.call(json!({ "title": title }), ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        parsed["saga_id"].as_str().unwrap().to_string()
    }

    fn force_status(ctx: &crate::mcp::McpContext, saga_id: &str, status: SagaStatus) {
        ctx.stores
            .sagas
            .force_status_for_test(saga_id, status)
            .unwrap();
    }

    #[tokio::test]
    async fn test_reopen_from_closed() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Reopen From Closed").await;
        force_status(&ctx, &saga_id, SagaStatus::Closed);

        let result = call_reopen(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["status"], "open");
        assert!(parsed["saga"]["closed_at"].is_null());
    }

    #[tokio::test]
    async fn test_reopen_from_cancelled() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Reopen From Cancelled").await;
        force_status(&ctx, &saga_id, SagaStatus::Cancelled);

        let result = call_reopen(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["status"], "open");
    }

    #[tokio::test]
    async fn test_reopen_from_planning_fails() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Planning Reopen").await;

        let result = call_reopen(json!({ "saga_id": saga_id }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("planning")
                || result.content[0].text.contains("reopen")
        );
    }

    #[tokio::test]
    async fn test_reopen_from_open_fails() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Open Reopen").await;
        force_status(&ctx, &saga_id, SagaStatus::Open);

        let result = call_reopen(json!({ "saga_id": saga_id }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_reopen_nonexistent_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_reopen(json!({ "saga_id": "01HXXNONEXISTENT0000000000" }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_reopen_missing_param_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_reopen(json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaReopen.underscore_alias(), "sagas_reopen");
    }
}
