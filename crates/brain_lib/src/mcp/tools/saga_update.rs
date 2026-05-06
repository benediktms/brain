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
    title: Option<String>,
    description: Option<String>,
    #[serde(default = "default_actor")]
    actor: String,
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct SagaUpdate;

impl SagaUpdate {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let row = match ctx.stores.sagas.update(
            &params.saga_id,
            params.title.as_deref(),
            params.description.as_deref(),
            &params.actor,
        ) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to update saga: {e}")),
        };

        json_response(&json!({
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
        }))
    }
}

impl McpTool for SagaUpdate {
    fn name(&self) -> &'static str {
        "sagas.update"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Update a saga's title and/or description. At least one field required. \
                Allowed in any status (closed/cancelled sagas can still have metadata edited). \
                Empty title is rejected."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": { "type": "string", "description": "Saga ID (bare 26-char ULID)" },
                    "title": { "type": "string", "description": "New title (non-empty)" },
                    "description": { "type": "string", "description": "New description" },
                    "actor": {
                        "type": "string",
                        "description": "Who is updating the saga. Default: mcp",
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
    use super::{McpTool, SagaUpdate};

    async fn call(params: Value, ctx: &crate::mcp::McpContext) -> crate::mcp::protocol::ToolCallResult {
        SagaUpdate.call(params, ctx).await
    }

    async fn create_saga(ctx: &crate::mcp::McpContext) -> String {
        let result = super::super::saga_create::SagaCreate
            .call(json!({ "title": "Original" }), ctx)
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        parsed["saga_id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_update_title() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx).await;
        let result = call(json!({ "saga_id": saga_id, "title": "Renamed" }), &ctx).await;
        assert!(result.is_error.is_none(), "should succeed: {:?}", result.content);
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["title"], "Renamed");
    }

    #[tokio::test]
    async fn test_update_description_only() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx).await;
        let result = call(json!({ "saga_id": saga_id, "description": "new desc" }), &ctx).await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["description"], "new desc");
        assert_eq!(parsed["saga"]["title"], "Original");
    }

    #[tokio::test]
    async fn test_update_no_fields_fails() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx).await;
        let result = call(json!({ "saga_id": saga_id }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaUpdate.underscore_alias(), "sagas_update");
    }
}
