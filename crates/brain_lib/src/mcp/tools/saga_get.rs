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
}

pub(super) struct SagaGet;

impl SagaGet {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        match ctx.stores.sagas.get(&params.saga_id) {
            Ok(Some(row)) => {
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
                        "members": [],
                    }
                });
                json_response(&response)
            }
            Ok(None) => {
                ToolCallResult::error(format!("Saga not found: {}", params.saga_id))
            }
            Err(e) => ToolCallResult::error(format!("Failed to fetch saga: {e}")),
        }
    }
}

impl McpTool for SagaGet {
    fn name(&self) -> &'static str {
        "sagas.get"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Fetch a single saga by its bare-ULID saga_id. Returns the saga row \
                and member task stubs (empty until tasks are added)."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": "Bare 26-char ULID saga ID"
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
    use super::{McpTool, SagaGet};
    use crate::mcp::tools::saga_create::SagaCreate;

    async fn call_get(params: Value, ctx: &crate::mcp::McpContext) -> crate::mcp::protocol::ToolCallResult {
        SagaGet.call(params, ctx).await
    }

    async fn call_create(params: Value, ctx: &crate::mcp::McpContext) -> crate::mcp::protocol::ToolCallResult {
        SagaCreate.call(params, ctx).await
    }

    #[tokio::test]
    async fn test_get_existing_saga() {
        let (_dir, ctx) = create_test_context().await;

        // Create a saga first
        let create_result = call_create(json!({ "title": "Fetch Me" }), &ctx).await;
        assert!(create_result.is_error.is_none());
        let created: Value = serde_json::from_str(&create_result.content[0].text).unwrap();
        let saga_id = created["saga_id"].as_str().unwrap().to_string();

        // Now fetch it
        let get_result = call_get(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(get_result.is_error.is_none(), "get should succeed: {:?}", get_result.content);

        let fetched: Value = serde_json::from_str(&get_result.content[0].text).unwrap();
        assert_eq!(fetched["saga"]["title"], "Fetch Me");
        assert_eq!(fetched["saga"]["status"], "planning");
        assert!(fetched["saga"]["members"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_nonexistent_saga() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_get(json!({ "saga_id": "01NONEXISTENT000000000000" }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("not found") || result.content[0].text.contains("Saga"));
    }

    #[tokio::test]
    async fn test_get_missing_param_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_get(json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaGet.underscore_alias(), "sagas_get");
    }
}
