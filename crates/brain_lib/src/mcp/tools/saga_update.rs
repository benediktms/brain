use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::sagas::compact_saga_id;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::saga_validation::{
    validate_actor, validate_description, validate_saga_id, validate_title,
};
use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    title: Option<String>,
    description: Option<Option<String>>,
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

        if let Err(msg) = validate_saga_id(&params.saga_id) {
            return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
        }
        if let Err(msg) = validate_actor(&params.actor) {
            return ToolCallResult::error(format!("Invalid actor: {msg}"));
        }
        if let Some(t) = params.title.as_deref()
            && let Err(msg) = validate_title(t)
        {
            return ToolCallResult::error(format!("Invalid title: {msg}"));
        }
        // description is Option<Option<String>>: outer None = not touching;
        // inner None = clear; inner Some = set to value (bound the length).
        if let Some(Some(d)) = params.description.as_ref()
            && let Err(msg) = validate_description(Some(d.as_str()))
        {
            return ToolCallResult::error(format!("Invalid description: {msg}"));
        }

        let row = match ctx.stores.sagas.update(
            &params.saga_id,
            params.title.as_deref(),
            params.description.as_ref().map(|d| d.as_deref()),
            &params.actor,
        ) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to update saga: {e}")),
        };

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
}

impl McpTool for SagaUpdate {
    fn name(&self) -> &'static str {
        "sagas.update"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Update a saga's title and/or description. At least one field required. \
                Allowed in any status (including closed/cancelled). Empty title is rejected."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": super::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
                    },
                    "title": {
                        "type": "string",
                        "description": "New title (must not be empty)",
                        "maxLength": 1024
                    },
                    "description": {
                        "description": "New description (null to clear)",
                        "oneOf": [
                            { "type": "string", "maxLength": 65536 },
                            { "type": "null" }
                        ]
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is updating the saga. Default: mcp",
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
    use super::{McpTool, SagaUpdate};
    use crate::mcp::tools::saga_create::SagaCreate;

    async fn call_create(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaCreate.call(params, ctx).await
    }

    async fn call_update(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaUpdate.call(params, ctx).await
    }

    async fn create_saga(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let result = call_create(json!({ "title": title }), ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        parsed["saga_id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_update_title_only() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Original").await;

        let result = call_update(
            json!({ "saga_id": saga_id, "title": "Updated Title" }),
            &ctx,
        )
        .await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["title"], "Updated Title");
    }

    #[tokio::test]
    async fn test_update_description_only() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "My Saga").await;

        let result = call_update(
            json!({ "saga_id": saga_id, "description": "New desc" }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["description"], "New desc");
    }

    #[tokio::test]
    async fn test_update_both_fields() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Old").await;

        let result = call_update(
            json!({ "saga_id": saga_id, "title": "New", "description": "New desc" }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["title"], "New");
        assert_eq!(parsed["saga"]["description"], "New desc");
    }

    #[tokio::test]
    async fn test_update_no_fields_fails() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "My Saga").await;

        let result = call_update(json!({ "saga_id": saga_id }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_update_empty_title_fails() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "My Saga").await;

        let result = call_update(json!({ "saga_id": saga_id, "title": "" }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_update_updated_at_bumped() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_saga(&ctx, "Timing").await;

        // Get initial updated_at
        let get_result = crate::mcp::tools::saga_get::SagaGet
            .call(json!({ "saga_id": saga_id }), &ctx)
            .await;
        let initial: Value = serde_json::from_str(&get_result.content[0].text).unwrap();
        let created_at = initial["saga"]["created_at"].as_i64().unwrap();

        // Small delay to ensure timestamp advances
        std::thread::sleep(std::time::Duration::from_millis(10));

        let result = call_update(json!({ "saga_id": saga_id, "title": "Updated" }), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let updated_at = parsed["saga"]["updated_at"].as_i64().unwrap();
        assert!(updated_at >= created_at, "updated_at must be >= created_at");
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaUpdate.underscore_alias(), "sagas_update");
    }
}
