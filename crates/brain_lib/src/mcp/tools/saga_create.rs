use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};
use super::saga_validation::{validate_actor, validate_description, validate_title};

#[derive(Deserialize)]
struct Params {
    title: String,
    description: Option<String>,
    #[serde(default = "default_actor")]
    actor: String,
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct SagaCreate;

impl SagaCreate {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if let Err(msg) = validate_actor(&params.actor) {
            return ToolCallResult::error(format!("Invalid actor: {msg}"));
        }
        if let Err(msg) = validate_title(&params.title) {
            return ToolCallResult::error(format!("Invalid title: {msg}"));
        }
        if let Err(msg) = validate_description(params.description.as_deref()) {
            return ToolCallResult::error(format!("Invalid description: {msg}"));
        }

        let row = match ctx.stores.sagas.create(
            &params.title,
            params.description.as_deref(),
            &params.actor,
        ) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to create saga: {e}")),
        };

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
                // members is always empty at creation time; populated once saga_tasks rows exist
                "members": [],
            }
        });

        json_response(&response)
    }
}

impl McpTool for SagaCreate {
    fn name(&self) -> &'static str {
        "sagas.create"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Create a new saga in 'planning' status. Sagas are registry-level \
                (not scoped to any brain) and use bare ULID IDs. Returns the saga_id and \
                initial state."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Saga title",
                        "maxLength": 1024
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional description",
                        "maxLength": 65536
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is creating the saga. Default: mcp",
                        "default": "mcp",
                        "maxLength": 64,
                        "pattern": "^[A-Za-z0-9_:-]+$"
                    }
                },
                "required": ["title"]
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
    use super::{McpTool, SagaCreate};

    async fn call(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaCreate.call(params, ctx).await
    }

    #[tokio::test]
    async fn test_create_basic() {
        let (_dir, ctx) = create_test_context().await;
        let result = call(json!({ "title": "My Saga" }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let saga_id = parsed["saga_id"].as_str().unwrap();
        assert_eq!(saga_id.len(), 26, "saga_id must be 26-char ULID");
        assert!(!saga_id.contains('-'), "saga_id must have no prefix");
        assert_eq!(parsed["saga"]["status"], "planning");
        assert_eq!(parsed["saga"]["title"], "My Saga");
        assert!(parsed["saga"]["members"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_with_description() {
        let (_dir, ctx) = create_test_context().await;
        let result = call(
            json!({ "title": "Described", "description": "A longer desc" }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["description"], "A longer desc");
    }

    #[tokio::test]
    async fn test_create_missing_title_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call(json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaCreate.underscore_alias(), "sagas_create");
    }
}
