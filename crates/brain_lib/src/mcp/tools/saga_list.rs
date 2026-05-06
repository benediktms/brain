use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::sagas::queries::SagaListFilter;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

#[derive(Deserialize, Default)]
struct Params {
    #[serde(default)]
    include_closed: bool,
    #[serde(default)]
    include_cancelled: bool,
    /// Convenience: if true, sets both include_closed and include_cancelled.
    #[serde(default)]
    all: bool,
    containing_brain: Option<String>,
}

pub(super) struct SagaList;

impl SagaList {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let filter = SagaListFilter {
            include_closed: params.include_closed || params.all,
            include_cancelled: params.include_cancelled || params.all,
            containing_brain: params.containing_brain,
        };

        let rows = match ctx.stores.sagas.list(filter) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to list sagas: {e}")),
        };

        let sagas: Vec<Value> = rows
            .into_iter()
            .map(|r| {
                json!({
                    "saga_id": r.saga_id,
                    "title": r.title,
                    "description": r.description,
                    "status": r.status,
                    "created_at": r.created_at,
                    "updated_at": r.updated_at,
                    "closed_at": r.closed_at,
                })
            })
            .collect();

        json_response(&json!({ "sagas": sagas, "total": sagas.len() }))
    }
}

impl McpTool for SagaList {
    fn name(&self) -> &'static str {
        "sagas.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List sagas. By default returns only planning and open sagas. \
                Use include_closed, include_cancelled, or all=true to widen the result set. \
                Use containing_brain to filter to sagas that have at least one member-task \
                in the given brain."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "include_closed": {
                        "type": "boolean",
                        "description": "Include closed sagas. Default: false",
                        "default": false
                    },
                    "include_cancelled": {
                        "type": "boolean",
                        "description": "Include cancelled sagas. Default: false",
                        "default": false
                    },
                    "all": {
                        "type": "boolean",
                        "description": "Include all sagas regardless of status. Overrides include_closed/include_cancelled.",
                        "default": false
                    },
                    "containing_brain": {
                        "type": "string",
                        "description": "Only return sagas with at least one member-task in this brain (brain_id)"
                    }
                }
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
    use super::{McpTool, SagaList};

    async fn call(params: Value, ctx: &crate::mcp::McpContext) -> crate::mcp::protocol::ToolCallResult {
        SagaList.call(params, ctx).await
    }

    #[tokio::test]
    async fn test_list_empty() {
        let (_dir, ctx) = create_test_context().await;
        let result = call(json!({}), &ctx).await;
        assert!(result.is_error.is_none(), "should succeed: {:?}", result.content);
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["total"], 0);
        assert!(parsed["sagas"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_default_excludes_closed() {
        let (_dir, ctx) = create_test_context().await;

        // Create two sagas, then close one via the store.
        let create = super::super::saga_create::SagaCreate;
        create.call(json!({ "title": "Open" }), &ctx).await;
        let closed_result = create.call(json!({ "title": "Closed" }), &ctx).await;
        let parsed: Value = serde_json::from_str(&closed_result.content[0].text).unwrap();
        let saga_id = parsed["saga_id"].as_str().unwrap().to_string();

        ctx.stores.sagas.db().with_write_conn(|conn| {
            conn.execute("UPDATE sagas SET status = 'closed' WHERE saga_id = ?1", [&saga_id])?;
            Ok(())
        }).unwrap();

        let result = call(json!({}), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 1);
        assert_eq!(listed["sagas"][0]["title"], "Open");
    }

    #[tokio::test]
    async fn test_list_all_flag() {
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        let r = create.call(json!({ "title": "One" }), &ctx).await;
        let p: Value = serde_json::from_str(&r.content[0].text).unwrap();
        let sid = p["saga_id"].as_str().unwrap().to_string();
        ctx.stores.sagas.db().with_write_conn(|conn| {
            conn.execute("UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1", [&sid])?;
            Ok(())
        }).unwrap();

        let result = call(json!({ "all": true }), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 1);
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaList.underscore_alias(), "sagas_list");
    }
}
