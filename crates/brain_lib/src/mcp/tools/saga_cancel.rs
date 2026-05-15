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

        if let Err(msg) = validate_saga_id(&params.saga_id) {
            return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
        }
        if let Err(msg) = validate_actor(&params.actor) {
            return ToolCallResult::error(format!("Invalid actor: {msg}"));
        }

        let (row, cascade_results) =
            match ctx
                .stores
                .sagas
                .cancel(&params.saga_id, params.cascade, &params.actor)
            {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Failed to cancel saga: {e}")),
            };

        let cascade_json = super::cascade_results_to_json(&cascade_results);

        json_response(&json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
            },
            "cascade": params.cascade,
            "cascade_results": cascade_json,
        }))
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
                        "description": super::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
                    },
                    "cascade": {
                        "type": "boolean",
                        "description": "If true, cancel non-terminal member tasks. Default: false",
                        "default": false
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is cancelling the saga. Default: mcp",
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
        let result = call_cancel(json!({ "saga_id": "01HXXNONEXISTENT0000000000" }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_cancel_sets_closed_at() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "S3").await;

        let result = call_cancel(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(result.is_error.is_none());
        let v: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Timestamps are RFC 3339 strings (consistent with brain_tasks::Task).
        let closed_at = v["saga"]["closed_at"]
            .as_str()
            .expect("closed_at must be a non-null RFC 3339 string");
        chrono::DateTime::parse_from_rfc3339(closed_at).expect("closed_at must parse as RFC 3339");
    }

    async fn make_task(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let r = crate::mcp::tools::task_create::TaskCreate
            .call(json!({ "title": title, "task_type": "feature" }), ctx)
            .await;
        let v: Value = serde_json::from_str(&r.content[0].text).unwrap();
        v["task_id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_cancel_cascade_marks_open_tasks_cancelled() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = make_saga(&ctx, "Cancel Cascade").await;
        let t1 = make_task(&ctx, "T1").await;
        let t2 = make_task(&ctx, "T2").await;
        crate::mcp::tools::saga_add_tasks::SagaAddTasks
            .call(
                json!({ "saga_id": &saga_id, "task_ids": [t1.clone(), t2.clone()] }),
                &ctx,
            )
            .await;

        let result = call_cancel(json!({ "saga_id": &saga_id, "cascade": true }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["status"], "cancelled");
        let cascade = parsed["cascade_results"].as_array().unwrap();
        assert_eq!(cascade.len(), 2);
        for entry in cascade {
            assert_eq!(
                entry["cancelled"], true,
                "open task should be cancelled: {entry:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaCancel.underscore_alias(), "sagas_cancel");
    }
}
