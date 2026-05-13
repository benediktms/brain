use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::sagas::compact_saga_id;
use brain_persistence::sql::SqlResultExt;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::saga_validation::{validate_actor, validate_saga_id};
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

        if let Err(msg) = validate_saga_id(&params.saga_id) {
            return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
        }
        if let Err(msg) = validate_actor(&params.actor) {
            return ToolCallResult::error(format!("Invalid actor: {msg}"));
        }

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

        let cascade_json = cascade_results_to_json(&cascade_results);

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
                        "description": super::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
                    },
                    "cascade": {
                        "type": "boolean",
                        "description": "If true, close all member tasks. Default: false",
                        "default": false
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is closing the saga. Default: mcp",
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
        // saga_id is the `saga-<hex>` short form; resolve to canonical for the
        // raw-SQL UPDATE since the `saga_id` column stores the ULID.
        let (canonical, _) = ctx.stores.sagas.resolve_short(&saga_id).unwrap();
        // Force status to open directly so close tests don't depend on start impl
        ctx.stores
            .db_for_tests()
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'open' WHERE saga_id = ?1",
                    [&canonical],
                )?;
                Ok(())
            })
                .into_brain_core()
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
        let result = call_close(json!({ "saga_id": "01HXXNONEXISTENT0000000000" }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    async fn make_task(ctx: &crate::mcp::McpContext, title: &str) -> String {
        let r = crate::mcp::tools::task_create::TaskCreate
            .call(json!({ "title": title, "task_type": "feature" }), ctx)
            .await;
        let v: Value = serde_json::from_str(&r.content[0].text).unwrap();
        v["task_id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_close_cascade_marks_open_tasks_done() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_open_saga(&ctx).await;
        let t1 = make_task(&ctx, "T1").await;
        let t2 = make_task(&ctx, "T2").await;
        crate::mcp::tools::saga_add_tasks::SagaAddTasks
            .call(
                json!({ "saga_id": &saga_id, "task_ids": [t1.clone(), t2.clone()] }),
                &ctx,
            )
            .await;

        let result = call_close(json!({ "saga_id": &saga_id, "cascade": true }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["saga"]["status"], "closed");
        let cascade = parsed["cascade_results"].as_array().unwrap();
        assert_eq!(cascade.len(), 2, "both members should appear in cascade");
        for entry in cascade {
            assert_eq!(
                entry["closed"], true,
                "open task should be marked closed: {entry:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_close_cascade_skips_already_done() {
        let (_dir, ctx) = create_test_context().await;
        let saga_id = create_open_saga(&ctx).await;
        let t1 = make_task(&ctx, "T1").await;
        let t2 = make_task(&ctx, "T2").await;
        crate::mcp::tools::saga_add_tasks::SagaAddTasks
            .call(
                json!({ "saga_id": &saga_id, "task_ids": [t1.clone(), t2.clone()] }),
                &ctx,
            )
            .await;

        // Pre-mark t1 as done by writing the projection directly (the cascade
        // logic only inspects `tasks.status`, so this is the minimal setup).
        let resolved_t1 = ctx.stores.tasks.resolve_task_id(&t1).unwrap();
        ctx.stores
            .db_for_tests()
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE tasks SET status = 'done' WHERE task_id = ?1",
                    [&resolved_t1],
                )?;
                Ok(())
            })
                .into_brain_core()
            .unwrap();

        let result = call_close(json!({ "saga_id": &saga_id, "cascade": true }), &ctx).await;
        assert!(result.is_error.is_none());
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let cascade = parsed["cascade_results"].as_array().unwrap();
        assert_eq!(cascade.len(), 2);
        // Find the entry for the pre-done task and verify it's skipped.
        let skipped_entry = cascade
            .iter()
            .find(|e| e["task_id"].as_str() == Some(resolved_t1.as_str()))
            .expect("pre-done task should appear in cascade results");
        assert_eq!(skipped_entry["skipped"], true);
        assert_eq!(skipped_entry["reason"], "done");
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaClose.underscore_alias(), "sagas_close");
    }
}
