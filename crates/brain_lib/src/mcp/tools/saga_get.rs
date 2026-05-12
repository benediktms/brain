use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::sagas::compact_saga_id;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::saga_validation::validate_saga_id;
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

        if let Err(msg) = validate_saga_id(&params.saga_id) {
            return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
        }

        let row = match ctx.stores.sagas.get(&params.saga_id) {
            Ok(Some(r)) => r,
            Ok(None) => return json_response(&json!({ "saga": null })),
            Err(e) => return ToolCallResult::error(format!("Failed to fetch saga: {e}")),
        };

        let brains = match ctx.stores.sagas.brains_for_saga(&params.saga_id) {
            Ok(b) => b,
            Err(e) => return ToolCallResult::error(format!("Failed to fetch saga brains: {e}")),
        };

        let brains_json: Vec<serde_json::Value> = brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();

        let members = match ctx.stores.sagas.list_member_stubs(&params.saga_id) {
            Ok(stubs) => stubs,
            Err(e) => {
                return ToolCallResult::error(format!("Failed to fetch saga members: {e}"));
            }
        };
        let members_json: Vec<serde_json::Value> = members
            .iter()
            .map(|m| {
                json!({
                    "task_id": ctx.stores.tasks.compact_id_or_raw(&m.task_id),
                    "brain_id": m.brain_id,
                    "title": m.title,
                    "status": m.status,
                    "task_type": m.task_type,
                })
            })
            .collect();

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
                "members": members_json,
                "brains": brains_json,
            }
        });
        json_response(&response)
    }
}

impl McpTool for SagaGet {
    fn name(&self) -> &'static str {
        "sagas.get"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Fetch a single saga by its compact `saga-<hex>` ID (e.g. `saga-3j5`); \
                26-char ULIDs are still accepted for back-compat. Returns the saga row \
                and member task stubs (empty until tasks are added)."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": super::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
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

    async fn call_get(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaGet.call(params, ctx).await
    }

    async fn call_create(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
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
        assert!(
            get_result.is_error.is_none(),
            "get should succeed: {:?}",
            get_result.content
        );

        let fetched: Value = serde_json::from_str(&get_result.content[0].text).unwrap();
        assert_eq!(fetched["saga"]["title"], "Fetch Me");
        assert_eq!(fetched["saga"]["status"], "planning");
        assert!(fetched["saga"]["members"].as_array().unwrap().is_empty());
        assert!(
            fetched["saga"]["brains"].as_array().unwrap().is_empty(),
            "new saga should have no brains"
        );
    }

    #[tokio::test]
    async fn test_get_nonexistent_saga() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_get(json!({ "saga_id": "01HXXNONEXISTENT0000000000" }), &ctx).await;
        // Returns Ok with {"saga": null} rather than an error, matching the read-side convention.
        assert!(
            result.is_error.is_none(),
            "should not be an error: {:?}",
            result.content
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert!(parsed["saga"].is_null());
    }

    #[tokio::test]
    async fn test_get_missing_param_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call_get(json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    /// End-to-end verification that the MCP `saga_id` boundary accepts BOTH
    /// the `saga-<hex>` short form (the wire-emitted form) and the bare
    /// 26-char ULID (for back-compat with historical references). Both routes
    /// must resolve to the same saga and emit the short form in the response.
    #[tokio::test]
    async fn test_get_accepts_both_input_forms_round_trip() {
        use super::super::tests::assert_short_saga_id;

        let (_dir, ctx) = create_test_context().await;
        let create_result = call_create(json!({ "title": "Round Trip" }), &ctx).await;
        let created: Value = serde_json::from_str(&create_result.content[0].text).unwrap();
        let short_id = created["saga_id"].as_str().unwrap().to_string();
        assert_short_saga_id(&short_id);

        // Recover the canonical ULID via the store's public API.
        let (canonical_ulid, returned_short) = ctx.stores.sagas.resolve_short(&short_id).unwrap();
        assert_eq!(returned_short, short_id);
        assert_eq!(canonical_ulid.len(), 26);

        // Fetch via the short form.
        let via_short = call_get(json!({ "saga_id": &short_id }), &ctx).await;
        assert!(via_short.is_error.is_none(), "short form get failed");
        let parsed_short: Value = serde_json::from_str(&via_short.content[0].text).unwrap();

        // Fetch via the bare ULID.
        let via_ulid = call_get(json!({ "saga_id": &canonical_ulid }), &ctx).await;
        assert!(via_ulid.is_error.is_none(), "bare ULID get failed");
        let parsed_ulid: Value = serde_json::from_str(&via_ulid.content[0].text).unwrap();

        // Both routes return the same saga; both responses emit the short form.
        assert_eq!(parsed_short["saga"]["title"], "Round Trip");
        assert_eq!(parsed_ulid["saga"]["title"], "Round Trip");
        assert_eq!(
            parsed_short["saga"]["saga_id"],
            parsed_ulid["saga"]["saga_id"]
        );
        assert_short_saga_id(parsed_short["saga"]["saga_id"].as_str().unwrap());
        assert_short_saga_id(parsed_ulid["saga"]["saga_id"].as_str().unwrap());

        // The canonical ULID must never appear in either response payload.
        assert!(!via_short.content[0].text.contains(&canonical_ulid));
        assert!(!via_ulid.content[0].text.contains(&canonical_ulid));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaGet.underscore_alias(), "sagas_get");
    }

    /// Saga member responses must emit each `task_id` in the compact
    /// `<prefix>-<hex>` form — never the canonical `PREFIX-ULID`. Regression
    /// test for the cross-domain leak where saga endpoints were emitting full
    /// task ULIDs even after the task-side cleanup.
    #[tokio::test]
    async fn test_members_emit_short_task_ids() {
        use super::super::saga_add_tasks::SagaAddTasks;
        use super::super::task_create::TaskCreate;
        use super::super::tests::assert_short_task_id;

        let (_dir, ctx) = create_test_context().await;

        // Create the saga.
        let saga_create = call_create(json!({ "title": "Members Leak" }), &ctx).await;
        let saga_payload: Value = serde_json::from_str(&saga_create.content[0].text).unwrap();
        let saga_id = saga_payload["saga_id"].as_str().unwrap().to_string();

        // Create a task and capture its short form for cross-checking.
        let task_create = TaskCreate
            .call(
                json!({ "title": "Member task", "task_type": "feature" }),
                &ctx,
            )
            .await;
        let task_payload: Value = serde_json::from_str(&task_create.content[0].text).unwrap();
        let short_task_id = task_payload["task_id"].as_str().unwrap().to_string();
        assert_short_task_id(&short_task_id);

        // Add the task to the saga.
        let add = SagaAddTasks
            .call(
                json!({ "saga_id": &saga_id, "task_ids": [&short_task_id] }),
                &ctx,
            )
            .await;
        assert!(
            add.is_error.is_none(),
            "add_tasks should succeed: {:?}",
            add.content
        );

        // Fetch the saga and verify the member emits the short form.
        let get_result = call_get(json!({ "saga_id": &saga_id }), &ctx).await;
        assert!(
            get_result.is_error.is_none(),
            "get should succeed: {:?}",
            get_result.content
        );
        let fetched: Value = serde_json::from_str(&get_result.content[0].text).unwrap();
        let members = fetched["saga"]["members"].as_array().unwrap();
        assert_eq!(members.len(), 1, "expected one member");
        let emitted = members[0]["task_id"].as_str().unwrap();
        assert_short_task_id(emitted);
        assert_eq!(
            emitted, short_task_id,
            "member task_id must match the task's wire-form short id"
        );
    }
}
