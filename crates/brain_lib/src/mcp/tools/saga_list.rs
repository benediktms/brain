use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::sagas::{SagaListFilter, compact_saga_id};
#[allow(unused_imports)]
use brain_persistence::sql::SqlResultExt;

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

        // Treat empty string as None to avoid querying for brain_id = '' (legacy default).
        let containing_brain = params
            .containing_brain
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(String::from);

        let filter = SagaListFilter {
            include_closed: params.include_closed || params.all,
            include_cancelled: params.include_cancelled || params.all,
            containing_brain,
        };

        let rows = match ctx.stores.sagas.list(filter) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to list sagas: {e}")),
        };

        let sagas: Vec<Value> = rows
            .into_iter()
            .map(|r| {
                json!({
                    "saga_id": compact_saga_id(&r.display_id),
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
                        "description": "If true, includes closed AND cancelled sagas regardless of other flags.",
                        "default": false
                    },
                    "containing_brain": {
                        "type": "string",
                        "description": "Filter by brain_id (not brain name). Only sagas with at least one live member task in this brain are returned."
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
    use brain_persistence::sql::SqlResultExt;
    use serde_json::{Value, json};

    use super::super::tests::create_test_context;
    use super::{McpTool, SagaList};

    async fn call(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaList.call(params, ctx).await
    }

    #[tokio::test]
    async fn test_list_empty() {
        let (_dir, ctx) = create_test_context().await;
        let result = call(json!({}), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
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
        let (canonical, _) = ctx.stores.sagas.resolve_short(&saga_id).unwrap();

        ctx.stores
            .sagas
            .db_for_tests()
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&canonical],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

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
        let (canonical, _) = ctx.stores.sagas.resolve_short(&sid).unwrap();
        ctx.stores
            .sagas
            .db_for_tests()
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&canonical],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let result = call(json!({ "all": true }), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 1);
    }

    #[tokio::test]
    async fn test_list_default_excludes_cancelled() {
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        let r = create.call(json!({ "title": "Active" }), &ctx).await;
        let p: Value = serde_json::from_str(&r.content[0].text).unwrap();
        let sid = p["saga_id"].as_str().unwrap().to_string();
        let (canonical, _) = ctx.stores.sagas.resolve_short(&sid).unwrap();
        ctx.stores
            .sagas
            .db_for_tests()
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&canonical],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();
        create.call(json!({ "title": "Open" }), &ctx).await;

        let result = call(json!({}), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 1);
        assert_eq!(listed["sagas"][0]["title"], "Open");
    }

    #[tokio::test]
    async fn test_containing_brain_filters_correctly() {
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        let r_a = create.call(json!({ "title": "Saga A" }), &ctx).await;
        let p_a: Value = serde_json::from_str(&r_a.content[0].text).unwrap();
        let saga_a = p_a["saga_id"].as_str().unwrap().to_string();
        let (saga_a, _) = ctx.stores.sagas.resolve_short(&saga_a).unwrap();

        let r_b = create.call(json!({ "title": "Saga B" }), &ctx).await;
        let p_b: Value = serde_json::from_str(&r_b.content[0].text).unwrap();
        let saga_b = p_b["saga_id"].as_str().unwrap().to_string();
        let (saga_b, _) = ctx.stores.sagas.resolve_short(&saga_b).unwrap();

        // Wire tasks into brains via direct DB inserts.
        // tasks.brain_id has a FK to brains, so insert brain rows first.
        ctx.stores.sagas.db_for_tests().with_write_conn(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO brains (brain_id, name, created_at) VALUES ('brain-x', 'brain-x', 1000)",
                [],
            )?;
            conn.execute(
                "INSERT OR IGNORE INTO brains (brain_id, name, created_at) VALUES ('brain-y', 'brain-y', 1000)",
                [],
            )?;
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
                 VALUES ('t-brain-x', 'brain-x', 'task', 'open', 4, 'task', 1000, 1000)",
                [],
            )?;
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
                 VALUES ('t-brain-y', 'brain-y', 'task', 'open', 4, 'task', 1000, 1000)",
                [],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, 't-brain-x', 1000)",
                [&saga_a],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, 't-brain-y', 1000)",
                [&saga_b],
            )?;
            Ok(())
        }).into_brain_core().unwrap();

        let result = call(json!({ "containing_brain": "brain-x" }), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 1);
        assert_eq!(listed["sagas"][0]["title"], "Saga A");
    }

    #[tokio::test]
    async fn test_containing_brain_nonexistent_returns_empty() {
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        create.call(json!({ "title": "Some Saga" }), &ctx).await;

        let result = call(json!({ "containing_brain": "no-such-brain" }), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 0);
    }

    #[tokio::test]
    async fn test_empty_containing_brain_ignored() {
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        create.call(json!({ "title": "Saga" }), &ctx).await;

        // Empty string should be treated as None — returns all active sagas.
        let result = call(json!({ "containing_brain": "" }), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 1);
    }

    #[tokio::test]
    async fn containing_brain_does_not_match_by_name() {
        // M11: containing_brain filters by brain_id, never by brain name.
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        let r = create.call(json!({ "title": "Saga A" }), &ctx).await;
        let p: Value = serde_json::from_str(&r.content[0].text).unwrap();
        let saga_a = p["saga_id"].as_str().unwrap().to_string();
        let (saga_a, _) = ctx.stores.sagas.resolve_short(&saga_a).unwrap();

        // Insert a brain whose `name` differs from its `brain_id`, then a task
        // belonging to that brain joined to saga_a.
        ctx.stores.sagas.db_for_tests().with_write_conn(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO brains (brain_id, name, created_at) VALUES ('bid-xyz', 'human-name', 1000)",
                [],
            )?;
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
                 VALUES ('t-xyz', 'bid-xyz', 'task', 'open', 4, 'task', 1000, 1000)",
                [],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, 't-xyz', 1000)",
                [&saga_a],
            )?;
            Ok(())
        }).into_brain_core().unwrap();

        // Filtering by the brain's name (not brain_id) must not match.
        let result = call(json!({ "containing_brain": "human-name" }), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 0, "name should not match brain_id filter");

        // Filtering by the actual brain_id should still match.
        let result = call(json!({ "containing_brain": "bid-xyz" }), &ctx).await;
        let listed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(listed["total"], 1);
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaList.underscore_alias(), "sagas_list");
    }
}
