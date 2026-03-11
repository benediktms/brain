use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::events::{StatusChangedPayload, TaskEvent, TaskStatus};

use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

/// Accepts either a single string or an array of strings.
#[derive(Deserialize)]
#[serde(untagged)]
enum TaskIds {
    Single(String),
    Multiple(Vec<String>),
}

impl TaskIds {
    fn into_vec(self) -> Vec<String> {
        match self {
            TaskIds::Single(s) => vec![s],
            TaskIds::Multiple(v) => v,
        }
    }
}

#[derive(Deserialize)]
struct Params {
    task_ids: TaskIds,
}

pub(super) struct TaskClose;

impl TaskClose {
    /// Returns (ToolCallResult, Vec<(full_task_id, title)>) for capsule embedding.
    fn execute_inner(
        &self,
        raw_params: Value,
        ctx: &McpContext,
    ) -> (ToolCallResult, Vec<(String, String)>) {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return (ToolCallResult::error(format!("Invalid parameters: {e}")), vec![]),
        };

        let ids = params.task_ids.into_vec();
        if ids.is_empty() {
            return (ToolCallResult::error("task_ids must not be empty"), vec![]);
        }

        let mut closed = Vec::new();
        let mut failed = Vec::new();
        let mut total_unblocked = 0usize;
        let mut warnings: Vec<Warning> = Vec::new();
        let mut closed_tasks: Vec<(String, String)> = Vec::new();

        for raw_id in &ids {
            let resolved = match ctx.tasks.resolve_task_id(raw_id) {
                Ok(r) => r,
                Err(e) => {
                    failed.push(json!({
                        "task_id": raw_id,
                        "error": format!("{e}"),
                    }));
                    continue;
                }
            };

            let event = TaskEvent::from_payload(
                &resolved,
                "mcp",
                StatusChangedPayload {
                    new_status: TaskStatus::Done,
                },
            );

            if let Err(e) = ctx.tasks.append(&event) {
                failed.push(json!({
                    "task_id": raw_id,
                    "error": format!("{e}"),
                }));
                continue;
            }

            // Collect task title for capsule embedding (best-effort)
            if let Ok(Some(task_row)) = ctx.tasks.get_task(&resolved) {
                closed_tasks.push((resolved.clone(), task_row.title));
            }

            let unblocked: Vec<String> = store_or_warn(
                ctx.tasks.list_newly_unblocked(&resolved),
                "list_newly_unblocked",
                &mut warnings,
            )
            .iter()
            .map(|id| ctx.tasks.compact_id(id).unwrap_or_else(|_| id.clone()))
            .collect();
            let short_id = ctx
                .tasks
                .compact_id(&resolved)
                .unwrap_or_else(|_| resolved.clone());

            total_unblocked += unblocked.len();
            closed.push(json!({
                "task_id": short_id,
                "unblocked_task_ids": unblocked,
            }));
        }

        let mut response = json!({
            "closed": closed,
            "failed": failed,
            "summary": {
                "closed": closed.len(),
                "failed": failed.len(),
                "unblocked": total_unblocked,
            },
        });
        inject_warnings(&mut response, warnings);
        (json_response(&response), closed_tasks)
    }
}

/// Embed both task capsule (refresh) and outcome capsule for a closed task.
async fn embed_capsules_for_closed_task(
    ctx: &crate::mcp::McpContext,
    task_id: &str,
    title: &str,
) -> crate::error::Result<()> {
    let (store, embedder) = match (ctx.writable_store.as_ref(), ctx.embedder.as_ref()) {
        (Some(s), Some(e)) => (s, e),
        _ => return Ok(()), // No store/embedder — skip silently
    };

    // Refresh the task capsule with current state
    let task = ctx.tasks.get_task(task_id)?;
    if let Some(task) = &task {
        let labels = ctx.tasks.get_task_labels(task_id).unwrap_or_default();
        if let Err(e) = crate::tasks::capsule::embed_task_capsule(
            store,
            embedder,
            &ctx.db,
            crate::tasks::capsule::TaskCapsuleParams {
                task_id,
                title: &task.title,
                description: task.description.as_deref(),
                labels: &labels,
                priority: task.priority,
            },
        )
        .await
        {
            tracing::warn!(error = %e, task_id, "task capsule refresh failed on close (best-effort)");
        }
    }

    // Embed the outcome capsule
    crate::tasks::capsule::embed_outcome_capsule(store, embedder, &ctx.db, task_id, title, None)
        .await
}

impl McpTool for TaskClose {
    fn name(&self) -> &'static str {
        "tasks.close"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Close one or more tasks (set status to done). Returns closed tasks and any newly unblocked task IDs. Partial failures (e.g. invalid IDs) return a success response with separate 'closed' and 'failed' arrays. Convenience shortcut for tasks.apply_event with status_changed.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_ids": {
                        "description": "Task ID or array of task IDs to close. Accepts full IDs or unique prefixes.",
                        "oneOf": [
                            { "type": "string" },
                            { "type": "array", "items": { "type": "string" } }
                        ]
                    }
                },
                "required": ["task_ids"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let (result, closed_tasks) = self.execute_inner(params, ctx);

            // Best-effort capsule embedding for each closed task (task + outcome)
            for (task_id, title) in &closed_tasks {
                if let Err(e) = embed_capsules_for_closed_task(ctx, task_id, title).await {
                    tracing::warn!(error = %e, task_id, "capsule embedding failed on close (best-effort)");
                }
            }

            result
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    async fn dispatch(
        registry: &ToolRegistry,
        name: &str,
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        registry.dispatch(name, params, ctx).await
    }

    async fn create_tasks(registry: &ToolRegistry, ctx: &crate::mcp::McpContext) {
        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2"), ("t3", "Task 3")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            dispatch(registry, "tasks.apply_event", p, ctx).await;
        }
    }

    #[tokio::test]
    async fn test_close_single_string() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let result = dispatch(&registry, "tasks.close", json!({ "task_ids": "t1" }), &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["closed"], 1);
        assert_eq!(parsed["summary"]["failed"], 0);
        assert_eq!(parsed["closed"][0]["task_id"], "t1");

        // Verify task is actually done
        let task = ctx.tasks.get_task("t1").unwrap().unwrap();
        assert_eq!(task.status, "done");
    }

    #[tokio::test]
    async fn test_close_multiple() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let result = dispatch(
            &registry,
            "tasks.close",
            json!({ "task_ids": ["t1", "t2"] }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["closed"], 2);
    }

    #[tokio::test]
    async fn test_close_with_unblocked() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // t2 depends on t1
        dispatch(
            &registry,
            "tasks.apply_event",
            json!({
                "event_type": "dependency_added",
                "task_id": "t2",
                "payload": { "depends_on_task_id": "t1" }
            }),
            &ctx,
        )
        .await;

        let result = dispatch(&registry, "tasks.close", json!({ "task_ids": "t1" }), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["unblocked"], 1);
        assert!(
            parsed["closed"][0]["unblocked_task_ids"]
                .as_array()
                .unwrap()
                .contains(&json!("t2"))
        );
    }

    #[tokio::test]
    async fn test_close_partial_failure() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let result = dispatch(
            &registry,
            "tasks.close",
            json!({ "task_ids": ["t1", "nonexistent"] }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none()); // partial success

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["closed"], 1);
        assert_eq!(parsed["summary"]["failed"], 1);
    }

    #[tokio::test]
    async fn test_close_empty_array() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = dispatch(&registry, "tasks.close", json!({ "task_ids": [] }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("must not be empty"));
    }
}
