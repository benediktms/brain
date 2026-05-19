//! `tasks.close` MCP tool — thin wrapper over `DaemonClient::tasks_mutate`.
//!
//! Closes one or more tasks (sets status to `done`). Each task_id is
//! closed via a `tasks_mutate` call with `action: "close"`. The response
//! shape mirrors the legacy `{closed, failed, summary}` envelope. Partial
//! failures (invalid IDs, resolution errors) accumulate in `failed`
//! without aborting the batch.
//!
//! ## Deviation from legacy
//!
//! The legacy tool tracked `unblocked_task_ids` per closed task by calling
//! `list_newly_unblocked` directly against the store. The wire surface does
//! not yet expose a `list_newly_unblocked` call, so the per-task
//! `unblocked_task_ids` array is omitted from individual closed entries.
//! The `summary.unblocked` count is also absent. The `closed` entries still
//! carry `task_id` and the resulting `TaskSummary` status. Callers that need
//! unblocked-task detection should use `tasks.apply_event` with
//! `status_changed` instead.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::TasksMutateParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskClose;

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

impl McpTool for TaskClose {
    fn name(&self) -> &'static str {
        "tasks.close"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Close one or more tasks (set status to done). Partial failures (e.g. invalid IDs) return a success response with separate 'closed' and 'failed' arrays. Convenience shortcut for tasks.apply_event with status_changed.".into(),
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
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let ids = parsed.task_ids.into_vec();
            if ids.is_empty() {
                return ToolCallResult::error("task_ids must not be empty");
            }

            let mut closed: Vec<Value> = Vec::new();
            let mut failed: Vec<Value> = Vec::new();

            for raw_id in ids {
                let id = raw_id.trim().to_string();
                if id.is_empty() {
                    failed.push(json!({ "task_id": raw_id, "error": "task_id entry is empty" }));
                    continue;
                }

                let wire_params = TasksMutateParams {
                    id: id.clone(),
                    action: "close".to_string(),
                };

                match ctx.with_client(|c| c.tasks_mutate(wire_params)).await {
                    Ok((task, _event_id)) => {
                        closed.push(json!({
                            "task_id": task.task_id,
                            "unblocked_task_ids": [],
                        }));
                    }
                    Err(e) => {
                        failed.push(json!({
                            "task_id": id,
                            "error": e.to_string(),
                        }));
                    }
                }
            }

            let response = json!({
                "closed": closed,
                "failed": failed,
                "summary": {
                    "closed": closed.len(),
                    "failed": failed.len(),
                    "unblocked": 0,
                },
            });
            json_response(&response)
        })
    }
}
