//! `tasks.next` MCP tool — thin wrapper over `DaemonClient::tasks_next`.
//!
//! Returns the single highest-priority ready task from the daemon. The wire
//! `tasks_next()` takes no parameters and returns `Option<TaskSummary>`.
//!
//! ## Deviations from legacy
//!
//! - `policy`, `k`, and `brains` params are accepted in the schema (preserved
//!   verbatim) but are not forwarded to the daemon — it always returns at most
//!   one task using its own built-in priority policy.
//! - Response shape: `{results, ready_count, blocked_count}` is preserved, but
//!   `results` is a single group `[{epic: null, tasks: [task]}]` or `[]` when
//!   no task is ready. `ready_count` and `blocked_count` are always 0 on this
//!   wire path (daemon does not return those counts).
//! - Per-task fields are the minimal `TaskSummary` wire shape; enriched fields
//!   (labels, linked_notes, dependency_summary, uri) are absent.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize, Default)]
struct Params {
    /// Sorting policy. Only the daemon default (`priority`) is currently
    /// honoured on this wire path; `due_date` is rejected up front so
    /// callers don't silently get priority-sorted results.
    #[serde(default)]
    policy: Option<String>,
    /// Number of tasks to return. The wire `tasks_next()` always returns
    /// at most one, so anything other than `1` (or unset) is rejected.
    #[serde(default)]
    k: Option<u32>,
    /// Federated-brain query is not yet wired; rejected when set so
    /// callers don't silently get current-brain-only results.
    #[serde(default)]
    brains: Option<Vec<String>>,
}

pub(super) struct TaskNext;

impl McpTool for TaskNext {
    fn name(&self) -> &'static str {
        "tasks.next"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get the next highest-priority ready task using the daemon's built-in priority policy. Federated/cross-brain queries (via `brains`) and configurable policy/k are not yet supported on the wire path — non-default `policy`, `k`, and non-empty `brains` are rejected.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "policy": {
                        "type": "string",
                        "enum": ["priority", "due_date"],
                        "description": "Sorting policy. 'priority' (default): by priority then due date. 'due_date': by due date then priority.",
                        "default": "priority"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of tasks to return. Default: 1",
                        "default": 1
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Brains to query. Pass [\"all\"] to query all registered brains. Pass a list of brain names or IDs for a federated query."
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
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };
            if let Some(policy) = parsed.policy.as_deref()
                && policy != "priority"
            {
                return ToolCallResult::error(format!(
                    "policy='{policy}' is not yet supported on this wire path; \
                     only the default 'priority' policy is honoured"
                ));
            }
            if let Some(k) = parsed.k
                && k != 1
            {
                return ToolCallResult::error(format!(
                    "k={k} is not yet supported; the wire returns at most one task per call. \
                     Omit `k` or set it to 1."
                ));
            }
            if parsed.brains.as_ref().is_some_and(|b| !b.is_empty()) {
                return ToolCallResult::error(
                    "Federated `brains` query is not yet wired for tasks.next; \
                     omit the parameter to query the current brain",
                );
            }

            match ctx.with_client(|c| c.tasks_next()).await {
                Ok(Some(task)) => {
                    let task_json = json!({
                        "task_id": task.task_id,
                        "title": task.title,
                        "status": task.status,
                        "priority": task.priority,
                        "brain_id": task.brain_id,
                    });
                    let response = json!({
                        "results": [{ "epic": null, "tasks": [task_json] }],
                        "ready_count": 0,
                        "blocked_count": 0,
                    });
                    json_response(&response)
                }
                Ok(None) => {
                    let response = json!({
                        "results": [],
                        "ready_count": 0,
                        "blocked_count": 0,
                    });
                    json_response(&response)
                }
                Err(e) => ToolCallResult::error(format!("Failed to get next task: {e}")),
            }
        })
    }
}
