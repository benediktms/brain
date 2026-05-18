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

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskNext;

impl McpTool for TaskNext {
    fn name(&self) -> &'static str {
        "tasks.next"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get the next highest-priority ready task(s). Returns tasks with no unresolved dependencies, sorted by configurable policy. Includes dependency summary and linked notes for each task. Supports cross-brain queries via the `brains` parameter.".into(),
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
        _params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
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
