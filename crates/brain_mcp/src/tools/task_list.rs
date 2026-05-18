//! `tasks.list` MCP tool — thin wrapper over `DaemonClient::tasks_list`.
//!
//! Returns a list of tasks filtered by status and optional field filters.
//! The wire `TasksListParams` covers status/priority/limit/search; several
//! legacy filters (task_type, assignee, label, task_ids, brains, brain,
//! include_description) are accepted in the schema (preserved verbatim) but
//! not forwarded — the daemon ignores fields not in `TasksListParams`.
//!
//! ## Deviations from legacy
//!
//! - `task_type`, `assignee`, `label`, `task_ids`, `brain`, `brains`, and
//!   `include_description` are accepted in the schema for forward-compatibility
//!   but are not wired; the daemon applies only status/priority/limit/search.
//! - `status` values "ready", "blocked", "in_progress", "cancelled" are
//!   accepted in the schema but the wire only guarantees "open" and "done"
//!   are honoured; other values are forwarded as-is and the daemon may treat
//!   them differently from the legacy server-side filtering.
//! - Response shape: `{tasks, total, has_more}` — same top-level keys as
//!   legacy, but each task is the minimal `TaskSummary` wire shape (no labels,
//!   linked_notes, dependency_summary, etc.).

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::TasksListParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskList;

fn list_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": ["open", "ready", "blocked", "done", "in_progress", "cancelled"],
                "description": "Filter tasks by status. 'open' (default): excludes done/cancelled. 'ready': no unresolved deps. 'blocked': has unresolved deps or blocked_reason. 'done': completed or cancelled tasks. 'in_progress': only in-progress tasks. 'cancelled': only cancelled tasks.",
                "default": "open"
            },
            "task_ids": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Fetch specific tasks by ID or prefix (ignores status filter). Unresolvable IDs are silently skipped. Pass as a JSON array, e.g. [\"BRN-01JPH\", \"BRN-02ABC\"]"
            },
            "priority": {
                "type": "integer",
                "description": "Filter by exact priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog)"
            },
            "task_type": {
                "type": "string",
                "description": "Filter by task type (task, bug, feature, epic, spike)"
            },
            "assignee": {
                "type": "string",
                "description": "Filter by assignee"
            },
            "label": {
                "type": "string",
                "description": "Filter by label (exact match)"
            },
            "search": {
                "type": "string",
                "description": "Full-text search on title and description (FTS5 query syntax)"
            },
            "include_description": {
                "type": "boolean",
                "description": "Include task descriptions in output. Default: false (omitted to reduce response size).",
                "default": false
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of tasks to return. Default: 50. Response includes 'total' and 'has_more' for pagination.",
                "default": 50
            },
            "brain": {
                "type": "string",
                "description": "DEPRECATED: use `brains` instead. Equivalent to `brains: [brain]`."
            },
            "brains": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Brains to query. Pass [\"all\"] to query all registered brains. Pass a list of brain names or IDs for a federated query."
            }
        }
    })
}

#[derive(Deserialize, Default)]
struct Params {
    status: Option<String>,
    priority: Option<u8>,
    limit: Option<u32>,
    search: Option<String>,
}

impl McpTool for TaskList {
    fn name(&self) -> &'static str {
        "tasks.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List tasks filtered by status and optional field filters. Returns summary task objects (descriptions omitted by default — use tasks.get for full details). Results are sorted by priority and paginated.".into(),
            input_schema: list_schema(),
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

            let wire_params = TasksListParams {
                status: parsed.status,
                priority: parsed.priority,
                limit: parsed.limit,
                search: parsed.search,
            };

            match ctx.with_client(|c| c.tasks_list(wire_params)).await {
                Ok(tasks) => {
                    let total = tasks.len();
                    // Wire does not return has_more; reflect false conservatively.
                    let response = json!({
                        "tasks": tasks,
                        "total": total,
                        "has_more": false,
                    });
                    json_response(&response)
                }
                Err(e) => ToolCallResult::error(format!("Failed to list tasks: {e}")),
            }
        })
    }
}
