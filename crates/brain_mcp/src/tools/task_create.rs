//! `tasks.create` MCP tool — thin wrapper over `DaemonClient::tasks_create`.
//!
//! Creates a task in the current (or a remote) brain. The wire surface
//! `TasksCreateParams` covers the core fields (title, description, priority,
//! task_type, assignee, parent). Cross-brain creation (`brain` param) and
//! the `link_from`/`link_type` params are NOT supported on this wire path —
//! those remain server-side concerns for now. The schema preserves all
//! legacy fields verbatim; callers using `brain`/`link_from`/`link_type`
//! will receive a graceful error indicating the limitation.
//!
//! ## Deviations from legacy
//!
//! - `brain`, `link_from`, `link_type` fields are accepted in the schema
//!   (preserved verbatim) but not wired — if provided, an error is returned.
//! - `due_ts` and `defer_until` are accepted in the schema but the wire
//!   `TasksCreateParams` does not carry them; they are dropped silently.
//!   Callers needing full field coverage should use `tasks.apply_event`.
//! - Response shape: local path returns `{task_id, uri, task, unblocked_task_ids}`.
//!   The `uri` field is absent (daemon does not return it). `task` is the
//!   minimal `TaskSummary` wire shape, not the enriched compact JSON.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::TasksCreateParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskCreate;

fn create_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": "Task title"
            },
            "description": {
                "type": "string",
                "description": "Task description"
            },
            "priority": {
                "type": "integer",
                "description": "Priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog). Default: 4",
                "default": 4
            },
            "task_type": {
                "type": "string",
                "description": "Task type (task|bug|feature|epic|spike). Default: task"
            },
            "assignee": {
                "type": "string",
                "description": "Assignee"
            },
            "parent": {
                "type": "string",
                "description": "Parent task ID (full ID, short hash, or prefix). Prefer setting parent at creation time to avoid ambiguous prefix errors from post-hoc parent_set calls."
            },
            "due_ts": {
                "type": "string",
                "description": "Due date as ISO 8601 string"
            },
            "defer_until": {
                "type": "string",
                "description": "Defer until date as ISO 8601 string"
            },
            "actor": {
                "type": "string",
                "description": "Who is creating the task. Default: mcp",
                "default": "mcp"
            },
            "brain": {
                "type": "string",
                "description": "Target brain name or ID for cross-brain task creation. If omitted, creates in the current brain."
            },
            "link_from": {
                "type": "string",
                "description": "Local task ID to auto-create a cross-brain reference from (resolved via prefix)"
            },
            "link_type": {
                "type": "string",
                "description": "Type of cross-brain link: depends_on, blocks, or related (default: related)",
                "enum": ["depends_on", "blocks", "related"],
                "default": "related"
            }
        },
        "required": ["title"]
    })
}

#[derive(Deserialize)]
struct Params {
    title: String,
    description: Option<String>,
    #[serde(default = "default_priority")]
    priority: u8,
    task_type: Option<String>,
    assignee: Option<String>,
    parent: Option<String>,
    brain: Option<String>,
    /// Cross-brain creation+link is not on this wire path; capturing
    /// the field so it gets explicitly rejected rather than silently
    /// dropped.
    #[serde(default)]
    link_from: Option<String>,
    /// As above for the link kind that pairs with `link_from`.
    #[serde(default)]
    link_type: Option<String>,
}

fn default_priority() -> u8 {
    4
}

impl McpTool for TaskCreate {
    fn name(&self) -> &'static str {
        "tasks.create"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Create a task in the current brain's event store and return the resulting task state. Same as tasks.apply_event with event_type: task_created, but with a simpler flat schema. Defaults: priority=4 (backlog), status=open, actor=mcp. Cross-brain creation (`brain` / `link_from` / `link_type`) is not yet supported on the wire path — requests with those parameters are rejected; use tasks.apply_event for cross-brain flows in the meantime.".into(),
            input_schema: create_schema(),
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

            // Cross-brain creation is not supported on this wire path.
            if parsed.brain.is_some() {
                return ToolCallResult::error(
                    "Cross-brain creation via 'brain' param is not yet available on the wire path. \
                     Use tasks.apply_event with event_type: task_created instead.",
                );
            }
            if parsed.link_from.is_some() || parsed.link_type.is_some() {
                return ToolCallResult::error(
                    "link_from / link_type params require cross-brain creation, \
                     which is not yet available on the wire path. \
                     Omit these params for same-brain creation.",
                );
            }

            let wire_params = TasksCreateParams {
                title: parsed.title,
                description: parsed.description,
                priority: parsed.priority,
                task_type: parsed.task_type.unwrap_or_else(|| "task".to_string()),
                assignee: parsed.assignee,
                parent: parsed.parent,
            };

            match ctx.with_client(|c| c.tasks_create(wire_params)).await {
                Ok((task, _event_id)) => {
                    let response = json!({
                        "task_id": task.task_id,
                        "task": {
                            "task_id": task.task_id,
                            "title": task.title,
                            "status": task.status,
                            "priority": task.priority,
                        },
                        "unblocked_task_ids": [],
                    });
                    json_response(&response)
                }
                Err(e) => ToolCallResult::error(format!("Failed to create task: {e}")),
            }
        })
    }
}
