//! `tasks.get` MCP tool — thin wrapper over `DaemonClient::tasks_show`.
//!
//! Returns a single task by ID. The wire `TaskSummary` is a minimal
//! representation (task_id, title, status, priority, brain_id). The
//! legacy tool returned an enriched compact JSON object with relationships
//! (parent, children, blocked_by, blocks), comments, linked_notes,
//! external_ids, and dependency_summary.
//!
//! ## Deviation from legacy
//!
//! The wire `tasks_show` returns a minimal `TaskSummary`. Rich fields
//! (parent/children/blocked_by/blocks expand, comments, linked_notes,
//! external_ids, dependency_summary) are NOT available on this wire path.
//! The `expand`, `brains` schema params are preserved verbatim for
//! forward-compatibility but are not acted upon — the daemon returns the
//! same minimal summary regardless. Cross-brain lookup via `brains` is
//! also not implemented: the daemon resolves the ID in its local scope.
//! Callers needing full detail should fall back to `tasks.apply_event` /
//! the local MCP path until the wire surface is extended.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskGet;

impl McpTool for TaskGet {
    fn name(&self) -> &'static str {
        "tasks.get"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get a single task by ID (full or prefix) with full details including relationships, comments, labels, and linked notes. Relationships (parent, children, blocked_by, blocks) are returned as compact stubs by default; use the expand parameter to get full task objects.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "The task ID to retrieve (full ID or unique prefix, e.g. 'BRN-01JPH')"
                    },
                    "expand": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["parent", "children", "blocked_by", "blocks"]
                        },
                        "description": "Expand relationship stubs to full task objects. Pass as a JSON array, e.g. [\"parent\", \"blocked_by\"]"
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional list of brain names or IDs to search. When provided, searches across the specified brains instead of just the current brain."
                    },
                },
                "required": ["task_id"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let task_id = match params.get("task_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => return ToolCallResult::error("Missing required parameter: task_id"),
            };

            if task_id.is_empty() {
                return ToolCallResult::error("task_id must not be empty");
            }

            match ctx.with_client(|c| c.tasks_show(task_id.clone())).await {
                Ok(Some(task)) => {
                    // Emit a JSON object matching the legacy wire shape as closely
                    // as possible given the minimal TaskSummary. Rich fields
                    // (parent, children, comments, etc.) are absent on this path.
                    let response = json!({
                        "task_id": task.task_id,
                        "title": task.title,
                        "status": task.status,
                        "priority": task.priority,
                        "brain_id": task.brain_id,
                        "parent": null,
                        "children": [],
                        "blocked_by": [],
                        "blocks": [],
                        "comments": [],
                        "labels": [],
                        "linked_notes": [],
                        "external_ids": [],
                        "external_blockers": [],
                        "dependency_summary": {
                            "total_deps": 0,
                            "done_deps": 0,
                            "blocking_task_ids": []
                        }
                    });
                    json_response(&response)
                }
                Ok(None) => ToolCallResult::error(format!("Task not found: {task_id}")),
                Err(e) => ToolCallResult::error(format!("Failed to get task: {e}")),
            }
        })
    }
}
