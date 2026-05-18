//! `tasks.apply_event` MCP tool — thin wrapper over
//! `DaemonClient::tasks_apply_event`.
//!
//! The daemon owns event validation (event_type recognition, task_id
//! resolution, timestamp normalization, archived-brain guard, cycle
//! detection) and event sourcing. The MCP tool body validates params can
//! be deserialized, wraps the raw input as opaque JSON, and echoes the
//! daemon's result JSON verbatim.
//!
//! Schema is preserved VERBATIM from the legacy
//! `brain_lib::mcp::tools::task_apply_event` definition — same event_type
//! enum (16 variants), same payload structure with additionalProperties:true.

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

use brain_rpc::TasksApplyEventParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskApplyEvent;

fn apply_event_schema() -> Value {
    let event_types = [
        "task_created",
        "task_updated",
        "status_changed",
        "dependency_added",
        "dependency_removed",
        "note_linked",
        "note_unlinked",
        "label_added",
        "label_removed",
        "comment_added",
        "comment_updated",
        "parent_set",
        "external_id_added",
        "external_id_removed",
        "external_blocker_added",
        "external_blocker_resolved",
    ];

    serde_json::json!({
        "type": "object",
        "properties": {
            "event_type": {
                "type": "string",
                "enum": event_types,
                "description": "The type of task event to apply"
            },
            "task_id": {
                "type": "string",
                "description": "Task ID (full or prefix). Optional for task_created (auto-generates prefixed ULID). For other events, accepts full ID or a unique prefix (e.g. 'BRN-01JPH')."
            },
            "actor": {
                "type": "string",
                "description": "Who is performing this action. Default: 'mcp'",
                "default": "mcp"
            },
            "payload": {
                "type": "object",
                "description": "Event-type-specific payload object. Timestamps (due_ts, defer_until) accept ISO 8601 strings (preferred) or Unix-seconds integers.\n\n\
                Per event_type payloads:\n\
                - task_created: {title (required), description, priority (0-5, default 4), status (open|in_progress|blocked|done|cancelled, default open), due_ts, task_type (task|bug|feature|epic|spike), assignee, defer_until, parent_task_id}\n\
                - task_updated: {title, description, priority, due_ts, blocked_reason, task_type, assignee, defer_until}\n\
                - status_changed: {new_status (required, open|in_progress|blocked|done|cancelled)}\n\
                - dependency_added/dependency_removed: {depends_on_task_id (required)}\n\
                - note_linked/note_unlinked: {chunk_id (required)}\n\
                - label_added/label_removed: {label (required)}\n\
                - comment_added: {body (required, max 64 KB)}\
                - comment_updated: {comment_id (required), body (required, max 64 KB)}\\n\
                - parent_set: {parent_task_id (string or null to clear)}\n\
                - external_id_added/external_id_removed: {source (required), external_id (required), external_url}\n\
                - external_blocker_added: {source (required), external_id (required), external_url, blocking (default true)}\n\
                - external_blocker_resolved: {source (required), external_id (required), resolved_at (ISO 8601 string or unix-seconds; default: now)}",
                "properties": {
                    "body": {
                        "type": "string",
                        "maxLength": 65536
                    },
                    "comment_id": {
                        "type": "string"
                    }
                },
                "additionalProperties": true
            }
        },
        "required": ["event_type", "payload"]
    })
}

impl McpTool for TaskApplyEvent {
    fn name(&self) -> &'static str {
        "tasks.apply_event"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Apply an event to the task system. Creates, updates, or changes tasks via event sourcing. Returns the resulting task state and any newly unblocked task IDs.".into(),
            input_schema: apply_event_schema(),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let wire_params = TasksApplyEventParams { event_json: params };

            match ctx.with_client(|c| c.tasks_apply_event(wire_params)).await {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Failed to apply event: {e}")),
            }
        })
    }
}
