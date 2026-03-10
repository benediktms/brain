use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::events::{
    EventType, TaskCreatedPayload, TaskEvent, TaskStatus, TaskType, new_task_id,
};
use crate::utils::{parse_timestamp, task_row_to_json};

use super::McpTool;

/// Shared JSON Schema fragment for timestamp fields that accept ISO 8601 strings
/// or Unix-seconds integers.
fn timestamp_schema(description: &str) -> Value {
    json!({
        "oneOf": [
            { "type": "string", "description": "ISO 8601 datetime (e.g. \"2026-03-15T00:00:00Z\")" },
            { "type": "integer", "description": "Unix seconds" }
        ],
        "description": description
    })
}

/// Shared JSON Schema fragment for the `task_type` enum.
fn task_type_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["task", "bug", "feature", "epic", "spike"]
    })
}

/// Build the full discriminated JSON Schema for `tasks.apply_event`.
///
/// Uses `allOf` with `if/then` blocks keyed on `event_type` so that MCP clients
/// (and agents) can validate per-event-type payloads at the schema level.
fn apply_event_schema() -> Value {
    let event_types = [
        "task_created", "task_updated", "status_changed",
        "dependency_added", "dependency_removed",
        "note_linked", "note_unlinked",
        "label_added", "label_removed", "comment_added",
        "parent_set", "external_id_added", "external_id_removed",
    ];

    // -- per-event payload schemas --

    let task_created_payload = json!({
        "type": "object",
        "properties": {
            "title": { "type": "string" },
            "description": { "type": ["string", "null"] },
            "priority": { "type": "integer", "minimum": 0, "maximum": 5, "default": 4 },
            "status": { "type": "string", "enum": ["open", "in_progress", "blocked", "done", "cancelled"], "default": "open" },
            "due_ts": timestamp_schema("Due date"),
            "task_type": task_type_schema(),
            "assignee": { "type": ["string", "null"] },
            "defer_until": timestamp_schema("Defer until date"),
            "parent_task_id": { "type": ["string", "null"] }
        },
        "required": ["title"],
        "additionalProperties": false
    });

    let task_updated_payload = json!({
        "type": "object",
        "properties": {
            "title": { "type": "string" },
            "description": { "type": ["string", "null"] },
            "priority": { "type": "integer", "minimum": 0, "maximum": 5 },
            "due_ts": timestamp_schema("Due date"),
            "blocked_reason": { "type": ["string", "null"] },
            "task_type": task_type_schema(),
            "assignee": { "type": ["string", "null"] },
            "defer_until": timestamp_schema("Defer until date")
        },
        "additionalProperties": false
    });

    let status_changed_payload = json!({
        "type": "object",
        "properties": {
            "new_status": {
                "type": "string",
                "enum": ["open", "in_progress", "blocked", "done", "cancelled"]
            }
        },
        "required": ["new_status"],
        "additionalProperties": false
    });

    let dependency_payload = json!({
        "type": "object",
        "properties": {
            "depends_on_task_id": { "type": "string" }
        },
        "required": ["depends_on_task_id"],
        "additionalProperties": false
    });

    let note_link_payload = json!({
        "type": "object",
        "properties": {
            "chunk_id": { "type": "string" }
        },
        "required": ["chunk_id"],
        "additionalProperties": false
    });

    let label_payload = json!({
        "type": "object",
        "properties": {
            "label": { "type": "string" }
        },
        "required": ["label"],
        "additionalProperties": false
    });

    let comment_payload = json!({
        "type": "object",
        "properties": {
            "body": { "type": "string" }
        },
        "required": ["body"],
        "additionalProperties": false
    });

    let parent_set_payload = json!({
        "type": "object",
        "properties": {
            "parent_task_id": { "type": ["string", "null"], "description": "Parent task ID, or null to clear" }
        },
        "additionalProperties": false
    });

    let external_id_payload = json!({
        "type": "object",
        "properties": {
            "source": { "type": "string" },
            "external_id": { "type": "string" },
            "external_url": { "type": ["string", "null"] }
        },
        "required": ["source", "external_id"],
        "additionalProperties": false
    });

    // -- if/then blocks --

    let if_then = |event_type: &str, payload_schema: Value| -> Value {
        json!({
            "if": { "properties": { "event_type": { "const": event_type } } },
            "then": { "properties": { "payload": payload_schema } }
        })
    };

    let all_of = vec![
        if_then("task_created", task_created_payload),
        if_then("task_updated", task_updated_payload),
        if_then("status_changed", status_changed_payload),
        if_then("dependency_added", dependency_payload.clone()),
        if_then("dependency_removed", dependency_payload),
        if_then("note_linked", note_link_payload.clone()),
        if_then("note_unlinked", note_link_payload),
        if_then("label_added", label_payload.clone()),
        if_then("label_removed", label_payload),
        if_then("comment_added", comment_payload),
        if_then("parent_set", parent_set_payload),
        if_then("external_id_added", external_id_payload.clone()),
        if_then("external_id_removed", external_id_payload),
    ];

    json!({
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
                "description": "Event-type-specific payload. See allOf constraints for per-event-type field definitions. Timestamps (due_ts, defer_until) accept ISO 8601 strings (preferred) or Unix-seconds integers."
            }
        },
        "required": ["event_type", "payload"],
        "allOf": all_of
    })
}

#[derive(Deserialize)]
struct Params {
    event_type: String,
    task_id: Option<String>,
    #[serde(default = "default_actor")]
    actor: String,
    payload: serde_json::Map<String, Value>,
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct TaskApplyEvent;

impl TaskApplyEvent {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // Parse event_type
        let event_type: EventType = match serde_json::from_value(json!(params.event_type)) {
            Ok(et) => et,
            Err(_) => {
                return ToolCallResult::error(format!(
                    "Invalid event_type: '{}'. Must be one of: task_created, \
                     task_updated, status_changed, dependency_added, dependency_removed, \
                     note_linked, note_unlinked, label_added, label_removed, comment_added, \
                     parent_set, external_id_added, external_id_removed",
                    params.event_type
                ));
            }
        };

        // Parse task_id: auto-generate for task_created, resolve prefix for others
        let task_id = match params.task_id.as_deref() {
            Some(id) if id.len() > 256 => {
                return ToolCallResult::error("task_id exceeds maximum length of 256 characters");
            }
            Some(id) => {
                if event_type == EventType::TaskCreated {
                    id.to_string()
                } else {
                    // Resolve prefix for non-create events
                    match ctx.tasks.resolve_task_id(id) {
                        Ok(resolved) => resolved,
                        Err(e) => {
                            return ToolCallResult::error(format!(
                                "Failed to resolve task_id: {e}"
                            ));
                        }
                    }
                }
            }
            None => {
                if event_type == EventType::TaskCreated {
                    let prefix = match ctx.tasks.get_project_prefix() {
                        Ok(p) => p,
                        Err(e) => {
                            return ToolCallResult::error(format!(
                                "Failed to get project prefix: {e}"
                            ));
                        }
                    };
                    new_task_id(&prefix)
                } else {
                    return ToolCallResult::error(
                        "Missing required parameter: task_id (required for all event types except task_created)",
                    );
                }
            }
        };

        if params.actor.len() > 256 {
            return ToolCallResult::error("actor exceeds maximum length of 256 characters");
        }

        // Normalize timestamp fields from ISO 8601 strings to i64 Unix seconds
        let mut payload = Value::Object(params.payload);
        for field in &["defer_until", "due_ts"] {
            if let Some(val) = payload.get(*field).filter(|v| v.is_string()) {
                match parse_timestamp(val) {
                    Some(ts) => payload[*field] = json!(ts),
                    None => {
                        return ToolCallResult::error(format!(
                            "Invalid timestamp for '{field}': expected ISO 8601 string or integer"
                        ));
                    }
                }
            }
        }

        // Validate task_type if provided
        if let Some(tt) = payload.get("task_type").and_then(|v| v.as_str())
            && tt.parse::<TaskType>().is_err()
        {
            return ToolCallResult::error(format!(
                "Invalid task_type: '{tt}'. Must be one of: task, bug, feature, epic, spike"
            ));
        }

        // Resolve depends_on_task_id and parent_task_id references in payload
        if let Some(dep_id) = payload.get("depends_on_task_id").and_then(|v| v.as_str())
            && !dep_id.is_empty()
        {
            match ctx.tasks.resolve_task_id(dep_id) {
                Ok(resolved) => payload["depends_on_task_id"] = json!(resolved),
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to resolve depends_on_task_id: {e}"
                    ));
                }
            }
        }
        if let Some(parent_id) = payload.get("parent_task_id").and_then(|v| v.as_str())
            && !parent_id.is_empty()
        {
            match ctx.tasks.resolve_task_id(parent_id) {
                Ok(resolved) => payload["parent_task_id"] = json!(resolved),
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to resolve parent_task_id: {e}"));
                }
            }
        }

        // For task_created, apply domain defaults via serde round-trip through TaskCreatedPayload
        let payload = if event_type == EventType::TaskCreated {
            match serde_json::from_value::<TaskCreatedPayload>(payload) {
                Ok(typed) => serde_json::to_value(typed).unwrap(),
                Err(e) => {
                    return ToolCallResult::error(format!("Invalid task_created payload: {e}"));
                }
            }
        } else {
            payload
        };

        let event = TaskEvent::from_raw(task_id.clone(), params.actor, event_type.clone(), payload);

        // Append (validates + writes JSONL + applies projection)
        if let Err(e) = ctx.tasks.append(&event) {
            return ToolCallResult::error(format!("Task event failed: {e}"));
        }

        // Fetch resulting task state
        let task_json = match ctx.tasks.get_task(&task_id) {
            Ok(Some(row)) => {
                let labels = ctx.tasks.get_task_labels(&task_id).unwrap_or_default();
                task_row_to_json(&row, labels)
            }
            Ok(None) => json!(null),
            Err(e) => {
                warn!(error = %e, "failed to fetch task after event");
                json!(null)
            }
        };

        // Detect newly unblocked tasks after status_changed to done/cancelled
        let unblocked_task_ids = if event_type == EventType::StatusChanged {
            let new_status = event
                .payload
                .get("new_status")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if new_status == TaskStatus::Done.as_ref()
                || new_status == TaskStatus::Cancelled.as_ref()
            {
                ctx.tasks.list_newly_unblocked(&task_id).unwrap_or_default()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let short_id = ctx
            .tasks
            .shortest_unique_prefix(&task_id)
            .unwrap_or_else(|_| task_id.clone());

        let response = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "short_id": short_id,
            "task": task_json,
            "unblocked_task_ids": unblocked_task_ids,
        });

        ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
    }
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
        Box::pin(std::future::ready(self.execute(params, ctx)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    async fn dispatch(
        registry: &super::super::ToolRegistry,
        name: &str,
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        registry.dispatch(name, params, ctx).await
    }

    #[tokio::test]
    async fn test_create() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "test-1",
            "payload": { "title": "My first task", "priority": 2 }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert!(result.is_error.is_none(), "should succeed");

        let text = &result.content[0].text;
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["task_id"], "test-1");
        assert!(parsed["event_id"].is_string());
        assert_eq!(parsed["task"]["title"], "My first task");
        assert_eq!(parsed["task"]["status"], "open");
        assert_eq!(parsed["task"]["priority"], 2);
        assert_eq!(parsed["unblocked_task_ids"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_auto_id() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "payload": { "title": "Auto ID task" }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert!(parsed["task_id"].is_string());
        assert!(!parsed["task_id"].as_str().unwrap().is_empty());
        assert_eq!(parsed["task"]["title"], "Auto ID task");
        assert_eq!(parsed["task"]["priority"], 4); // default
    }

    #[tokio::test]
    async fn test_status_change() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create task first
        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Task" }
        });
        dispatch(&registry, "tasks.apply_event", create, &ctx).await;

        // Change status
        let update = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "in_progress" }
        });
        let result = dispatch(&registry, "tasks.apply_event", update, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["status"], "in_progress");
    }

    #[tokio::test]
    async fn test_unblocked() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create two tasks, t2 depends on t1
        for (id, title) in &[("t1", "Blocker"), ("t2", "Blocked")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            dispatch(&registry, "tasks.apply_event", p, &ctx).await;
        }

        let dep = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        dispatch(&registry, "tasks.apply_event", dep, &ctx).await;

        // Complete t1 — t2 should be unblocked
        let done = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "done" }
        });
        let result = dispatch(&registry, "tasks.apply_event", done, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let unblocked = parsed["unblocked_task_ids"].as_array().unwrap();
        assert_eq!(unblocked.len(), 1);
        assert_eq!(unblocked[0], "t2");
    }

    #[tokio::test]
    async fn test_cycle_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create two tasks
        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            dispatch(&registry, "tasks.apply_event", p, &ctx).await;
        }

        // t1 depends on t2
        let dep1 = json!({
            "event_type": "dependency_added",
            "task_id": "t1",
            "payload": { "depends_on_task_id": "t2" }
        });
        dispatch(&registry, "tasks.apply_event", dep1, &ctx).await;

        // t2 depends on t1 — cycle!
        let dep2 = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        let result = dispatch(&registry, "tasks.apply_event", dep2, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("cycle"));
    }

    #[tokio::test]
    async fn test_missing_event_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({ "payload": { "title": "No event type" } });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_invalid_event_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "bogus_event",
            "payload": { "title": "Bad type" }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid event_type"));
    }

    #[tokio::test]
    async fn test_missing_task_id_for_update() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "status_changed",
            "payload": { "new_status": "done" }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("task_id"));
    }

    #[tokio::test]
    async fn test_with_type_and_assignee() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Bug fix",
                "task_type": "bug",
                "assignee": "alice",
                "priority": 1
            }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["task_type"], "bug");
        assert_eq!(parsed["task"]["assignee"], "alice");
    }

    #[tokio::test]
    async fn test_labels() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create task
        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Labeled task" }
        });
        dispatch(&registry, "tasks.apply_event", create, &ctx).await;

        // Add labels
        let add1 = json!({
            "event_type": "label_added",
            "task_id": "t1",
            "payload": { "label": "urgent" }
        });
        let add2 = json!({
            "event_type": "label_added",
            "task_id": "t1",
            "payload": { "label": "backend" }
        });
        dispatch(&registry, "tasks.apply_event", add1, &ctx).await;
        let result = dispatch(&registry, "tasks.apply_event", add2, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let labels = parsed["task"]["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&json!("backend")));
        assert!(labels.contains(&json!("urgent")));

        // Remove a label
        let rm = json!({
            "event_type": "label_removed",
            "task_id": "t1",
            "payload": { "label": "urgent" }
        });
        let result = dispatch(&registry, "tasks.apply_event", rm, &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let labels = parsed["task"]["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "backend");
    }

    #[tokio::test]
    async fn test_comment() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Commented task" }
        });
        dispatch(&registry, "tasks.apply_event", create, &ctx).await;

        let comment = json!({
            "event_type": "comment_added",
            "task_id": "t1",
            "actor": "bob",
            "payload": { "body": "This needs review" }
        });
        let result = dispatch(&registry, "tasks.apply_event", comment, &ctx).await;
        assert!(result.is_error.is_none());

        // Verify comment stored by fetching via TaskStore
        let comments = ctx.tasks.get_task_comments("t1").unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].body, "This needs review");
        assert_eq!(comments[0].author, "bob");
    }

    #[tokio::test]
    async fn test_default_task_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "No explicit type" }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["task_type"], "task");
    }

    #[tokio::test]
    async fn test_invalid_task_type_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Bad type", "task_type": "story" }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid task_type"));
        assert!(result.content[0].text.contains("story"));
    }

    #[tokio::test]
    async fn test_valid_spike_task_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Spike task", "task_type": "spike" }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert!(
            result.is_error.is_none(),
            "spike should be a valid task type"
        );

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["task_type"], "spike");
    }

    #[tokio::test]
    async fn test_iso8601_defer_until() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Deferred task",
                "defer_until": "2026-12-01T00:00:00Z"
            }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert!(result.is_error.is_none(), "should succeed");

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Response should be ISO 8601 string
        assert_eq!(parsed["task"]["defer_until"], "2026-12-01T00:00:00+00:00");

        // Verify stored as i64 internally
        let row = ctx.tasks.get_task("t1").unwrap().unwrap();
        assert_eq!(row.defer_until, Some(1796083200));
    }

    #[tokio::test]
    async fn test_integer_defer_until_backward_compat() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Deferred task int",
                "defer_until": 1796083200
            }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert!(result.is_error.is_none(), "should succeed");

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Response should still be ISO 8601 string
        assert_eq!(parsed["task"]["defer_until"], "2026-12-01T00:00:00+00:00");

        let row = ctx.tasks.get_task("t1").unwrap().unwrap();
        assert_eq!(row.defer_until, Some(1796083200));
    }

    #[tokio::test]
    async fn test_iso8601_due_ts() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Due task",
                "due_ts": "2026-06-15T12:00:00Z"
            }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["due_ts"], "2026-06-15T12:00:00+00:00");
    }

    #[tokio::test]
    async fn test_timestamps_returned_as_iso_strings() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Timestamp check" }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();

        // created_at and updated_at should be ISO strings, not integers
        assert!(parsed["task"]["created_at"].is_string());
        assert!(parsed["task"]["updated_at"].is_string());
        // They should be parseable as RFC 3339
        let created = parsed["task"]["created_at"].as_str().unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(created).is_ok(),
            "created_at should be valid RFC 3339"
        );
    }

    #[tokio::test]
    async fn test_invalid_iso_timestamp_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Bad timestamp",
                "defer_until": "not-a-date"
            }
        });
        let result = dispatch(&registry, "tasks.apply_event", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid timestamp"));
    }
}
