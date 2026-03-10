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
use super::{Warning, inject_warnings, json_response, store_or_warn};

/// The result of pure validation — all static constraints checked, no I/O performed.
#[derive(Debug)]
struct ValidatedEvent {
    event_type: EventType,
    /// Raw task_id from the caller (length-checked, but not DB-resolved).
    task_id_raw: Option<String>,
    actor: String,
    /// Payload with timestamps normalized to i64 and task_type validated.
    payload: Value,
}

/// Validate all static (non-I/O) constraints on the incoming parameters.
///
/// Checks:
/// - `event_type` is a recognized variant
/// - `task_id`, if present, is ≤ 256 characters
/// - `actor` is ≤ 256 characters
/// - Timestamp fields (`defer_until`, `due_ts`) are valid ISO 8601 or integers
/// - `task_type`, if present, is a recognized variant
///
/// Returns `Err(String)` with a human-readable message on any violation.
/// No database access or other side effects are performed.
fn parse_and_validate_event(params: &Params) -> Result<ValidatedEvent, String> {
    // Parse event_type
    let event_type: EventType = serde_json::from_value(json!(params.event_type)).map_err(|_| {
        format!(
            "Invalid event_type: '{}'. Must be one of: task_created, \
             task_updated, status_changed, dependency_added, dependency_removed, \
             note_linked, note_unlinked, label_added, label_removed, comment_added, \
             parent_set, external_id_added, external_id_removed",
            params.event_type
        )
    })?;

    // Validate task_id length
    if let Some(id) = &params.task_id
        && id.len() > 256
    {
        return Err("task_id exceeds maximum length of 256 characters".into());
    }

    // Validate actor length
    if params.actor.len() > 256 {
        return Err("actor exceeds maximum length of 256 characters".into());
    }

    // Normalize timestamp fields from ISO 8601 strings to i64 Unix seconds
    let mut payload = Value::Object(params.payload.clone());
    for field in &["defer_until", "due_ts"] {
        if let Some(val) = payload.get(*field).filter(|v| v.is_string()) {
            match parse_timestamp(val) {
                Some(ts) => payload[*field] = json!(ts),
                None => {
                    return Err(format!(
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
        return Err(format!(
            "Invalid task_type: '{tt}'. Must be one of: task, bug, feature, epic, spike"
        ));
    }

    Ok(ValidatedEvent {
        event_type,
        task_id_raw: params.task_id.clone(),
        actor: params.actor.clone(),
        payload,
    })
}

/// Build the JSON Schema for `tasks.apply_event`.
///
/// Note: The Anthropic API does not support `oneOf`/`allOf`/`anyOf` at the
/// top level of `input_schema`, so we use a flat schema with per-event-type
/// payload fields documented in the description. Runtime validation is handled
/// by serde deserialization.
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
        "parent_set",
        "external_id_added",
        "external_id_removed",
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
                "description": "Event-type-specific payload object. Timestamps (due_ts, defer_until) accept ISO 8601 strings (preferred) or Unix-seconds integers.\n\n\
                Per event_type payloads:\n\
                - task_created: {title (required), description, priority (0-5, default 4), status (open|in_progress|blocked|done|cancelled, default open), due_ts, task_type (task|bug|feature|epic|spike), assignee, defer_until, parent_task_id}\n\
                - task_updated: {title, description, priority, due_ts, blocked_reason, task_type, assignee, defer_until}\n\
                - status_changed: {new_status (required, open|in_progress|blocked|done|cancelled)}\n\
                - dependency_added/dependency_removed: {depends_on_task_id (required)}\n\
                - note_linked/note_unlinked: {chunk_id (required)}\n\
                - label_added/label_removed: {label (required)}\n\
                - comment_added: {body (required)}\n\
                - parent_set: {parent_task_id (string or null to clear)}\n\
                - external_id_added/external_id_removed: {source (required), external_id (required), external_url}"
            }
        },
        "required": ["event_type", "payload"]
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

        // Pure validation — no DB access
        let validated = match parse_and_validate_event(&params) {
            Ok(v) => v,
            Err(msg) => return ToolCallResult::error(msg),
        };

        let mut warnings: Vec<Warning> = Vec::new();
        let event_type = validated.event_type;
        let mut payload = validated.payload;

        // Resolve task_id: auto-generate for task_created, resolve prefix for others (I/O)
        let task_id = match validated.task_id_raw.as_deref() {
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

        // Resolve depends_on_task_id and parent_task_id references in payload (I/O)
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

        let event = TaskEvent::from_raw(task_id.clone(), validated.actor, event_type.clone(), payload);

        // Append (validates + writes JSONL + applies projection)
        if let Err(e) = ctx.tasks.append(&event) {
            return ToolCallResult::error(format!("Task event failed: {e}"));
        }

        // Fetch resulting task state
        let task_json = match ctx.tasks.get_task(&task_id) {
            Ok(Some(row)) => {
                let labels = store_or_warn(
                    ctx.tasks.get_task_labels(&task_id),
                    "get_task_labels",
                    &mut warnings,
                );
                task_row_to_json(&row, labels)
            }
            Ok(None) => json!(null),
            Err(e) => {
                warn!(error = %e, "failed to fetch task after event");
                json!(null)
            }
        };

        // Detect newly unblocked tasks after status_changed to done/cancelled
        let unblocked_task_ids: Vec<String> = if event_type == EventType::StatusChanged {
            let new_status = event
                .payload
                .get("new_status")
                .and_then(|v| v.as_str())
                .unwrap_or(""); // Type coercion: extracting string from JSON Value, not a store error
            if new_status == TaskStatus::Done.as_ref()
                || new_status == TaskStatus::Cancelled.as_ref()
            {
                store_or_warn(
                    ctx.tasks.list_newly_unblocked(&task_id),
                    "list_newly_unblocked",
                    &mut warnings,
                )
                .iter()
                .map(|id| ctx.tasks.compact_id(id).unwrap_or_else(|_| id.clone()))
                .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let short_id = ctx
            .tasks
            .compact_id(&task_id)
            .unwrap_or_else(|_| task_id.clone());

        let mut response = json!({
            "task_id": short_id,
            "task": task_json,
            "unblocked_task_ids": unblocked_task_ids,
        });

        inject_warnings(&mut response, warnings);
        json_response(&response)
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

    // --- Unit tests for parse_and_validate_event (no DB required) ---

    fn make_params(event_type: &str, task_id: Option<&str>, payload: Value) -> super::Params {
        super::Params {
            event_type: event_type.to_string(),
            task_id: task_id.map(|s| s.to_string()),
            actor: "test-actor".to_string(),
            payload: match payload {
                Value::Object(m) => m,
                _ => panic!("payload must be an object"),
            },
        }
    }

    #[test]
    fn unit_invalid_event_type() {
        let params = make_params("bogus_event", None, json!({"title": "x"}));
        let err = super::parse_and_validate_event(&params).unwrap_err();
        assert!(err.contains("Invalid event_type"), "got: {err}");
        assert!(err.contains("bogus_event"), "got: {err}");
    }

    #[test]
    fn unit_task_id_too_long() {
        let long_id = "a".repeat(257);
        let params = make_params("status_changed", Some(&long_id), json!({"new_status": "done"}));
        let err = super::parse_and_validate_event(&params).unwrap_err();
        assert!(err.contains("task_id exceeds maximum length"), "got: {err}");
    }

    #[test]
    fn unit_task_id_exactly_256_ok() {
        let id_256 = "a".repeat(256);
        let params = make_params("status_changed", Some(&id_256), json!({"new_status": "done"}));
        assert!(
            super::parse_and_validate_event(&params).is_ok(),
            "256-char task_id should be accepted"
        );
    }

    #[test]
    fn unit_actor_too_long() {
        let mut params = make_params("status_changed", Some("t1"), json!({"new_status": "done"}));
        params.actor = "a".repeat(257);
        let err = super::parse_and_validate_event(&params).unwrap_err();
        assert!(err.contains("actor exceeds maximum length"), "got: {err}");
    }

    #[test]
    fn unit_invalid_task_type() {
        let params = make_params(
            "task_created",
            None,
            json!({"title": "Bad type", "task_type": "story"}),
        );
        let err = super::parse_and_validate_event(&params).unwrap_err();
        assert!(err.contains("Invalid task_type"), "got: {err}");
        assert!(err.contains("story"), "got: {err}");
    }

    #[test]
    fn unit_valid_task_type_accepted() {
        for tt in &["task", "bug", "feature", "epic", "spike"] {
            let params = make_params(
                "task_created",
                None,
                json!({"title": "x", "task_type": tt}),
            );
            assert!(
                super::parse_and_validate_event(&params).is_ok(),
                "task_type '{tt}' should be valid"
            );
        }
    }

    #[test]
    fn unit_invalid_iso_timestamp_rejected() {
        let params = make_params(
            "task_created",
            None,
            json!({"title": "x", "defer_until": "not-a-date"}),
        );
        let err = super::parse_and_validate_event(&params).unwrap_err();
        assert!(err.contains("Invalid timestamp"), "got: {err}");
        assert!(err.contains("defer_until"), "got: {err}");
    }

    #[test]
    fn unit_valid_iso_timestamp_normalized() {
        let params = make_params(
            "task_created",
            None,
            json!({"title": "x", "defer_until": "2026-12-01T00:00:00Z"}),
        );
        let validated = super::parse_and_validate_event(&params).unwrap();
        // After normalization the field should be an integer, not a string
        assert!(
            validated.payload["defer_until"].is_i64() || validated.payload["defer_until"].is_u64(),
            "defer_until should be normalized to integer, got: {}",
            validated.payload["defer_until"]
        );
    }

    #[test]
    fn unit_valid_event_type_preserved() {
        let params = make_params("status_changed", Some("t1"), json!({"new_status": "done"}));
        let validated = super::parse_and_validate_event(&params).unwrap();
        assert_eq!(
            validated.event_type,
            crate::tasks::events::EventType::StatusChanged
        );
        assert_eq!(validated.task_id_raw.as_deref(), Some("t1"));
        assert_eq!(validated.actor, "test-actor");
    }

    #[test]
    fn unit_no_task_id_for_non_create_passes_validation() {
        // parse_and_validate_event doesn't check for missing task_id on non-create events —
        // that's an I/O concern (execute() handles it).
        let params = make_params("status_changed", None, json!({"new_status": "done"}));
        assert!(
            super::parse_and_validate_event(&params).is_ok(),
            "missing task_id is not a pure-validation concern"
        );
    }
}
