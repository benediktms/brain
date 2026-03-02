use serde_json::{Value, json};
use tracing::warn;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::tasks::events::{EventType, TaskCreatedPayload, TaskEvent, TaskStatus, new_event_id};

use crate::utils::{parse_timestamp, task_row_to_json};

pub(super) fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    // Parse event_type
    let event_type_str = match params.get("event_type").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return ToolCallResult::error("Missing required parameter: event_type"),
    };

    let event_type: EventType = match serde_json::from_value(json!(event_type_str)) {
        Ok(et) => et,
        Err(_) => {
            return ToolCallResult::error(format!(
                "Invalid event_type: '{event_type_str}'. Must be one of: task_created, \
                 task_updated, status_changed, dependency_added, dependency_removed, \
                 note_linked, note_unlinked, label_added, label_removed, comment_added, \
                 parent_set"
            ));
        }
    };

    // Parse payload
    let payload = match params.get("payload") {
        Some(p) if p.is_object() => p.clone(),
        Some(_) => return ToolCallResult::error("Parameter 'payload' must be an object"),
        None => return ToolCallResult::error("Missing required parameter: payload"),
    };

    // Parse task_id: auto-generate for task_created if not provided
    let task_id = match params.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if id.len() > 256 => {
            return ToolCallResult::error("task_id exceeds maximum length of 256 characters");
        }
        Some(id) => id.to_string(),
        None => {
            if event_type == EventType::TaskCreated {
                new_event_id() // UUID v7 as task ID
            } else {
                return ToolCallResult::error(
                    "Missing required parameter: task_id (required for all event types except task_created)",
                );
            }
        }
    };

    let actor_str = params
        .get("actor")
        .and_then(|v| v.as_str())
        .unwrap_or("mcp");
    if actor_str.len() > 256 {
        return ToolCallResult::error("actor exceeds maximum length of 256 characters");
    }
    let actor = actor_str.to_string();

    // Normalize timestamp fields from ISO 8601 strings to i64 Unix seconds
    let payload = {
        let mut p = payload;
        for field in &["defer_until", "due_ts"] {
            if let Some(val) = p.get(*field).filter(|v| v.is_string()) {
                match parse_timestamp(val) {
                    Some(ts) => p[*field] = json!(ts),
                    None => {
                        return ToolCallResult::error(format!(
                            "Invalid timestamp for '{field}': expected ISO 8601 string or integer"
                        ));
                    }
                }
            }
        }
        p
    };

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

    let event = TaskEvent::from_raw(task_id.clone(), actor, event_type.clone(), payload);

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
        if new_status == TaskStatus::Done.as_ref() || new_status == TaskStatus::Cancelled.as_ref() {
            ctx.tasks.list_newly_unblocked(&task_id).unwrap_or_default()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    let response = json!({
        "event_id": event.event_id,
        "task_id": task_id,
        "task": task_json,
        "unblocked_task_ids": unblocked_task_ids,
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_create() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "test-1",
            "payload": { "title": "My first task", "priority": 2 }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
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

    #[test]
    fn test_auto_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "payload": { "title": "Auto ID task" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert!(parsed["task_id"].is_string());
        assert!(!parsed["task_id"].as_str().unwrap().is_empty());
        assert_eq!(parsed["task"]["title"], "Auto ID task");
        assert_eq!(parsed["task"]["priority"], 4); // default
    }

    #[test]
    fn test_status_change() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create task first
        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Task" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));

        // Change status
        let update = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "in_progress" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &update, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["status"], "in_progress");
    }

    #[test]
    fn test_unblocked() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create two tasks, t2 depends on t1
        for (id, title) in &[("t1", "Blocker"), ("t2", "Blocked")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        let dep = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep, &ctx));

        // Complete t1 — t2 should be unblocked
        let done = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "done" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &done, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let unblocked = parsed["unblocked_task_ids"].as_array().unwrap();
        assert_eq!(unblocked.len(), 1);
        assert_eq!(unblocked[0], "t2");
    }

    #[test]
    fn test_cycle_rejected() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create two tasks
        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        // t1 depends on t2
        let dep1 = json!({
            "event_type": "dependency_added",
            "task_id": "t1",
            "payload": { "depends_on_task_id": "t2" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep1, &ctx));

        // t2 depends on t1 — cycle!
        let dep2 = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &dep2, &ctx));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("cycle"));
    }

    #[test]
    fn test_missing_event_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({ "payload": { "title": "No event type" } });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn test_invalid_event_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "bogus_event",
            "payload": { "title": "Bad type" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid event_type"));
    }

    #[test]
    fn test_missing_task_id_for_update() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "status_changed",
            "payload": { "new_status": "done" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("task_id"));
    }

    #[test]
    fn test_with_type_and_assignee() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

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
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["task_type"], "bug");
        assert_eq!(parsed["task"]["assignee"], "alice");
    }

    #[test]
    fn test_labels() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create task
        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Labeled task" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));

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
        rt.block_on(dispatch_tool_call("tasks.apply_event", &add1, &ctx));
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &add2, &ctx));
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
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &rm, &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let labels = parsed["task"]["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "backend");
    }

    #[test]
    fn test_comment() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Commented task" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));

        let comment = json!({
            "event_type": "comment_added",
            "task_id": "t1",
            "actor": "bob",
            "payload": { "body": "This needs review" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &comment, &ctx));
        assert!(result.is_error.is_none());

        // Verify comment stored by fetching via TaskStore
        let comments = ctx.tasks.get_task_comments("t1").unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].body, "This needs review");
        assert_eq!(comments[0].author, "bob");
    }

    #[test]
    fn test_default_task_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "No explicit type" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["task_type"], "task");
    }

    #[test]
    fn test_iso8601_defer_until() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Deferred task",
                "defer_until": "2026-12-01T00:00:00Z"
            }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none(), "should succeed");

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Response should be ISO 8601 string
        assert_eq!(parsed["task"]["defer_until"], "2026-12-01T00:00:00+00:00");

        // Verify stored as i64 internally
        let row = ctx.tasks.get_task("t1").unwrap().unwrap();
        assert_eq!(row.defer_until, Some(1796083200));
    }

    #[test]
    fn test_integer_defer_until_backward_compat() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Deferred task int",
                "defer_until": 1796083200
            }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none(), "should succeed");

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Response should still be ISO 8601 string
        assert_eq!(parsed["task"]["defer_until"], "2026-12-01T00:00:00+00:00");

        let row = ctx.tasks.get_task("t1").unwrap().unwrap();
        assert_eq!(row.defer_until, Some(1796083200));
    }

    #[test]
    fn test_iso8601_due_ts() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Due task",
                "due_ts": "2026-06-15T12:00:00Z"
            }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["due_ts"], "2026-06-15T12:00:00+00:00");
    }

    #[test]
    fn test_timestamps_returned_as_iso_strings() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Timestamp check" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
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

    #[test]
    fn test_invalid_iso_timestamp_rejected() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Bad timestamp",
                "defer_until": "not-a-date"
            }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid timestamp"));
    }
}
