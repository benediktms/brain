use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;

use crate::utils::task_row_to_json;

pub(super) fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let policy = params
        .get("policy")
        .and_then(|v| v.as_str())
        .unwrap_or("priority");

    let k = params
        .get("k")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
        .min(100) as usize;

    // Get ready tasks (already sorted by priority policy)
    let ready_tasks = match ctx.tasks.list_ready() {
        Ok(tasks) => tasks,
        Err(e) => {
            error!(error = %e, "failed to list ready tasks");
            return ToolCallResult::error(format!("Failed to list ready tasks: {e}"));
        }
    };

    // Re-sort if due_date policy requested
    let mut tasks = ready_tasks;
    if policy == "due_date" {
        tasks.sort_by(|a, b| {
            // due_ts ASC NULLS LAST, then priority ASC, then task_id ASC
            let due_cmp = match (a.due_ts, b.due_ts) {
                (Some(a_ts), Some(b_ts)) => a_ts.cmp(&b_ts),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            due_cmp
                .then(a.priority.cmp(&b.priority))
                .then(a.task_id.cmp(&b.task_id))
        });
    }

    // Take top-k
    let selected: Vec<_> = tasks.into_iter().take(k).collect();

    // Build response with dependency summaries and note links
    let results_json: Vec<Value> = selected
        .iter()
        .map(|task| {
            let dep_summary = ctx
                .tasks
                .get_dependency_summary(&task.task_id)
                .unwrap_or_else(|_| crate::tasks::queries::DependencySummary {
                    total_deps: 0,
                    done_deps: 0,
                    blocking_task_ids: vec![],
                });

            let note_links = ctx
                .tasks
                .get_task_note_links(&task.task_id)
                .unwrap_or_default();

            let labels = ctx.tasks.get_task_labels(&task.task_id).unwrap_or_default();

            let linked_notes: Vec<Value> = note_links
                .iter()
                .map(|nl| {
                    json!({
                        "chunk_id": nl.chunk_id,
                        "file_path": nl.file_path,
                    })
                })
                .collect();

            let mut task_json = task_row_to_json(task, labels);
            if let Some(obj) = task_json.as_object_mut() {
                obj.insert(
                    "dependency_summary".into(),
                    json!({
                        "total_deps": dep_summary.total_deps,
                        "done_deps": dep_summary.done_deps,
                        "blocking_tasks": dep_summary.blocking_task_ids,
                    }),
                );
                obj.insert("linked_notes".into(), json!(linked_notes));
            }
            task_json
        })
        .collect();

    // Get aggregate counts
    let (ready_count, blocked_count) = ctx.tasks.count_ready_blocked().unwrap_or((0, 0));

    let response = json!({
        "results": results_json,
        "ready_count": ready_count,
        "blocked_count": blocked_count,
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_returns_highest_priority() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create tasks with different priorities
        for (id, title, priority) in &[("t1", "Low", 4), ("t2", "High", 1), ("t3", "Medium", 2)] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title, "priority": priority }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let results = parsed["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["task_id"], "t2");
        assert_eq!(results[0]["priority"], 1);
        assert_eq!(parsed["ready_count"], 3);
        assert_eq!(parsed["blocked_count"], 0);
    }

    #[test]
    fn test_excludes_blocked() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // t1 (P2), t2 (P1) depends on t1
        let p1 = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Blocker", "priority": 2 }
        });
        let p2 = json!({
            "event_type": "task_created",
            "task_id": "t2",
            "payload": { "title": "Blocked", "priority": 1 }
        });
        let dep = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p1, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p2, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let results = parsed["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["task_id"], "t1"); // t2 is blocked
        assert_eq!(parsed["ready_count"], 1);
        assert_eq!(parsed["blocked_count"], 1);
    }

    #[test]
    fn test_k_multiple() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2"), ("t3", "Task 3")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title, "priority": 2 }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({ "k": 2 }), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["results"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["results"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["ready_count"], 0);
    }

    #[test]
    fn test_includes_dependency_summary() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create t1 (done), t2 depends on t1 (now ready)
        let p1 = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Done task", "priority": 2 }
        });
        let p2 = json!({
            "event_type": "task_created",
            "task_id": "t2",
            "payload": { "title": "Ready task", "priority": 1 }
        });
        let dep = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        let done = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "done" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p1, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p2, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &done, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let task = &parsed["results"][0];
        assert_eq!(task["task_id"], "t2");
        assert_eq!(task["dependency_summary"]["total_deps"], 1);
        assert_eq!(task["dependency_summary"]["done_deps"], 1);
        assert_eq!(
            task["dependency_summary"]["blocking_tasks"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn test_includes_labels() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Labeled", "priority": 1 }
        });
        let label = json!({
            "event_type": "label_added",
            "task_id": "t1",
            "payload": { "label": "critical" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &label, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let task = &parsed["results"][0];
        let labels = task["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "critical");
    }
}
