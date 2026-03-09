use std::collections::HashMap;

use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::tasks::enrichment::enrich_task_summaries;

pub(super) fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    use super::{opt_str, opt_u64};
    let policy = opt_str(params, "policy", "priority");
    let k = opt_u64(params, "k", 1).min(100) as usize;

    // Get ready actionable tasks (excludes epics)
    let ready_tasks = match ctx.tasks.list_ready_actionable() {
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

    // Build enriched task JSON with batch label fetching
    let mut results_json = enrich_task_summaries(&ctx.tasks, &selected);

    // Add short_id and strip description from each task
    for task_val in &mut results_json {
        if let Some(obj) = task_val.as_object_mut() {
            if let Some(tid) = obj
                .get("task_id")
                .and_then(|v| v.as_str())
                .map(String::from)
            {
                let short = ctx
                    .tasks
                    .shortest_unique_prefix(&tid)
                    .unwrap_or_else(|_| tid.clone());
                obj.insert("short_id".into(), json!(short));
            }
            obj.remove("description");
        }
    }

    // Collect unique parent_task_ids that are epics
    let mut epic_cache: HashMap<String, Option<Value>> = HashMap::new();
    for task in &selected {
        if let Some(ref parent_id) = task.parent_task_id {
            if epic_cache.contains_key(parent_id) {
                continue;
            }
            let epic_val = ctx
                .tasks
                .get_task(parent_id)
                .ok()
                .flatten()
                .filter(|t| t.task_type == "epic")
                .map(|t| {
                    let short_id = ctx
                        .tasks
                        .shortest_unique_prefix(&t.task_id)
                        .unwrap_or_else(|_| t.task_id.clone());
                    json!({
                        "short_id": short_id,
                        "task_id": t.task_id,
                        "title": t.title,
                    })
                });
            epic_cache.insert(parent_id.clone(), epic_val);
        }
    }

    // Group tasks by parent epic, preserving selection order
    let mut groups: Vec<(Option<Value>, Vec<Value>)> = Vec::new();
    let mut group_index: HashMap<Option<String>, usize> = HashMap::new();

    for (task, task_json) in selected.iter().zip(results_json) {
        // Determine the epic key for this task
        let epic_key: Option<String> = task
            .parent_task_id
            .as_ref()
            .and_then(|pid| epic_cache.get(pid))
            .and_then(|v| v.as_ref())
            .map(|_| task.parent_task_id.clone())
            .unwrap_or(None);

        if let Some(&idx) = group_index.get(&epic_key) {
            groups[idx].1.push(task_json);
        } else {
            let epic_val: Option<Value> = epic_key
                .as_ref()
                .and_then(|pid| epic_cache.get(pid))
                .and_then(|v| v.clone());
            let idx = groups.len();
            group_index.insert(epic_key, idx);
            groups.push((epic_val, vec![task_json]));
        }
    }

    let groups_json: Vec<Value> = groups
        .into_iter()
        .map(|(epic, tasks)| {
            json!({
                "epic": epic,
                "tasks": tasks,
            })
        })
        .collect();

    // Get aggregate counts
    let (ready_count, blocked_count) = ctx.tasks.count_ready_blocked().unwrap_or((0, 0));

    let response = json!({
        "results": groups_json,
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

    /// Helper: collect all tasks from the grouped results structure.
    fn collect_tasks(parsed: &Value) -> Vec<&Value> {
        parsed["results"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["tasks"].as_array().unwrap().iter())
            .collect()
    }

    #[test]
    fn test_returns_highest_priority() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

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
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], "t2");
        assert_eq!(tasks[0]["priority"], 1);
        assert_eq!(parsed["ready_count"], 3);
        assert_eq!(parsed["blocked_count"], 0);
    }

    #[test]
    fn test_excludes_blocked() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

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
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], "t1");
        assert_eq!(parsed["ready_count"], 1);
        assert_eq!(parsed["blocked_count"], 1);
    }

    #[test]
    fn test_k_multiple() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

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
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn test_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["results"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["ready_count"], 0);
    }

    #[test]
    fn test_includes_dependency_summary() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

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
        let tasks = collect_tasks(&parsed);
        let task = &tasks[0];
        assert_eq!(task["task_id"], "t2");
        assert_eq!(task["dependency_summary"]["total_deps"], 1);
        assert_eq!(task["dependency_summary"]["done_deps"], 1);
        assert_eq!(
            task["dependency_summary"]["blocking_task_ids"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn test_includes_labels() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

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
        let tasks = collect_tasks(&parsed);
        let task = &tasks[0];
        let labels = task["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "critical");
    }

    #[test]
    fn test_excludes_epics() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        // Create an epic and a regular task
        let epic = json!({
            "event_type": "task_created",
            "task_id": "e1",
            "payload": { "title": "Epic", "priority": 1, "task_type": "epic" }
        });
        let task = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Task", "priority": 2 }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &epic, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &task, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({ "k": 10 }), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        // Only the regular task, not the epic
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], "t1");
    }

    #[test]
    fn test_groups_by_parent_epic() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        // Create an epic parent
        let epic = json!({
            "event_type": "task_created",
            "task_id": "e1",
            "payload": { "title": "My Epic", "priority": 1, "task_type": "epic" }
        });
        // Create child tasks under the epic
        let child1 = json!({
            "event_type": "task_created",
            "task_id": "c1",
            "payload": { "title": "Child 1", "priority": 2, "parent_task_id": "e1" }
        });
        let child2 = json!({
            "event_type": "task_created",
            "task_id": "c2",
            "payload": { "title": "Child 2", "priority": 2, "parent_task_id": "e1" }
        });
        // Create an orphan task (no parent epic)
        let orphan = json!({
            "event_type": "task_created",
            "task_id": "o1",
            "payload": { "title": "Orphan", "priority": 2 }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &epic, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &child1, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &child2, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &orphan, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({ "k": 10 }), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let groups = parsed["results"].as_array().unwrap();

        // Should have 2 groups: one for the epic's children, one for the orphan
        assert_eq!(groups.len(), 2);

        // Find the group with the epic
        let epic_group = groups.iter().find(|g| !g["epic"].is_null()).unwrap();
        assert_eq!(epic_group["epic"]["title"], "My Epic");
        assert_eq!(epic_group["tasks"].as_array().unwrap().len(), 2);

        // Find the null-epic group
        let null_group = groups.iter().find(|g| g["epic"].is_null()).unwrap();
        assert_eq!(null_group["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(null_group["tasks"][0]["task_id"], "o1");
    }

    #[test]
    fn test_omits_description() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Task", "description": "A long description", "priority": 1 }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert!(
            tasks[0].get("description").is_none(),
            "tasks_next should omit description to reduce context pollution"
        );
    }
}
