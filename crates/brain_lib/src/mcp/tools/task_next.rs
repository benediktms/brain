use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::enrichment::enrich_task_summaries;
use crate::tasks::events::TaskType;

use super::{McpTool, inject_warnings, json_response, store_or_warn};

#[derive(Deserialize)]
struct Params {
    #[serde(default = "default_policy")]
    policy: String,
    #[serde(default = "default_k")]
    k: u64,
}

fn default_policy() -> String {
    "priority".into()
}
fn default_k() -> u64 {
    1
}

pub(super) struct TaskNext;

impl TaskNext {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let k = params.k.min(100) as usize;

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
        if params.policy == "due_date" {
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

        // Replace task_id with short form and strip description from each task
        for task_val in &mut results_json {
            if let Some(obj) = task_val.as_object_mut() {
                if let Some(tid) = obj
                    .get("task_id")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                {
                    let short = ctx.tasks.compact_id(&tid).unwrap_or_else(|_| tid.clone());
                    obj.insert("task_id".into(), json!(short));
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
                    .filter(|t| t.task_type == TaskType::Epic)
                    .map(|t| {
                        let short_id = ctx
                            .tasks
                            .compact_id(&t.task_id)
                            .unwrap_or_else(|_| t.task_id.clone());
                        json!({
                            "task_id": short_id,
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
        let mut warnings = Vec::new();
        let (ready_count, blocked_count) = store_or_warn(
            ctx.tasks.count_ready_blocked(),
            "count_ready_blocked",
            &mut warnings,
        );

        let mut response = json!({
            "results": groups_json,
            "ready_count": ready_count,
            "blocked_count": blocked_count,
        });

        inject_warnings(&mut response, warnings);
        json_response(&response)
    }
}

impl McpTool for TaskNext {
    fn name(&self) -> &'static str {
        "tasks.next"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get the next highest-priority ready task(s). Returns tasks with no unresolved dependencies, sorted by configurable policy. Includes dependency summary and linked notes for each task.".into(),
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
                    }
                }
            }),
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

    /// Helper: collect all tasks from the grouped results structure.
    fn collect_tasks(parsed: &Value) -> Vec<&Value> {
        parsed["results"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["tasks"].as_array().unwrap().iter())
            .collect()
    }

    async fn apply(registry: &ToolRegistry, ctx: &crate::mcp::McpContext, params: Value) {
        registry.dispatch("tasks.apply_event", params, ctx).await;
    }

    #[tokio::test]
    async fn test_returns_highest_priority() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        for (id, title, priority) in &[("t1", "Low", 4), ("t2", "High", 1), ("t3", "Medium", 2)] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title, "priority": priority }
            });
            apply(&registry, &ctx, p).await;
        }

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], "t2");
        assert_eq!(tasks[0]["priority"], 1);
        assert_eq!(parsed["ready_count"], 3);
        assert_eq!(parsed["blocked_count"], 0);
    }

    #[tokio::test]
    async fn test_excludes_blocked() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Blocker", "priority": 2}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Blocked", "priority": 1}})).await;
        apply(&registry, &ctx, json!({"event_type": "dependency_added", "task_id": "t2", "payload": {"depends_on_task_id": "t1"}})).await;

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], "t1");
        assert_eq!(parsed["ready_count"], 1);
        assert_eq!(parsed["blocked_count"], 1);
    }

    #[tokio::test]
    async fn test_k_multiple() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2"), ("t3", "Task 3")] {
            apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": id, "payload": {"title": title, "priority": 2}})).await;
        }

        let result = registry
            .dispatch("tasks.next", json!({ "k": 2 }), &ctx)
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 2);
    }

    #[tokio::test]
    async fn test_empty() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["results"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["ready_count"], 0);
    }

    #[tokio::test]
    async fn test_includes_dependency_summary() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Done task", "priority": 2}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Ready task", "priority": 1}})).await;
        apply(&registry, &ctx, json!({"event_type": "dependency_added", "task_id": "t2", "payload": {"depends_on_task_id": "t1"}})).await;
        apply(&registry, &ctx, json!({"event_type": "status_changed", "task_id": "t1", "payload": {"new_status": "done"}})).await;

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
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

    #[tokio::test]
    async fn test_includes_labels() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Labeled", "priority": 1}})).await;
        apply(
            &registry,
            &ctx,
            json!({"event_type": "label_added", "task_id": "t1", "payload": {"label": "critical"}}),
        )
        .await;

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        let task = &tasks[0];
        let labels = task["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "critical");
    }

    #[tokio::test]
    async fn test_excludes_epics() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "e1", "payload": {"title": "Epic", "priority": 1, "task_type": "epic"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Task", "priority": 2}})).await;

        let result = registry
            .dispatch("tasks.next", json!({ "k": 10 }), &ctx)
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        // Only the regular task, not the epic
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], "t1");
    }

    #[tokio::test]
    async fn test_groups_by_parent_epic() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "e1", "payload": {"title": "My Epic", "priority": 1, "task_type": "epic"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "c1", "payload": {"title": "Child 1", "priority": 2, "parent_task_id": "e1"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "c2", "payload": {"title": "Child 2", "priority": 2, "parent_task_id": "e1"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "o1", "payload": {"title": "Orphan", "priority": 2}})).await;

        let result = registry
            .dispatch("tasks.next", json!({ "k": 10 }), &ctx)
            .await;
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

    #[tokio::test]
    async fn test_omits_description() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Task", "description": "A long description", "priority": 1}})).await;

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert!(
            tasks[0].get("description").is_none(),
            "tasks_next should omit description to reduce context pollution"
        );
    }
}
