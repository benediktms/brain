use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::enrichment::enrich_task_summaries;
use crate::tasks::events::TaskType;
use crate::tasks::queries::TaskRow;
use crate::uri::SynapseUri;

use super::scope::{BRAINS_PARAM_DESCRIPTION, BrainRef, resolve_scope};
use super::{McpTool, inject_warnings, json_response, store_or_warn};

#[derive(Deserialize)]
struct Params {
    #[serde(default = "default_policy")]
    policy: String,
    #[serde(default = "default_k")]
    k: u64,
    /// Brains to query. See `BRAINS_PARAM_DESCRIPTION`.
    #[serde(default)]
    brains: Option<Vec<String>>,
}

fn default_policy() -> String {
    "priority".into()
}
fn default_k() -> u64 {
    1
}

pub(super) struct TaskNext;

/// (Brain that owns the task, the task row).
type BrainTask = (BrainRef, TaskRow);

impl TaskNext {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let scope = match resolve_scope(ctx, params.brains.as_deref()) {
            Ok(s) => s,
            Err(err) => return err,
        };

        // Build per-brain ctxs and gather every ready task across the scope.
        let mut per_brain: Vec<(BrainRef, Arc<McpContext>)> = Vec::new();
        for brain_ref in scope.brains() {
            let scoped_ctx = match ctx.with_brain_id(&brain_ref.brain_id, &brain_ref.brain_name) {
                Ok(c) => c,
                Err(e) => {
                    error!(brain = %brain_ref.brain_name, error = %e, "failed to scope ctx");
                    return ToolCallResult::error(format!(
                        "Failed to scope to brain '{}': {e}",
                        brain_ref.brain_name
                    ));
                }
            };
            per_brain.push((brain_ref.clone(), scoped_ctx));
        }

        let mut all_ready: Vec<BrainTask> = Vec::new();
        let mut warnings = Vec::new();
        let mut total_ready: usize = 0;
        let mut total_blocked: usize = 0;

        for (brain_ref, scoped_ctx) in &per_brain {
            let ready = match scoped_ctx.stores.tasks.list_ready_actionable() {
                Ok(t) => t,
                Err(e) => {
                    error!(brain = %brain_ref.brain_name, error = %e, "failed to list ready tasks");
                    return ToolCallResult::error(format!(
                        "Failed to list ready tasks for brain '{}': {e}",
                        brain_ref.brain_name
                    ));
                }
            };
            for t in ready {
                all_ready.push((brain_ref.clone(), t));
            }
            let (r, b) = store_or_warn(
                scoped_ctx.stores.tasks.count_ready_blocked(),
                "count_ready_blocked",
                &mut warnings,
            );
            total_ready += r;
            total_blocked += b;
        }

        // Global sort across the merged ready set.
        let status_ord = |status: &str| -> u8 { if status == "in_progress" { 0 } else { 1 } };
        let policy_due_date = params.policy == "due_date";
        all_ready.sort_by(|(_, a), (_, b)| {
            let s = status_ord(&a.status).cmp(&status_ord(&b.status));
            if policy_due_date {
                let due_cmp = match (a.due_ts, b.due_ts) {
                    (Some(a_ts), Some(b_ts)) => a_ts.cmp(&b_ts),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                s.then(due_cmp)
                    .then(a.priority.cmp(&b.priority))
                    .then(a.task_id.cmp(&b.task_id))
            } else {
                s.then(a.priority.cmp(&b.priority))
                    .then(a.task_id.cmp(&b.task_id))
            }
        });

        let k = params.k.min(100) as usize;
        let selected: Vec<BrainTask> = all_ready.into_iter().take(k).collect();

        // Build groups response. For Single scope, preserve existing per-epic
        // grouping. For Federated, group by brain (epic IDs aren't comparable
        // across brains), still tagging each task with `brain_name`.
        let groups_json = if scope.is_federated() {
            build_federated_groups(&selected, &per_brain)
        } else {
            // All selected tasks share the single brain in `per_brain[0]`.
            let (single_brain, single_ctx) = &per_brain[0];
            build_single_groups(&selected, single_brain, single_ctx)
        };

        let mut response = json!({
            "results": groups_json,
            "ready_count": total_ready,
            "blocked_count": total_blocked,
        });
        if scope.is_federated() {
            let brain_names: Vec<&str> = per_brain
                .iter()
                .map(|(b, _)| b.brain_name.as_str())
                .collect();
            if let Some(obj) = response.as_object_mut() {
                obj.insert("brains".into(), json!(brain_names));
            }
        }

        inject_warnings(&mut response, warnings);
        json_response(&response)
    }
}

/// Build the per-epic groups for a Single-scope response (existing shape).
fn build_single_groups(
    selected: &[BrainTask],
    brain_ref: &BrainRef,
    ctx: &Arc<McpContext>,
) -> Vec<Value> {
    let task_rows: Vec<TaskRow> = selected.iter().map(|(_, t)| t.clone()).collect();
    let mut tasks_json = enrich_task_summaries(&ctx.stores.tasks, &task_rows);
    decorate_tasks(&mut tasks_json, &brain_ref.brain_name, false);

    // Resolve epic stubs for each unique parent_task_id.
    let mut epic_cache: HashMap<String, Option<Value>> = HashMap::new();
    for (_, task) in selected {
        if let Some(ref parent_id) = task.parent_task_id {
            if epic_cache.contains_key(parent_id) {
                continue;
            }
            let epic_val = ctx
                .stores
                .tasks
                .get_task(parent_id)
                .ok()
                .flatten()
                .filter(|t| t.task_type == TaskType::Epic)
                .map(|t| {
                    let short = ctx
                        .stores
                        .tasks
                        .compact_id(&t.task_id)
                        .unwrap_or(t.task_id.clone());
                    json!({
                        "task_id": short,
                        "title": t.title,
                    })
                });
            epic_cache.insert(parent_id.clone(), epic_val);
        }
    }

    let mut groups: Vec<(Option<Value>, Vec<Value>)> = Vec::new();
    let mut group_index: HashMap<Option<String>, usize> = HashMap::new();
    for ((_, task), task_json) in selected.iter().zip(tasks_json) {
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

    groups
        .into_iter()
        .map(|(epic, tasks)| {
            json!({
                "epic": epic,
                "tasks": tasks,
            })
        })
        .collect()
}

/// Build per-brain groups for a Federated-scope response.
/// Epic grouping is dropped because epic IDs aren't comparable across brains.
fn build_federated_groups(
    selected: &[BrainTask],
    per_brain: &[(BrainRef, Arc<McpContext>)],
) -> Vec<Value> {
    let mut by_brain: HashMap<String, Vec<TaskRow>> = HashMap::new();
    for (brain_ref, task) in selected {
        by_brain
            .entry(brain_ref.brain_name.clone())
            .or_default()
            .push(task.clone());
    }

    // Preserve the order brains appear in `per_brain` (the registry's name order).
    let mut groups: Vec<Value> = Vec::new();
    for (brain_ref, ctx) in per_brain {
        let Some(rows) = by_brain.remove(&brain_ref.brain_name) else {
            continue;
        };
        let mut tasks_json = enrich_task_summaries(&ctx.stores.tasks, &rows);
        decorate_tasks(&mut tasks_json, &brain_ref.brain_name, true);
        groups.push(json!({
            "brain": brain_ref.brain_name,
            "tasks": tasks_json,
        }));
    }
    groups
}

/// Add `uri` (and optional `brain` fields) to each enriched task and strip the
/// description to keep tasks.next responses concise.
fn decorate_tasks(tasks_json: &mut [Value], brain_name: &str, federated: bool) {
    for task_val in tasks_json.iter_mut() {
        if let Some(obj) = task_val.as_object_mut() {
            if let Some(tid) = obj.get("task_id").and_then(|v| v.as_str()) {
                let uri = SynapseUri::for_task(brain_name, tid).to_string();
                obj.insert("uri".into(), json!(uri));
            }
            if federated {
                obj.insert("brain".into(), json!(brain_name));
            }
            obj.remove("description");
        }
    }
}

impl McpTool for TaskNext {
    fn name(&self) -> &'static str {
        "tasks.next"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get the next highest-priority ready task(s). Returns tasks with no unresolved dependencies, sorted by configurable policy. Includes dependency summary and linked notes for each task. Supports cross-brain queries via the `brains` parameter.".into(),
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
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": BRAINS_PARAM_DESCRIPTION
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
    use crate::tasks::TaskStore;
    use crate::tasks::events::{TaskCreatedPayload, TaskEvent, TaskStatus};
    use brain_persistence::db::schema::BrainUpsert;

    /// Compute the expected compact ID for a task created via in_memory stores.
    /// `create_test_context` registers brain "test-brain" (id "test-brain-id")
    /// with prefix "TST", so newly-created tasks get display_id = first 3 hex
    /// chars of blake3(task_id) and compact IDs render as "tst-{hash}".
    fn compact_id_for(task_id: &str) -> String {
        let hex = blake3::hash(task_id.as_bytes()).to_hex().to_string();
        format!("tst-{}", &hex[..3])
    }

    /// Helper: collect all tasks from the grouped results structure.
    fn collect_tasks(parsed: &Value) -> Vec<&Value> {
        parsed["results"]
            .as_array()
            .expect("checked in test assertions")
            .iter()
            .flat_map(|group| {
                group["tasks"]
                    .as_array()
                    .expect("checked in test assertions")
                    .iter()
            })
            .collect()
    }

    async fn apply(registry: &ToolRegistry, ctx: &crate::mcp::McpContext, params: Value) {
        registry.dispatch("tasks.apply_event", params, ctx).await;
    }

    /// Set up an ambient ctx scoped to brain A, with brain B also registered as active.
    /// Returns (tmp dir guard, ctx scoped to brain A).
    async fn ctx_with_two_brains() -> (tempfile::TempDir, std::sync::Arc<crate::mcp::McpContext>) {
        let (tmp, base_ctx) = create_test_context().await;
        for (id, name, prefix) in [
            ("brain-a-id", "brain-a", "AAA"),
            ("brain-b-id", "brain-b", "BBB"),
        ] {
            base_ctx
                .stores
                .db_for_tests()
                .upsert_brain(&BrainUpsert {
                    brain_id: id,
                    name,
                    prefix,
                    roots_json: "[]",
                    notes_json: "[]",
                    aliases_json: "[]",
                    archived: false,
                })
                .unwrap();
        }
        let ctx = base_ctx.with_brain_id("brain-a-id", "brain-a").unwrap();
        (tmp, ctx)
    }

    fn write_task_to(
        brain_id: &str,
        brain_name: &str,
        db: &brain_persistence::db::Db,
        id: &str,
        title: &str,
        priority: i32,
    ) {
        let store = TaskStore::with_brain_id(db.clone(), brain_id, brain_name).unwrap();
        store
            .append(&TaskEvent::from_payload(
                id,
                "test",
                TaskCreatedPayload {
                    title: title.into(),
                    description: None,
                    priority,
                    status: TaskStatus::Open,
                    due_ts: None,
                    task_type: None,
                    assignee: None,
                    defer_until: None,
                    parent_task_id: None,
                    display_id: None,
                },
            ))
            .unwrap();
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

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], compact_id_for("t2"));
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
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], compact_id_for("t1"));
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
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 2);
    }

    #[tokio::test]
    async fn test_empty() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(
            parsed["results"]
                .as_array()
                .expect("checked in test assertions")
                .len(),
            0
        );
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
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        let task = &tasks[0];
        assert_eq!(task["task_id"], compact_id_for("t2"));
        assert_eq!(task["dependency_summary"]["total_deps"], 1);
        assert_eq!(task["dependency_summary"]["done_deps"], 1);
        assert_eq!(
            task["dependency_summary"]["blocking_task_ids"]
                .as_array()
                .expect("checked in test assertions")
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
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        let task = &tasks[0];
        let labels = task["labels"]
            .as_array()
            .expect("checked in test assertions");
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
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        // Only the regular task, not the epic
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], compact_id_for("t1"));
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
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let groups = parsed["results"]
            .as_array()
            .expect("checked in test assertions");

        // Should have 2 groups: one for the epic's children, one for the orphan
        assert_eq!(groups.len(), 2);

        // Find the group with the epic
        let epic_group = groups
            .iter()
            .find(|g| !g["epic"].is_null())
            .expect("checked in test assertions");
        assert_eq!(epic_group["epic"]["title"], "My Epic");
        assert_eq!(
            epic_group["tasks"]
                .as_array()
                .expect("checked in test assertions")
                .len(),
            2
        );

        // Find the null-epic group
        let null_group = groups
            .iter()
            .find(|g| g["epic"].is_null())
            .expect("checked in test assertions");
        assert_eq!(
            null_group["tasks"]
                .as_array()
                .expect("checked in test assertions")
                .len(),
            1
        );
        assert_eq!(null_group["tasks"][0]["task_id"], compact_id_for("o1"));
    }

    #[tokio::test]
    async fn test_in_progress_before_open() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Two tasks at the same priority — t1 open, t2 in_progress
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Open Task", "priority": 2}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "In Progress Task", "priority": 2}})).await;
        apply(&registry, &ctx, json!({"event_type": "status_changed", "task_id": "t2", "payload": {"new_status": "in_progress"}})).await;

        let result = registry
            .dispatch("tasks.next", json!({ "k": 2 }), &ctx)
            .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 2);
        assert_eq!(
            tasks[0]["task_id"],
            compact_id_for("t2"),
            "in_progress task must appear before open task at the same priority"
        );
        assert_eq!(tasks[1]["task_id"], compact_id_for("t1"));
    }

    #[tokio::test]
    async fn test_in_progress_before_open_cross_priority() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // t1: priority 3, in_progress — t2: priority 1 (higher), open
        // in_progress must dominate regardless of priority
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Low Priority In Progress", "priority": 3}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "High Priority Open", "priority": 1}})).await;
        apply(&registry, &ctx, json!({"event_type": "status_changed", "task_id": "t1", "payload": {"new_status": "in_progress"}})).await;

        let result = registry
            .dispatch("tasks.next", json!({ "k": 2 }), &ctx)
            .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 2);
        assert_eq!(
            tasks[0]["task_id"],
            compact_id_for("t1"),
            "in_progress task (P3) must appear before open task (P1) — status dominates priority"
        );
        assert_eq!(tasks[1]["task_id"], compact_id_for("t2"));
    }

    #[tokio::test]
    async fn test_omits_description() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Task", "description": "A long description", "priority": 1}})).await;

        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert!(
            tasks[0].get("description").is_none(),
            "tasks_next should omit description to reduce context pollution"
        );
    }

    /// Regression for brn-a3e: explicit `brains: ["B"]` must scope `tasks.next`
    /// to brain B's rows even when ambient ctx is brain A.
    #[tokio::test]
    async fn test_next_with_explicit_brain_returns_target_brain_tasks() {
        let (_dir, ctx) = ctx_with_two_brains().await;
        let registry = ToolRegistry::new();

        // Insert one task into brain B.
        write_task_to(
            "brain-b-id",
            "brain-b",
            ctx.stores.db_for_tests(),
            "task-in-b",
            "Task in brain B",
            1,
        );

        // Default scope (brain A) returns nothing.
        let result = registry.dispatch("tasks.next", json!({}), &ctx).await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(
            collect_tasks(&parsed).len(),
            0,
            "ambient brain-a should not see brain-b tasks: {parsed}"
        );

        // brains=["brain-b"] surfaces the brain-b task.
        let result = registry
            .dispatch("tasks.next", json!({ "brains": ["brain-b"] }), &ctx)
            .await;
        assert!(
            result.is_error.is_none(),
            "tasks.next with brains param should not error: {:?}",
            result.content[0].text
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        assert_eq!(
            tasks.len(),
            1,
            "tasks.next should return the brain-b task: {parsed}"
        );
        assert_eq!(tasks[0]["title"], "Task in brain B");
    }

    /// Federated query must merge ready tasks from every brain in scope.
    #[tokio::test]
    async fn test_next_federated_all_returns_tasks_from_every_brain() {
        let (_dir, ctx) = ctx_with_two_brains().await;
        let registry = ToolRegistry::new();

        write_task_to(
            "brain-a-id",
            "brain-a",
            ctx.stores.db_for_tests(),
            "task-in-a",
            "Task in brain A",
            2,
        );
        write_task_to(
            "brain-b-id",
            "brain-b",
            ctx.stores.db_for_tests(),
            "task-in-b",
            "Task in brain B",
            1,
        );

        let result = registry
            .dispatch("tasks.next", json!({ "brains": ["all"], "k": 10 }), &ctx)
            .await;
        assert!(
            result.is_error.is_none(),
            "federated tasks.next should not error: {:?}",
            result.content[0].text
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        let titles: Vec<&str> = tasks.iter().map(|t| t["title"].as_str().unwrap()).collect();
        assert!(
            titles.contains(&"Task in brain A"),
            "should include brain-a task: {parsed}"
        );
        assert!(
            titles.contains(&"Task in brain B"),
            "should include brain-b task: {parsed}"
        );
        // Each task has a brain field in federated mode.
        for t in &tasks {
            assert!(
                t.get("brain").is_some(),
                "federated task must include brain field: {t}"
            );
        }
        // Top-level brains array names the queried scope.
        let brains_arr = parsed["brains"]
            .as_array()
            .expect("federated has brains array");
        let brain_names: Vec<&str> = brains_arr.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(brain_names.contains(&"brain-a"));
        assert!(brain_names.contains(&"brain-b"));
    }

    /// Federated top-k merges across brains and sorts by priority globally.
    #[tokio::test]
    async fn test_next_federated_global_top_k_by_priority() {
        let (_dir, ctx) = ctx_with_two_brains().await;
        let registry = ToolRegistry::new();

        // brain-a has a low-priority task; brain-b has a high-priority task.
        write_task_to(
            "brain-a-id",
            "brain-a",
            ctx.stores.db_for_tests(),
            "low-in-a",
            "Low in A",
            4,
        );
        write_task_to(
            "brain-b-id",
            "brain-b",
            ctx.stores.db_for_tests(),
            "high-in-b",
            "High in B",
            1,
        );

        // k=1 across both brains should return ONLY the high-priority task.
        let result = registry
            .dispatch("tasks.next", json!({ "brains": ["all"], "k": 1 }), &ctx)
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let tasks = collect_tasks(&parsed);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["title"], "High in B");
    }
}
