use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::TaskStore;
use crate::tasks::enrichment::enrich_task_list;
use crate::tasks::events::TaskType;
use crate::tasks::queries::{TaskFilter, apply_filters};
use crate::uri::SynapseUri;

use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StatusFilter {
    Open,
    Ready,
    Blocked,
    Done,
    InProgress,
    Cancelled,
}

impl fmt::Display for StatusFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open => write!(f, "open"),
            Self::Ready => write!(f, "ready"),
            Self::Blocked => write!(f, "blocked"),
            Self::Done => write!(f, "done"),
            Self::InProgress => write!(f, "in_progress"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

fn default_status() -> StatusFilter {
    StatusFilter::Open
}
fn default_limit() -> u64 {
    50
}

#[derive(Deserialize)]
struct Params {
    #[serde(default = "default_status")]
    status: StatusFilter,
    task_ids: Option<Vec<String>>,
    priority: Option<i32>,
    task_type: Option<String>,
    assignee: Option<String>,
    label: Option<String>,
    search: Option<String>,
    #[serde(default)]
    include_description: bool,
    #[serde(default = "default_limit")]
    limit: u64,
    brain: Option<String>,
}

pub(super) struct TaskList;

impl TaskList {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // Remote brain path
        if let Some(ref brain) = params.brain {
            let (bid, remote_brain_name) = match ctx.resolve_brain_id(brain) {
                Ok(r) => r,
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to resolve brain: {e}"));
                }
            };
            let remote_tasks = match ctx.stores.with_brain_id(&bid, &remote_brain_name) {
                Ok(s) => s.tasks,
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to open brain stores: {e}"));
                }
            };
            let mut result = Self::execute_with_store(params, &remote_tasks, &remote_brain_name);
            // Inject brain name into response
            if let Ok(ref mut val) =
                serde_json::from_str::<serde_json::Value>(&result.content[0].text)
            {
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("brain".into(), json!(remote_brain_name));
                }
                result.content[0].text = val.to_string();
            }
            return result;
        }

        Self::execute_with_store(params, &ctx.stores.tasks, ctx.brain_name())
    }

    fn execute_with_store(params: Params, store: &TaskStore, brain_name: &str) -> ToolCallResult {
        let limit = params.limit as usize;

        // If task_ids provided, fetch those specifically
        if let Some(ref ids) = params.task_ids {
            let task_ids: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
            return Self::handle_batch(
                &task_ids,
                params.include_description,
                limit,
                store,
                brain_name,
            );
        }

        // Parse per-field filters
        let task_type = match params.task_type {
            Some(ref s) => match s.parse::<TaskType>() {
                Ok(tt) => Some(tt),
                Err(e) => return ToolCallResult::error(e),
            },
            None => None,
        };
        let filter = TaskFilter {
            priority: params.priority,
            task_type,
            assignee: params.assignee,
            label: params.label,
            search: params.search,
        };

        // FTS pre-filter: get matching task_ids
        let fts_ids = if let Some(ref query) = filter.search {
            match store.search_fts(query, 1000) {
                Ok(ids) => Some(ids.into_iter().collect::<HashSet<String>>()),
                Err(e) => {
                    error!(error = %e, "FTS search failed");
                    return ToolCallResult::error(format!("Full-text search failed: {e}"));
                }
            }
        } else {
            None
        };

        let status = params.status;
        let tasks = match status {
            StatusFilter::Open => store.list_open(),
            StatusFilter::Ready => store.list_ready(),
            StatusFilter::Blocked => store.list_blocked(),
            StatusFilter::Done => store.list_done(),
            StatusFilter::InProgress => store.list_in_progress(),
            StatusFilter::Cancelled => store.list_cancelled(),
        };

        let tasks = match tasks {
            Ok(t) => t,
            Err(e) => {
                error!(error = %e, %status, "failed to list tasks");
                return ToolCallResult::error(format!("Failed to list tasks: {e}"));
            }
        };

        // Apply per-field filters if any are set
        let tasks = if !filter.is_empty() {
            // Batch-fetch labels if label filter is active
            let labels_map = if filter.label.is_some() {
                let task_ids: Vec<&str> = tasks.iter().map(|t| t.task_id.as_str()).collect();
                match store.get_labels_for_tasks(&task_ids) {
                    Ok(map) => Some(map),
                    Err(e) => {
                        error!(error = %e, "failed to fetch labels for list filtering");
                        return ToolCallResult::error(format!(
                            "Failed to fetch labels for filtering: {e}"
                        ));
                    }
                }
            } else {
                None
            };

            apply_filters(tasks, &filter, fts_ids.as_ref(), labels_map.as_ref())
        } else {
            tasks
        };

        Self::build_response(&tasks, params.include_description, limit, store, brain_name)
    }

    fn handle_batch(
        task_ids: &[&str],
        include_description: bool,
        limit: usize,
        store: &TaskStore,
        brain_name: &str,
    ) -> ToolCallResult {
        let mut tasks = Vec::new();
        for id in task_ids {
            // Resolve each ID (supports prefix matching)
            let resolved = match store.resolve_task_id(id) {
                Ok(r) => r,
                Err(_) => continue, // skip unresolvable
            };
            match store.get_task(&resolved) {
                Ok(Some(t)) => tasks.push(t),
                Ok(None) => {} // skip missing
                Err(e) => {
                    error!(error = %e, task_id = id, "failed to get task in batch");
                }
            }
        }
        Self::build_response(&tasks, include_description, limit, store, brain_name)
    }

    fn build_response(
        tasks: &[crate::tasks::queries::TaskRow],
        include_description: bool,
        limit: usize,
        store: &TaskStore,
        brain_name: &str,
    ) -> ToolCallResult {
        let mut warnings: Vec<Warning> = Vec::new();
        let total = tasks.len();
        let capped = if limit > 0 && total > limit {
            &tasks[..limit]
        } else {
            tasks
        };

        // Batch-fetch labels for displayed tasks (eliminates N+1 queries)
        let task_ids: Vec<&str> = capped.iter().map(|t| t.task_id.as_str()).collect();
        let labels_map = store_or_warn(
            store.get_labels_for_tasks(&task_ids),
            "get_labels_for_tasks",
            &mut warnings,
        );
        let (mut tasks_json, ready_count, blocked_count) =
            enrich_task_list(store, capped, &labels_map);

        // Add uri and optionally strip description
        for task_val in &mut tasks_json {
            if let Some(obj) = task_val.as_object_mut() {
                if let Some(tid) = obj.get("task_id").and_then(|v| v.as_str()) {
                    let uri = SynapseUri::for_task(brain_name, tid).to_string();
                    obj.insert("uri".into(), json!(uri));
                }
                if !include_description {
                    obj.remove("description");
                }
            }
        }

        let count = tasks_json.len();
        let has_more = limit > 0 && total > limit;
        let mut response = json!({
            "tasks": tasks_json,
            "count": count,
            "total": total,
            "has_more": has_more,
            "ready_count": ready_count,
            "blocked_count": blocked_count,
        });
        inject_warnings(&mut response, warnings);
        json_response(&response)
    }
}

impl McpTool for TaskList {
    fn name(&self) -> &'static str {
        "tasks.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List tasks filtered by status and optional field filters. Returns summary task objects (descriptions omitted by default — use tasks.get for full details). Results are sorted by priority and paginated.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["open", "ready", "blocked", "done", "in_progress", "cancelled"],
                        "description": "Filter tasks by status. 'open' (default): excludes done/cancelled. 'ready': no unresolved deps. 'blocked': has unresolved deps or blocked_reason. 'done': completed or cancelled tasks. 'in_progress': only in-progress tasks. 'cancelled': only cancelled tasks.",
                        "default": "open"
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Fetch specific tasks by ID or prefix (ignores status filter). Unresolvable IDs are silently skipped. Pass as a JSON array, e.g. [\"BRN-01JPH\", \"BRN-02ABC\"]"
                    },
                    "priority": {
                        "type": "integer",
                        "description": "Filter by exact priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog)"
                    },
                    "task_type": {
                        "type": "string",
                        "description": "Filter by task type (task, bug, feature, epic, spike)"
                    },
                    "assignee": {
                        "type": "string",
                        "description": "Filter by assignee"
                    },
                    "label": {
                        "type": "string",
                        "description": "Filter by label (exact match)"
                    },
                    "search": {
                        "type": "string",
                        "description": "Full-text search on title and description (FTS5 query syntax)"
                    },
                    "include_description": {
                        "type": "boolean",
                        "description": "Include task descriptions in output. Default: false (omitted to reduce response size).",
                        "default": false
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of tasks to return. Default: 50. Response includes 'total' and 'has_more' for pagination.",
                        "default": 50
                    },
                    "brain": {
                        "type": "string",
                        "description": "Target brain name or ID. When provided, lists tasks from that brain instead of locally."
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
    /// In-memory stores use brain_id = "" which maps to the "(unscoped)" sentinel
    /// brain inserted by migration v21→v22. That brain's prefix is "NSX" (derived
    /// from generate_prefix("(unscoped)")), so compact IDs are "nsx-{hash}".
    fn compact_id_for(task_id: &str) -> String {
        let hex = blake3::hash(task_id.as_bytes()).to_hex().to_string();
        format!("nsx-{}", &hex[..3])
    }

    async fn apply(registry: &ToolRegistry, ctx: &crate::mcp::McpContext, params: Value) {
        registry.dispatch("tasks.apply_event", params, ctx).await;
    }

    #[tokio::test]
    async fn test_list_done() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Task 1", "priority": 2 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Task 2", "priority": 1 }
            }),
        )
        .await;
        // Mark t1 as done
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "status_changed",
                "task_id": "t1",
                "payload": { "new_status": "done" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.list", json!({ "status": "done" }), &ctx)
            .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], compact_id_for("t1"));
    }

    #[tokio::test]
    async fn test_list_all_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry
            .dispatch("tasks.list", json!({ "status": "all" }), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_list_default_excludes_done() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Open task", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Done task", "priority": 2 }
            }),
        )
        .await;
        // Mark t2 as done
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "status_changed",
                "task_id": "t2",
                "payload": { "new_status": "done" }
            }),
        )
        .await;

        // Default (no status param) should only return open tasks
        let result = registry.dispatch("tasks.list", json!({}), &ctx).await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Open task");

        // Explicit "done" should return only done tasks
        let result = registry
            .dispatch("tasks.list", json!({ "status": "done" }), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Done task");
    }

    #[tokio::test]
    async fn test_list_ready_filter() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Blocked", "priority": 2 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "t2",
                "payload": { "depends_on_task_id": "t1" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.list", json!({ "status": "ready" }), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], compact_id_for("t1"));
    }

    #[tokio::test]
    async fn test_list_blocked_filter() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Blocked", "priority": 2 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "t2",
                "payload": { "depends_on_task_id": "t1" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.list", json!({ "status": "blocked" }), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], compact_id_for("t2"));
    }

    #[tokio::test]
    async fn test_list_batch_by_task_ids() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        for (id, title) in &[("t1", "A"), ("t2", "B"), ("t3", "C")] {
            apply(
                &registry,
                &ctx,
                json!({
                    "event_type": "task_created",
                    "task_id": id,
                    "payload": { "title": title, "priority": 2 }
                }),
            )
            .await;
        }

        let result = registry
            .dispatch(
                "tasks.list",
                json!({ "task_ids": ["t1", "t3", "nonexistent"] }),
                &ctx,
            )
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        // Only t1 and t3 found; nonexistent skipped
        assert_eq!(parsed["count"], 2);
        let ids: Vec<String> = parsed["tasks"]
            .as_array()
            .expect("checked in test assertions")
            .iter()
            .map(|t| {
                t["task_id"]
                    .as_str()
                    .expect("checked in test assertions")
                    .to_string()
            })
            .collect();
        assert!(ids.contains(&compact_id_for("t1")));
        assert!(ids.contains(&compact_id_for("t3")));
    }

    #[tokio::test]
    async fn test_list_empty() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry.dispatch("tasks.list", json!({}), &ctx).await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 0);
        assert_eq!(parsed["ready_count"], 0);
        assert_eq!(parsed["blocked_count"], 0);
        assert!(
            parsed["tasks"]
                .as_array()
                .expect("checked in test assertions")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_list_correct_aggregate_counts() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Ready 1", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Ready 2", "priority": 2 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t3",
                "payload": { "title": "Blocked", "priority": 3 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "t3",
                "payload": { "depends_on_task_id": "t1" }
            }),
        )
        .await;

        // List open — count is 3 (all open), ready_count 2, blocked_count 1
        let result = registry
            .dispatch("tasks.list", json!({ "status": "open" }), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 3);
        assert_eq!(parsed["ready_count"], 2);
        assert_eq!(parsed["blocked_count"], 1);
    }

    #[tokio::test]
    async fn test_list_omits_description_by_default() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Task", "description": "A long description", "priority": 1 }
            }),
        )
        .await;

        // Default: no description
        let result = registry.dispatch("tasks.list", json!({}), &ctx).await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert!(parsed["tasks"][0].get("description").is_none());

        // With include_description: true
        let result = registry
            .dispatch("tasks.list", json!({ "include_description": true }), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["tasks"][0]["description"], "A long description");
    }

    #[tokio::test]
    async fn test_filter_by_priority() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "High", "priority": 1}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Medium", "priority": 2}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "High too", "priority": 1}})).await;

        let result = registry
            .dispatch("tasks.list", json!({"priority": 1}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 2);
        let titles: Vec<&str> = parsed["tasks"]
            .as_array()
            .expect("checked in test assertions")
            .iter()
            .map(|t| t["title"].as_str().expect("checked in test assertions"))
            .collect();
        assert!(titles.contains(&"High"));
        assert!(titles.contains(&"High too"));
    }

    #[tokio::test]
    async fn test_filter_by_task_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Bug fix", "priority": 1, "task_type": "bug"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Feature", "priority": 2, "task_type": "feature"}})).await;

        let result = registry
            .dispatch("tasks.list", json!({"task_type": "bug"}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Bug fix");
    }

    #[tokio::test]
    async fn test_filter_by_assignee() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Alice task", "priority": 1, "assignee": "alice"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Bob task", "priority": 2, "assignee": "bob"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "Unassigned", "priority": 2}})).await;

        let result = registry
            .dispatch("tasks.list", json!({"assignee": "alice"}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Alice task");
    }

    #[tokio::test]
    async fn test_filter_by_assignee_case_insensitive() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Alice task", "priority": 1, "assignee": "alice"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Bob task", "priority": 2, "assignee": "Bob"}})).await;

        // Filter with uppercase should match lowercase stored value
        let result = registry
            .dispatch("tasks.list", json!({"assignee": "Alice"}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Alice task");

        // Filter with lowercase should match mixed-case stored value
        let result = registry
            .dispatch("tasks.list", json!({"assignee": "bob"}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Bob task");
    }

    #[tokio::test]
    async fn test_filter_by_label() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Labeled", "priority": 1}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "No label", "priority": 2}})).await;
        apply(
            &registry,
            &ctx,
            json!({"event_type": "label_added", "task_id": "t1", "payload": {"label": "urgent"}}),
        )
        .await;

        let result = registry
            .dispatch("tasks.list", json!({"label": "urgent"}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Labeled");
    }

    #[tokio::test]
    async fn test_filter_label_no_match() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Task", "priority": 1}})).await;

        let result = registry
            .dispatch("tasks.list", json!({"label": "nonexistent"}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 0);
    }

    #[tokio::test]
    async fn test_combined_status_priority_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "P1 bug", "priority": 1, "task_type": "bug"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "P2 bug", "priority": 2, "task_type": "bug"}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "P1 feature", "priority": 1, "task_type": "feature"}})).await;
        // Close t1 so it's done
        apply(&registry, &ctx, json!({"event_type": "status_changed", "task_id": "t1", "payload": {"new_status": "done"}})).await;

        // Open + P1 + bug = nothing (t1 is done)
        let result = registry
            .dispatch(
                "tasks.list",
                json!({"status": "open", "priority": 1, "task_type": "bug"}),
                &ctx,
            )
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 0);

        // Open + P2 + bug = t2
        let result = registry
            .dispatch(
                "tasks.list",
                json!({"status": "open", "priority": 2, "task_type": "bug"}),
                &ctx,
            )
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "P2 bug");
    }

    #[tokio::test]
    async fn test_fts_search() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Implement filtering", "priority": 1}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Fix database bug", "priority": 2}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "Add search feature", "description": "Full text filtering support", "priority": 2}})).await;

        let result = registry
            .dispatch("tasks.list", json!({"search": "filtering"}), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 2); // t1 (title) + t3 (description)
        let ids: Vec<String> = parsed["tasks"]
            .as_array()
            .expect("checked in test assertions")
            .iter()
            .map(|t| {
                t["task_id"]
                    .as_str()
                    .expect("checked in test assertions")
                    .to_string()
            })
            .collect();
        assert!(ids.contains(&compact_id_for("t1")));
        assert!(ids.contains(&compact_id_for("t3")));
    }

    #[tokio::test]
    async fn test_fts_search_combined_with_status() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Fix permissions bug", "priority": 1}})).await;
        apply(&registry, &ctx, json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Permissions audit", "priority": 2}})).await;
        // Close t1
        apply(&registry, &ctx, json!({"event_type": "status_changed", "task_id": "t1", "payload": {"new_status": "done"}})).await;

        // Search "permissions" in open tasks only
        let result = registry
            .dispatch(
                "tasks.list",
                json!({"search": "permissions", "status": "open"}),
                &ctx,
            )
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], compact_id_for("t2"));

        // Search "permissions" in done tasks
        let result = registry
            .dispatch(
                "tasks.list",
                json!({"search": "permissions", "status": "done"}),
                &ctx,
            )
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], compact_id_for("t1"));
    }

    #[tokio::test]
    async fn test_invalid_task_type_filter_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry
            .dispatch("tasks.list", json!({"task_type": "story"}), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("invalid task type"));
    }

    #[tokio::test]
    async fn test_limit() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        for i in 0..5 {
            apply(
                &registry,
                &ctx,
                json!({
                    "event_type": "task_created",
                    "task_id": format!("t{i}"),
                    "payload": { "title": format!("Task {i}"), "priority": 2 }
                }),
            )
            .await;
        }

        // Limit to 2
        let result = registry
            .dispatch("tasks.list", json!({ "status": "open", "limit": 2 }), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 2);
        assert_eq!(parsed["total"], 5);
        assert_eq!(parsed["has_more"], true);

        // No limit override — uses default (50), all 5 fit
        let result = registry
            .dispatch("tasks.list", json!({ "status": "open" }), &ctx)
            .await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 5);
        assert_eq!(parsed["total"], 5);
        assert_eq!(parsed["has_more"], false);
    }

    #[tokio::test]
    async fn test_list_in_progress_filter() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Open task", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "In progress task", "priority": 2 }
            }),
        )
        .await;
        // Mark t2 as in_progress
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "status_changed",
                "task_id": "t2",
                "payload": { "new_status": "in_progress" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.list", json!({ "status": "in_progress" }), &ctx)
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], compact_id_for("t2"));
    }

    #[tokio::test]
    async fn test_list_cancelled_filter() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Open task", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Cancelled task", "priority": 2 }
            }),
        )
        .await;
        // Mark t2 as cancelled
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "status_changed",
                "task_id": "t2",
                "payload": { "new_status": "cancelled" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.list", json!({ "status": "cancelled" }), &ctx)
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], compact_id_for("t2"));
    }

    #[tokio::test]
    async fn test_list_parent_task_id_is_compact() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create parent
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "parent",
                "payload": { "title": "Parent", "priority": 1, "task_type": "epic" }
            }),
        )
        .await;
        // Create child with parent
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "child",
                "payload": { "title": "Child", "priority": 2, "parent_task_id": "parent" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.list", json!({ "status": "open" }), &ctx)
            .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let tasks = parsed["tasks"]
            .as_array()
            .expect("checked in test assertions");
        let child_task = tasks
            .iter()
            .find(|t| t["title"] == "Child")
            .expect("child task should be in list");

        // parent_task_id must be compact, not the raw ULID "parent"
        assert_eq!(
            child_task["parent_task_id"],
            compact_id_for("parent"),
            "parent_task_id should be compact form, got: {}",
            child_task["parent_task_id"]
        );
    }

    /// Regression for brn-a3e: explicit `brain: "<name>"` must scope `tasks.list`
    /// to that brain's rows. Sets up an ambient ctx scoped to brain A, registers
    /// brain B, writes a task into B, then asserts:
    ///   - Default (no brain param) returns 0 (brain A is empty).
    ///   - Explicit brain="brain-b" returns the brain-b task.
    /// Validates the in-handler rescoping path at lines 78-101.
    #[tokio::test]
    async fn test_list_with_explicit_brain_returns_target_brain_tasks() {
        let (_dir, base_ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Register both brains as active.
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

        // Rescope ctx to brain A so the ambient query filters to brain A only.
        let ctx = base_ctx.with_brain_id("brain-a-id", "brain-a").unwrap();

        // Insert a task scoped to brain B (NOT brain A).
        let brain_b_tasks =
            TaskStore::with_brain_id(ctx.stores.db_for_tests().clone(), "brain-b-id", "brain-b")
                .unwrap();
        brain_b_tasks
            .append(&TaskEvent::from_payload(
                "task-in-b",
                "test",
                TaskCreatedPayload {
                    title: "Task in brain B".into(),
                    description: None,
                    priority: 2,
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

        // Default scope (brain A) does not see the brain-b task.
        let result = registry
            .dispatch("tasks.list", json!({"status": "open"}), &ctx)
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(
            parsed["count"], 0,
            "brain-a should not see brain-b tasks: {parsed}"
        );

        // Explicit brain="brain-b" must surface the brain-b task.
        let result = registry
            .dispatch(
                "tasks.list",
                json!({"status": "open", "brain": "brain-b"}),
                &ctx,
            )
            .await;
        assert!(
            result.is_error.is_none(),
            "tasks.list with brain param should not error: {:?}",
            result.content[0].text
        );
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(
            parsed["count"], 1,
            "tasks.list with brain='brain-b' should return the brain-b task: {parsed}"
        );
        assert_eq!(parsed["tasks"][0]["title"], "Task in brain B");
        assert_eq!(parsed["brain"], "brain-b");
    }
}
