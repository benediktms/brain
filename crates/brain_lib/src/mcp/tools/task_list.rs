use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::tasks::enrichment::enrich_task_list;
use crate::tasks::queries::{TaskFilter, apply_filters};

const DEFAULT_LIMIT: u64 = 50;

#[derive(Debug, Clone, Copy)]
enum StatusFilter {
    Open,
    Ready,
    Blocked,
    Done,
}

impl fmt::Display for StatusFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open => write!(f, "open"),
            Self::Ready => write!(f, "ready"),
            Self::Blocked => write!(f, "blocked"),
            Self::Done => write!(f, "done"),
        }
    }
}

impl FromStr for StatusFilter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(Self::Open),
            "ready" => Ok(Self::Ready),
            "blocked" => Ok(Self::Blocked),
            "done" => Ok(Self::Done),
            other => Err(format!(
                "unknown status filter '{other}', valid values: \
                 [\"open\", \"ready\", \"blocked\", \"done\"]"
            )),
        }
    }
}

pub(super) fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let include_description = params
        .get("include_description")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let limit = super::opt_u64(params, "limit", DEFAULT_LIMIT) as usize;

    // If task_ids provided, fetch those specifically
    if let Some(ids) = params.get("task_ids").and_then(|v| v.as_array()) {
        let task_ids: Vec<&str> = ids.iter().filter_map(|v| v.as_str()).collect();
        return handle_batch(&task_ids, include_description, limit, ctx);
    }

    // Parse per-field filters
    let filter = TaskFilter {
        priority: params
            .get("priority")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        task_type: params
            .get("task_type")
            .and_then(|v| v.as_str())
            .map(String::from),
        assignee: params
            .get("assignee")
            .and_then(|v| v.as_str())
            .map(String::from),
        label: params
            .get("label")
            .and_then(|v| v.as_str())
            .map(String::from),
        search: params
            .get("search")
            .and_then(|v| v.as_str())
            .map(String::from),
    };

    let status = match super::opt_str(params, "status", "open").parse::<StatusFilter>() {
        Ok(s) => s,
        Err(msg) => return ToolCallResult::error(msg),
    };

    // FTS pre-filter: get matching task_ids
    let fts_ids = if let Some(ref query) = filter.search {
        match ctx.tasks.search_fts(query, 1000) {
            Ok(ids) => Some(ids.into_iter().collect::<HashSet<String>>()),
            Err(e) => {
                error!(error = %e, "FTS search failed");
                return ToolCallResult::error(format!("Full-text search failed: {e}"));
            }
        }
    } else {
        None
    };

    let tasks = match status {
        StatusFilter::Open => ctx.tasks.list_open(),
        StatusFilter::Ready => ctx.tasks.list_ready(),
        StatusFilter::Blocked => ctx.tasks.list_blocked(),
        StatusFilter::Done => ctx.tasks.list_done(),
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
            ctx.tasks.get_labels_for_tasks(&task_ids).ok()
        } else {
            None
        };

        apply_filters(tasks, &filter, fts_ids.as_ref(), labels_map.as_ref())
    } else {
        tasks
    };

    build_response(&tasks, include_description, limit, ctx)
}

fn handle_batch(
    task_ids: &[&str],
    include_description: bool,
    limit: usize,
    ctx: &McpContext,
) -> ToolCallResult {
    let mut tasks = Vec::new();
    for id in task_ids {
        // Resolve each ID (supports prefix matching)
        let resolved = match ctx.tasks.resolve_task_id(id) {
            Ok(r) => r,
            Err(_) => continue, // skip unresolvable
        };
        match ctx.tasks.get_task(&resolved) {
            Ok(Some(t)) => tasks.push(t),
            Ok(None) => {} // skip missing
            Err(e) => {
                error!(error = %e, task_id = id, "failed to get task in batch");
            }
        }
    }
    build_response(&tasks, include_description, limit, ctx)
}

fn build_response(
    tasks: &[crate::tasks::queries::TaskRow],
    include_description: bool,
    limit: usize,
    ctx: &McpContext,
) -> ToolCallResult {
    let total = tasks.len();
    let capped = if limit > 0 && total > limit {
        &tasks[..limit]
    } else {
        tasks
    };

    // Batch-fetch labels for displayed tasks (eliminates N+1 queries)
    let task_ids: Vec<&str> = capped.iter().map(|t| t.task_id.as_str()).collect();
    let labels_map = ctx
        .tasks
        .get_labels_for_tasks(&task_ids)
        .unwrap_or_default();
    let (mut tasks_json, ready_count, blocked_count) =
        enrich_task_list(&ctx.tasks, capped, &labels_map);

    // Add short_id per task using O(log n) index seeks (replaces loading all IDs)
    for task_val in &mut tasks_json {
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
            if !include_description {
                obj.remove("description");
            }
        }
    }

    let count = tasks_json.len();
    let has_more = limit > 0 && total > limit;
    let response = json!({
        "tasks": tasks_json,
        "count": count,
        "total": total,
        "has_more": has_more,
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

    fn apply(rt: &tokio::runtime::Runtime, ctx: &crate::mcp::McpContext, params: Value) {
        rt.block_on(dispatch_tool_call("tasks.apply_event", &params, ctx));
    }

    #[test]
    fn test_list_done() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Task 1", "priority": 2 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Task 2", "priority": 1 }
            }),
        );
        // Mark t1 as done
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "status_changed",
                "task_id": "t1",
                "payload": { "new_status": "done" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "done" }),
            &ctx,
        ));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], "t1");
    }

    #[test]
    fn test_list_all_returns_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "all" }),
            &ctx,
        ));
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn test_list_default_excludes_done() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Open task", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Done task", "priority": 2 }
            }),
        );
        // Mark t2 as done
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "status_changed",
                "task_id": "t2",
                "payload": { "new_status": "done" }
            }),
        );

        // Default (no status param) should only return open tasks
        let result = rt.block_on(dispatch_tool_call("tasks.list", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Open task");

        // Explicit "done" should return only done tasks
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "done" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Done task");
    }

    #[test]
    fn test_list_ready_filter() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Blocked", "priority": 2 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "t2",
                "payload": { "depends_on_task_id": "t1" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "ready" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], "t1");
    }

    #[test]
    fn test_list_blocked_filter() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Blocked", "priority": 2 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "t2",
                "payload": { "depends_on_task_id": "t1" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "blocked" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], "t2");
    }

    #[test]
    fn test_list_batch_by_task_ids() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        for (id, title) in &[("t1", "A"), ("t2", "B"), ("t3", "C")] {
            apply(
                &rt,
                &ctx,
                json!({
                    "event_type": "task_created",
                    "task_id": id,
                    "payload": { "title": title, "priority": 2 }
                }),
            );
        }

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "task_ids": ["t1", "t3", "nonexistent"] }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Only t1 and t3 found; nonexistent skipped
        assert_eq!(parsed["count"], 2);
        let ids: Vec<&str> = parsed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["task_id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"t1"));
        assert!(ids.contains(&"t3"));
    }

    #[test]
    fn test_list_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        let result = rt.block_on(dispatch_tool_call("tasks.list", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 0);
        assert_eq!(parsed["ready_count"], 0);
        assert_eq!(parsed["blocked_count"], 0);
        assert!(parsed["tasks"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_list_correct_aggregate_counts() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Ready 1", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t2",
                "payload": { "title": "Ready 2", "priority": 2 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t3",
                "payload": { "title": "Blocked", "priority": 3 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "t3",
                "payload": { "depends_on_task_id": "t1" }
            }),
        );

        // List open — count is 3 (all open), ready_count 2, blocked_count 1
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "open" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 3);
        assert_eq!(parsed["ready_count"], 2);
        assert_eq!(parsed["blocked_count"], 1);
    }

    #[test]
    fn test_list_omits_description_by_default() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Task", "description": "A long description", "priority": 1 }
            }),
        );

        // Default: no description
        let result = rt.block_on(dispatch_tool_call("tasks.list", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert!(parsed["tasks"][0].get("description").is_none());

        // With include_description: true
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "include_description": true }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["tasks"][0]["description"], "A long description");
    }

    #[test]
    fn test_filter_by_priority() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "High", "priority": 1}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Medium", "priority": 2}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "High too", "priority": 1}}),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"priority": 1}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 2);
        let titles: Vec<&str> = parsed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["title"].as_str().unwrap())
            .collect();
        assert!(titles.contains(&"High"));
        assert!(titles.contains(&"High too"));
    }

    #[test]
    fn test_filter_by_task_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Bug fix", "priority": 1, "task_type": "bug"}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Feature", "priority": 2, "task_type": "feature"}}),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"task_type": "bug"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Bug fix");
    }

    #[test]
    fn test_filter_by_assignee() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Alice task", "priority": 1, "assignee": "alice"}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Bob task", "priority": 2, "assignee": "bob"}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "Unassigned", "priority": 2}}),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"assignee": "alice"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Alice task");
    }

    #[test]
    fn test_filter_by_label() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Labeled", "priority": 1}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "No label", "priority": 2}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "label_added", "task_id": "t1", "payload": {"label": "urgent"}}),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"label": "urgent"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "Labeled");
    }

    #[test]
    fn test_filter_label_no_match() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Task", "priority": 1}}),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"label": "nonexistent"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 0);
    }

    #[test]
    fn test_combined_status_priority_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "P1 bug", "priority": 1, "task_type": "bug"}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "P2 bug", "priority": 2, "task_type": "bug"}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "P1 feature", "priority": 1, "task_type": "feature"}}),
        );
        // Close t1 so it's done
        apply(
            &rt,
            &ctx,
            json!({"event_type": "status_changed", "task_id": "t1", "payload": {"new_status": "done"}}),
        );

        // Open + P1 + bug = nothing (t1 is done)
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"status": "open", "priority": 1, "task_type": "bug"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 0);

        // Open + P2 + bug = t2
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"status": "open", "priority": 2, "task_type": "bug"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["title"], "P2 bug");
    }

    #[test]
    fn test_fts_search() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Implement filtering", "priority": 1}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Fix database bug", "priority": 2}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t3", "payload": {"title": "Add search feature", "description": "Full text filtering support", "priority": 2}}),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"search": "filtering"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 2); // t1 (title) + t3 (description)
        let ids: Vec<&str> = parsed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["task_id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"t1"));
        assert!(ids.contains(&"t3"));
    }

    #[test]
    fn test_fts_search_combined_with_status() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t1", "payload": {"title": "Fix permissions bug", "priority": 1}}),
        );
        apply(
            &rt,
            &ctx,
            json!({"event_type": "task_created", "task_id": "t2", "payload": {"title": "Permissions audit", "priority": 2}}),
        );
        // Close t1
        apply(
            &rt,
            &ctx,
            json!({"event_type": "status_changed", "task_id": "t1", "payload": {"new_status": "done"}}),
        );

        // Search "permissions" in open tasks only
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"search": "permissions", "status": "open"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], "t2");

        // Search "permissions" in done tasks
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({"search": "permissions", "status": "done"}),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["tasks"][0]["task_id"], "t1");
    }

    #[test]
    fn test_limit() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        for i in 0..5 {
            apply(
                &rt,
                &ctx,
                json!({
                    "event_type": "task_created",
                    "task_id": format!("t{i}"),
                    "payload": { "title": format!("Task {i}"), "priority": 2 }
                }),
            );
        }

        // Limit to 2
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "open", "limit": 2 }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 2);
        assert_eq!(parsed["total"], 5);
        assert_eq!(parsed["has_more"], true);

        // No limit override — uses default (50), all 5 fit
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "open" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 5);
        assert_eq!(parsed["total"], 5);
        assert_eq!(parsed["has_more"], false);
    }
}
