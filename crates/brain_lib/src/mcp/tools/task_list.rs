use std::fmt;
use std::str::FromStr;

use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::tasks::enrichment::enrich_task_list;

const DEFAULT_LIMIT: u64 = 50;

#[derive(Debug, Clone, Copy)]
enum StatusFilter {
    All,
    Open,
    Ready,
    Blocked,
}

impl fmt::Display for StatusFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Open => write!(f, "open"),
            Self::Ready => write!(f, "ready"),
            Self::Blocked => write!(f, "blocked"),
        }
    }
}

impl FromStr for StatusFilter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(Self::All),
            "open" => Ok(Self::Open),
            "ready" => Ok(Self::Ready),
            "blocked" => Ok(Self::Blocked),
            other => Err(format!(
                "unknown status filter '{other}', valid values: \
                 [\"all\", \"open\", \"ready\", \"blocked\"]"
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

    let status = match super::opt_str(params, "status", "open").parse::<StatusFilter>() {
        Ok(s) => s,
        Err(msg) => return ToolCallResult::error(msg),
    };

    let tasks = match status {
        StatusFilter::All => ctx.tasks.list_all(),
        StatusFilter::Open => ctx.tasks.list_open(),
        StatusFilter::Ready => ctx.tasks.list_ready(),
        StatusFilter::Blocked => ctx.tasks.list_blocked(),
    };

    let tasks = match tasks {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, %status, "failed to list tasks");
            return ToolCallResult::error(format!("Failed to list tasks: {e}"));
        }
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

    let (mut tasks_json, ready_count, blocked_count) = enrich_task_list(&ctx.tasks, capped);

    // Add short_id to each task, and optionally strip descriptions
    let short_ids = ctx.tasks.shortest_unique_prefixes().unwrap_or_default();
    for task_val in &mut tasks_json {
        if let Some(obj) = task_val.as_object_mut() {
            if let Some(tid) = obj.get("task_id").and_then(|v| v.as_str()).map(String::from) {
                let short = short_ids
                    .get(tid.as_str())
                    .cloned()
                    .unwrap_or(tid);
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
    fn test_list_all() {
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

        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "all" }),
            &ctx,
        ));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 2);
        assert_eq!(parsed["ready_count"], 2);
        assert_eq!(parsed["blocked_count"], 0);
        let tasks = parsed["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 2);
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

        // Explicit "all" should return both
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "all" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 2);
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

        // List all — count is 3 (all), ready_count 2, blocked_count 1
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "all" }),
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
    fn test_list_limit() {
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
            &json!({ "status": "all", "limit": 2 }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 2);
        assert_eq!(parsed["total"], 5);
        assert_eq!(parsed["has_more"], true);

        // No limit override — uses default (50), all 5 fit
        let result = rt.block_on(dispatch_tool_call(
            "tasks.list",
            &json!({ "status": "all" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 5);
        assert_eq!(parsed["total"], 5);
        assert_eq!(parsed["has_more"], false);
    }
}
