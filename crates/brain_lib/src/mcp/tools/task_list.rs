use std::fmt;
use std::str::FromStr;

use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::utils::task_row_to_json;

#[derive(Debug, Clone, Copy)]
enum StatusFilter {
    All,
    Ready,
    Blocked,
}

impl fmt::Display for StatusFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "all"),
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
            "ready" => Ok(Self::Ready),
            "blocked" => Ok(Self::Blocked),
            other => Err(format!(
                "unknown status filter '{other}', valid values: \
                 [\"all\", \"ready\", \"blocked\"]"
            )),
        }
    }
}

pub(super) fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    // If task_ids provided, fetch those specifically
    if let Some(ids) = params.get("task_ids").and_then(|v| v.as_array()) {
        let task_ids: Vec<&str> = ids.iter().filter_map(|v| v.as_str()).collect();
        return handle_batch(&task_ids, ctx);
    }

    let status = match params
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("all")
        .parse::<StatusFilter>()
    {
        Ok(s) => s,
        Err(msg) => return ToolCallResult::error(msg),
    };

    let tasks = match status {
        StatusFilter::All => ctx.tasks.list_all(),
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

    build_response(&tasks, ctx)
}

fn handle_batch(task_ids: &[&str], ctx: &McpContext) -> ToolCallResult {
    let mut tasks = Vec::new();
    for id in task_ids {
        match ctx.tasks.get_task(id) {
            Ok(Some(t)) => tasks.push(t),
            Ok(None) => {} // skip missing
            Err(e) => {
                error!(error = %e, task_id = id, "failed to get task in batch");
            }
        }
    }
    build_response(&tasks, ctx)
}

fn build_response(tasks: &[crate::tasks::queries::TaskRow], ctx: &McpContext) -> ToolCallResult {
    let tasks_json: Vec<Value> = tasks
        .iter()
        .map(|task| {
            let labels = ctx.tasks.get_task_labels(&task.task_id).unwrap_or_default();
            task_row_to_json(task, labels)
        })
        .collect();

    let (ready_count, blocked_count) = ctx.tasks.count_ready_blocked().unwrap_or((0, 0));

    let response = json!({
        "tasks": tasks_json,
        "count": tasks_json.len(),
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

        let result = rt.block_on(dispatch_tool_call("tasks.list", &json!({}), &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 2);
        assert_eq!(parsed["ready_count"], 2);
        assert_eq!(parsed["blocked_count"], 0);
        let tasks = parsed["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 2);
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
}
