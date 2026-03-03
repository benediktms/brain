use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::tasks::enrichment::{comments_to_json, dep_summary_to_json, note_links_to_json};
use crate::utils::task_row_to_json;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ExpandField {
    Parent,
    Children,
    BlockedBy,
    Blocks,
}

impl fmt::Display for ExpandField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parent => write!(f, "parent"),
            Self::Children => write!(f, "children"),
            Self::BlockedBy => write!(f, "blocked_by"),
            Self::Blocks => write!(f, "blocks"),
        }
    }
}

impl FromStr for ExpandField {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "parent" => Ok(Self::Parent),
            "children" => Ok(Self::Children),
            "blocked_by" => Ok(Self::BlockedBy),
            "blocks" => Ok(Self::Blocks),
            other => Err(format!(
                "unknown expand value '{other}', valid values: \
                 [\"parent\", \"children\", \"blocked_by\", \"blocks\"]"
            )),
        }
    }
}

fn parse_expand(params: &Value) -> Result<HashSet<ExpandField>, String> {
    let Some(arr) = params.get("expand") else {
        return Ok(HashSet::new());
    };
    let Some(arr) = arr.as_array() else {
        return Err("'expand' must be an array".into());
    };
    let mut set = HashSet::new();
    for v in arr {
        let Some(s) = v.as_str() else {
            return Err("expand items must be strings".into());
        };
        set.insert(s.parse::<ExpandField>()?);
    }
    Ok(set)
}

/// Build a compact stub: `{task_id, title}`.
fn task_stub(task_id: &str, title: &str) -> Value {
    json!({ "task_id": task_id, "title": title })
}

/// Build a full task JSON with labels.
fn expanded_task(task_id: &str, ctx: &McpContext) -> Value {
    let Some(row) = ctx.tasks.get_task(task_id).ok().flatten() else {
        return task_stub(task_id, "(not found)");
    };
    let labels = ctx.tasks.get_task_labels(task_id).unwrap_or_default();
    task_row_to_json(&row, labels)
}

pub(super) fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    // 1. Extract task_id
    let task_id = match params.get("task_id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return ToolCallResult::error("Missing required parameter: task_id"),
    };

    // 2. Parse expand
    let expand = match parse_expand(params) {
        Ok(e) => e,
        Err(msg) => return ToolCallResult::error(msg),
    };

    // 3. Get task
    let task = match ctx.tasks.get_task(task_id) {
        Ok(Some(t)) => t,
        Ok(None) => return ToolCallResult::error(format!("Task not found: {task_id}")),
        Err(e) => {
            error!(error = %e, task_id, "failed to get task");
            return ToolCallResult::error(format!("Failed to get task: {e}"));
        }
    };

    // 4. Fetch enrichment data
    let labels = ctx.tasks.get_task_labels(task_id).unwrap_or_default();

    let comments = ctx.tasks.get_task_comments(task_id).unwrap_or_default();
    let comments_json = comments_to_json(&comments);

    let dep_summary = ctx
        .tasks
        .get_dependency_summary(task_id)
        .unwrap_or_else(|_| crate::tasks::queries::DependencySummary {
            total_deps: 0,
            done_deps: 0,
            blocking_task_ids: vec![],
        });

    let note_links = ctx.tasks.get_task_note_links(task_id).unwrap_or_default();
    let linked_notes_json = note_links_to_json(&note_links);

    let children = ctx.tasks.get_children(task_id).unwrap_or_default();
    let blocks = ctx.tasks.get_tasks_blocking(task_id).unwrap_or_default();

    // 5. Build parent field
    let parent_json = if expand.contains(&ExpandField::Parent) {
        task.parent_task_id
            .as_deref()
            .map(|pid| expanded_task(pid, ctx))
            .unwrap_or(Value::Null)
    } else {
        task.parent_task_id
            .as_deref()
            .map(|pid| {
                ctx.tasks
                    .get_task(pid)
                    .ok()
                    .flatten()
                    .map(|t| task_stub(&t.task_id, &t.title))
                    .unwrap_or_else(|| task_stub(pid, "(not found)"))
            })
            .unwrap_or(Value::Null)
    };

    // 6. Build children field
    let children_json: Vec<Value> = if expand.contains(&ExpandField::Children) {
        children
            .iter()
            .map(|c| {
                let labels = ctx.tasks.get_task_labels(&c.task_id).unwrap_or_default();
                task_row_to_json(c, labels)
            })
            .collect()
    } else {
        children
            .iter()
            .map(|c| task_stub(&c.task_id, &c.title))
            .collect()
    };

    // 7. Build blocked_by field (tasks this depends on that aren't done)
    let blocked_by_json: Vec<Value> = if expand.contains(&ExpandField::BlockedBy) {
        dep_summary
            .blocking_task_ids
            .iter()
            .map(|id| expanded_task(id, ctx))
            .collect()
    } else {
        dep_summary
            .blocking_task_ids
            .iter()
            .filter_map(|id| {
                ctx.tasks
                    .get_task(id)
                    .ok()
                    .flatten()
                    .map(|t| task_stub(&t.task_id, &t.title))
            })
            .collect()
    };

    // 8. Build blocks field (reverse deps)
    let blocks_json: Vec<Value> = if expand.contains(&ExpandField::Blocks) {
        blocks
            .iter()
            .map(|b| {
                let labels = ctx.tasks.get_task_labels(&b.task_id).unwrap_or_default();
                task_row_to_json(b, labels)
            })
            .collect()
    } else {
        blocks
            .iter()
            .map(|b| task_stub(&b.task_id, &b.title))
            .collect()
    };

    // 9. Build base task JSON
    let mut task_json = task_row_to_json(&task, labels);
    if let Some(obj) = task_json.as_object_mut() {
        obj.insert("parent".into(), parent_json);
        obj.insert("children".into(), json!(children_json));
        obj.insert("blocked_by".into(), json!(blocked_by_json));
        obj.insert("blocks".into(), json!(blocks_json));
        obj.insert("comments".into(), json!(comments_json));
        obj.insert("linked_notes".into(), json!(linked_notes_json));
        obj.insert("dependency_summary".into(), dep_summary_to_json(&dep_summary));
    }

    ToolCallResult::text(serde_json::to_string_pretty(&task_json).unwrap_or_default())
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
    fn test_get_found_with_stubs() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        // Create parent and child
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "parent",
                "payload": { "title": "Parent", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "child",
                "payload": { "title": "Child", "priority": 2, "parent_task_id": "parent" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.get",
            &json!({ "task_id": "parent" }),
            &ctx,
        ));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task_id"], "parent");
        assert_eq!(parsed["title"], "Parent");
        // parent field is null (no parent)
        assert!(parsed["parent"].is_null());
        // children as stubs
        let children = parsed["children"].as_array().unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["task_id"], "child");
        assert_eq!(children[0]["title"], "Child");
        // No status field in stub
        assert!(children[0].get("status").is_none());
        // Always-present fields
        assert!(parsed["comments"].is_array());
        assert!(parsed["labels"].is_array());
        assert!(parsed["linked_notes"].is_array());
        assert!(parsed["dependency_summary"].is_object());
    }

    #[test]
    fn test_get_not_found() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        let result = rt.block_on(dispatch_tool_call(
            "tasks.get",
            &json!({ "task_id": "nonexistent" }),
            &ctx,
        ));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("not found"));
    }

    #[test]
    fn test_get_expand_parent() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "p1",
                "payload": { "title": "Parent", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "c1",
                "payload": { "title": "Child", "priority": 2, "parent_task_id": "p1" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.get",
            &json!({ "task_id": "c1", "expand": ["parent"] }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Expanded parent has full fields
        assert_eq!(parsed["parent"]["task_id"], "p1");
        assert_eq!(parsed["parent"]["status"], "open");
    }

    #[test]
    fn test_get_expand_children() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "p1",
                "payload": { "title": "Parent", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "c1",
                "payload": { "title": "Child", "priority": 2, "parent_task_id": "p1" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.get",
            &json!({ "task_id": "p1", "expand": ["children"] }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let children = parsed["children"].as_array().unwrap();
        assert_eq!(children.len(), 1);
        // Expanded child has full fields
        assert_eq!(children[0]["task_id"], "c1");
        assert_eq!(children[0]["status"], "open");
    }

    #[test]
    fn test_get_expand_blocked_by() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "blocker",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "blocked",
                "payload": { "title": "Blocked", "priority": 2 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "blocked",
                "payload": { "depends_on_task_id": "blocker" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.get",
            &json!({ "task_id": "blocked", "expand": ["blocked_by"] }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let blocked_by = parsed["blocked_by"].as_array().unwrap();
        assert_eq!(blocked_by.len(), 1);
        assert_eq!(blocked_by[0]["task_id"], "blocker");
        assert_eq!(blocked_by[0]["status"], "open");
    }

    #[test]
    fn test_get_includes_comments_and_labels() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Task", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "label_added",
                "task_id": "t1",
                "payload": { "label": "urgent" }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "comment_added",
                "task_id": "t1",
                "payload": { "body": "A comment" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.get",
            &json!({ "task_id": "t1" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let labels = parsed["labels"].as_array().unwrap();
        assert_eq!(labels, &[json!("urgent")]);
        let comments = parsed["comments"].as_array().unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0]["body"], "A comment");
        assert!(comments[0].get("created_at").is_some());
    }

    #[test]
    fn test_get_blocks_reverse_deps() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "blocker",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "waiting",
                "payload": { "title": "Waiting", "priority": 2 }
            }),
        );
        apply(
            &rt,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "waiting",
                "payload": { "depends_on_task_id": "blocker" }
            }),
        );

        let result = rt.block_on(dispatch_tool_call(
            "tasks.get",
            &json!({ "task_id": "blocker" }),
            &ctx,
        ));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let blocks = parsed["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["task_id"], "waiting");
        assert_eq!(blocks[0]["title"], "Waiting");
    }
}
