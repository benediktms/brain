use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::enrichment::{comments_to_json, dep_summary_to_json, note_links_to_json};
use crate::tasks::queries::TaskRow;
use crate::uri::{BrainUri, resolve_id};
use crate::utils::task_row_to_json;

use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Deserialize)]
struct Params {
    task_id: String,
    #[serde(default)]
    expand: HashSet<ExpandField>,
}

/// Build a compact stub: `{task_id, title}`.
fn task_stub(task_id: &str, title: &str) -> Value {
    json!({ "task_id": task_id, "title": title })
}

/// Build a full task JSON with labels but without description (expanded relations
/// omit descriptions to keep responses concise — use `tasks.get` on the specific
/// task to retrieve its full description).
fn expanded_task(row: &TaskRow, labels: Vec<String>) -> Value {
    let mut json = task_row_to_json(row, labels);
    if let Some(obj) = json.as_object_mut() {
        obj.remove("description");
    }
    json
}

pub(super) struct TaskGet;

impl TaskGet {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let mut warnings: Vec<Warning> = Vec::new();

        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // 1. Resolve task_id (strip brain:// URI if present, then resolve prefix)
        let task_id_input = resolve_id(&params.task_id);
        let task_id = match ctx.stores.tasks.resolve_task_id(&task_id_input) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve task_id: {e}")),
        };

        // 2. Get task
        let task_id = task_id.as_str();
        let task = match ctx.stores.tasks.get_task(task_id) {
            Ok(Some(t)) => t,
            Ok(None) => return ToolCallResult::error(format!("Task not found: {task_id}")),
            Err(e) => {
                error!(error = %e, task_id, "failed to get task");
                return ToolCallResult::error(format!("Failed to get task: {e}"));
            }
        };

        // 3. Fetch enrichment data
        let labels = store_or_warn(
            ctx.stores.tasks.get_task_labels(task_id),
            "get_task_labels",
            &mut warnings,
        );
        let external_ids = store_or_warn(
            ctx.stores.tasks.get_external_ids(task_id),
            "get_external_ids",
            &mut warnings,
        );

        let comments = store_or_warn(
            ctx.stores.tasks.get_task_comments(task_id),
            "get_task_comments",
            &mut warnings,
        );
        let comments_json = comments_to_json(&comments);

        let dep_summary = match ctx.stores.tasks.get_dependency_summary(task_id) {
            Ok(summary) => summary,
            Err(err) => {
                warnings.push(Warning {
                    source: "get_dependency_summary".to_string(),
                    error: err.to_string(),
                });
                crate::tasks::queries::DependencySummary {
                    total_deps: 0,
                    done_deps: 0,
                    blocking_task_ids: vec![],
                }
            }
        };

        let note_links = store_or_warn(
            ctx.stores.tasks.get_task_note_links(task_id),
            "get_task_note_links",
            &mut warnings,
        );
        let linked_notes_json = note_links_to_json(&note_links);

        let children = store_or_warn(
            ctx.stores.tasks.get_children(task_id),
            "get_children",
            &mut warnings,
        );
        let blocks = store_or_warn(
            ctx.stores.tasks.get_tasks_blocking(task_id),
            "get_tasks_blocking",
            &mut warnings,
        );

        // 4. Build parent field
        let parent_json = if params.expand.contains(&ExpandField::Parent) {
            task.parent_task_id
                .as_deref()
                .map(|pid| {
                    let Some(parent) =
                        store_or_warn(ctx.stores.tasks.get_task(pid), "get_task", &mut warnings)
                    else {
                        return task_stub(pid, "(not found)");
                    };
                    let parent_labels = store_or_warn(
                        ctx.stores.tasks.get_task_labels(pid),
                        "get_task_labels",
                        &mut warnings,
                    );
                    expanded_task(&parent, parent_labels)
                })
                .unwrap_or(Value::Null)
        } else {
            task.parent_task_id
                .as_deref()
                .map(|pid| {
                    ctx.stores
                        .tasks
                        .get_task(pid)
                        .ok()
                        .flatten()
                        .map(|t| task_stub(&t.task_id, &t.title))
                        .unwrap_or_else(|| task_stub(pid, "(not found)"))
                })
                .unwrap_or(Value::Null)
        };

        // 5. Build children field
        let children_json: Vec<Value> = if params.expand.contains(&ExpandField::Children) {
            children
                .iter()
                .map(|c| {
                    let labels = store_or_warn(
                        ctx.stores.tasks.get_task_labels(&c.task_id),
                        "get_task_labels",
                        &mut warnings,
                    );
                    let mut json = task_row_to_json(c, labels);
                    if let Some(obj) = json.as_object_mut() {
                        obj.remove("description");
                    }
                    json
                })
                .collect()
        } else {
            children
                .iter()
                .map(|c| task_stub(&c.task_id, &c.title))
                .collect()
        };

        // 6. Build blocked_by field (tasks this depends on that aren't done)
        let blocked_by_json: Vec<Value> = if params.expand.contains(&ExpandField::BlockedBy) {
            dep_summary
                .blocking_task_ids
                .iter()
                .map(|id| {
                    let Some(blocking_task) =
                        store_or_warn(ctx.stores.tasks.get_task(id), "get_task", &mut warnings)
                    else {
                        return task_stub(id, "(not found)");
                    };
                    let blocking_labels = store_or_warn(
                        ctx.stores.tasks.get_task_labels(id),
                        "get_task_labels",
                        &mut warnings,
                    );
                    expanded_task(&blocking_task, blocking_labels)
                })
                .collect()
        } else {
            dep_summary
                .blocking_task_ids
                .iter()
                .filter_map(|id| {
                    ctx.stores
                        .tasks
                        .get_task(id)
                        .ok()
                        .flatten()
                        .map(|t| task_stub(&t.task_id, &t.title))
                })
                .collect()
        };

        // 7. Build blocks field (reverse deps)
        let blocks_json: Vec<Value> = if params.expand.contains(&ExpandField::Blocks) {
            blocks
                .iter()
                .map(|b| {
                    let labels = store_or_warn(
                        ctx.stores.tasks.get_task_labels(&b.task_id),
                        "get_task_labels",
                        &mut warnings,
                    );
                    let mut json = task_row_to_json(b, labels);
                    if let Some(obj) = json.as_object_mut() {
                        obj.remove("description");
                    }
                    json
                })
                .collect()
        } else {
            blocks
                .iter()
                .map(|b| task_stub(&b.task_id, &b.title))
                .collect()
        };

        // 8. Build base task JSON
        let short_id = ctx
            .stores
            .tasks
            .compact_id(task_id)
            .unwrap_or_else(|_| task_id.to_string());
        let mut task_json = task_row_to_json(&task, labels);
        if let Some(obj) = task_json.as_object_mut() {
            obj.insert("task_id".into(), json!(short_id));
            obj.insert("parent".into(), parent_json);
            obj.insert("children".into(), json!(children_json));
            obj.insert("blocked_by".into(), json!(blocked_by_json));
            obj.insert("blocks".into(), json!(blocks_json));
            obj.insert("comments".into(), json!(comments_json));
            obj.insert("linked_notes".into(), json!(linked_notes_json));
            obj.insert(
                "external_ids".into(),
                json!(
                    external_ids
                        .iter()
                        .map(|e| json!({
                            "source": e.source,
                            "external_id": e.external_id,
                            "external_url": e.external_url,
                            "imported_at": e.imported_at,
                        }))
                        .collect::<Vec<_>>()
                ),
            );
            obj.insert(
                "dependency_summary".into(),
                dep_summary_to_json(&dep_summary),
            );
        }

        let uri = BrainUri::for_task(ctx.brain_name(), &short_id).to_string();
        let task_copy = task_json.clone();
        if let Some(obj) = task_json.as_object_mut() {
            obj.insert("task".into(), task_copy);
            obj.insert("uri".into(), json!(uri));
        }

        inject_warnings(&mut task_json, warnings);
        json_response(&task_json)
    }
}

impl McpTool for TaskGet {
    fn name(&self) -> &'static str {
        "tasks.get"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get a single task by ID (full or prefix) with full details including relationships, comments, labels, and linked notes. Relationships (parent, children, blocked_by, blocks) are returned as compact stubs by default; use the expand parameter to get full task objects.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "The task ID to retrieve (full ID or unique prefix, e.g. 'BRN-01JPH')"
                    },
                    "expand": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["parent", "children", "blocked_by", "blocks"]
                        },
                        "description": "Expand relationship stubs to full task objects. Pass as a JSON array, e.g. [\"parent\", \"blocked_by\"]"
                    },
                },
                "required": ["task_id"]
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

    async fn apply(registry: &ToolRegistry, ctx: &crate::mcp::McpContext, params: Value) {
        registry.dispatch("tasks.apply_event", params, ctx).await;
    }

    #[tokio::test]
    async fn test_get_found_with_stubs() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create parent and child
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "parent",
                "payload": { "title": "Parent", "priority": 1 }
            }),
        )
        .await;
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
            .dispatch("tasks.get", json!({ "task_id": "parent" }), &ctx)
            .await;
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

    #[tokio::test]
    async fn test_get_not_found() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry
            .dispatch("tasks.get", json!({ "task_id": "nonexistent" }), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("no task found"));
    }

    #[tokio::test]
    async fn test_get_expand_parent() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "p1",
                "payload": { "title": "Parent", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "c1",
                "payload": { "title": "Child", "priority": 2, "parent_task_id": "p1" }
            }),
        )
        .await;

        let result = registry
            .dispatch(
                "tasks.get",
                json!({ "task_id": "c1", "expand": ["parent"] }),
                &ctx,
            )
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        // Expanded parent has full fields
        assert_eq!(parsed["parent"]["task_id"], "p1");
        assert_eq!(parsed["parent"]["status"], "open");
    }

    #[tokio::test]
    async fn test_get_expand_children() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "p1",
                "payload": { "title": "Parent", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "c1",
                "payload": { "title": "Child", "priority": 2, "parent_task_id": "p1" }
            }),
        )
        .await;

        let result = registry
            .dispatch(
                "tasks.get",
                json!({ "task_id": "p1", "expand": ["children"] }),
                &ctx,
            )
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let children = parsed["children"].as_array().unwrap();
        assert_eq!(children.len(), 1);
        // Expanded child has full fields
        assert_eq!(children[0]["task_id"], "c1");
        assert_eq!(children[0]["status"], "open");
    }

    #[tokio::test]
    async fn test_get_expand_blocked_by() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "blocker",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "blocked",
                "payload": { "title": "Blocked", "priority": 2 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "blocked",
                "payload": { "depends_on_task_id": "blocker" }
            }),
        )
        .await;

        let result = registry
            .dispatch(
                "tasks.get",
                json!({ "task_id": "blocked", "expand": ["blocked_by"] }),
                &ctx,
            )
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let blocked_by = parsed["blocked_by"].as_array().unwrap();
        assert_eq!(blocked_by.len(), 1);
        assert_eq!(blocked_by[0]["task_id"], "blocker");
        assert_eq!(blocked_by[0]["status"], "open");
    }

    #[tokio::test]
    async fn test_get_includes_comments_and_labels() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "t1",
                "payload": { "title": "Task", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "label_added",
                "task_id": "t1",
                "payload": { "label": "urgent" }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "comment_added",
                "task_id": "t1",
                "payload": { "body": "A comment" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.get", json!({ "task_id": "t1" }), &ctx)
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let labels = parsed["labels"].as_array().unwrap();
        assert_eq!(labels, &[json!("urgent")]);
        let comments = parsed["comments"].as_array().unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0]["body"], "A comment");
        assert!(comments[0].get("created_at").is_some());
    }

    #[tokio::test]
    async fn test_expand_omits_descriptions() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "epic",
                "payload": { "title": "Epic", "description": "Epic description", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "child1",
                "payload": { "title": "Child 1", "description": "Child 1 long description", "priority": 2, "parent_task_id": "epic" }
            }),
        )
        .await;

        // Primary task keeps its description
        let result = registry
            .dispatch(
                "tasks.get",
                json!({ "task_id": "epic", "expand": ["children"] }),
                &ctx,
            )
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["description"], "Epic description");

        // Expanded children omit descriptions
        let children = parsed["children"].as_array().unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["title"], "Child 1");
        assert!(
            children[0].get("description").is_none(),
            "expanded children should omit description"
        );
    }

    #[tokio::test]
    async fn test_get_blocks_reverse_deps() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "blocker",
                "payload": { "title": "Blocker", "priority": 1 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "task_created",
                "task_id": "waiting",
                "payload": { "title": "Waiting", "priority": 2 }
            }),
        )
        .await;
        apply(
            &registry,
            &ctx,
            json!({
                "event_type": "dependency_added",
                "task_id": "waiting",
                "payload": { "depends_on_task_id": "blocker" }
            }),
        )
        .await;

        let result = registry
            .dispatch("tasks.get", json!({ "task_id": "blocker" }), &ctx)
            .await;
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let blocks = parsed["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["task_id"], "waiting");
        assert_eq!(blocks[0]["title"], "Waiting");
    }
}
