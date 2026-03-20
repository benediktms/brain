use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::events::{DependencyPayload, EventType, TaskEvent};
use crate::uri::{SynapseUri, resolve_id};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct DepPair {
    task_id: String,
    depends_on_task_id: String,
}

#[derive(Deserialize)]
struct Params {
    action: String,
    #[serde(default)]
    pairs: Option<Vec<DepPair>>,
    #[serde(default)]
    task_ids: Option<Vec<String>>,
    #[serde(default)]
    source_task_id: Option<String>,
    #[serde(default)]
    dependent_task_ids: Option<Vec<String>>,
    #[serde(default)]
    task_id: Option<String>,
}

pub(super) struct TaskDepsBatch;

impl TaskDepsBatch {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        match params.action.as_str() {
            "add" => self.dep_pairs(ctx, &params, EventType::DependencyAdded),
            "remove" => self.dep_pairs(ctx, &params, EventType::DependencyRemoved),
            "chain" => self.dep_chain(ctx, &params),
            "fan" => self.dep_fan(ctx, &params),
            "clear" => self.dep_clear(ctx, &params),
            other => ToolCallResult::error(format!(
                "Invalid action: '{other}'. Must be one of: add, remove, chain, fan, clear"
            )),
        }
    }

    fn dep_pairs(
        &self,
        ctx: &McpContext,
        params: &Params,
        event_type: EventType,
    ) -> ToolCallResult {
        let pairs = match &params.pairs {
            Some(p) => p,
            None => return ToolCallResult::error("Missing required parameter: pairs"),
        };

        if pairs.is_empty() {
            return batch_response(vec![], vec![], ctx.brain_name());
        }

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();

        // Process sequentially so cycle detection sees accumulated state
        for pair in pairs {
            let task_id_input = resolve_id(&pair.task_id);
            let task_id = match ctx.stores.tasks.resolve_task_id(&task_id_input) {
                Ok(id) => id,
                Err(e) => {
                    failed.push(json!({
                        "task_id": pair.task_id,
                        "depends_on_task_id": pair.depends_on_task_id,
                        "error": format!("{e}"),
                    }));
                    continue;
                }
            };
            let depends_on_input = resolve_id(&pair.depends_on_task_id);
            let depends_on = match ctx.stores.tasks.resolve_task_id(&depends_on_input) {
                Ok(id) => id,
                Err(e) => {
                    failed.push(json!({
                        "task_id": pair.task_id,
                        "depends_on_task_id": pair.depends_on_task_id,
                        "error": format!("{e}"),
                    }));
                    continue;
                }
            };

            let event = TaskEvent::new(
                &task_id,
                "mcp",
                event_type.clone(),
                &DependencyPayload {
                    depends_on_task_id: depends_on.clone(),
                },
            );

            match ctx.stores.tasks.append(&event) {
                Ok(()) => {
                    succeeded.push(json!({
                        "task_id": task_id,
                        "depends_on_task_id": depends_on,
                    }));
                }
                Err(e) => {
                    failed.push(json!({
                        "task_id": task_id,
                        "depends_on_task_id": depends_on,
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        batch_response(succeeded, failed, ctx.brain_name())
    }

    fn dep_chain(&self, ctx: &McpContext, params: &Params) -> ToolCallResult {
        let task_ids = match &params.task_ids {
            Some(ids) => ids,
            None => return ToolCallResult::error("Missing required parameter: task_ids"),
        };

        if task_ids.len() < 2 {
            return ToolCallResult::error("chain requires at least 2 task IDs");
        }

        // Resolve all IDs first
        let mut resolved = Vec::new();
        let mut failed = Vec::new();
        for raw_id in task_ids {
            let resolved_input = resolve_id(raw_id);
            match ctx.stores.tasks.resolve_task_id(&resolved_input) {
                Ok(id) => resolved.push(id),
                Err(e) => {
                    failed.push(json!({
                        "task_id": raw_id,
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        if !failed.is_empty() {
            // Can't build a chain with missing tasks — report all as failed
            return batch_response(vec![], failed, ctx.brain_name());
        }

        // Create edges: B→A, C→B, etc. (each task depends on the previous)
        let mut succeeded = Vec::new();
        for i in 1..resolved.len() {
            let task_id = &resolved[i];
            let depends_on = &resolved[i - 1];

            let event = TaskEvent::new(
                task_id,
                "mcp",
                EventType::DependencyAdded,
                &DependencyPayload {
                    depends_on_task_id: depends_on.clone(),
                },
            );

            match ctx.stores.tasks.append(&event) {
                Ok(()) => {
                    succeeded.push(json!({
                        "task_id": task_id,
                        "depends_on_task_id": depends_on,
                    }));
                }
                Err(e) => {
                    failed.push(json!({
                        "task_id": task_id,
                        "depends_on_task_id": depends_on,
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        batch_response(succeeded, failed, ctx.brain_name())
    }

    fn dep_fan(&self, ctx: &McpContext, params: &Params) -> ToolCallResult {
        let source = match &params.source_task_id {
            Some(id) if !id.is_empty() => id,
            _ => return ToolCallResult::error("Missing required parameter: source_task_id"),
        };
        let dependents = match &params.dependent_task_ids {
            Some(ids) => ids,
            None => return ToolCallResult::error("Missing required parameter: dependent_task_ids"),
        };

        if dependents.is_empty() {
            return batch_response(vec![], vec![], ctx.brain_name());
        }

        let source_input = resolve_id(source);
        let source_resolved = match ctx.stores.tasks.resolve_task_id(&source_input) {
            Ok(id) => id,
            Err(e) => {
                return ToolCallResult::error(format!("Failed to resolve source_task_id: {e}"));
            }
        };

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();

        for raw_id in dependents {
            let dep_input = resolve_id(raw_id);
            let dep_id = match ctx.stores.tasks.resolve_task_id(&dep_input) {
                Ok(id) => id,
                Err(e) => {
                    failed.push(json!({
                        "task_id": raw_id,
                        "depends_on_task_id": source_resolved,
                        "error": format!("{e}"),
                    }));
                    continue;
                }
            };

            let event = TaskEvent::new(
                &dep_id,
                "mcp",
                EventType::DependencyAdded,
                &DependencyPayload {
                    depends_on_task_id: source_resolved.clone(),
                },
            );

            match ctx.stores.tasks.append(&event) {
                Ok(()) => {
                    succeeded.push(json!({
                        "task_id": dep_id,
                        "depends_on_task_id": source_resolved,
                    }));
                }
                Err(e) => {
                    failed.push(json!({
                        "task_id": dep_id,
                        "depends_on_task_id": source_resolved,
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        batch_response(succeeded, failed, ctx.brain_name())
    }

    fn dep_clear(&self, ctx: &McpContext, params: &Params) -> ToolCallResult {
        let task_id = match &params.task_id {
            Some(id) if !id.is_empty() => id,
            _ => return ToolCallResult::error("Missing required parameter: task_id"),
        };

        let task_id_input = resolve_id(task_id);
        let resolved = match ctx.stores.tasks.resolve_task_id(&task_id_input) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve task_id: {e}")),
        };

        let deps = match ctx.stores.tasks.get_deps_for_task(&resolved) {
            Ok(d) => d,
            Err(e) => {
                return ToolCallResult::error(format!("Failed to query dependencies: {e}"));
            }
        };

        if deps.is_empty() {
            return batch_response(vec![], vec![], ctx.brain_name());
        }

        let events: Vec<TaskEvent> = deps
            .iter()
            .map(|dep| {
                TaskEvent::new(
                    &resolved,
                    "mcp",
                    EventType::DependencyRemoved,
                    &DependencyPayload {
                        depends_on_task_id: dep.clone(),
                    },
                )
            })
            .collect();

        let results = ctx.stores.tasks.append_batch(&events);
        let mut succeeded = Vec::new();
        let mut failed = Vec::new();

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(()) => {
                    succeeded.push(json!({
                        "task_id": resolved,
                        "depends_on_task_id": deps[i],
                    }));
                }
                Err(e) => {
                    failed.push(json!({
                        "task_id": resolved,
                        "depends_on_task_id": deps[i],
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        batch_response(succeeded, failed, ctx.brain_name())
    }
}

fn batch_response(mut succeeded: Vec<Value>, failed: Vec<Value>, brain_name: &str) -> ToolCallResult {
    // Add uri to each succeeded item that has a task_id
    for item in &mut succeeded {
        if let Some(obj) = item.as_object_mut()
            && let Some(task_id) = obj.get("task_id").and_then(|v| v.as_str()).map(String::from)
        {
            let uri = SynapseUri::for_task(brain_name, &task_id).to_string();
            obj.insert("uri".into(), json!(uri));
        }
    }
    let response = json!({
        "succeeded": succeeded,
        "failed": failed,
        "summary": {
            "succeeded": succeeded.len(),
            "failed": failed.len(),
        },
    });
    json_response(&response)
}

impl McpTool for TaskDepsBatch {
    fn name(&self) -> &'static str {
        "tasks.deps_batch"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Batch dependency operations on tasks. Supports add/remove pairs, chain (sequential dependencies), fan (multiple tasks depend on one), and clear (remove all deps for a task). Returns succeeded/failed/summary.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["add", "remove", "chain", "fan", "clear"],
                        "description": "The batch operation to perform"
                    },
                    "pairs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "task_id": { "type": "string" },
                                "depends_on_task_id": { "type": "string" }
                            },
                            "required": ["task_id", "depends_on_task_id"]
                        },
                        "description": "Dependency pairs for add/remove. Pass as a JSON array of objects, e.g. [{\"task_id\": \"BRN-02\", \"depends_on_task_id\": \"BRN-01\"}]"
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Ordered task IDs for chain (at least 2). Each task depends on the previous. Pass as a JSON array, e.g. [\"BRN-01\", \"BRN-02\", \"BRN-03\"]"
                    },
                    "source_task_id": {
                        "type": "string",
                        "description": "Source task for fan (the one others depend on)"
                    },
                    "dependent_task_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tasks that depend on the source (required for fan). Pass as a JSON array, e.g. [\"BRN-02\", \"BRN-03\"]"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID for clear (removes all its dependencies)"
                    }
                },
                "required": ["action"]
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

    async fn dispatch(
        registry: &ToolRegistry,
        name: &str,
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        registry.dispatch(name, params, ctx).await
    }

    async fn create_tasks(registry: &ToolRegistry, ctx: &crate::mcp::McpContext) {
        for (id, title) in &[
            ("t1", "Task 1"),
            ("t2", "Task 2"),
            ("t3", "Task 3"),
            ("t4", "Task 4"),
        ] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            dispatch(registry, "tasks.apply_event", p, ctx).await;
        }
    }

    #[tokio::test]
    async fn test_batch_add_deps() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let params = json!({
            "action": "add",
            "pairs": [
                { "task_id": "t2", "depends_on_task_id": "t1" },
                { "task_id": "t3", "depends_on_task_id": "t1" }
            ]
        });
        let result = dispatch(&registry, "tasks.deps_batch", params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 2);
        assert_eq!(parsed["summary"]["failed"], 0);
    }

    #[tokio::test]
    async fn test_batch_remove_deps() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // Add deps first
        let add = json!({
            "action": "add",
            "pairs": [
                { "task_id": "t2", "depends_on_task_id": "t1" },
                { "task_id": "t3", "depends_on_task_id": "t1" }
            ]
        });
        dispatch(&registry, "tasks.deps_batch", add, &ctx).await;

        // Remove them
        let remove = json!({
            "action": "remove",
            "pairs": [
                { "task_id": "t2", "depends_on_task_id": "t1" },
                { "task_id": "t3", "depends_on_task_id": "t1" }
            ]
        });
        let result = dispatch(&registry, "tasks.deps_batch", remove, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 2);
    }

    #[tokio::test]
    async fn test_dep_chain() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let params = json!({
            "action": "chain",
            "task_ids": ["t1", "t2", "t3"]
        });
        let result = dispatch(&registry, "tasks.deps_batch", params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 2); // t2→t1, t3→t2
        assert_eq!(parsed["summary"]["failed"], 0);

        // t2 should depend on t1
        let deps = ctx.stores.tasks.get_deps_for_task("t2").unwrap();
        assert!(deps.contains(&"t1".to_string()));

        // t3 should depend on t2
        let deps = ctx.stores.tasks.get_deps_for_task("t3").unwrap();
        assert!(deps.contains(&"t2".to_string()));
    }

    #[tokio::test]
    async fn test_chain_too_few_tasks() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "action": "chain",
            "task_ids": ["t1"]
        });
        let result = dispatch(&registry, "tasks.deps_batch", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("at least 2"));
    }

    #[tokio::test]
    async fn test_dep_fan() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let params = json!({
            "action": "fan",
            "source_task_id": "t1",
            "dependent_task_ids": ["t2", "t3", "t4"]
        });
        let result = dispatch(&registry, "tasks.deps_batch", params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 3);

        // All should depend on t1
        for tid in &["t2", "t3", "t4"] {
            let deps = ctx.stores.tasks.get_deps_for_task(tid).unwrap();
            assert!(deps.contains(&"t1".to_string()));
        }
    }

    #[tokio::test]
    async fn test_dep_clear() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // Add multiple deps to t3
        let add = json!({
            "action": "add",
            "pairs": [
                { "task_id": "t3", "depends_on_task_id": "t1" },
                { "task_id": "t3", "depends_on_task_id": "t2" }
            ]
        });
        dispatch(&registry, "tasks.deps_batch", add, &ctx).await;

        // Clear all deps from t3
        let clear = json!({
            "action": "clear",
            "task_id": "t3"
        });
        let result = dispatch(&registry, "tasks.deps_batch", clear, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 2);

        let deps = ctx.stores.tasks.get_deps_for_task("t3").unwrap();
        assert!(deps.is_empty());
    }

    #[tokio::test]
    async fn test_clear_no_deps() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let clear = json!({
            "action": "clear",
            "task_id": "t1"
        });
        let result = dispatch(&registry, "tasks.deps_batch", clear, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 0);
        assert_eq!(parsed["summary"]["failed"], 0);
    }

    #[tokio::test]
    async fn test_cycle_detection_in_chain() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // First create t2→t1
        let add = json!({
            "action": "add",
            "pairs": [{ "task_id": "t2", "depends_on_task_id": "t1" }]
        });
        dispatch(&registry, "tasks.deps_batch", add, &ctx).await;

        // Now try chain t2, t1 — t1→t2 would create a cycle
        let chain = json!({
            "action": "chain",
            "task_ids": ["t2", "t1"]
        });
        let result = dispatch(&registry, "tasks.deps_batch", chain, &ctx).await;
        assert!(result.is_error.is_none()); // partial success response

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["failed"], 1);
        assert!(
            parsed["failed"][0]["error"]
                .as_str()
                .unwrap()
                .contains("cycle")
        );
    }

    #[tokio::test]
    async fn test_invalid_action() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({ "action": "bogus" });
        let result = dispatch(&registry, "tasks.deps_batch", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid action"));
    }
}
