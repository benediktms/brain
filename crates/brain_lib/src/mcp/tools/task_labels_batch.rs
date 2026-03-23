use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::TaskStore;
use crate::tasks::events::{EventType, LabelPayload, TaskEvent};
use crate::uri::{SynapseUri, resolve_id};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    action: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    task_ids: Option<Vec<String>>,
    #[serde(default)]
    old_label: Option<String>,
    #[serde(default)]
    new_label: Option<String>,
    brain: Option<String>,
}

pub(super) struct TaskLabelsBatch;

impl TaskLabelsBatch {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // Remote brain path
        if let Some(ref brain) = params.brain {
            let (brain_name, bid) = match ctx.resolve_brain_id(brain) {
                Ok(r) => r,
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to resolve brain: {e}"));
                }
            };
            let remote_tasks =
                match crate::tasks::TaskStore::with_brain_id(ctx.db().clone(), &bid, &brain_name) {
                    Ok(t) => t,
                    Err(e) => {
                        return ToolCallResult::error(format!("Failed to open brain stores: {e}"));
                    }
                };
            return self.execute_with_store(params, &remote_tasks, &brain_name);
        }

        self.execute_with_store(params, &ctx.stores.tasks, ctx.brain_name())
    }

    fn execute_with_store(
        &self,
        params: Params,
        store: &TaskStore,
        brain_name: &str,
    ) -> ToolCallResult {
        match params.action.as_str() {
            "add" => self.label_add_remove(store, &params, EventType::LabelAdded, brain_name),
            "remove" => self.label_add_remove(store, &params, EventType::LabelRemoved, brain_name),
            "rename" => self.label_rename(store, &params, brain_name),
            "purge" => self.label_purge(store, &params, brain_name),
            other => ToolCallResult::error(format!(
                "Invalid action: '{other}'. Must be one of: add, remove, rename, purge"
            )),
        }
    }

    fn label_add_remove(
        &self,
        store: &TaskStore,
        params: &Params,
        event_type: EventType,
        brain_name: &str,
    ) -> ToolCallResult {
        let label = match &params.label {
            Some(l) if !l.is_empty() => l,
            _ => return ToolCallResult::error("Missing required parameter: label"),
        };
        let task_ids = match &params.task_ids {
            Some(ids) => ids,
            None => return ToolCallResult::error("Missing required parameter: task_ids"),
        };

        if task_ids.is_empty() {
            return batch_response(vec![], vec![]);
        }

        let mut events = Vec::new();
        let mut failed = Vec::new();

        for raw_id in task_ids {
            let resolved_input = resolve_id(raw_id);
            match store.resolve_task_id(&resolved_input) {
                Ok(resolved) => {
                    events.push(TaskEvent::new(
                        &resolved,
                        "mcp",
                        event_type.clone(),
                        &LabelPayload {
                            label: label.to_string(),
                        },
                    ));
                }
                Err(e) => {
                    failed.push(json!({
                        "task_id": raw_id,
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        let results = store.append_batch(&events);
        let mut succeeded = Vec::new();
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(()) => {
                    let tid = &events[i].task_id;
                    let uri = SynapseUri::for_task(brain_name, tid).to_string();
                    succeeded.push(json!({ "task_id": tid, "uri": uri }));
                }
                Err(e) => {
                    failed.push(json!({
                        "task_id": events[i].task_id,
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        batch_response(succeeded, failed)
    }

    fn label_rename(&self, store: &TaskStore, params: &Params, brain_name: &str) -> ToolCallResult {
        let old_label = match &params.old_label {
            Some(l) if !l.is_empty() => l,
            _ => return ToolCallResult::error("Missing required parameter: old_label"),
        };
        let new_label = match &params.new_label {
            Some(l) if !l.is_empty() => l,
            _ => return ToolCallResult::error("Missing required parameter: new_label"),
        };

        let task_ids = match store.get_task_ids_with_label(old_label) {
            Ok(ids) => ids,
            Err(e) => return ToolCallResult::error(format!("Failed to query tasks: {e}")),
        };

        if task_ids.is_empty() {
            return batch_response(vec![], vec![]);
        }

        let mut events = Vec::new();
        for tid in &task_ids {
            events.push(TaskEvent::new(
                tid,
                "mcp",
                EventType::LabelRemoved,
                &LabelPayload {
                    label: old_label.to_string(),
                },
            ));
            events.push(TaskEvent::new(
                tid,
                "mcp",
                EventType::LabelAdded,
                &LabelPayload {
                    label: new_label.to_string(),
                },
            ));
        }

        let results = store.append_batch(&events);
        let mut succeeded = Vec::new();
        let mut failed = Vec::new();

        // Results come in pairs (remove + add) per task
        for (i, tid) in task_ids.iter().enumerate() {
            let remove_idx = i * 2;
            let add_idx = i * 2 + 1;
            let remove_ok = results[remove_idx].is_ok();
            let add_ok = results[add_idx].is_ok();

            if remove_ok && add_ok {
                let uri = SynapseUri::for_task(brain_name, tid).to_string();
                succeeded.push(json!({ "task_id": tid, "uri": uri }));
            } else {
                let mut errors = Vec::new();
                if let Err(e) = &results[remove_idx] {
                    errors.push(format!("remove: {e}"));
                }
                if let Err(e) = &results[add_idx] {
                    errors.push(format!("add: {e}"));
                }
                failed.push(json!({
                    "task_id": tid,
                    "error": errors.join("; "),
                }));
            }
        }

        batch_response(succeeded, failed)
    }

    fn label_purge(&self, store: &TaskStore, params: &Params, brain_name: &str) -> ToolCallResult {
        let label = match &params.label {
            Some(l) if !l.is_empty() => l,
            _ => return ToolCallResult::error("Missing required parameter: label"),
        };

        let task_ids = match store.get_task_ids_with_label(label) {
            Ok(ids) => ids,
            Err(e) => return ToolCallResult::error(format!("Failed to query tasks: {e}")),
        };

        if task_ids.is_empty() {
            return batch_response(vec![], vec![]);
        }

        let events: Vec<TaskEvent> = task_ids
            .iter()
            .map(|tid| {
                TaskEvent::new(
                    tid,
                    "mcp",
                    EventType::LabelRemoved,
                    &LabelPayload {
                        label: label.to_string(),
                    },
                )
            })
            .collect();

        let results = store.append_batch(&events);
        let mut succeeded = Vec::new();
        let mut failed = Vec::new();

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(()) => {
                    let tid = &task_ids[i];
                    let uri = SynapseUri::for_task(brain_name, tid).to_string();
                    succeeded.push(json!({ "task_id": tid, "uri": uri }));
                }
                Err(e) => {
                    failed.push(json!({
                        "task_id": task_ids[i],
                        "error": format!("{e}"),
                    }));
                }
            }
        }

        batch_response(succeeded, failed)
    }
}

fn batch_response(succeeded: Vec<Value>, failed: Vec<Value>) -> ToolCallResult {
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

impl McpTool for TaskLabelsBatch {
    fn name(&self) -> &'static str {
        "tasks.labels_batch"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Batch label operations on tasks. Supports add/remove labels across multiple tasks, rename a label globally, or purge a label from all tasks. Returns succeeded/failed/summary.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["add", "remove", "rename", "purge"],
                        "description": "The batch operation to perform"
                    },
                    "label": {
                        "type": "string",
                        "description": "Label name (required for add, remove, purge)"
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Task IDs (full or prefix). Required for add/remove. Pass as a JSON array, e.g. [\"BRN-01JPH\", \"BRN-02ABC\"]"
                    },
                    "old_label": {
                        "type": "string",
                        "description": "Current label name (required for rename)"
                    },
                    "new_label": {
                        "type": "string",
                        "description": "New label name (required for rename)"
                    },
                    "brain": {
                        "type": "string",
                        "description": "Target brain name or ID. When provided, operates on that brain's task store instead of locally."
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
        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2"), ("t3", "Task 3")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            dispatch(registry, "tasks.apply_event", p, ctx).await;
        }
    }

    #[tokio::test]
    async fn test_batch_add_labels() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let params = json!({
            "action": "add",
            "label": "urgent",
            "task_ids": ["t1", "t2", "t3"]
        });
        let result = dispatch(&registry, "tasks.labels_batch", params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 3);
        assert_eq!(parsed["summary"]["failed"], 0);

        // Verify labels were actually added
        let labels = ctx.stores.tasks.get_task_labels("t1").unwrap();
        assert!(labels.contains(&"urgent".to_string()));
    }

    #[tokio::test]
    async fn test_batch_remove_labels() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // Add label first
        let add = json!({
            "action": "add",
            "label": "old-label",
            "task_ids": ["t1", "t2"]
        });
        dispatch(&registry, "tasks.labels_batch", add, &ctx).await;

        // Remove
        let remove = json!({
            "action": "remove",
            "label": "old-label",
            "task_ids": ["t1", "t2"]
        });
        let result = dispatch(&registry, "tasks.labels_batch", remove, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 2);

        let labels = ctx.stores.tasks.get_task_labels("t1").unwrap();
        assert!(!labels.contains(&"old-label".to_string()));
    }

    #[tokio::test]
    async fn test_label_rename() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // Add label to all tasks
        let add = json!({
            "action": "add",
            "label": "old-name",
            "task_ids": ["t1", "t2", "t3"]
        });
        dispatch(&registry, "tasks.labels_batch", add, &ctx).await;

        // Rename
        let rename = json!({
            "action": "rename",
            "old_label": "old-name",
            "new_label": "new-name"
        });
        let result = dispatch(&registry, "tasks.labels_batch", rename, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 3);

        // Verify old label gone, new label present
        let labels = ctx.stores.tasks.get_task_labels("t1").unwrap();
        assert!(!labels.contains(&"old-name".to_string()));
        assert!(labels.contains(&"new-name".to_string()));
    }

    #[tokio::test]
    async fn test_label_purge() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // Add label
        let add = json!({
            "action": "add",
            "label": "doomed",
            "task_ids": ["t1", "t2"]
        });
        dispatch(&registry, "tasks.labels_batch", add, &ctx).await;

        // Purge
        let purge = json!({
            "action": "purge",
            "label": "doomed"
        });
        let result = dispatch(&registry, "tasks.labels_batch", purge, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 2);

        let ids = ctx.stores.tasks.get_task_ids_with_label("doomed").unwrap();
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn test_purge_no_matching_tasks() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let purge = json!({
            "action": "purge",
            "label": "nonexistent"
        });
        let result = dispatch(&registry, "tasks.labels_batch", purge, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 0);
        assert_eq!(parsed["summary"]["failed"], 0);
    }

    #[tokio::test]
    async fn test_batch_with_invalid_task_id() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let params = json!({
            "action": "add",
            "label": "test",
            "task_ids": ["t1", "nonexistent", "t3"]
        });
        let result = dispatch(&registry, "tasks.labels_batch", params, &ctx).await;
        assert!(result.is_error.is_none()); // partial success, not an error

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 2);
        assert_eq!(parsed["summary"]["failed"], 1);
    }

    #[tokio::test]
    async fn test_empty_task_ids() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "action": "add",
            "label": "test",
            "task_ids": []
        });
        let result = dispatch(&registry, "tasks.labels_batch", params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["summary"]["succeeded"], 0);
        assert_eq!(parsed["summary"]["failed"], 0);
    }

    #[tokio::test]
    async fn test_invalid_action() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({ "action": "bogus" });
        let result = dispatch(&registry, "tasks.labels_batch", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid action"));
    }
}
