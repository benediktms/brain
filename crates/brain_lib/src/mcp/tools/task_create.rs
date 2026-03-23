use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::events::{
    EventType, ExternalIdPayload, TaskCreatedPayload, TaskEvent, TaskStatus, TaskType, new_task_id,
};
use crate::tasks::queries::{MIN_SHORT_HASH_LEN, blake3_short_hex};
use crate::uri::SynapseUri;
use crate::utils::{parse_timestamp, task_row_to_json};

use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

fn create_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": "Task title"
            },
            "description": {
                "type": "string",
                "description": "Task description"
            },
            "priority": {
                "type": "integer",
                "description": "Priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog). Default: 4",
                "default": 4
            },
            "task_type": {
                "type": "string",
                "description": "Task type (task|bug|feature|epic|spike). Default: task"
            },
            "assignee": {
                "type": "string",
                "description": "Assignee"
            },
            "parent": {
                "type": "string",
                "description": "Parent task ID (full ID, short hash, or prefix). Prefer setting parent at creation time to avoid ambiguous prefix errors from post-hoc parent_set calls."
            },
            "due_ts": {
                "type": "string",
                "description": "Due date as ISO 8601 string"
            },
            "defer_until": {
                "type": "string",
                "description": "Defer until date as ISO 8601 string"
            },
            "actor": {
                "type": "string",
                "description": "Who is creating the task. Default: mcp",
                "default": "mcp"
            },
            "brain": {
                "type": "string",
                "description": "Target brain name or ID for cross-brain task creation. If omitted, creates in the current brain."
            },
            "link_from": {
                "type": "string",
                "description": "Local task ID to auto-create a cross-brain reference from (resolved via prefix)"
            },
            "link_type": {
                "type": "string",
                "description": "Type of cross-brain link: depends_on, blocks, or related (default: related)",
                "enum": ["depends_on", "blocks", "related"],
                "default": "related"
            }
        },
        "required": ["title"]
    })
}

#[derive(Deserialize)]
struct Params {
    title: String,
    description: Option<String>,
    #[serde(default = "default_priority")]
    priority: i32,
    task_type: Option<String>,
    assignee: Option<String>,
    parent: Option<String>,
    due_ts: Option<Value>,
    defer_until: Option<Value>,
    #[serde(default = "default_actor")]
    actor: String,
    brain: Option<String>,
    link_from: Option<String>,
    link_type: Option<String>,
}

fn default_priority() -> i32 {
    4
}

fn default_actor() -> String {
    "mcp".into()
}

pub(super) struct TaskCreate;

impl TaskCreate {
    async fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        // Reject task creation on archived brains
        match super::is_brain_archived(ctx.db(), ctx.brain_id()) {
            Ok(true) => {
                return ToolCallResult::error(
                    "Cannot create tasks: brain is archived. Use `brain link` to add a root and unarchive.",
                );
            }
            Err(e) => {
                return ToolCallResult::error(format!(
                    "Failed to check brain archived status: {e}"
                ));
            }
            Ok(false) => {}
        }

        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // Validate task_type if provided
        let task_type = if let Some(ref tt) = params.task_type {
            match tt.parse::<TaskType>() {
                Ok(t) => Some(t),
                Err(_) => {
                    return ToolCallResult::error(format!(
                        "Invalid task_type: '{tt}'. Must be one of: task, bug, feature, epic, spike"
                    ));
                }
            }
        } else {
            None
        };

        // Cross-brain path
        if let Some(ref brain) = params.brain {
            let (remote_brain_name, bid) = match ctx.resolve_brain_id(brain) {
                Ok(r) => r,
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to resolve brain: {e}"));
                }
            };
            // Guard: reject writes to archived brains
            match super::is_brain_archived(ctx.db(), &bid) {
                Ok(true) => {
                    return ToolCallResult::error(format!(
                        "Target brain '{remote_brain_name}' is archived"
                    ));
                }
                Ok(false) => {}
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to check archived status: {e}"));
                }
            }
            let remote_tasks = match crate::tasks::TaskStore::with_brain_id(
                ctx.db().clone(),
                &bid,
                &remote_brain_name,
            ) {
                Ok(t) => t,
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to open brain stores: {e}"));
                }
            };

            let mut warnings: Vec<Warning> = Vec::new();

            // Parse timestamps
            let due_ts = params.due_ts.as_ref().and_then(parse_timestamp);
            let defer_until = params.defer_until.as_ref().and_then(parse_timestamp);

            // Generate task ID using remote brain prefix
            let prefix = match remote_tasks.get_project_prefix() {
                Ok(p) => p,
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to get project prefix: {e}"));
                }
            };
            let task_id = new_task_id(&prefix);

            // Resolve parent task ID against remote brain if provided
            let parent_task_id = if let Some(ref parent) = params.parent {
                match remote_tasks.resolve_task_id(parent) {
                    Ok(resolved) => Some(resolved),
                    Err(e) => {
                        return ToolCallResult::error(format!(
                            "Failed to resolve parent task ID: {e}"
                        ));
                    }
                }
            } else {
                None
            };

            // Build and append the TaskCreated event to remote brain
            let payload = TaskCreatedPayload {
                title: params.title,
                description: params.description,
                priority: params.priority,
                status: TaskStatus::Open,
                due_ts,
                task_type,
                assignee: params.assignee,
                defer_until,
                parent_task_id,
                id: Some(blake3_short_hex(&task_id)[..MIN_SHORT_HASH_LEN].to_string()),
            };

            let event = TaskEvent::from_payload(&task_id, &params.actor, payload);

            if let Err(e) = remote_tasks.append(&event) {
                return ToolCallResult::error(format!("Task event failed: {e}"));
            }

            // Fetch resulting task state from remote brain
            let task_json = match remote_tasks.get_task(&task_id) {
                Ok(Some(row)) => {
                    let labels = store_or_warn(
                        remote_tasks.get_task_labels(&task_id),
                        "get_task_labels",
                        &mut warnings,
                    );
                    task_row_to_json(&row, labels)
                }
                Ok(None) => json!(null),
                Err(e) => {
                    warn!(error = %e, "failed to fetch task after event");
                    json!(null)
                }
            };

            let short_id = remote_tasks
                .compact_id(&task_id)
                .unwrap_or_else(|_| task_id.clone());

            // Optionally create bidirectional cross-brain references
            let mut local_ref_created = false;
            if let Some(ref link_from) = params.link_from {
                let local_task_id = match ctx.stores.tasks.resolve_task_id(link_from) {
                    Ok(id) => id,
                    Err(e) => {
                        return ToolCallResult::error(format!(
                            "Failed to resolve link_from task ID: {e}"
                        ));
                    }
                };

                let resolved_link_type = params.link_type.as_deref().unwrap_or("related");
                let link_source = format!("brain:{remote_brain_name}:{resolved_link_type}");
                let local_source = format!("brain:{}:{resolved_link_type}", ctx.brain_name());

                // ExternalIdAdded on the local task pointing to the remote task
                let local_ref_event = TaskEvent::new(
                    &local_task_id,
                    &params.actor,
                    EventType::ExternalIdAdded,
                    &ExternalIdPayload {
                        source: link_source.clone(),
                        external_id: task_id.clone(),
                        external_url: None,
                    },
                );
                if let Err(e) = ctx.stores.tasks.append(&local_ref_event) {
                    warn!(error = %e, "failed to create local cross-brain ref");
                } else {
                    // ExternalIdAdded on the remote task pointing back to the local task
                    let remote_ref_event = TaskEvent::new(
                        &task_id,
                        &params.actor,
                        EventType::ExternalIdAdded,
                        &ExternalIdPayload {
                            source: local_source,
                            external_id: local_task_id.clone(),
                            external_url: None,
                        },
                    );
                    if let Err(e) = remote_tasks.append(&remote_ref_event) {
                        warn!(error = %e, "failed to create remote cross-brain ref");
                    } else {
                        local_ref_created = true;
                    }
                }
            }

            let mut response = json!({
                "remote_task_id": short_id,
                "remote_brain_name": remote_brain_name,
                "remote_brain_id": bid,
                "task": task_json,
                "local_ref_created": local_ref_created,
            });

            inject_warnings(&mut response, warnings);

            return json_response(&response);
        }

        // Local path (unchanged)
        {
            let mut warnings: Vec<Warning> = Vec::new();

            // Parse timestamps
            let due_ts = params.due_ts.as_ref().and_then(parse_timestamp);
            let defer_until = params.defer_until.as_ref().and_then(parse_timestamp);

            // Generate task ID
            let prefix = match ctx.stores.tasks.get_project_prefix() {
                Ok(p) => p,
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to get project prefix: {e}"));
                }
            };
            let task_id = new_task_id(&prefix);

            // Resolve parent task ID if provided
            let parent_task_id = if let Some(ref parent) = params.parent {
                match ctx.stores.tasks.resolve_task_id(parent) {
                    Ok(resolved) => Some(resolved),
                    Err(e) => {
                        return ToolCallResult::error(format!(
                            "Failed to resolve parent task ID: {e}"
                        ));
                    }
                }
            } else {
                None
            };

            // Build and append the TaskCreated event
            let payload = TaskCreatedPayload {
                title: params.title,
                description: params.description,
                priority: params.priority,
                status: TaskStatus::Open,
                due_ts,
                task_type,
                assignee: params.assignee,
                defer_until,
                parent_task_id,
                id: None,
            };

            let event = TaskEvent::from_payload(&task_id, &params.actor, payload);

            if let Err(e) = ctx.stores.tasks.append(&event) {
                return ToolCallResult::error(format!("Task event failed: {e}"));
            }

            // Fetch resulting task state
            let task_json = match ctx.stores.tasks.get_task(&task_id) {
                Ok(Some(row)) => {
                    let labels = store_or_warn(
                        ctx.stores.tasks.get_task_labels(&task_id),
                        "get_task_labels",
                        &mut warnings,
                    );
                    task_row_to_json(&row, labels)
                }
                Ok(None) => json!(null),
                Err(e) => {
                    warn!(error = %e, "failed to fetch task after event");
                    json!(null)
                }
            };

            let short_id = ctx
                .stores
                .tasks
                .compact_id(&task_id)
                .unwrap_or_else(|_| task_id.clone());

            let uri = SynapseUri::for_task(ctx.brain_name(), &short_id).to_string();

            let mut response = json!({
                "task_id": short_id,
                "uri": uri,
                "task": task_json,
                "unblocked_task_ids": [],
            });

            inject_warnings(&mut response, warnings);

            json_response(&response)
        }
    }
}

impl McpTool for TaskCreate {
    fn name(&self) -> &'static str {
        "tasks.create"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Create a task in the current brain's event store (or a remote brain via the 'brain' parameter) and returns the resulting task state. Same as tasks.apply_event with event_type: task_created, but with a simpler flat schema. Defaults: priority=4 (backlog), status=open, actor=mcp. When 'brain' is provided, creates the task in the target brain and returns remote_task_id, remote_brain_name, remote_brain_id, and task. Optionally links a local task to the remote task via 'link_from'.".into(),
            input_schema: create_schema(),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move { self.execute(params, ctx).await })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::tests::create_test_context;
    use super::McpTool;
    use super::TaskCreate;

    fn mark_brain_archived(ctx: &crate::mcp::McpContext) {
        ctx.db()
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE brains SET archived = 1 WHERE brain_id = ?1",
                    [ctx.brain_id()],
                )?;
                Ok(())
            })
            .expect("failed to archive brain in test");
    }

    async fn call(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        TaskCreate.call(params, ctx).await
    }

    #[tokio::test]
    async fn test_create_local_basic() {
        let (_dir, ctx) = create_test_context().await;

        let params = json!({ "title": "My first task" });
        let result = call(params, &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert!(parsed["task_id"].is_string());
        assert!(!parsed["task_id"].as_str().unwrap().is_empty());
        assert_eq!(parsed["task"]["title"], "My first task");
        assert_eq!(parsed["task"]["status"], "open");
        assert_eq!(parsed["task"]["priority"], 4);
        assert_eq!(parsed["unblocked_task_ids"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_create_local_all_fields() {
        let (_dir, ctx) = create_test_context().await;

        let params = json!({
            "title": "Full task",
            "description": "A detailed description",
            "priority": 2,
            "task_type": "bug",
            "assignee": "alice",
            "due_ts": "2026-12-31T00:00:00Z",
            "defer_until": "2026-06-01T00:00:00Z",
            "actor": "test-actor"
        });
        let result = call(params, &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["title"], "Full task");
        assert_eq!(parsed["task"]["description"], "A detailed description");
        assert_eq!(parsed["task"]["priority"], 2);
        assert_eq!(parsed["task"]["task_type"], "bug");
        assert_eq!(parsed["task"]["assignee"], "alice");
        assert!(parsed["task"]["due_ts"].is_string());
        assert!(parsed["task"]["defer_until"].is_string());
    }

    #[tokio::test]
    async fn test_create_local_auto_id() {
        let (_dir, ctx) = create_test_context().await;

        let params = json!({ "title": "Auto ID task" });
        let result = call(params, &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let task_id = parsed["task_id"].as_str().unwrap();
        // Should contain a dash (prefix-ULID format)
        assert!(
            task_id.contains('-'),
            "task_id should have prefix: {task_id}"
        );
    }

    #[tokio::test]
    async fn test_create_local_invalid_task_type() {
        let (_dir, ctx) = create_test_context().await;

        let params = json!({
            "title": "My task",
            "task_type": "story"
        });
        let result = call(params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("Invalid task_type"),
            "expected 'Invalid task_type', got: {}",
            result.content[0].text
        );
        assert!(result.content[0].text.contains("story"));
    }

    #[tokio::test]
    async fn test_create_local_parent_resolution() {
        let (_dir, ctx) = create_test_context().await;

        // Create parent task first
        let parent_params = json!({ "title": "Parent task" });
        let parent_result = call(parent_params, &ctx).await;
        assert!(parent_result.is_error.is_none());
        let parent_parsed: Value = serde_json::from_str(&parent_result.content[0].text).unwrap();
        let parent_id = parent_parsed["task_id"].as_str().unwrap().to_string();

        // Create child with parent ID
        let child_params = json!({
            "title": "Child task",
            "parent": parent_id
        });
        let child_result = call(child_params, &ctx).await;
        assert!(
            child_result.is_error.is_none(),
            "child creation should succeed: {}",
            child_result.content[0].text
        );
        let child_parsed: Value = serde_json::from_str(&child_result.content[0].text).unwrap();
        assert_eq!(child_parsed["task"]["title"], "Child task");
    }

    #[tokio::test]
    async fn test_create_archived_brain_rejected() {
        let (_dir, ctx) = create_test_context().await;

        mark_brain_archived(&ctx);

        let params = json!({ "title": "Should be blocked" });
        let result = call(params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("archived"),
            "expected archived error, got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        // Verify the tool reports the correct underscore alias
        let tool = TaskCreate;
        assert_eq!(tool.underscore_alias(), "tasks_create");
    }

    #[tokio::test]
    async fn test_schema_includes_brain_params() {
        let tool = TaskCreate;
        let schema = tool.definition().input_schema;
        let props = &schema["properties"];
        assert!(props["brain"].is_object(), "schema must include 'brain'");
        assert!(
            props["link_from"].is_object(),
            "schema must include 'link_from'"
        );
        assert!(
            props["link_type"].is_object(),
            "schema must include 'link_type'"
        );
        // link_type must have enum values
        let enum_vals = props["link_type"]["enum"].as_array().unwrap();
        let enum_strs: Vec<&str> = enum_vals.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(enum_strs.contains(&"depends_on"));
        assert!(enum_strs.contains(&"blocks"));
        assert!(enum_strs.contains(&"related"));
    }

    #[tokio::test]
    async fn test_params_deserialize_without_brain() {
        // Params without brain fields should deserialize correctly (local path)
        let raw = json!({ "title": "No brain param" });
        let params: super::Params = serde_json::from_value(raw).unwrap();
        assert!(params.brain.is_none());
        assert!(params.link_from.is_none());
        assert!(params.link_type.is_none());
    }

    #[tokio::test]
    async fn test_params_deserialize_with_brain() {
        // Params with brain fields should deserialize correctly
        let raw = json!({
            "title": "Remote task",
            "brain": "some-brain",
            "link_from": "LOCAL-01ABC",
            "link_type": "depends_on"
        });
        let params: super::Params = serde_json::from_value(raw).unwrap();
        assert_eq!(params.brain.as_deref(), Some("some-brain"));
        assert_eq!(params.link_from.as_deref(), Some("LOCAL-01ABC"));
        assert_eq!(params.link_type.as_deref(), Some("depends_on"));
    }

    #[tokio::test]
    async fn test_create_remote_brain_not_found() {
        // Passing an unknown brain name must return an error
        let (_dir, ctx) = create_test_context().await;

        let params = json!({
            "title": "Remote task",
            "brain": "nonexistent-brain-xyz"
        });
        let result = call(params, &ctx).await;
        assert_eq!(
            result.is_error,
            Some(true),
            "unknown brain should return error"
        );
        assert!(
            result.content[0].text.contains("Failed to resolve brain"),
            "error should mention brain resolution: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_local_path_unaffected_by_new_params() {
        // Ensure existing local creation still works after adding brain/link params
        let (_dir, ctx) = create_test_context().await;

        let params = json!({
            "title": "Still local",
            "priority": 1,
            "assignee": "drone"
        });
        let result = call(params, &ctx).await;
        assert!(
            result.is_error.is_none(),
            "local creation must still succeed: {:?}",
            result.content
        );

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert!(parsed["task_id"].is_string());
        assert_eq!(parsed["task"]["title"], "Still local");
        assert_eq!(parsed["task"]["assignee"], "drone");
        // Local response has task_id, not remote_task_id
        assert!(parsed.get("remote_task_id").is_none());
    }
}
