use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::cross_brain::{CrossBrainCreateParams, cross_brain_create};
use crate::tasks::events::TaskType;

use super::McpTool;
use super::json_response;

fn create_remote_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "brain": {
                "type": "string",
                "description": "Target brain name (from registry) or brain ID (8-char nanoid)"
            },
            "title": { "type": "string", "description": "Task title" },
            "description": { "type": "string", "description": "Task description" },
            "priority": {
                "type": "integer",
                "description": "Priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog). Default: 4",
                "default": 4
            },
            "task_type": {
                "type": "string",
                "description": "Task type (task|bug|feature|epic|spike). Default: task"
            },
            "assignee": { "type": "string", "description": "Assignee" },
            "parent": { "type": "string", "description": "Parent task ID in the remote brain" },
            "link_from": {
                "type": "string",
                "description": "Local task ID to create a cross-brain reference from. When provided, a cross_brain_ref_added event is appended to the local task."
            },
            "link_type": {
                "type": "string",
                "description": "Cross-brain ref type when linking (depends_on|blocks|related). Default: related"
            }
        },
        "required": ["brain", "title"]
    })
}

#[derive(Deserialize)]
struct Params {
    brain: String,
    title: String,
    description: Option<String>,
    #[serde(default = "default_priority")]
    priority: i32,
    task_type: Option<String>,
    assignee: Option<String>,
    parent: Option<String>,
    link_from: Option<String>,
    link_type: Option<String>,
}

fn default_priority() -> i32 {
    4
}

pub(super) struct TaskCreateRemote;

impl TaskCreateRemote {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
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

        // Validate link_type if provided
        if let Some(ref lt) = params.link_type
            && !matches!(lt.as_str(), "depends_on" | "blocks" | "related")
        {
            return ToolCallResult::error(format!(
                "Invalid link_type: '{lt}'. Must be one of: depends_on, blocks, related"
            ));
        }

        let cross_params = CrossBrainCreateParams {
            target_brain: params.brain,
            title: params.title,
            description: params.description,
            priority: params.priority,
            task_type,
            assignee: params.assignee,
            parent: params.parent,
            link_from: params.link_from,
            link_type: params.link_type,
        };

        match cross_brain_create(&ctx.tasks, cross_params) {
            Ok(result) => {
                let response = json!({
                    "remote_task_id": result.remote_task_id,
                    "remote_brain_name": result.remote_brain_name,
                    "remote_brain_id": result.remote_brain_id,
                    "local_ref_created": result.local_ref_created,
                });
                json_response(&response)
            }
            Err(e) => ToolCallResult::error(format!("Failed to create remote task: {e}")),
        }
    }
}

impl McpTool for TaskCreateRemote {
    fn name(&self) -> &'static str {
        "tasks.create_remote"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Create a task in another registered brain project. Resolves the target brain from the global registry (~/.brain/config.toml): tries registry name first, then falls back to matching by brain ID. Creates the task in the remote brain's event store (the task ID carries the remote brain's prefix). When link_from is provided (a local task ID), a cross_brain_ref_added event is appended to that local task, linking it to the newly created remote task; link_type controls the ref direction (depends_on|blocks|related, default: related). Returns remote_task_id, remote_brain_name, remote_brain_id, and local_ref_created. Use brains.list first to discover available brain names and prefixes.".into(),
            input_schema: create_remote_schema(),
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

    // --- Parameter validation tests (no real remote brain needed) ---
    //
    // Full integration tests (creating a task in an actual remote brain) require
    // a multi-brain setup with a real ~/.brain/config.toml registry entry.
    // Those are covered by `tasks/cross_brain.rs` tests. Here we focus on
    // the MCP-layer validation: missing required fields and invalid enum values.

    #[tokio::test]
    async fn test_create_remote_missing_brain() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // `brain` is required; omitting it should produce a deserialization error.
        let params = json!({ "title": "My task" });
        let result = dispatch(&registry, "tasks.create_remote", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("Invalid parameters"),
            "expected 'Invalid parameters', got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_create_remote_missing_title() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // `title` is required; omitting it should produce a deserialization error.
        let params = json!({ "brain": "infra" });
        let result = dispatch(&registry, "tasks.create_remote", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("Invalid parameters"),
            "expected 'Invalid parameters', got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_create_remote_invalid_task_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "brain": "infra",
            "title": "My task",
            "task_type": "story"
        });
        let result = dispatch(&registry, "tasks.create_remote", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("Invalid task_type"),
            "expected 'Invalid task_type', got: {}",
            result.content[0].text
        );
        assert!(result.content[0].text.contains("story"));
    }

    #[tokio::test]
    async fn test_create_remote_valid_task_types_pass_validation() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Valid task_type values must not trigger the validation error.
        // They will ultimately fail at the registry lookup stage (no real brain
        // named "infra" exists), but the task_type error must not appear.
        for tt in &["task", "bug", "feature", "epic", "spike"] {
            let params = json!({
                "brain": "infra",
                "title": "My task",
                "task_type": tt
            });
            let result = dispatch(&registry, "tasks.create_remote", params, &ctx).await;
            assert!(
                !result
                    .content
                    .first()
                    .map(|c| c.text.contains("Invalid task_type"))
                    .unwrap_or(false),
                "task_type '{tt}' should be valid but got error: {}",
                result.content[0].text
            );
        }
    }

    #[tokio::test]
    async fn test_create_remote_invalid_link_type() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "brain": "infra",
            "title": "My task",
            "link_type": "references"
        });
        let result = dispatch(&registry, "tasks.create_remote", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0].text.contains("Invalid link_type"),
            "expected 'Invalid link_type', got: {}",
            result.content[0].text
        );
        assert!(result.content[0].text.contains("references"));
    }

    #[tokio::test]
    async fn test_create_remote_valid_link_types_pass_validation() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Valid link_type values must not trigger the link_type validation error.
        for lt in &["depends_on", "blocks", "related"] {
            let params = json!({
                "brain": "infra",
                "title": "My task",
                "link_type": lt
            });
            let result = dispatch(&registry, "tasks.create_remote", params, &ctx).await;
            assert!(
                !result
                    .content
                    .first()
                    .map(|c| c.text.contains("Invalid link_type"))
                    .unwrap_or(false),
                "link_type '{lt}' should be valid but got link_type error: {}",
                result.content[0].text
            );
        }
    }

    #[tokio::test]
    async fn test_create_remote_invalid_brain_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // No real brain registry is set up in the test context, so any brain
        // lookup will fail. This verifies that the error is surfaced correctly.
        let params = json!({
            "brain": "nonexistent-brain",
            "title": "Should fail"
        });
        let result = dispatch(&registry, "tasks.create_remote", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0]
                .text
                .contains("Failed to create remote task"),
            "expected 'Failed to create remote task', got: {}",
            result.content[0].text
        );
    }
}
