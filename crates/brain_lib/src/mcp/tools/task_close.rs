use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tasks::events::{StatusChangedPayload, TaskEvent, TaskStatus};
use crate::uri::{SynapseUri, resolve_id};

use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

/// Accepts either a single string or an array of strings.
#[derive(Deserialize)]
#[serde(untagged)]
enum TaskIds {
    Single(String),
    Multiple(Vec<String>),
}

impl TaskIds {
    fn into_vec(self) -> Vec<String> {
        match self {
            TaskIds::Single(s) => vec![s],
            TaskIds::Multiple(v) => v,
        }
    }
}

#[derive(Deserialize)]
struct Params {
    task_ids: TaskIds,
}

const NORMALIZE_SOURCE: &str = "tasks.close:task_ids";

fn expand_task_id_entry(entry: &str, warnings: &mut Vec<Warning>) -> Result<Vec<String>, String> {
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return Err("task_id entry is empty".into());
    }

    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        match serde_json::from_str::<Vec<String>>(trimmed) {
            Ok(values) => {
                if values.is_empty() {
                    return Err("task_id JSON array was empty".into());
                }
                warnings.push(Warning {
                    source: NORMALIZE_SOURCE.into(),
                    error: format!(
                        "Detected JSON array encoded as string; expanding {trimmed} into {} entries",
                        values.len()
                    ),
                });
                return Ok(values.into_iter().map(|v| v.trim().to_string()).collect());
            }
            Err(e) => {
                return Err(format!("Failed to parse task_id entry as JSON array: {e}"));
            }
        }
    }

    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        match serde_json::from_str::<String>(trimmed) {
            Ok(value) => {
                warnings.push(Warning {
                    source: NORMALIZE_SOURCE.into(),
                    error: format!("Detected JSON string encoded task_id; expanding {trimmed}"),
                });
                return Ok(vec![value]);
            }
            Err(e) => {
                return Err(format!("Failed to parse task_id entry as JSON string: {e}"));
            }
        }
    }

    Ok(vec![trimmed.to_string()])
}

fn normalize_single_id(id: &str) -> Result<String, String> {
    let trimmed = id.trim();
    if trimmed.is_empty() {
        return Err("task_id entry resolved to empty string".into());
    }
    Ok(resolve_id(trimmed))
}

pub(super) struct TaskClose;

impl TaskClose {
    fn execute_inner(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => {
                return ToolCallResult::error(format!("Invalid parameters: {e}"));
            }
        };

        let ids = params.task_ids.into_vec();
        if ids.is_empty() {
            return ToolCallResult::error("task_ids must not be empty");
        }

        let mut closed = Vec::new();
        let mut failed = Vec::new();
        let mut total_unblocked = 0usize;
        let mut warnings: Vec<Warning> = Vec::new();

        let mut normalized_ids: Vec<String> = Vec::new();

        for raw_id in ids {
            match expand_task_id_entry(&raw_id, &mut warnings) {
                Ok(entries) => {
                    for entry in entries {
                        match normalize_single_id(&entry) {
                            Ok(normalized) => normalized_ids.push(normalized),
                            Err(err) => {
                                failed.push(json!({
                                    "task_id": entry,
                                    "input": raw_id,
                                    "error": err,
                                }));
                            }
                        }
                    }
                }
                Err(err) => {
                    failed.push(json!({
                        "task_id": raw_id,
                        "error": err,
                    }));
                }
            }
        }

        for normalized in normalized_ids {
            let resolved = match ctx.stores.tasks.resolve_task_id(&normalized) {
                Ok(r) => r,
                Err(e) => {
                    failed.push(json!({
                        "task_id": normalized,
                        "error": format!("{e}"),
                    }));
                    continue;
                }
            };

            let event = TaskEvent::from_payload(
                &resolved,
                "mcp",
                StatusChangedPayload {
                    new_status: TaskStatus::Done,
                },
            );

            if let Err(e) = ctx.stores.tasks.append(&event) {
                failed.push(json!({
                    "task_id": resolved,
                    "error": format!("{e}"),
                }));
                continue;
            }

            let unblocked: Vec<String> = store_or_warn(
                ctx.stores.tasks.list_newly_unblocked(&resolved),
                "list_newly_unblocked",
                &mut warnings,
            )
            .iter()
            .map(|id| ctx.stores.tasks.compact_id(id).unwrap_or(id.clone()))
            .collect();
            let short_id = ctx
                .stores
                .tasks
                .compact_id(&resolved)
                .unwrap_or(resolved.clone());

            let uri = SynapseUri::for_task(ctx.brain_name(), &short_id).to_string();
            total_unblocked += unblocked.len();
            closed.push(json!({
                "task_id": short_id,
                "uri": uri,
                "unblocked_task_ids": unblocked,
            }));
        }

        let mut response = json!({
            "closed": closed,
            "failed": failed,
            "summary": {
                "closed": closed.len(),
                "failed": failed.len(),
                "unblocked": total_unblocked,
            },
        });
        inject_warnings(&mut response, warnings);
        json_response(&response)
    }
}

impl McpTool for TaskClose {
    fn name(&self) -> &'static str {
        "tasks.close"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Close one or more tasks (set status to done). Returns closed tasks and any newly unblocked task IDs. Partial failures (e.g. invalid IDs) return a success response with separate 'closed' and 'failed' arrays. Convenience shortcut for tasks.apply_event with status_changed.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_ids": {
                        "description": "Task ID or array of task IDs to close. Accepts full IDs or unique prefixes.",
                        "oneOf": [
                            { "type": "string" },
                            { "type": "array", "items": { "type": "string" } }
                        ]
                    }
                },
                "required": ["task_ids"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move { self.execute_inner(params, ctx) })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;
    use super::{Warning, expand_task_id_entry, normalize_single_id};

    /// Compute the expected compact ID for a task created via in_memory stores.
    /// In-memory stores use brain_id = "" which maps to the "(unscoped)" sentinel
    /// brain inserted by migration v21→v22. That brain's prefix is "NSX" (derived
    /// from generate_prefix("(unscoped)")), so compact IDs are "nsx-{hash}".
    fn compact_id_for(task_id: &str) -> String {
        let hex = blake3::hash(task_id.as_bytes()).to_hex().to_string();
        format!("nsx-{}", &hex[..3])
    }

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
    async fn test_close_single_string() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let result = dispatch(&registry, "tasks.close", json!({ "task_ids": "t1" }), &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["summary"]["closed"], 1);
        assert_eq!(parsed["summary"]["failed"], 0);
        assert_eq!(parsed["closed"][0]["task_id"], compact_id_for("t1"));

        // Verify task is actually done
        let task = ctx
            .stores
            .tasks
            .get_task("t1")
            .expect("checked in test assertions")
            .expect("checked in test assertions");
        assert_eq!(task.status, "done");
    }

    #[tokio::test]
    async fn test_close_multiple() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let result = dispatch(
            &registry,
            "tasks.close",
            json!({ "task_ids": ["t1", "t2"] }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["summary"]["closed"], 2);
    }

    #[tokio::test]
    async fn test_close_with_unblocked() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        // t2 depends on t1
        dispatch(
            &registry,
            "tasks.apply_event",
            json!({
                "event_type": "dependency_added",
                "task_id": "t2",
                "payload": { "depends_on_task_id": "t1" }
            }),
            &ctx,
        )
        .await;

        let result = dispatch(&registry, "tasks.close", json!({ "task_ids": "t1" }), &ctx).await;
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["summary"]["unblocked"], 1);
        assert!(
            parsed["closed"][0]["unblocked_task_ids"]
                .as_array()
                .expect("checked in test assertions")
                .contains(&json!(compact_id_for("t2")))
        );
    }

    #[tokio::test]
    async fn test_close_partial_failure() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks(&registry, &ctx).await;

        let result = dispatch(
            &registry,
            "tasks.close",
            json!({ "task_ids": ["t1", "nonexistent"] }),
            &ctx,
        )
        .await;
        assert!(result.is_error.is_none()); // partial success

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["summary"]["closed"], 1);
        assert_eq!(parsed["summary"]["failed"], 1);
    }

    #[tokio::test]
    async fn test_close_empty_array() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = dispatch(&registry, "tasks.close", json!({ "task_ids": [] }), &ctx).await;
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("must not be empty"));
    }

    #[test]
    fn expand_plain_string_returns_entry() {
        let mut warnings = Vec::<Warning>::new();
        let result =
            expand_task_id_entry(" brn-123 ", &mut warnings).expect("checked in test assertions");
        assert_eq!(result, vec!["brn-123".to_string()]);
        assert!(warnings.is_empty());
    }

    #[test]
    fn expand_stringified_array_adds_warning() {
        let mut warnings = Vec::<Warning>::new();
        let result = expand_task_id_entry("[\"brn-123\", \"brn-456\"]", &mut warnings)
            .expect("checked in test assertions");
        assert_eq!(result, vec!["brn-123".to_string(), "brn-456".to_string()]);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].error.contains("JSON array"));
    }

    #[test]
    fn expand_invalid_json_array_errors() {
        let mut warnings = Vec::<Warning>::new();
        let err = expand_task_id_entry("[brn-123]", &mut warnings).unwrap_err();
        assert!(err.contains("Failed to parse"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn normalize_single_id_handles_synapse_uri() {
        let normalized =
            normalize_single_id("synapse://test/task/BRN-123").expect("checked in test assertions");
        assert_eq!(normalized, "BRN-123");
    }

    #[test]
    fn normalize_single_id_errors_on_empty() {
        let err = normalize_single_id("   ").unwrap_err();
        assert!(err.contains("empty"));
    }
}
