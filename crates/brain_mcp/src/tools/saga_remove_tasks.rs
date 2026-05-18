//! `sagas.remove_tasks` MCP tool — thin wrapper over `DaemonClient::sagas_remove_tasks`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::{MAX_TASKS_PER_BATCH, validate_saga_id, validate_task_id};

pub(super) struct SagaRemoveTasks;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    task_ids: Vec<String>,
    #[serde(default)]
    cascade: bool,
}

impl McpTool for SagaRemoveTasks {
    fn name(&self) -> &'static str {
        "sagas.remove_tasks"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Remove one or more tasks from a saga. Idempotent: task IDs that are \
                not members of the saga are silently ignored. Returns the count of tasks \
                actually removed. Allowed in any saga status. Set `cascade: true` to also remove \
                every transitive descendant of each input task (via the parent_of graph) that is \
                currently a member of the saga — useful for stripping an entire epic subtree out \
                of the saga in one call. \
                Accepts compact `saga-<hex>` IDs (e.g. `saga-3j5`); 26-char ULIDs are still accepted for back-compat."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": crate::saga_validation::SAGA_ID_PARAM_DESCRIPTION
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string", "minLength": 1, "maxLength": 128 },
                        "description": "Task IDs to remove from the saga (empty array is a valid no-op)",
                        "maxItems": 500
                    },
                    "cascade": {
                        "type": "boolean",
                        "description": "When true, also remove every transitive descendant of each input task currently in the saga. Default: false.",
                        "default": false
                    }
                },
                "required": ["saga_id", "task_ids"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            if let Err(msg) = validate_saga_id(&parsed.saga_id) {
                return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
            }
            // Empty task_ids is intentionally a no-op per idempotent remove semantics.
            if parsed.task_ids.len() > MAX_TASKS_PER_BATCH {
                return ToolCallResult::error(format!(
                    "task_ids exceeds maximum batch size of {MAX_TASKS_PER_BATCH}"
                ));
            }
            for tid in &parsed.task_ids {
                if let Err(msg) = validate_task_id(tid) {
                    return ToolCallResult::error(format!("Invalid task_id '{tid}': {msg}"));
                }
            }

            let (saga_id_short, removed_task_ids) = match ctx
                .with_client(|c| {
                    c.sagas_remove_tasks(
                        parsed.saga_id.clone(),
                        parsed.task_ids.clone(),
                        parsed.cascade,
                    )
                })
                .await
            {
                Ok(pair) => pair,
                Err(err) => {
                    return ToolCallResult::error(format!(
                        "Failed to remove tasks from saga: {err}"
                    ));
                }
            };

            json_response(&json!({
                "saga_id": saga_id_short,
                "removed": removed_task_ids.len(),
                "removed_task_ids": removed_task_ids,
            }))
        })
    }
}
