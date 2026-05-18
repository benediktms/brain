//! `sagas.add_tasks` MCP tool — thin wrapper over `DaemonClient::sagas_add_tasks`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::{MAX_TASKS_PER_BATCH, validate_saga_id, validate_task_id};

pub(super) struct SagaAddTasks;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    task_ids: Vec<String>,
    #[serde(default)]
    cascade: bool,
}

impl McpTool for SagaAddTasks {
    fn name(&self) -> &'static str {
        "sagas.add_tasks"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Atomically add one or more tasks to a saga. All task IDs must resolve \
                (cross-brain short IDs are supported). The saga must not be closed or cancelled. \
                Already-member tasks and intra-batch duplicates are silently skipped (idempotent). \
                Unresolvable IDs cause the entire batch to fail. Set `cascade: true` to also add \
                every transitive descendant of each input task (via the parent_of graph) — useful \
                for pulling an entire epic and its subtasks into the saga in one call."
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
                        "description": "Task IDs to add — `<brain>-<hex>` short form or full task ID; cross-brain aware",
                        "minItems": 1,
                        "maxItems": 500
                    },
                    "cascade": {
                        "type": "boolean",
                        "description": "When true, expand each input task to itself plus every transitive descendant in the parent_of graph. Default: false.",
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
            if parsed.task_ids.is_empty() {
                return ToolCallResult::error("task_ids must not be empty");
            }
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

            let (saga_id_short, added_task_ids) = match ctx
                .with_client(|c| {
                    c.sagas_add_tasks(
                        parsed.saga_id.clone(),
                        parsed.task_ids.clone(),
                        parsed.cascade,
                    )
                })
                .await
            {
                Ok(pair) => pair,
                Err(err) => return ToolCallResult::error(format!("Failed to add tasks: {err}")),
            };

            json_response(&json!({
                "saga_id": saga_id_short,
                "added": added_task_ids.len(),
                "added_task_ids": added_task_ids,
            }))
        })
    }
}
