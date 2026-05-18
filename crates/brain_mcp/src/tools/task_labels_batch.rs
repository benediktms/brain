//! `tasks.labels_batch` MCP tool — thin wrapper over
//! `DaemonClient::tasks_labels_batch`.
//!
//! Batch label operations (add/remove/rename/purge). The entire params
//! object is forwarded as opaque JSON to the daemon which owns the action
//! dispatch logic. The daemon's result JSON is echoed verbatim.
//!
//! Schema is preserved VERBATIM from the legacy
//! `brain_lib::mcp::tools::task_labels_batch` definition.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use brain_rpc::TasksLabelsBatchParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskLabelsBatch;

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
        Box::pin(async move {
            let wire_params = TasksLabelsBatchParams {
                params_json: params,
            };

            match ctx.with_client(|c| c.tasks_labels_batch(wire_params)).await {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Failed to execute labels_batch: {e}")),
            }
        })
    }
}
