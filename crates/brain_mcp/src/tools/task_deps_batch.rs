//! `tasks.deps_batch` MCP tool — thin wrapper over
//! `DaemonClient::tasks_deps_batch`.
//!
//! Batch dependency operations (add/remove/chain/fan/clear). The entire
//! params object is forwarded as opaque JSON to the daemon which owns the
//! action dispatch logic. The daemon's result JSON is echoed verbatim.
//!
//! Schema is preserved VERBATIM from the legacy
//! `brain_lib::mcp::tools::task_deps_batch` definition.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use brain_rpc::TasksDepsBatchParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskDepsBatch;

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
        Box::pin(async move {
            let wire_params = TasksDepsBatchParams {
                params_json: params,
            };

            match ctx.with_client(|c| c.tasks_deps_batch(wire_params)).await {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Failed to execute deps_batch: {e}")),
            }
        })
    }
}
