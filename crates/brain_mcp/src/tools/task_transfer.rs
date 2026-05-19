//! `tasks.transfer` MCP tool — thin wrapper over
//! `DaemonClient::tasks_transfer`.
//!
//! Transfers a task from the current brain to a target brain. The wire
//! `TasksTransferParams` takes `task_id` and `target_brain`; the daemon
//! resolves the prefix and handles the atomic transfer. Returns the
//! transfer result with from/to brain IDs and display IDs.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::TasksTransferParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskTransfer;

#[derive(Deserialize)]
struct Params {
    task_id: String,
    target_brain: String,
}

impl McpTool for TaskTransfer {
    fn name(&self) -> &'static str {
        "tasks.transfer"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Transfer a task from the current brain to a target brain. \
                Preserves the task_id. Updates brain_id and recomputes display_id \
                (collision-safe). Returns the resulting task summary. \
                No-op if source and target brain are the same."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to transfer (full ID or short hash)"
                    },
                    "target_brain": {
                        "type": "string",
                        "description": "Target brain (name, brain_id, or alias)"
                    }
                },
                "required": ["task_id", "target_brain"]
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

            let wire_params = TasksTransferParams {
                task_id: parsed.task_id,
                target_brain: parsed.target_brain,
            };

            match ctx.with_client(|c| c.tasks_transfer(wire_params)).await {
                Ok((task, _event_id)) => {
                    let response = json!({
                        "task_id": task.task_id,
                        "brain_id": task.brain_id,
                        "title": task.title,
                        "status": task.status,
                        "priority": task.priority,
                    });
                    json_response(&response)
                }
                Err(e) => ToolCallResult::error(format!("Failed to transfer task: {e}")),
            }
        })
    }
}
