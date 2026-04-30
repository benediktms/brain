use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::error::BrainCoreError;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response, resolve_single_scope};

fn transfer_schema() -> Value {
    json!({
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
    })
}

#[derive(Deserialize)]
struct Params {
    task_id: String,
    target_brain: String,
}

pub(super) struct TaskTransfer;

impl TaskTransfer {
    async fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // Resolve source task ID (may be a short prefix).
        let task_id = match ctx.stores.tasks.resolve_task_id(&params.task_id) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("task not found: {e}")),
        };

        // Resolve target brain.
        let target = match resolve_single_scope(ctx, Some(&params.target_brain)) {
            Ok(b) => b,
            Err(e) => return e,
        };

        // Pass the writable LanceDB store so vector rows are re-stamped atomically
        // after the SQLite commit. None in tasks-only / test mode.
        let vector_store = ctx.writable_store.as_ref();

        match ctx
            .stores
            .tasks
            .transfer_task(&task_id, &target.brain_id, vector_store)
            .await
        {
            Ok(result) => {
                let response = json!({
                    "task_id": task_id,
                    "from_brain_id": result.from_brain_id,
                    "to_brain_id": result.to_brain_id,
                    "from_display_id": result.from_display_id,
                    "to_display_id": result.to_display_id,
                    "was_no_op": result.was_no_op,
                    "lance_synced": result.lance_synced,
                });
                json_response(&response)
            }
            Err(e) => match e {
                BrainCoreError::TaskNotFound(_) => {
                    ToolCallResult::error(format!("task not found: {task_id}"))
                }
                BrainCoreError::TaskTransferCasFailed(_) => ToolCallResult::error(
                    "task changed concurrently — retry the transfer".to_string(),
                ),
                BrainCoreError::BrainNotFound(ref bid) => {
                    ToolCallResult::error(format!("target brain not found: {bid}"))
                }
                other => ToolCallResult::error(format!("transfer failed: {other}")),
            },
        }
    }
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
                (collision-safe). Returns from/to brain_id and display_id. \
                No-op if source and target brain are the same."
                .into(),
            input_schema: transfer_schema(),
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
