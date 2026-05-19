//! `sagas.frontier` MCP tool — thin wrapper over `DaemonClient::sagas_frontier`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::validate_saga_id;

pub(super) struct SagaFrontier;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
}

impl McpTool for SagaFrontier {
    fn name(&self) -> &'static str {
        "sagas.frontier"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Return the ready-actionable tasks in a saga (same qualification rules as \
                tasks.next: open/in_progress, no blocked_reason, no unresolved deps, not deferred, \
                not epic), together with the brains those tasks belong to. \
                Planning/closed/cancelled sagas return an empty task list but still populate brains. \
                Accepts compact `saga-<hex>` IDs (e.g. `saga-3j5`); 26-char ULIDs are still accepted for back-compat."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": crate::saga_validation::SAGA_ID_PARAM_DESCRIPTION
                    }
                },
                "required": ["saga_id"]
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

            let (saga_id_short, saga_status, tasks, brains) = match ctx
                .with_client(|c| c.sagas_frontier(parsed.saga_id.clone()))
                .await
            {
                Ok(tuple) => tuple,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to compute frontier: {err}"));
                }
            };

            let tasks_json: Vec<Value> = tasks
                .iter()
                .map(|t| {
                    json!({
                        "task_id": t.task_id,
                        "title": t.title,
                        "status": t.status,
                        "priority": t.priority,
                        "task_type": t.task_type,
                    })
                })
                .collect();

            let brains_json: Vec<Value> = brains
                .iter()
                .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
                .collect();

            let total = tasks_json.len();

            json_response(&json!({
                "saga_id": saga_id_short,
                "saga_status": saga_status,
                "tasks": tasks_json,
                "brains": brains_json,
                "total": total,
            }))
        })
    }
}
