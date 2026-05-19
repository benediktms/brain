//! `sagas.start` MCP tool — thin wrapper over `DaemonClient::sagas_start`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::validate_saga_id;

pub(super) struct SagaStart;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
}

impl McpTool for SagaStart {
    fn name(&self) -> &'static str {
        "sagas.start"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Transition a saga from 'planning' to 'open'. \
                Returns an error if the saga is already open, closed, or cancelled."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": crate::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
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

            let saga = match ctx
                .with_client(|c| c.sagas_start(parsed.saga_id.clone()))
                .await
            {
                Ok(s) => s,
                Err(err) => return ToolCallResult::error(format!("Failed to start saga: {err}")),
            };

            json_response(&json!({
                "saga_id": saga.saga_id,
                "saga": {
                    "saga_id": saga.saga_id,
                    "title": saga.title,
                    "description": saga.description,
                    "status": saga.status,
                    "created_at": saga.created_at,
                    "updated_at": saga.updated_at,
                    "closed_at": saga.closed_at,
                }
            }))
        })
    }
}
