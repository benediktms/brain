//! `sagas.create` MCP tool — thin wrapper over `DaemonClient::sagas_create`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::SagasCreateParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::{validate_description, validate_title};

pub(super) struct SagaCreate;

#[derive(Deserialize)]
struct Params {
    title: String,
    description: Option<String>,
}

impl McpTool for SagaCreate {
    fn name(&self) -> &'static str {
        "sagas.create"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Create a new saga in 'planning' status. Sagas are registry-level \
                (not scoped to any brain) and use compact `saga-<hex>` IDs (e.g. `saga-3j5`); \
                26-char ULIDs are still accepted for back-compat. Returns the saga_id and \
                initial state."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Saga title",
                        "maxLength": 1024
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional description",
                        "maxLength": 65536
                    }
                },
                "required": ["title"]
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

            if let Err(msg) = validate_title(&parsed.title) {
                return ToolCallResult::error(format!("Invalid title: {msg}"));
            }
            if let Err(msg) = validate_description(parsed.description.as_deref()) {
                return ToolCallResult::error(format!("Invalid description: {msg}"));
            }

            let wire_params = SagasCreateParams {
                title: parsed.title.clone(),
                description: parsed.description.clone(),
            };

            let saga = match ctx.with_client(|c| c.sagas_create(wire_params)).await {
                Ok(s) => s,
                Err(err) => return ToolCallResult::error(format!("Failed to create saga: {err}")),
            };

            let saga_value =
                serde_json::to_value(&saga).expect("SagaSummary should always serialize");

            json_response(&json!({
                "saga_id": saga.saga_id,
                "saga": saga_value,
            }))
        })
    }
}
