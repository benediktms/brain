//! `sagas.close` MCP tool — thin wrapper over `DaemonClient::sagas_close`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, cascade_results_to_json, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::validate_saga_id;

pub(super) struct SagaClose;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    #[serde(default)]
    cascade: bool,
}

impl McpTool for SagaClose {
    fn name(&self) -> &'static str {
        "sagas.close"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Close a saga (open → closed). Only open sagas can be closed. \
                With cascade=true, member tasks are transitioned to done (best-effort: \
                already-done and already-cancelled tasks are skipped)."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": crate::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
                    },
                    "cascade": {
                        "type": "boolean",
                        "description": "If true, close all member tasks. Default: false",
                        "default": false
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

            let (saga, cascade_results) = match ctx
                .with_client(|c| c.sagas_close(parsed.saga_id.clone(), parsed.cascade))
                .await
            {
                Ok(pair) => pair,
                Err(err) => return ToolCallResult::error(format!("Failed to close saga: {err}")),
            };

            let cascade_json = cascade_results_to_json(&cascade_results);

            let saga_value = serde_json::to_value(&saga)
                .expect("SagaSummary should always serialize");

            json_response(&json!({
                "saga_id": saga.saga_id,
                "saga": saga_value,
                "cascade": parsed.cascade,
                "cascade_results": cascade_json,
            }))
        })
    }
}
