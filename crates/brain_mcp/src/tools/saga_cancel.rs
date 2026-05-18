//! `sagas.cancel` MCP tool — thin wrapper over `DaemonClient::sagas_cancel`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, cascade_results_to_json, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::{validate_actor, validate_saga_id};

pub(super) struct SagaCancel;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    #[serde(default)]
    cascade: bool,
    #[serde(default = "default_actor")]
    actor: String,
}

fn default_actor() -> String {
    "mcp".into()
}

impl McpTool for SagaCancel {
    fn name(&self) -> &'static str {
        "sagas.cancel"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Cancel a saga. Allowed from active states (planning, open). \
                Closed sagas must be reopened before cancelling. Sets closed_at and emits \
                SagaCancelled. With cascade=true, non-terminal member tasks are transitioned \
                to cancelled."
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
                        "description": "If true, cancel non-terminal member tasks. Default: false",
                        "default": false
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is cancelling the saga. Default: mcp",
                        "default": "mcp",
                        "maxLength": 64,
                        "pattern": "^[A-Za-z0-9_:-]+$"
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
            if let Err(msg) = validate_actor(&parsed.actor) {
                return ToolCallResult::error(format!("Invalid actor: {msg}"));
            }

            let (saga, cascade_results) = match ctx
                .with_client(|c| c.sagas_cancel(parsed.saga_id.clone(), parsed.cascade))
                .await
            {
                Ok(pair) => pair,
                Err(err) => return ToolCallResult::error(format!("Failed to cancel saga: {err}")),
            };

            let cascade_json = cascade_results_to_json(&cascade_results);

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
                },
                "cascade": parsed.cascade,
                "cascade_results": cascade_json,
            }))
        })
    }
}
