//! `sagas.update` MCP tool — thin wrapper over `DaemonClient::sagas_update`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::{SagaDescriptionUpdate, SagasUpdateParams};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::{validate_description, validate_saga_id, validate_title};

pub(super) struct SagaUpdate;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
    title: Option<String>,
    /// Outer None = don't touch; inner None = clear; inner Some = set.
    description: Option<Option<String>>,
}

impl McpTool for SagaUpdate {
    fn name(&self) -> &'static str {
        "sagas.update"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Update a saga's title and/or description. At least one field required. \
                Allowed in any status (including closed/cancelled). Empty title is rejected."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": crate::saga_validation::SAGA_ID_PARAM_DESCRIPTION,
                    },
                    "title": {
                        "type": "string",
                        "description": "New title (must not be empty)",
                        "maxLength": 1024
                    },
                    "description": {
                        "description": "New description (null to clear)",
                        "oneOf": [
                            { "type": "string", "maxLength": 65536 },
                            { "type": "null" }
                        ]
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
            if parsed.title.is_none() && parsed.description.is_none() {
                return ToolCallResult::error(
                    "At least one of 'title' or 'description' must be provided".to_string(),
                );
            }
            if let Some(t) = parsed.title.as_deref()
                && let Err(msg) = validate_title(t)
            {
                return ToolCallResult::error(format!("Invalid title: {msg}"));
            }
            if let Some(Some(d)) = parsed.description.as_ref()
                && let Err(msg) = validate_description(Some(d.as_str()))
            {
                return ToolCallResult::error(format!("Invalid description: {msg}"));
            }

            // Map Option<Option<String>> to Option<SagaDescriptionUpdate>.
            let description_wire: Option<SagaDescriptionUpdate> = match parsed.description.as_ref()
            {
                None => None,
                Some(None) => Some(SagaDescriptionUpdate::Clear),
                Some(Some(value)) => Some(SagaDescriptionUpdate::Set {
                    value: value.clone(),
                }),
            };

            let wire_params = SagasUpdateParams {
                saga_id: parsed.saga_id.clone(),
                title: parsed.title.clone(),
                description: description_wire,
            };

            let saga = match ctx.with_client(|c| c.sagas_update(wire_params)).await {
                Ok(s) => s,
                Err(err) => return ToolCallResult::error(format!("Failed to update saga: {err}")),
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
