//! `sagas.list` MCP tool — thin wrapper over `DaemonClient::sagas_list`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::SagasListParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct SagaList;

#[derive(Deserialize, Default)]
struct Params {
    #[serde(default)]
    include_closed: bool,
    #[serde(default)]
    include_cancelled: bool,
    /// Convenience: if true, sets both include_closed and include_cancelled.
    #[serde(default)]
    all: bool,
    containing_brain: Option<String>,
}

impl McpTool for SagaList {
    fn name(&self) -> &'static str {
        "sagas.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List sagas. By default returns only planning and open sagas. \
                Use include_closed, include_cancelled, or all=true to widen the result set. \
                Use containing_brain to filter to sagas that have at least one member-task \
                in the given brain."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "include_closed": {
                        "type": "boolean",
                        "description": "Include closed sagas. Default: false",
                        "default": false
                    },
                    "include_cancelled": {
                        "type": "boolean",
                        "description": "Include cancelled sagas. Default: false",
                        "default": false
                    },
                    "all": {
                        "type": "boolean",
                        "description": "If true, includes closed AND cancelled sagas regardless of other flags.",
                        "default": false
                    },
                    "containing_brain": {
                        "type": "string",
                        "description": "Filter by brain_id (not brain name). Only sagas with at least one live member task in this brain are returned."
                    }
                }
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

            // Treat empty string as None (legacy default guard).
            let containing_brain = parsed
                .containing_brain
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(String::from);

            let wire_params = SagasListParams {
                include_closed: parsed.include_closed || parsed.all,
                include_cancelled: parsed.include_cancelled || parsed.all,
                containing_brain,
            };

            let sagas = match ctx.with_client(|c| c.sagas_list(wire_params)).await {
                Ok(s) => s,
                Err(err) => return ToolCallResult::error(format!("Failed to list sagas: {err}")),
            };

            let sagas_json: Vec<Value> = sagas
                .iter()
                .map(|s| {
                    json!({
                        "saga_id": s.saga_id,
                        "title": s.title,
                        "description": s.description,
                        "status": s.status,
                        "created_at": s.created_at,
                        "updated_at": s.updated_at,
                        "closed_at": s.closed_at,
                    })
                })
                .collect();

            let total = sagas_json.len();
            json_response(&json!({ "sagas": sagas_json, "total": total }))
        })
    }
}
