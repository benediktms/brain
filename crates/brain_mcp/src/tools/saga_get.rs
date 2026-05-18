//! `sagas.get` MCP tool — thin wrapper over `DaemonClient::sagas_get`.
//!
//! Note: the daemon's `sagas_get` RPC returns a `SagaSummary` (row-level
//! fields only). The legacy tool made additional store calls for `members`
//! and `brains`. Those are not exposed by the current typed RPC client, so
//! the migrated response emits an empty `members` array and an empty `brains`
//! array. Callers that need frontier data should use `sagas.frontier`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::validate_saga_id;

pub(super) struct SagaGet;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
}

impl McpTool for SagaGet {
    fn name(&self) -> &'static str {
        "sagas.get"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Fetch a single saga by its compact `saga-<hex>` ID (e.g. `saga-3j5`); \
                26-char ULIDs are still accepted for back-compat. Returns the saga row \
                and member task stubs (empty until tasks are added)."
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
                .with_client(|c| c.sagas_get(parsed.saga_id.clone()))
                .await
            {
                Ok(opt) => opt,
                Err(err) => return ToolCallResult::error(format!("Failed to fetch saga: {err}")),
            };

            match saga {
                None => json_response(&json!({ "saga": null })),
                Some(s) => json_response(&json!({
                    "saga_id": s.saga_id,
                    "saga": {
                        "saga_id": s.saga_id,
                        "title": s.title,
                        "description": s.description,
                        "status": s.status,
                        "created_at": s.created_at,
                        "updated_at": s.updated_at,
                        "closed_at": s.closed_at,
                        "members": [],
                        "brains": [],
                    }
                })),
            }
        })
    }
}
