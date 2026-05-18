//! `links.remove` MCP tool — thin wrapper over `DaemonClient::links_remove`.
//!
//! Mutation: removes a typed edge between two entities. The wire
//! `LinksRemoveParams` carries `(from, to, edge_kind)`. The daemon
//! returns `bool` indicating whether a row was actually deleted
//! (false = no such edge existed).

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::{LinksRemoveParams, WireEntityRef};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct LinksRemove;

#[derive(Deserialize)]
struct InputEntity {
    #[serde(rename = "type")]
    kind: String,
    id: String,
}

#[derive(Deserialize)]
struct Params {
    from: InputEntity,
    to: InputEntity,
    edge_kind: String,
}

impl McpTool for LinksRemove {
    fn name(&self) -> &'static str {
        "links.remove"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Remove a typed edge between two entities. Returns `{removed: bool}` — false when no matching edge existed.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "description": "Entity type (TASK, EPISODE, PROCEDURE, RECORD, SAGA, BRAIN)"},
                            "id": {"type": "string", "description": "Entity ID"}
                        },
                        "required": ["type", "id"]
                    },
                    "to": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "id": {"type": "string"}
                        },
                        "required": ["type", "id"]
                    },
                    "edge_kind": {
                        "type": "string",
                        "description": "One of parent_of, blocks, covers, relates_to, see_also, supersedes, contradicts, continues"
                    }
                },
                "required": ["from", "to", "edge_kind"]
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

            let wire_params = LinksRemoveParams {
                from: WireEntityRef {
                    kind: parsed.from.kind,
                    id: parsed.from.id,
                },
                to: WireEntityRef {
                    kind: parsed.to.kind,
                    id: parsed.to.id,
                },
                edge_kind: parsed.edge_kind,
            };

            let removed = match ctx.with_client(|c| c.links_remove(wire_params)).await {
                Ok(b) => b,
                Err(err) => return ToolCallResult::error(format!("Failed to remove link: {err}")),
            };

            json_response(&json!({ "removed": removed }))
        })
    }
}
