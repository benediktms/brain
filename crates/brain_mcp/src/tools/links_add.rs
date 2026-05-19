//! `links.add` MCP tool — thin wrapper over `DaemonClient::links_add`.
//!
//! Adds a directed polymorphic edge between two entities. Defaults to
//! `relates_to` when `edge_kind` is omitted. DAG-validated kinds
//! (`parent_of`, `blocks`, `supersedes`, `continues`) reject cycles
//! daemon-side. Idempotent: re-adding an existing edge succeeds and
//! returns the same synthesised id without inserting a new row.
//!
//! The returned id is a deterministic compound key shaped
//! `FROM_TYPE:from_id->edge->TO_TYPE:to_id` — preserved verbatim from
//! the legacy tool body for byte-shape stability.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::{LinksAddParams, WireEntityRef};

use super::helpers::entity_ref_schema;
use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct LinksAdd;

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
    edge_kind: Option<String>,
}

impl McpTool for LinksAdd {
    fn name(&self) -> &'static str {
        "links.add"
    }

    fn definition(&self) -> ToolDefinition {
        let entity_ref = entity_ref_schema();
        ToolDefinition {
            name: self.name().into(),
            description: "Add a directed polymorphic edge between two entities. Defaults to 'relates_to' when edge_kind is omitted. DAG kinds (parent_of, blocks, supersedes, continues) are cycle-checked. Idempotent: re-adding an existing edge returns the same synthesised id without inserting a new row. The returned id is a deterministic compound key (FROM_TYPE:from_id->edge->TO_TYPE:to_id), not a durable ULID.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": entity_ref.clone(),
                    "to": entity_ref,
                    "edge_kind": {
                        "type": "string",
                        "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts", "continues"],
                        "description": "Edge kind (default: relates_to). Use 'continues' to attach episode-thread continuation edges to existing episodes (DAG-validated)."
                    }
                },
                "required": ["from", "to"]
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

            let edge_kind_wire = parsed.edge_kind.unwrap_or_else(|| "relates_to".to_string());

            let from_kind = parsed.from.kind.clone();
            let from_id = parsed.from.id.clone();
            let to_kind = parsed.to.kind.clone();
            let to_id = parsed.to.id.clone();

            let wire_params = LinksAddParams {
                from: WireEntityRef {
                    kind: parsed.from.kind,
                    id: parsed.from.id,
                },
                to: WireEntityRef {
                    kind: parsed.to.kind,
                    id: parsed.to.id,
                },
                edge_kind: edge_kind_wire.clone(),
            };

            match ctx.with_client(|c| c.links_add(wire_params)).await {
                Ok(_added) => {
                    let id = format!("{from_kind}:{from_id}->{edge_kind_wire}->{to_kind}:{to_id}");
                    json_response(&json!({ "id": id }))
                }
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("cycle") || msg.contains("would create a cycle") {
                        ToolCallResult::error(format!(
                            "would create a cycle in {edge_kind_wire} graph"
                        ))
                    } else if msg.contains("unknown edge_kind") || msg.contains("unknown entity") {
                        ToolCallResult::error(msg)
                    } else {
                        ToolCallResult::error(format!("Failed to add link: {msg}"))
                    }
                }
            }
        })
    }
}
