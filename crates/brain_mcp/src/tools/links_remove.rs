//! `links.remove` MCP tool — thin wrapper over `DaemonClient::links_remove`.
//!
//! Mutation: removes a typed edge between two entities. The wire
//! `LinksRemoveParams` carries `(from, to, edge_kind)`. The daemon
//! returns `bool` indicating whether a row was actually deleted
//! (idempotent: `{removed: false}` when no matching edge existed).
//!
//! Input schema is preserved byte-identical to the legacy
//! `brain_lib::mcp::tools::links_remove` definition — same entity-type
//! enum (TASK/RECORD/EPISODE/PROCEDURE/CHUNK/NOTE), same 7-kind edge
//! enum (parent_of/blocks/covers/relates_to/see_also/supersedes/
//! contradicts), same `from`/`to`/`edge_kind` required fields.

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

/// Shared JSON Schema fragment for a polymorphic entity reference.
///
/// Byte-identical to the legacy `entity_ref_schema()` in
/// `brain_lib::mcp::tools::links_add`. Migrates here so brain_mcp's
/// link tools no longer reach back into brain_lib. Once `links.add`
/// and `links.for_entity` migrate, this can be reused from
/// `super::helpers` instead of being duplicated.
fn entity_ref_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": ["TASK", "RECORD", "EPISODE", "PROCEDURE", "CHUNK", "NOTE"],
                "description": "The entity type. TASK/RECORD/EPISODE/PROCEDURE are agent-writable; CHUNK and NOTE are read-only entities created by the file-watcher pipeline — only link to them when you have a specific chunk_id or note_id from prior retrieval."
            },
            "id": {
                "type": "string",
                "description": "The entity ID"
            }
        },
        "required": ["type", "id"]
    })
}

impl McpTool for LinksRemove {
    fn name(&self) -> &'static str {
        "links.remove"
    }

    fn definition(&self) -> ToolDefinition {
        let entity_ref = entity_ref_schema();
        ToolDefinition {
            name: self.name().into(),
            description: "Remove a directed polymorphic edge between two entities. Returns { removed: true } when the edge existed and was deleted, { removed: false } when no matching edge was found (idempotent).".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": entity_ref.clone(),
                    "to": entity_ref,
                    "edge_kind": {
                        "type": "string",
                        "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts"],
                        "description": "Edge kind to remove"
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
