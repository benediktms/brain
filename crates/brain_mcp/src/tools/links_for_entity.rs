//! `links.for_entity` MCP tool — thin wrapper over `DaemonClient::links_for_entity`.
//!
//! Returns all polymorphic edges where the given entity participates,
//! partitioned into `outgoing` (entity is source) and `incoming`
//! (entity is target). The `direction` filter (`out` / `in` / `both`)
//! controls which side is populated; the suppressed side is `[]`, not
//! absent — byte-shape stability with the legacy MCP response.
//!
//! Passes `direction: "both"` to the wire unconditionally so the tool
//! can partition client-side against the requested entity. This
//! preserves the legacy invariant that the response always carries
//! both `outgoing` and `incoming` keys regardless of input filter.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::{LinksForEntityParams, WireEntityRef};

use super::helpers::entity_ref_schema;
use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct LinksForEntity;

#[derive(Deserialize)]
struct InputEntity {
    #[serde(rename = "type")]
    kind: String,
    id: String,
}

#[derive(Deserialize)]
struct Params {
    entity: InputEntity,
    direction: Option<String>,
}

impl McpTool for LinksForEntity {
    fn name(&self) -> &'static str {
        "links.for_entity"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Return all polymorphic edges where the given entity participates. Partitioned into outgoing (entity is source) and incoming (entity is target). Use direction to filter.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "entity": entity_ref_schema(),
                    "direction": {
                        "type": "string",
                        "enum": ["out", "in", "both"],
                        "description": "Filter direction (default: both)"
                    }
                },
                "required": ["entity"]
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

            let direction = parsed.direction.as_deref().unwrap_or("both").to_string();
            if !matches!(direction.as_str(), "out" | "in" | "both") {
                return ToolCallResult::error(format!(
                    "invalid direction '{direction}': must be 'out', 'in', or 'both'"
                ));
            }

            let entity_kind = parsed.entity.kind.clone();
            let entity_id = parsed.entity.id.clone();

            // Wire always sees "both" so we can partition client-side and
            // populate the requested side without losing the empty-array
            // counterpart that legacy callers depend on.
            let wire_params = LinksForEntityParams {
                entity: WireEntityRef {
                    kind: parsed.entity.kind,
                    id: parsed.entity.id,
                },
                direction: "both".into(),
                limit: None,
            };

            let links = match ctx.with_client(|c| c.links_for_entity(wire_params)).await {
                Ok(v) => v,
                Err(err) => {
                    return ToolCallResult::error(format!("Internal error: {err}"));
                }
            };

            let mut outgoing: Vec<Value> = Vec::new();
            let mut incoming: Vec<Value> = Vec::new();

            for link in &links {
                let entry = json!({
                    "from": { "type": link.from.kind, "id": link.from.id },
                    "to":   { "type": link.to.kind,   "id": link.to.id },
                    "edge_kind": link.edge_kind,
                });
                if link.from.kind == entity_kind && link.from.id == entity_id {
                    outgoing.push(entry);
                } else {
                    incoming.push(entry);
                }
            }

            let response = match direction.as_str() {
                "out" => json!({ "outgoing": outgoing, "incoming": [] }),
                "in" => json!({ "outgoing": [], "incoming": incoming }),
                _ => json!({ "outgoing": outgoing, "incoming": incoming }),
            };

            json_response(&response)
        })
    }
}
