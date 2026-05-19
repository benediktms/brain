//! `memory.write_procedure` MCP tool — thin wrapper over
//! `DaemonClient::memory_write_procedure`.
//!
//! As with `mem_write_episode`, the wire variant only carries core
//! procedure fields; the MCP-layer `links` array is applied via per-
//! link `links_add` round-trips after the procedure persists.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::MemoryWriteProcedureParams;

use super::helpers::{InlineLinkInput, apply_inline_links, inline_links_schema};
use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize)]
struct Params {
    title: String,
    steps: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_importance")]
    importance: f64,
    #[serde(default)]
    links: Vec<InlineLinkInput>,
}

fn default_importance() -> f64 {
    0.9
}

pub(super) struct MemoryWriteProcedure;

impl McpTool for MemoryWriteProcedure {
    fn name(&self) -> &'static str {
        "memory.write_procedure"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Record a reusable procedure (title, markdown steps) to memory. Returns `{summary_id, uri, ...}`. Optionally pass `links` to add edges in the entity graph from the new procedure (type PROCEDURE) to the EPISODE it was distilled from and any TASK/RECORD/PROCEDURE/EPISODE/CHUNK/NOTE entities — the procedure persists even if every link fails. When `links` is provided the response carries `links: {succeeded:[{to, edge_kind}], failed:[{to, edge_kind, error}], summary:{succeeded, failed}}`. Use `links_add` for any links discovered after the write."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string", "description": "Procedure title" },
                    "steps": { "type": "string", "description": "Procedure steps as markdown" },
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "Tags for categorization. Pass as a JSON array, e.g. [\"ci\", \"workflow\"]" },
                    "importance": { "type": "number", "description": "Importance score (0.0 to 1.0). Default: 0.9", "default": 0.9 },
                    "links": inline_links_schema("Optional. After the procedure is stored, create polymorphic edges from it (as PROCEDURE) to the listed entities. Partial failures are reported per-link without aborting the write.")
                },
                "required": ["title", "steps"]
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

            let importance_millis = (parsed.importance.clamp(0.0, 1.0) * 1000.0) as u32;

            let wire_params = MemoryWriteProcedureParams {
                title: parsed.title.clone(),
                steps: parsed.steps,
                tags: parsed.tags.clone(),
                importance_millis,
            };

            let (summary_id, uri) = match ctx
                .with_client(|c| c.memory_write_procedure(wire_params))
                .await
            {
                Ok(pair) => pair,
                Err(e) => return ToolCallResult::error(format!("Failed to store procedure: {e}")),
            };

            let mut response = json!({
                "status": "stored",
                "summary_id": summary_id,
                "uri": uri,
                "title": parsed.title,
                "tags": parsed.tags,
                "importance": (importance_millis as f64) / 1000.0,
            });

            if !parsed.links.is_empty() {
                let links_block =
                    apply_inline_links("PROCEDURE", &summary_id, parsed.links, ctx).await;
                response["links"] = links_block;
            }

            json_response(&response)
        })
    }
}
